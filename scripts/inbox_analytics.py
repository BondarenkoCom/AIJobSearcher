import argparse
import csv
import email
import imaplib
import os
import re
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from email.header import decode_header
from email.message import Message
from email.utils import getaddresses, parsedate_to_datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.email_sender import load_env_file  # noqa: E402

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def _decode_header(value: str) -> str:
    if not value:
        return ""
    parts: List[str] = []
    for chunk, enc in decode_header(value):
        if isinstance(chunk, bytes):
            try:
                parts.append(chunk.decode(enc or "utf-8", errors="replace"))
            except Exception:
                parts.append(chunk.decode("utf-8", errors="replace"))
        else:
            parts.append(str(chunk))
    return "".join(parts).strip()


def _normalize_email(value: str) -> str:
    return (value or "").strip().lower()


def _extract_addrs(msg: Message, header: str) -> List[str]:
    addrs: List[str] = []
    for _name, addr in getaddresses(msg.get_all(header, []) or []):
        addr = _normalize_email(addr)
        if addr:
            addrs.append(addr)
    return addrs


def _parse_date(msg: Message) -> Optional[datetime]:
    raw = msg.get("Date")
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        return None


def _text_snippet(msg: Message, limit: int = 240) -> str:
    texts: List[str] = []

    def add_text(data: bytes, charset: Optional[str]) -> None:
        if not data:
            return
        try:
            txt = data.decode(charset or "utf-8", errors="replace")
        except Exception:
            txt = data.decode("utf-8", errors="replace")
        txt = re.sub(r"\s+", " ", txt).strip()
        if txt:
            texts.append(txt)

    if msg.is_multipart():
        for part in msg.walk():
            ctype = (part.get_content_type() or "").lower()
            disp = (part.get_content_disposition() or "").lower()
            if disp == "attachment":
                continue
            if ctype == "text/plain":
                data = part.get_payload(decode=True)
                charset = part.get_content_charset()
                if data:
                    add_text(data, charset)
                    break
    else:
        if (msg.get_content_type() or "").lower() == "text/plain":
            data = msg.get_payload(decode=True)
            add_text(data or b"", msg.get_content_charset())

    snippet = texts[0] if texts else ""
    return snippet[:limit]


def _looks_like_bounce(msg: Message) -> bool:
    subj = _decode_header(msg.get("Subject", "")).lower()
    frm = " ".join(_extract_addrs(msg, "From")).lower()

    if any(x in frm for x in ["mailer-daemon", "postmaster"]):
        return True

    keywords = [
        "undelivered",
        "delivery status",
        "returned mail",
        "failure notice",
        "mail delivery",
        "delivery failure",
        "address not found",
        "message not delivered",
    ]
    if any(k in subj for k in keywords):
        return True

    try:
        for part in msg.walk():
            if (part.get_content_type() or "").lower() == "message/delivery-status":
                return True
    except Exception:
        pass

    return False


def _extract_bounced_recipients(msg: Message) -> Set[str]:
    recipients: Set[str] = set()

    for part in msg.walk():
        ctype = (part.get_content_type() or "").lower()
        if ctype != "message/delivery-status":
            continue
        payload = part.get_payload()
        if isinstance(payload, list):
            for item in payload:
                text = item.as_string()
                for line in text.splitlines():
                    m = re.search(r"rfc822;\s*([^\s>]+)", line, re.IGNORECASE)
                    if m:
                        recipients.add(_normalize_email(m.group(1)))
        else:
            data = part.get_payload(decode=True) or b""
            text = data.decode(errors="ignore")
            for line in text.splitlines():
                m = re.search(r"rfc822;\s*([^\s>]+)", line, re.IGNORECASE)
                if m:
                    recipients.add(_normalize_email(m.group(1)))

    if recipients:
        return {r for r in recipients if r}

    raw = msg.as_string()
    for addr in EMAIL_RE.findall(raw):
        recipients.add(_normalize_email(addr))
    return {r for r in recipients if r}


def _looks_like_auto_reply(msg: Message) -> bool:
    auto = (msg.get("Auto-Submitted") or "").strip().lower()
    if auto and auto != "no":
        return True

    subj = _decode_header(msg.get("Subject", "")).lower()
    keywords = [
        "out of office",
        "automatic reply",
        "auto reply",
        "autoreply",
        "vacation",
    ]
    if any(k in subj for k in keywords):
        return True

    if (msg.get("Precedence") or "").strip().lower() in {"bulk", "junk", "list"}:
        return True

    return False


def _strip_reply_prefix(subject: str) -> str:
    s = subject.strip()
    while True:
        lowered = s.lower()
        for p in ["re:", "fw:", "fwd:", "sv:"]:
            if lowered.startswith(p):
                s = s[len(p) :].strip()
                break
        else:
            return s


def _load_sent_log(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _sent_maps(sent_rows: List[Dict[str, str]]) -> Tuple[Dict[str, Dict[str, str]], Set[str]]:
    by_recipient: Dict[str, Dict[str, str]] = {}
    recipients: Set[str] = set()
    for row in sent_rows:
        to_email = _normalize_email(row.get("to_email", ""))
        if not to_email:
            continue
        recipients.add(to_email)
        prev = by_recipient.get(to_email)
        if not prev or (row.get("timestamp") or "") > (prev.get("timestamp") or ""):
            by_recipient[to_email] = row
    return by_recipient, recipients


@dataclass
class InboxRow:
    received_at: str
    type: str
    from_email: str
    subject: str
    snippet: str
    related_to_email: str
    related_job_title: str
    related_company: str
    message_id: str
    in_reply_to: str


def main() -> int:
    ap = argparse.ArgumentParser(description="Inbox analytics (Gmail IMAP)")
    ap.add_argument("--days", type=int, default=7, help="How many days back to scan")
    ap.add_argument("--max", type=int, default=500, help="Max messages to fetch")
    ap.add_argument("--mailbox", default="INBOX", help="IMAP mailbox")
    ap.add_argument("--out", default="data/out", help="Output dir")
    args = ap.parse_args()

    root = Path(__file__).resolve().parents[1]
    load_env_file(root / ".env")
    load_env_file(root / ".env.accounts")

    username = (os.getenv("IMAP_USERNAME") or os.getenv("GMAIL_USERNAME") or "your.email@example.com").strip()
    password = os.environ.get("IMAP_PASSWORD") or os.environ.get("SMTP_PASSWORD")
    host = os.getenv("IMAP_HOST") or "imap.gmail.com"
    if not password:
        print("[inbox] missing IMAP_PASSWORD/SMTP_PASSWORD")
        return 1

    sent_log_path = root / "data" / "out" / "sent_log.csv"
    sent_rows = _load_sent_log(sent_log_path)
    sent_by_recipient, sent_recipients = _sent_maps(sent_rows)

    since = (datetime.now() - timedelta(days=args.days)).strftime("%d-%b-%Y")

    imap = imaplib.IMAP4_SSL(host)
    imap.login(username, password)
    mailbox = args.mailbox
    mailbox_select = mailbox
    # Gmail folders like "[Gmail]/All Mail" need quoting for IMAP SELECT.
    if " " in mailbox_select and not (mailbox_select.startswith('"') and mailbox_select.endswith('"')):
        mailbox_select = f'"{mailbox_select}"'
    imap.select(mailbox_select)

    status, data = imap.search(None, f"SINCE {since}")
    if status != "OK" or not data or not data[0]:
        print("[inbox] no messages")
        try:
            imap.close()
        except Exception:
            pass
        imap.logout()
        return 0

    ids = data[0].split()
    if args.max and len(ids) > args.max:
        ids = ids[-args.max :]

    out_rows: List[InboxRow] = []

    types = Counter()
    replied_recipients: Set[str] = set()
    bounced_recipients: Set[str] = set()
    our_email = _normalize_email(username)

    for msg_id in ids:
        status, msg_data = imap.fetch(msg_id, "(RFC822)")
        if status != "OK" or not msg_data or not msg_data[0]:
            continue
        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)

        subject = _decode_header(msg.get("Subject", ""))
        subject_norm = _strip_reply_prefix(subject)

        from_addrs = _extract_addrs(msg, "From")
        from_addr = from_addrs[0] if from_addrs else ""
        # If scanning "[Gmail]/All Mail", it includes our own outgoing messages too.
        # Skip those, since this script is meant for inbound analytics.
        if _normalize_email(from_addr) == our_email:
            continue

        received_at_dt = _parse_date(msg)
        received_at = received_at_dt.isoformat(timespec="seconds") if received_at_dt else ""

        message_id = (msg.get("Message-ID") or "").strip()
        in_reply_to = (msg.get("In-Reply-To") or "").strip()

        snippet = _text_snippet(msg)

        msg_type = "other"
        related_to = ""
        related_job = ""
        related_company = ""

        is_bounce = _looks_like_bounce(msg)
        if is_bounce:
            msg_type = "bounce"
            bounced = _extract_bounced_recipients(msg)
            bounced_match = {e for e in bounced if e in sent_recipients}
            bounced_recipients.update(bounced_match)
            if bounced_match:
                related_to = sorted(bounced_match)[0]
                meta = sent_by_recipient.get(related_to)
                if meta:
                    related_job = meta.get("job_title", "")
                    related_company = meta.get("company", "")
        else:
            if from_addr and from_addr in sent_recipients:
                msg_type = "auto_reply" if _looks_like_auto_reply(msg) else "reply"
                replied_recipients.add(from_addr)
                related_to = from_addr
                meta = sent_by_recipient.get(from_addr)
                if meta:
                    related_job = meta.get("job_title", "")
                    related_company = meta.get("company", "")
            else:
                if subject_norm.lower().startswith("application for "):
                    msg_type = "auto_reply" if _looks_like_auto_reply(msg) else "reply"

        types[msg_type] += 1

        out_rows.append(
            InboxRow(
                received_at=received_at,
                type=msg_type,
                from_email=from_addr,
                subject=subject,
                snippet=snippet,
                related_to_email=related_to,
                related_job_title=related_job,
                related_company=related_company,
                message_id=message_id,
                in_reply_to=in_reply_to,
            )
        )

    try:
        imap.close()
    except Exception:
        pass
    imap.logout()

    out_dir = (root / args.out).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    mailbox_slug = re.sub(r"[^a-z0-9]+", "_", mailbox.lower()).strip("_") or "inbox"
    days_slug = f"{args.days}d"

    out_csv = out_dir / f"inbox_messages_{mailbox_slug}_{days_slug}_{stamp}.csv"
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "received_at",
                "type",
                "from_email",
                "subject",
                "snippet",
                "related_to_email",
                "related_job_title",
                "related_company",
                "message_id",
                "in_reply_to",
            ],
        )
        writer.writeheader()
        for r in out_rows:
            writer.writerow(r.__dict__)

    total_sent = len(sent_rows)
    unique_sent = len(sent_recipients)
    reply_count = types.get("reply", 0)
    auto_count = types.get("auto_reply", 0)
    bounce_count = types.get("bounce", 0)

    reply_unique = len(replied_recipients)
    bounce_unique = len(bounced_recipients)

    def pct(n: int, d: int) -> str:
        if d <= 0:
            return "0%"
        return f"{(n * 100.0 / d):.1f}%"

    out_txt = out_dir / f"inbox_summary_{mailbox_slug}_{days_slug}_{stamp}.txt"
    with out_txt.open("w", encoding="utf-8") as f:
        f.write(f"Inbox analytics (last {args.days} days)\n")
        f.write(f"Mailbox: {mailbox}\n")
        f.write(f"Messages scanned: {len(out_rows)}\n\n")
        f.write(f"Sent (total): {total_sent}\n")
        f.write(f"Sent (unique recipients): {unique_sent}\n\n")
        f.write(f"Replies: {reply_count} (unique matching our recipients: {reply_unique})\n")
        f.write(f"Auto replies: {auto_count}\n")
        f.write(f"Bounces: {bounce_count} (unique bounced recipients: {bounce_unique})\n\n")
        f.write(f"Approx reply rate (unique): {pct(reply_unique, unique_sent)}\n")
        f.write(f"Approx bounce rate (unique): {pct(bounce_unique, unique_sent)}\n\n")
        f.write(f"Output CSV: {out_csv}\n")

    print(f"[inbox] wrote {out_csv}")
    print(f"[inbox] wrote {out_txt}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

