import datetime as dt
import email
import imaplib
import re
from email.message import Message
from typing import Iterable, Optional, Set

EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def _normalize(addr: str) -> str:
    return addr.strip().lower()


def _extract_from_delivery_status(text: str) -> Set[str]:
    addresses: Set[str] = set()
    for line in text.splitlines():
        if "recipient" not in line.lower():
            continue
        match = re.search(r"rfc822;\s*([^\s>]+)", line, re.IGNORECASE)
        if match:
            addresses.add(match.group(1))
    return addresses


def _extract_from_message(msg: Message) -> Set[str]:
    addresses: Set[str] = set()

    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype == "message/delivery-status":
            payload = part.get_payload()
            if isinstance(payload, list):
                for item in payload:
                    addresses.update(_extract_from_delivery_status(item.as_string()))
            else:
                data = part.get_payload(decode=True)
                if data:
                    addresses.update(_extract_from_delivery_status(data.decode(errors="ignore")))
        elif ctype == "text/plain":
            data = part.get_payload(decode=True)
            if data:
                addresses.update(_extract_from_delivery_status(data.decode(errors="ignore")))

    if not addresses:
        texts = []
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    data = part.get_payload(decode=True)
                    if data:
                        texts.append(data.decode(errors="ignore"))
        else:
            data = msg.get_payload(decode=True)
            if data:
                texts.append(data.decode(errors="ignore"))

        for text in texts:
            for match in EMAIL_RE.findall(text):
                addresses.add(match)

    return addresses


def collect_bounced_addresses(
    *,
    host: str,
    username: str,
    password: str,
    search: Optional[str] = None,
    days: Optional[int] = None,
    max_messages: Optional[int] = None,
    mark_seen: bool = False,
    ignore: Optional[Iterable[str]] = None,
    mailbox: str = "INBOX",
) -> Set[str]:
    if not username or not password:
        return set()

    ignore_set = {_normalize(x) for x in (ignore or []) if x}

    imap = imaplib.IMAP4_SSL(host)
    imap.login(username, password)
    imap.select(mailbox)

    criteria = []
    if days:
        since_date = (dt.datetime.now() - dt.timedelta(days=days)).strftime("%d-%b-%Y")
        criteria.append(f"SINCE {since_date}")
    if search:
        criteria.append(search)
    if not criteria:
        criteria = ["ALL"]

    status, data = imap.search(None, *criteria)
    if status != "OK" or not data or not data[0]:
        imap.close()
        imap.logout()
        return set()

    ids = data[0].split()
    if max_messages:
        ids = ids[-max_messages:]

    bounced: Set[str] = set()
    for msg_id in ids:
        status, msg_data = imap.fetch(msg_id, "(RFC822)")
        if status != "OK" or not msg_data or not msg_data[0]:
            continue
        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)
        addresses = _extract_from_message(msg)
        for addr in addresses:
            norm = _normalize(addr)
            if not norm or norm in ignore_set:
                continue
            bounced.add(norm)
        if mark_seen:
            imap.store(msg_id, "+FLAGS", "\\Seen")

    imap.close()
    imap.logout()
    return bounced
