import argparse
import asyncio
import csv
import random
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from telethon import functions
from telethon.errors import FloodWaitError, RPCError
from telethon.tl import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402
from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


SPAM_TERMS = {
    "proxy support",
    "interview support",
    "assignment assistance",
    "coding tests",
    "interview co-pilot",
    "training | placement",
    "placement assistance",
    "work support",
    "undetectable",
    "guaranteed income",
    "signal group",
    "registration fee",
    "activation fee",
    "upfront payment",
    "pay to apply",
    "crypto investment",
    "сигналы",
    "предоплата",
    "вступительный взнос",
    "оплата за трудоустройство",
}

HARD_SPAM_TERMS = {
    "proxy support",
    "interview support",
    "assignment assistance",
    "interview co-pilot",
    "undetectable",
    "registration fee",
    "activation fee",
    "pay to apply",
    "оплата за трудоустройство",
}

QA_TERMS = {
    "qa",
    "sdet",
    "tester",
    "testing",
    "test automation",
    "automation",
    "playwright",
    "selenium",
    "c#",
    ".net",
    "api testing",
    "тестировщик",
    "тестирование",
    "автотест",
    "автоматизация тестирования",
}

JOB_TERMS = {
    "hiring",
    "vacancy",
    "job",
    "position",
    "contract",
    "freelance",
    "gig",
    "remote",
    "pay",
    "budget",
    "оплата",
    "вакансия",
    "ищем",
    "нужен",
    "проект",
    "удален",
    "удаленно",
}


def clean_chat_ref(value: str) -> str:
    s = str(value or "").strip()
    if s.startswith("https://t.me/") or s.startswith("http://t.me/"):
        s = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE)
        s = s.split("?", 1)[0].split("#", 1)[0].strip("/")
        s = s.split("/", 1)[0]
        if s:
            return "@" + s
    return s


def read_lines(path: Path) -> List[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def source_refs(path: Path) -> List[str]:
    out: List[str] = []
    for line in read_lines(path):
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        c = clean_chat_ref(s)
        if c:
            out.append(c)
    return list(dict.fromkeys(out))


def find_hits(low: str, terms: Sequence[str]) -> List[str]:
    return [t for t in terms if t and t in low]


def classify_message(text: str) -> Dict[str, Any]:
    low = str(text or "").lower()
    spam_hits = find_hits(low, SPAM_TERMS)
    hard_hits = find_hits(low, HARD_SPAM_TERMS)
    qa_hits = find_hits(low, QA_TERMS)
    job_hits = find_hits(low, JOB_TERMS)

    is_good = bool(qa_hits) and bool(job_hits)
    is_spam = bool(hard_hits) or (bool(spam_hits) and not is_good)

    return {
        "is_good": is_good,
        "is_spam": is_spam,
        "qa_hits": qa_hits,
        "job_hits": job_hits,
        "spam_hits": spam_hits,
        "hard_hits": hard_hits,
    }


def decision_for_chat(
    *,
    scanned: int,
    spam_msgs: int,
    good_msgs: int,
    hard_spam_msgs: int,
    spam_terms_counter: Counter,
    spam_ratio_threshold: float,
    min_spam_messages: int,
    max_good_ratio: float,
    hard_spam_min: int,
) -> Tuple[bool, str]:
    if scanned <= 0:
        return False, "no_text_messages"

    spam_ratio = spam_msgs / scanned
    good_ratio = good_msgs / scanned

    garbage = False
    reason = []

    if hard_spam_msgs >= hard_spam_min:
        garbage = True
        reason.append(f"hard_spam_msgs={hard_spam_msgs}")
    if spam_msgs >= min_spam_messages and spam_ratio >= spam_ratio_threshold and good_ratio <= max_good_ratio:
        garbage = True
        reason.append(
            f"spam_ratio={spam_ratio:.2f}>=th={spam_ratio_threshold:.2f},good_ratio={good_ratio:.2f}<=max={max_good_ratio:.2f}"
        )
    if good_msgs == 0 and spam_msgs >= max(2, min_spam_messages - 1):
        garbage = True
        reason.append("no_good_msgs_and_spam_present")

    top_terms = ",".join([k for k, _ in spam_terms_counter.most_common(4)])
    if top_terms:
        reason.append(f"top_spam_terms={top_terms}")

    if not reason:
        reason.append("looks_ok")
    return garbage, " | ".join(reason)


def rewrite_sources_file(path: Path, remove_refs: Set[str]) -> int:
    lines = read_lines(path)
    if not lines:
        return 0
    removed = 0
    kept: List[str] = []
    for ln in lines:
        s = ln.strip()
        if s and not s.startswith("#"):
            cref = clean_chat_ref(s).lower()
            if cref in remove_refs:
                removed += 1
                continue
        kept.append(ln)
    text = "\n".join(kept).rstrip() + "\n"
    path.write_text(text, encoding="utf-8")
    return removed


def out_csv_path(value: str) -> Path:
    if value.strip():
        p = Path(value)
        return p if p.is_absolute() else ROOT / p
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return ROOT / "data" / "out" / f"telegram_source_prune_{stamp}.csv"


async def leave_chat(client: Any, entity: Any) -> None:
    try:
        if isinstance(entity, types.Channel):
            await client(functions.channels.LeaveChannelRequest(entity))
            return
    except Exception:
        pass
    await client.delete_dialog(entity)


async def run(args: argparse.Namespace) -> int:
    source_file = Path(args.sources_file) if Path(args.sources_file).is_absolute() else (ROOT / args.sources_file)
    refs = source_refs(source_file)
    if not refs:
        print(f"[tg-prune] no sources in {source_file}")
        return 2

    auth = load_telethon_auth(ROOT)
    client = make_telethon_client(auth)

    conn = None
    db_path = Path(args.db) if Path(args.db).is_absolute() else (ROOT / args.db)
    if args.write_db:
        conn = db_connect(db_path)
        init_db(conn)

    rows: List[Dict[str, Any]] = []
    flagged_refs: List[str] = []
    removed_refs: List[str] = []

    async with client:
        for ref in refs:
            try:
                entity = await client.get_entity(ref)
            except Exception as e:
                rows.append(
                    {
                        "chat_ref": ref,
                        "chat_title": "",
                        "messages_scanned": 0,
                        "spam_msgs": 0,
                        "good_msgs": 0,
                        "hard_spam_msgs": 0,
                        "spam_ratio": 0,
                        "good_ratio": 0,
                        "decision": "error",
                        "reason": f"resolve_failed:{e}",
                    }
                )
                continue

            title = str(
                getattr(entity, "title", "")
                or getattr(entity, "first_name", "")
                or getattr(entity, "username", "")
                or ref
            ).strip()

            scanned = 0
            spam_msgs = 0
            good_msgs = 0
            hard_spam_msgs = 0
            term_counter: Counter = Counter()

            async for msg in client.iter_messages(entity, limit=args.sample_messages):
                text = str(getattr(msg, "message", "") or "").strip()
                if not text:
                    continue
                scanned += 1
                cls = classify_message(text)
                if cls["is_good"]:
                    good_msgs += 1
                if cls["is_spam"]:
                    spam_msgs += 1
                if cls["hard_hits"]:
                    hard_spam_msgs += 1
                for t in cls["spam_hits"]:
                    term_counter[t] += 1
                for t in cls["hard_hits"]:
                    term_counter[t] += 2

            spam_ratio = (spam_msgs / scanned) if scanned else 0.0
            good_ratio = (good_msgs / scanned) if scanned else 0.0
            garbage, reason = decision_for_chat(
                scanned=scanned,
                spam_msgs=spam_msgs,
                good_msgs=good_msgs,
                hard_spam_msgs=hard_spam_msgs,
                spam_terms_counter=term_counter,
                spam_ratio_threshold=args.spam_ratio_threshold,
                min_spam_messages=args.min_spam_messages,
                max_good_ratio=args.max_good_ratio,
                hard_spam_min=args.hard_spam_min,
            )

            decision = "garbage" if garbage else "keep"
            rows.append(
                {
                    "chat_ref": ref,
                    "chat_title": title,
                    "messages_scanned": scanned,
                    "spam_msgs": spam_msgs,
                    "good_msgs": good_msgs,
                    "hard_spam_msgs": hard_spam_msgs,
                    "spam_ratio": round(spam_ratio, 4),
                    "good_ratio": round(good_ratio, 4),
                    "decision": decision,
                    "reason": reason,
                }
            )

            source_contact = f"tg_source:{clean_chat_ref(ref).lstrip('@').lower()}"
            source_url = f"https://t.me/{clean_chat_ref(ref).lstrip('@')}" if clean_chat_ref(ref).startswith("@") else ""
            lead_id = None
            if conn is not None:
                lead = LeadUpsert(
                    platform="telegram_source",
                    lead_type="source",
                    contact=source_contact,
                    url=source_url,
                    company=title,
                    job_title="telegram source",
                    location="",
                    source="telegram_source_prune",
                    raw={
                        "chat_ref": ref,
                        "messages_scanned": scanned,
                        "spam_msgs": spam_msgs,
                        "good_msgs": good_msgs,
                        "hard_spam_msgs": hard_spam_msgs,
                        "spam_ratio": spam_ratio,
                        "good_ratio": good_ratio,
                        "decision": decision,
                        "reason": reason,
                    },
                )
                lead_id, _ = upsert_lead_with_flag(conn, lead)
                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="tg_source_audited",
                    status="ok",
                    details={
                        "chat_ref": ref,
                        "decision": decision,
                        "reason": reason,
                        "spam_ratio": spam_ratio,
                        "good_ratio": good_ratio,
                    },
                )

            if garbage:
                flagged_refs.append(ref)
                if args.apply and len(removed_refs) < args.max_leave:
                    try:
                        await leave_chat(client, entity)
                        removed_refs.append(ref)
                        print(f"[tg-prune] left {ref} ({title})")
                        if conn is not None and lead_id:
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type="tg_source_left",
                                status="ok",
                                details={"chat_ref": ref, "reason": reason},
                            )
                    except FloodWaitError as e:
                        sec = int(getattr(e, "seconds", 0) or 0)
                        print(f"[tg-prune] flood wait for {ref}: {sec}s")
                        if conn is not None and lead_id:
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type="tg_source_left",
                                status="flood_wait",
                                details={"chat_ref": ref, "seconds": sec},
                            )
                        if sec > 0 and sec <= 120:
                            await asyncio.sleep(sec + random.uniform(1.0, 3.0))
                    except RPCError as e:
                        print(f"[tg-prune] leave failed {ref}: {e.__class__.__name__}")
                        if conn is not None and lead_id:
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type="tg_source_left",
                                status="rpc_error",
                                details={"chat_ref": ref, "error": e.__class__.__name__},
                            )
                    except Exception as e:
                        print(f"[tg-prune] leave failed {ref}: {e}")
                        if conn is not None and lead_id:
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type="tg_source_left",
                                status="failed",
                                details={"chat_ref": ref, "error": str(e)},
                            )

                    if args.max_delay_sec > 0:
                        await asyncio.sleep(random.uniform(max(0.0, args.min_delay_sec), max(args.min_delay_sec, args.max_delay_sec)))

    removed_from_file = 0
    if args.apply and removed_refs:
        removed_from_file = rewrite_sources_file(source_file, {clean_chat_ref(x).lower() for x in removed_refs})

    out_csv = out_csv_path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "chat_ref",
                "chat_title",
                "messages_scanned",
                "spam_msgs",
                "good_msgs",
                "hard_spam_msgs",
                "spam_ratio",
                "good_ratio",
                "decision",
                "reason",
            ],
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)

    if conn is not None:
        conn.commit()
        conn.close()

    print(
        f"[tg-prune] scanned_sources={len(refs)} flagged={len(flagged_refs)} "
        f"left={len(removed_refs)} removed_from_file={removed_from_file}"
    )
    print(f"[tg-prune] csv={out_csv}")

    if args.telegram:
        send_telegram_message(
            "\n".join(
                [
                    "AIJobSearcher: Telegram source prune",
                    f"Sources scanned: {len(refs)}",
                    f"Flagged as garbage: {len(flagged_refs)}",
                    f"Left chats: {len(removed_refs)}",
                    f"Removed from file: {removed_from_file}",
                    f"CSV: {out_csv}",
                    "Mode: APPLY" if args.apply else "Mode: DRY-RUN",
                ]
            )
        )

    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Prune garbage Telegram sources: flag spam channels, optionally leave and remove.")
    ap.add_argument("--sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--sample-messages", type=int, default=70)
    ap.add_argument("--spam-ratio-threshold", type=float, default=0.35)
    ap.add_argument("--min-spam-messages", type=int, default=3)
    ap.add_argument("--max-good-ratio", type=float, default=0.20)
    ap.add_argument("--hard-spam-min", type=int, default=2)
    ap.add_argument("--apply", action="store_true", help="Leave flagged chats and remove them from sources file.")
    ap.add_argument("--max-leave", type=int, default=8, help="Safety cap for leaves per run.")
    ap.add_argument("--min-delay-sec", type=float, default=8.0)
    ap.add_argument("--max-delay-sec", type=float, default=20.0)
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db", default="data/out/activity.sqlite")
    ap.add_argument("--telegram", action="store_true")
    return ap


def main() -> None:
    ap = build_parser()
    args = ap.parse_args()
    rc = asyncio.run(run(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
