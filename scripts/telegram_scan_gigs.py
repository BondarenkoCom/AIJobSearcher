import argparse
import asyncio
import csv
import os
import random
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from telethon import functions
from telethon.errors import FloodWaitError, RPCError, UserAlreadyParticipantError
from telethon.tl import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402
from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


QA_TERMS = {
    "qa", "quality assurance", "qa engineer", "qa automation", "tester", "test engineer",
    "test automation", "automation testing", "sdet", "playwright", "selenium", "api testing",
    "c#", ".net", "postman", "graphql", "тестировщик", "тестирование", "автотест",
    "автоматизация тестирования",
}
GIG_TERMS = {
    "gig", "task", "one-off", "one off", "short-term", "short term", "freelance", "contract",
    "part-time", "urgent", "need qa", "need tester", "bug fix", "разовая", "разовый", "проект",
    "задача", "срочно",
}
PAY_TERMS = {
    "pay", "paid", "payment", "budget", "rate", "hourly", "fixed price", "usd", "eur", "usdt",
    "$", "€", "оплата", "бюджет", "ставка", "руб", "₽",
}
REMOTE_TERMS = {
    "remote", "worldwide", "anywhere", "wfh", "work from home", "удален", "удалён", "удаленно", "дистанцион",
}
EXCLUDE_TERMS = {"intern", "internship", "director", "vp", "head of qa", "head of quality"}
SCAM_TERMS = {
    "registration fee", "activation fee", "upfront payment", "pay to apply", "advance payment",
    "guaranteed income", "crypto investment", "signal group", "mlm", "сетевой маркетинг", "предоплата",
    "вступительный взнос", "оплата за трудоустройство", "оплата перед началом",
    "proxy support", "interview support", "assignment assistance", "coding tests", "interview co-pilot",
    "training | placement", "placement assistance", "work support", "undetectable",
}
HIRING_TERMS = {
    "hiring",
    "we're hiring",
    "we are hiring",
    "looking for",
    "vacancy",
    "job",
    "position",
    "contractor",
    "нужен",
    "ищем",
    "ищу",
    "вакансия",
    "оплачу",
    "проект",
}
NOISE_TERMS = {
    "[li-posts]",
    "[tg-scan]",
    "activity.sqlite",
    "python.exe",
    "scripts\\",
    "scripts/",
    "debug\\",
    "debug/",
}
OWN_CHAT_HINTS = {"my ai", "ai auto gig"}
DEFAULT_DISCOVER_QUERIES = [
    "qa remote gig",
    "sdet contract remote",
    "test automation freelance",
    "manual qa contract",
    "нужен тестировщик удаленно",
    "разовая задача qa",
    "qa automation проект",
]

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s)>\]}]+", re.IGNORECASE)
TG_HANDLE_RE = re.compile(r"(?<![\w])@[A-Za-z0-9_]{4,}")


def split_items(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[\r\n,;]+", str(raw or "")) if x.strip()]


def read_list_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff").strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def clean_chat_ref(v: str) -> str:
    s = str(v or "").strip()
    if s.startswith("https://t.me/") or s.startswith("http://t.me/"):
        s = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE)
        s = s.split("?", 1)[0].split("#", 1)[0].strip("/")
        s = s.split("/", 1)[0]
        if s:
            return "@" + s
    return s


def hits(text_low: str, terms: Sequence[str]) -> List[str]:
    return sorted({t for t in terms if t and t in text_low})


def evaluate_fit(
    text: str,
    include_terms: Sequence[str],
    exclude_terms: Sequence[str],
    min_score: int,
    require_pay: bool,
) -> Dict[str, Any]:
    low = str(text or "").lower()
    qa = hits(low, include_terms)
    gig = hits(low, GIG_TERMS)
    pay = hits(low, PAY_TERMS)
    rem = hits(low, REMOTE_TERMS)
    bad = hits(low, exclude_terms)
    scam = hits(low, SCAM_TERMS)
    hire = hits(low, HIRING_TERMS)
    noise = hits(low, NOISE_TERMS)
    score = (2 * len(qa)) + len(gig) + len(pay) + len(rem) - (2 * len(bad))
    is_gig = any(x in low for x in ("one-off", "short-term", "bug fix", "urgent", "разов"))
    ok = bool(qa) and bool(gig or pay) and score >= int(min_score) and (bool(hire) or bool(gig))
    if require_pay and not pay:
        ok = False
    if bad:
        ok = False
    if scam or noise:
        ok = False
    return {
        "ok": ok,
        "score": score,
        "qa": qa,
        "gig": gig,
        "pay": pay,
        "rem": rem,
        "scam": scam,
        "hire": hire,
        "noise": noise,
        "lead_type": "gig" if is_gig else "project",
    }


def extract_contacts(text: str) -> Tuple[List[str], List[str], List[str]]:
    emails = sorted({m.strip().lower() for m in EMAIL_RE.findall(text or "")})
    urls = sorted({m.strip().rstrip(".,;:!?)]}") for m in URL_RE.findall(text or "")})
    handles = sorted({m.strip() for m in TG_HANDLE_RE.findall(text or "")})
    return emails, urls, handles


def iso(dt: Optional[datetime]) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat(timespec="seconds")


def slug(s: str, fallback: str = "unknown") -> str:
    out = re.sub(r"\W+", "_", str(s or "").lower()).strip("_")
    return out or fallback


def append_unique_lines(path: Path, values: Sequence[str]) -> List[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = {line.strip().lower() for line in read_list_file(path)}
    added: List[str] = []
    for v in values:
        s = clean_chat_ref(v)
        if not s:
            continue
        if s.lower() in existing:
            continue
        existing.add(s.lower())
        added.append(s)
    if added:
        prefix = "\n" if path.exists() and path.read_text(encoding="utf-8").strip() else ""
        with path.open("a", encoding="utf-8") as f:
            f.write(prefix)
            for x in added:
                f.write(x + "\n")
    return added

def build_row(
    *,
    text: str,
    fit: Dict[str, Any],
    msg: Any,
    chat_title: str,
    chat_ref: str,
    chat_username: str,
    source_mode: str,
    discovery_query: str,
    require_contact: bool,
) -> Optional[Dict[str, Any]]:
    msg_id = int(getattr(msg, "id", 0) or 0)
    msg_date = getattr(msg, "date", None)
    peer = getattr(msg, "peer_id", None)
    peer_slug = "unknown"
    for attr in ("channel_id", "chat_id", "user_id"):
        val = getattr(peer, attr, None) if peer is not None else None
        if val is not None:
            peer_slug = f"peer{val}"
            break

    emails, urls, handles = extract_contacts(text)
    permalink = f"https://t.me/{chat_username}/{msg_id}" if chat_username and msg_id else ""
    primary_url = urls[0] if urls else permalink
    if require_contact and not (emails or handles or urls or permalink):
        return None

    chat_slug = slug(chat_username or chat_ref or chat_title or peer_slug, fallback=peer_slug)
    msg_key = f"{chat_slug}:{msg_id}"
    contact = emails[0] if emails else (f"tg_username:{handles[0].lstrip('@').lower()}" if handles else f"tgmsg:{chat_slug}:{msg_id}")
    title = next((ln.strip(" -*\t") for ln in text.splitlines() if ln.strip()), "Telegram gig")
    if len(title) > 140:
        title = title[:140].rstrip() + "..."

    return {
        "source_mode": source_mode,
        "discovery_query": discovery_query,
        "chat": chat_title,
        "chat_ref": chat_ref,
        "chat_username": ("@" + chat_username) if chat_username else "",
        "chat_slug": chat_slug,
        "message_id": msg_id,
        "message_key": msg_key,
        "title": title,
        "posted_at": iso(msg_date if isinstance(msg_date, datetime) else None),
        "lead_type": fit["lead_type"],
        "score": fit["score"],
        "contact": contact,
        "contact_email": emails[0] if emails else "",
        "contact_handle": handles[0] if handles else "",
        "url": primary_url,
        "pay_signal": "yes" if fit["pay"] else "no",
        "remote_signal": "yes" if fit["rem"] else "no",
        "scam_signal": "yes" if fit["scam"] else "no",
        "hire_signal": "yes" if fit["hire"] else "no",
        "qa_hits": "|".join(fit["qa"]),
        "gig_hits": "|".join(fit["gig"]),
        "pay_hits": "|".join(fit["pay"]),
        "scam_hits": "|".join(fit["scam"]),
        "hire_hits": "|".join(fit["hire"]),
        "noise_hits": "|".join(fit["noise"]),
        "snippet": re.sub(r"\s+", " ", text).strip()[:450],
        "raw_text": text,
    }


async def join_public_sources(client: Any, refs: Sequence[str], max_join: int, min_delay: float, max_delay: float) -> Tuple[List[str], List[Tuple[str, str]]]:
    known: Set[str] = set()
    async for dlg in client.iter_dialogs(limit=None):
        uname = str(getattr(dlg.entity, "username", "") or "").strip()
        if uname:
            known.add(("@" + uname).lower())

    joined: List[str] = []
    skipped: List[Tuple[str, str]] = []
    for raw in refs:
        if len(joined) >= max_join:
            break
        ref = clean_chat_ref(raw)
        if not ref:
            continue
        if not ref.startswith("@"):
            skipped.append((ref, "not_public_username"))
            continue
        if ref.lower() in known:
            skipped.append((ref, "already_joined"))
            continue

        try:
            ent = await client.get_entity(ref)
            if isinstance(ent, types.User):
                skipped.append((ref, "is_user_not_channel"))
                continue
            await client(functions.channels.JoinChannelRequest(ent))
            joined.append(ref)
            known.add(ref.lower())
            if max_delay > 0:
                await asyncio.sleep(random.uniform(max(0.0, min_delay), max(min_delay, max_delay)))
        except UserAlreadyParticipantError:
            skipped.append((ref, "already_joined"))
            known.add(ref.lower())
        except FloodWaitError as e:
            sec = int(getattr(e, "seconds", 0) or 0)
            skipped.append((ref, f"flood_wait_{sec}s"))
            if 0 < sec <= 90:
                await asyncio.sleep(sec + random.uniform(1.0, 3.0))
        except RPCError as e:
            skipped.append((ref, f"rpc_error:{e.__class__.__name__}"))
        except Exception as e:
            skipped.append((ref, f"join_failed:{e}"))

    return joined, skipped


def prepare_chats(args: argparse.Namespace) -> List[str]:
    chats = split_items(args.chats or os.getenv("TELEGRAM_SOURCE_CHATS", ""))
    chat_file = Path(args.chats_file) if Path(args.chats_file).is_absolute() else ROOT / args.chats_file
    chats.extend(read_list_file(chat_file))
    cleaned = [clean_chat_ref(x) for x in chats if clean_chat_ref(x)]
    return list(dict.fromkeys(cleaned))


def prepare_queries(args: argparse.Namespace) -> List[str]:
    queries: List[str] = []
    if args.discover_queries:
        queries.extend(split_items(args.discover_queries))
    q_file = Path(args.discover_queries_file) if Path(args.discover_queries_file).is_absolute() else ROOT / args.discover_queries_file
    queries.extend(read_list_file(q_file))
    if not queries:
        queries.extend(DEFAULT_DISCOVER_QUERIES)
    return list(dict.fromkeys([q.strip() for q in queries if q.strip()]))


def csv_path(value: str) -> Path:
    if value.strip():
        p = Path(value)
        return p if p.is_absolute() else ROOT / p
    return ROOT / "data" / "out" / f"telegram_gigs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}

async def run(args: argparse.Namespace) -> int:
    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    include = list(dict.fromkeys(list(QA_TERMS) + [str(x).lower() for x in cfg_get(cfg, "profile.keywords.include", []) or []]))
    exclude = list(dict.fromkeys(list(EXCLUDE_TERMS) + [str(x).lower() for x in cfg_get(cfg, "profile.keywords.exclude", []) or []]))

    chats = prepare_chats(args)
    queries = prepare_queries(args) if args.discover else []
    if not chats and not args.discover:
        print("[tg-scan] no sources. Fill --chats/--chats-file or run --discover")
        return 2

    auth = load_telethon_auth(ROOT)
    client = make_telethon_client(auth)

    db_path = Path(args.db) if Path(args.db).is_absolute() else ROOT / args.db
    conn = None
    if args.write_db:
        conn = db_connect(db_path)
        init_db(conn)

    rows: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    discovered_refs: Set[str] = set()
    total_scanned = 0
    matched = 0
    inserted = 0
    scanned_chat = 0
    scanned_discover = 0

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(args.days))) if int(args.days) > 0 else None

    async with client:
        for chat_ref in chats:
            if matched >= args.max_results:
                break
            try:
                ent = await client.get_entity(chat_ref)
            except Exception as e:
                print(f"[tg-scan] chat skipped {chat_ref}: {e}")
                continue

            title = str(getattr(ent, "title", "") or getattr(ent, "first_name", "") or getattr(ent, "username", "") or chat_ref).strip()
            uname = str(getattr(ent, "username", "") or "").strip()
            if any(h in title.lower() for h in OWN_CHAT_HINTS):
                print(f"[tg-scan] skip own/noise chat: {title}")
                continue
            effective_ref = ("@" + uname) if uname else chat_ref
            if uname:
                discovered_refs.add("@" + uname)

            async for msg in client.iter_messages(ent, limit=args.limit_per_chat):
                if matched >= args.max_results:
                    break
                text = str(getattr(msg, "message", "") or "").strip()
                if not text:
                    continue
                dt = getattr(msg, "date", None)
                if dt and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if cutoff and dt and dt < cutoff:
                    break

                total_scanned += 1
                scanned_chat += 1
                fit = evaluate_fit(text, include, exclude, args.min_score, args.require_pay_signal)
                if not fit["ok"]:
                    continue
                row = build_row(
                    text=text,
                    fit=fit,
                    msg=msg,
                    chat_title=title,
                    chat_ref=effective_ref,
                    chat_username=uname,
                    source_mode="chat_scan",
                    discovery_query="",
                    require_contact=args.require_contact_signal,
                )
                if not row or row["message_key"] in seen:
                    continue

                seen.add(row["message_key"])
                rows.append(row)
                matched += 1

                if conn is not None:
                    lead = LeadUpsert(
                        platform="telegram",
                        lead_type=str(row["lead_type"]),
                        contact=str(row["contact"]),
                        url=str(row["url"]),
                        company=str(row["chat"]),
                        job_title=str(row["title"]),
                        location="Remote",
                        source=f"telegram:{row['chat_slug']}",
                        created_at=str(row["posted_at"]),
                        raw={
                            "source": "telegram_scan_gigs",
                            "source_mode": row["source_mode"],
                            "chat_ref": row["chat_ref"],
                            "message_id": row["message_id"],
                            "text": row["raw_text"],
                            "score": row["score"],
                            "qa_hits": row["qa_hits"],
                            "gig_hits": row["gig_hits"],
                            "pay_hits": row["pay_hits"],
                            "scam_hits": row["scam_hits"],
                            "hire_hits": row["hire_hits"],
                            "noise_hits": row["noise_hits"],
                        },
                    )
                    lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                    if was_inserted:
                        inserted += 1
                        add_event(conn, lead_id=lead_id, event_type="tg_gig_collected", status="ok", occurred_at=str(row["posted_at"]), details={"chat_ref": row["chat_ref"], "message_id": row["message_id"], "score": row["score"]})

        if args.discover:
            for query in queries:
                if matched >= args.max_results:
                    break
                try:
                    found = await client(functions.contacts.SearchRequest(q=query, limit=args.discover_source_limit))
                    for ch in list(getattr(found, "chats", []) or []):
                        uname = str(getattr(ch, "username", "") or "").strip()
                        title = str(getattr(ch, "title", "") or "").strip()
                        if not uname:
                            continue
                        if any(h in title.lower() for h in OWN_CHAT_HINTS):
                            continue
                        discovered_refs.add("@" + uname)

                    async for msg in client.iter_messages(None, search=query, limit=args.discover_limit_per_query):
                        if matched >= args.max_results:
                            break
                        text = str(getattr(msg, "message", "") or "").strip()
                        if not text:
                            continue
                        dt = getattr(msg, "date", None)
                        if dt and dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        if cutoff and dt and dt < cutoff:
                            continue

                        total_scanned += 1
                        scanned_discover += 1
                        fit = evaluate_fit(text, include, exclude, args.min_score, args.require_pay_signal)
                        if not fit["ok"]:
                            continue

                        chat_ent = None
                        try:
                            chat_ent = await msg.get_chat()
                        except Exception:
                            pass
                        title = str(getattr(chat_ent, "title", "") or getattr(chat_ent, "first_name", "") or getattr(chat_ent, "username", "") or "Telegram").strip()
                        if any(h in title.lower() for h in OWN_CHAT_HINTS):
                            continue
                        uname = str(getattr(chat_ent, "username", "") or "").strip()
                        chat_ref = ("@" + uname) if uname else ""
                        if chat_ref:
                            discovered_refs.add(chat_ref)

                        row = build_row(
                            text=text,
                            fit=fit,
                            msg=msg,
                            chat_title=title,
                            chat_ref=chat_ref,
                            chat_username=uname,
                            source_mode="global_discovery",
                            discovery_query=query,
                            require_contact=args.require_contact_signal,
                        )
                        if not row or row["message_key"] in seen:
                            continue

                        seen.add(row["message_key"])
                        rows.append(row)
                        matched += 1

                        if conn is not None:
                            lead = LeadUpsert(
                                platform="telegram",
                                lead_type=str(row["lead_type"]),
                                contact=str(row["contact"]),
                                url=str(row["url"]),
                                company=str(row["chat"]),
                                job_title=str(row["title"]),
                                location="Remote",
                                source=f"telegram:{row['chat_slug']}",
                                created_at=str(row["posted_at"]),
                                raw={
                                    "source": "telegram_scan_gigs",
                                    "source_mode": row["source_mode"],
                                    "discovery_query": row["discovery_query"],
                                    "chat_ref": row["chat_ref"],
                                    "message_id": row["message_id"],
                                    "text": row["raw_text"],
                                    "score": row["score"],
                                    "qa_hits": row["qa_hits"],
                                    "gig_hits": row["gig_hits"],
                                    "pay_hits": row["pay_hits"],
                                    "scam_hits": row["scam_hits"],
                                    "hire_hits": row["hire_hits"],
                                    "noise_hits": row["noise_hits"],
                                },
                            )
                            lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                            if was_inserted:
                                inserted += 1
                                add_event(conn, lead_id=lead_id, event_type="tg_gig_collected", status="ok", occurred_at=str(row["posted_at"]), details={"chat_ref": row["chat_ref"], "message_id": row["message_id"], "score": row["score"], "query": row["discovery_query"]})
                except Exception as e:
                    print(f"[tg-scan] discovery query failed '{query}': {e}")

        added_sources: List[str] = []
        if args.append_discovered_sources and discovered_refs:
            source_file = Path(args.append_sources_file)
            if not source_file.is_absolute():
                source_file = ROOT / source_file
            added_sources = append_unique_lines(source_file, sorted(discovered_refs))
            if added_sources:
                print(f"[tg-scan] added_sources_to_file={len(added_sources)} file={source_file}")

        joined: List[str] = []
        skipped: List[Tuple[str, str]] = []
        if args.join_discovered and discovered_refs:
            joined, skipped = await join_public_sources(client, sorted(discovered_refs), args.join_max, args.join_min_delay_sec, args.join_max_delay_sec)
            print(f"[tg-scan] joined={len(joined)} skipped={len(skipped)}")
            for ref, reason in skipped[:12]:
                print(f"[tg-scan] join-skip {ref}: {reason}")

    if conn is not None:
        conn.commit()
        conn.close()

    out_csv = csv_path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "source_mode", "discovery_query", "chat", "chat_ref", "chat_username", "message_id", "title",
        "posted_at", "lead_type", "score", "contact", "contact_email", "contact_handle", "url",
        "pay_signal", "remote_signal", "scam_signal", "hire_signal", "qa_hits", "gig_hits", "pay_hits",
        "scam_hits", "hire_hits", "noise_hits", "snippet",
    ]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    print(f"[tg-scan] chats={len(chats)} queries={len(queries)} scanned_msgs={total_scanned} (chat={scanned_chat}, discover={scanned_discover}) matched={matched} inserted={inserted}")
    print(f"[tg-scan] csv={out_csv}")
    if args.write_db:
        print(f"[tg-scan] db={db_path}")

    if args.telegram and bool_env("TELEGRAM_REPORT", True):
        send_telegram_message("\n".join([
            "AIJobSearcher: Telegram gig scan",
            f"Chats configured: {len(chats)}",
            f"Discovery queries: {len(queries)}",
            f"Scanned messages: {total_scanned}",
            f"Matched gigs/projects: {matched}",
            f"Inserted to DB: {inserted}" if args.write_db else "Inserted to DB: disabled",
            f"CSV: {out_csv}",
        ]))

    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Scan Telegram chats/channels via Telethon for QA gigs/projects.")
    ap.add_argument("--chats", default="", help="Comma/newline-separated chat refs (@name or t.me/name).")
    ap.add_argument("--chats-file", default="data/inbox/telegram_chats.txt", help="Text file with chat refs.")
    ap.add_argument("--limit-per-chat", type=int, default=int(os.getenv("TELEGRAM_SCAN_LIMIT_PER_CHAT", "120")))
    ap.add_argument("--days", type=int, default=int(os.getenv("TELEGRAM_SCAN_DAYS", "14")))
    ap.add_argument("--max-results", type=int, default=200)
    ap.add_argument("--min-score", type=int, default=3)
    ap.add_argument("--require-pay-signal", dest="require_pay_signal", action="store_true")
    ap.add_argument("--no-require-pay-signal", dest="require_pay_signal", action="store_false")
    ap.set_defaults(require_pay_signal=True)
    ap.add_argument("--require-contact-signal", dest="require_contact_signal", action="store_true")
    ap.add_argument("--no-require-contact-signal", dest="require_contact_signal", action="store_false")
    ap.set_defaults(require_contact_signal=True)

    ap.add_argument("--discover", action="store_true", help="Use Telegram global search to discover sources.")
    ap.add_argument("--discover-queries", default="", help="Comma/newline-separated global search queries.")
    ap.add_argument("--discover-queries-file", default="data/inbox/telegram_discovery_queries.txt")
    ap.add_argument("--discover-limit-per-query", type=int, default=70)
    ap.add_argument("--discover-source-limit", type=int, default=40, help="Source candidates per query via contacts search.")

    ap.add_argument("--append-sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--append-discovered-sources", dest="append_discovered_sources", action="store_true")
    ap.add_argument("--no-append-discovered-sources", dest="append_discovered_sources", action="store_false")
    ap.set_defaults(append_discovered_sources=True)

    ap.add_argument("--join-discovered", action="store_true")
    ap.add_argument("--join-max", type=int, default=8)
    ap.add_argument("--join-min-delay-sec", type=float, default=12.0)
    ap.add_argument("--join-max-delay-sec", type=float, default=28.0)

    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db", default="data/out/activity.sqlite")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--telegram", action="store_true")
    return ap


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    rc = asyncio.run(run(args))
    raise SystemExit(rc)


if __name__ == "__main__":
    main()
