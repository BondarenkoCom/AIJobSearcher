import argparse
import asyncio
import csv
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from telethon.errors import FloodWaitError, RPCError
from telethon.tl import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import add_event, connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.profile_store import load_profile, normalize_person_name  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402
from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


STRICT_TG_PERSONAL_DAILY_MAX = 15
CONTACT_EVENTS = (
    "tg_dm_sent",
    "email_sent",
    "li_connect_sent",
    "li_dm_sent",
    "li_comment_posted",
    "li_apply_submitted",
    "external_apply_submitted",
    "fm_apply_submitted",
    "wa_apply_submitted",
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _handle(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.lower().startswith("tg_username:"):
        s = s.split(":", 1)[1].strip()
    if s.startswith("@"):
        core = s.lstrip("@").strip()
        return f"@{core}" if core else ""
    if s.lower().startswith("https://t.me/") or s.lower().startswith("http://t.me/"):
        core = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE).strip()
        core = core.split("?", 1)[0].split("#", 1)[0].strip("/")
        core = core.split("/", 1)[0].strip()
        return f"@{core}" if core else ""
    return ""


def _is_tg_username(h: str) -> bool:
    return bool(re.match(r"^@[A-Za-z0-9_]{4,}$", str(h or "")))


def _load_targets(path: Path, limit: int) -> List[Dict[str, str]]:
    rows = list(csv.DictReader(path.open("r", encoding="utf-8-sig", newline="")))
    if int(limit) > 0:
        rows = rows[: int(limit)]

    out: List[Dict[str, str]] = []
    seen = set()
    for r in rows:
        lead_id = str(r.get("lead_id") or "").strip()
        h = _handle(str(r.get("handle") or ""))
        if (not lead_id) or (not _is_tg_username(h)):
            continue
        key = f"{lead_id.lower()}|{h.lower()}"
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "lead_id": lead_id,
                "handle": h,
                "title": str(r.get("title") or "").strip(),
                "company": str(r.get("company") or "").strip(),
                "url": str(r.get("url") or "").strip(),
                "source": str(r.get("source") or "").strip(),
            }
        )
    return out


def _sent_today(conn) -> int:
    day = datetime.now().date().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(1) AS cnt
        FROM events
        WHERE event_type = 'tg_dm_sent'
          AND substr(COALESCE(occurred_at, ''), 1, 10) = ?
        """,
        (day,),
    ).fetchone()
    try:
        return int((row["cnt"] if row is not None else 0) or 0)
    except Exception:
        return 0


def _already_contacted(conn, lead_id: str, handle: str) -> bool:
    h = str(handle or "").strip().lower()
    row = conn.execute(
        f"""
        SELECT 1
        FROM events
        WHERE lead_id = ?
          AND event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
        LIMIT 1
        """,
        (lead_id, *CONTACT_EVENTS),
    ).fetchone()
    if row is not None:
        return True

    # Cross-lead dedupe by Telegram handle in event details.
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE event_type = 'tg_dm_sent'
          AND json_valid(details_json)
          AND lower(COALESCE(json_extract(details_json, '$.handle'), '')) = ?
        LIMIT 1
        """,
        (h,),
    ).fetchone()
    return row is not None


def _dm_text(candidate_name: str, title: str, company: str) -> str:
    role = (title or "a QA/automation task").strip()
    comp = (company or "your team").strip()
    return (
        f"Hi! I saw your post about {role} at {comp}. "
        f"I'm {candidate_name}, QA Engineer (manual + automation), strong in API testing and C#/.NET automation. "
        "I can take short paid gigs and start quickly. If relevant, I can send CV and a short plan."
    )


def bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if str(args.db or "").strip():
        db_path = resolve_path(ROOT, str(args.db).strip())

    in_csv = resolve_path(ROOT, args.csv)
    if not in_csv.exists():
        print(f"[tg-outreach] missing input: {in_csv}")
        return 2

    conn = db_connect(db_path)
    init_db(conn)
    try:
        profile = load_profile(conn)
        candidate_name = normalize_person_name(profile.get("candidate.name") or "") or "Candidate Name"

        sent_before = _sent_today(conn)
        remaining = max(0, STRICT_TG_PERSONAL_DAILY_MAX - sent_before)
        if remaining <= 0:
            print(
                f"[tg-outreach] daily personal cap reached: "
                f"{sent_before}/{STRICT_TG_PERSONAL_DAILY_MAX}. stop."
            )
            return 0

        effective_limit = max(0, min(int(args.limit), remaining))
        if effective_limit < int(args.limit):
            print(
                f"[tg-outreach] strict cap active: requested={args.limit}, "
                f"allowed_now={effective_limit}, already_sent_today={sent_before}"
            )

        targets = _load_targets(in_csv, limit=effective_limit)
        if not targets:
            print("[tg-outreach] no eligible targets.")
            return 0
        print(f"[tg-outreach] loaded targets={len(targets)}")

        if args.max_delay_sec < args.min_delay_sec:
            args.min_delay_sec, args.max_delay_sec = args.max_delay_sec, args.min_delay_sec
        if args.long_break_max_sec < args.long_break_min_sec:
            args.long_break_min_sec, args.long_break_max_sec = args.long_break_max_sec, args.long_break_min_sec

        sent = 0
        skipped = 0
        failed = 0
        paced = 0

        auth = load_telethon_auth(ROOT)
        client = make_telethon_client(auth)
        async with client:
            for idx, t in enumerate(targets, start=1):
                if sent_before + sent >= STRICT_TG_PERSONAL_DAILY_MAX:
                    print(
                        f"[tg-outreach] strict daily cap reached during run: "
                        f"{sent_before + sent}/{STRICT_TG_PERSONAL_DAILY_MAX}. stop."
                    )
                    break

                lead_id = t["lead_id"]
                handle = t["handle"]
                if _already_contacted(conn, lead_id, handle):
                    skipped += 1
                    print(f"[tg-outreach] {idx}/{len(targets)} skip already_contacted {handle}")
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_outreach_skipped",
                        occurred_at=_now_iso(),
                        details={"reason": "already_contacted", "handle": handle},
                    )
                    conn.commit()
                    continue

                text = _dm_text(candidate_name, t["title"], t["company"])
                print(f"[tg-outreach] {idx}/{len(targets)} -> {handle}")
                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="tg_outreach_started",
                    occurred_at=_now_iso(),
                    details={"handle": handle, "title": t["title"], "url": t["url"]},
                )
                conn.commit()

                if args.dry_run:
                    print(f"[tg-outreach] dry-run send -> {handle}")
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_outreach_needs_manual",
                        occurred_at=_now_iso(),
                        details={"reason": "dry_run", "handle": handle, "preview": text[:180]},
                    )
                    conn.commit()
                    skipped += 1
                    continue

                try:
                    ent = await client.get_entity(handle)
                    if not isinstance(ent, types.User):
                        skipped += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type="tg_outreach_needs_manual",
                            occurred_at=_now_iso(),
                            details={"reason": "not_user_chat", "handle": handle},
                        )
                        conn.commit()
                        print(f"[tg-outreach] skip non-user target: {handle}")
                        continue
                    await client.send_message(handle, text)
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_dm_sent",
                        occurred_at=_now_iso(),
                        details={"handle": handle, "title": t["title"], "company": t["company"], "url": t["url"]},
                    )
                    conn.commit()
                    sent += 1
                    paced += 1
                except FloodWaitError as e:
                    sec = int(getattr(e, "seconds", 0) or 0)
                    failed += 1
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_outreach_failed",
                        status="error",
                        occurred_at=_now_iso(),
                        details={"reason": f"flood_wait_{sec}s", "handle": handle},
                    )
                    conn.commit()
                    print(f"[tg-outreach] flood-wait {sec}s on {handle}")
                    if 0 < sec <= 120:
                        await asyncio.sleep(sec + random.uniform(1.0, 3.0))
                    else:
                        break
                except RPCError as e:
                    failed += 1
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_outreach_failed",
                        status="error",
                        occurred_at=_now_iso(),
                        details={"reason": f"rpc_error:{e.__class__.__name__}", "handle": handle},
                    )
                    conn.commit()
                except Exception as e:
                    failed += 1
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="tg_outreach_failed",
                        status="error",
                        occurred_at=_now_iso(),
                        details={"reason": str(e), "handle": handle},
                    )
                    conn.commit()

                await asyncio.sleep(random.uniform(float(args.min_delay_sec), float(args.max_delay_sec)))
                if args.long_break_every > 0 and (paced % int(args.long_break_every) == 0) and idx < len(targets):
                    await asyncio.sleep(random.uniform(float(args.long_break_min_sec), float(args.long_break_max_sec)))

        print(
            f"[tg-outreach] done: sent={sent} skipped={skipped} failed={failed} "
            f"daily={sent_before + sent}/{STRICT_TG_PERSONAL_DAILY_MAX}"
        )
        if args.telegram and bool_env("TELEGRAM_REPORT", True):
            send_telegram_message(
                "\n".join(
                    [
                        "AIJobSearcher: Telegram outreach batch",
                        f"Targets loaded: {len(targets)}",
                        f"Sent: {sent}",
                        f"Skipped: {skipped}",
                        f"Failed: {failed}",
                        f"Daily personal: {sent_before + sent}/{STRICT_TG_PERSONAL_DAILY_MAX}",
                    ]
                )
            )
        return 0
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Telegram personal outreach batch with strict daily cap.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--csv", required=True, help="CSV with lead_id,handle,title,company,url,source")
    ap.add_argument("--limit", type=int, default=15)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-delay-sec", type=float, default=18.0)
    ap.add_argument("--max-delay-sec", type=float, default=45.0)
    ap.add_argument("--long-break-every", type=int, default=5)
    ap.add_argument("--long-break-min-sec", type=float, default=120.0)
    ap.add_argument("--long-break-max-sec", type=float, default=240.0)
    ap.add_argument("--telegram", action="store_true", help="Send summary to bot report channel")
    ap.add_argument("--timeout-seconds", type=int, default=3600)
    args = ap.parse_args()
    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[tg-outreach] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())

