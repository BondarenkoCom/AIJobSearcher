import argparse
import asyncio
import csv
import os
import random
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List

from telethon.errors import FloodWaitError, RPCError
from telethon.tl import types

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file, render_template  # noqa: E402
from src.profile_store import load_profile, normalize_person_name  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402
from src.telegram_telethon import load_telethon_auth, make_telethon_client  # noqa: E402


EVENT_POSTED = "tg_public_share_posted"
EVENT_STARTED = "tg_public_share_started"
EVENT_SKIPPED = "tg_public_share_skipped"
EVENT_FAILED = "tg_public_share_failed"
EVENT_MANUAL = "tg_public_share_needs_manual"

DEFAULT_DAILY_CAP = 8
WRITE_FORBIDDEN_ERRORS = {
    "ChatAdminRequiredError",
    "ChatWriteForbiddenError",
    "ChatGuestSendForbiddenError",
    "UserBannedInChannelError",
}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe(v: object) -> str:
    return str(v or "").strip()


def _chat_ref(value: str) -> str:
    s = _safe(value)
    if not s:
        return ""
    if s.startswith("@"):
        core = s.lstrip("@").strip()
        return f"@{core}" if core else ""
    if s.lower().startswith("https://t.me/") or s.lower().startswith("http://t.me/"):
        core = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE).strip()
        core = core.split("?", 1)[0].split("#", 1)[0].strip("/")
        core = core.split("/", 1)[0].strip()
        return f"@{core}" if core else ""
    if re.match(r"^[A-Za-z0-9_]{4,}$", s):
        return f"@{s}"
    return ""


def _is_chat_ref(value: str) -> bool:
    return bool(re.match(r"^@[A-Za-z0-9_]{4,}$", _safe(value)))


def _load_targets(path: Path, limit: int) -> List[Dict[str, str]]:
    rows = list(csv.DictReader(path.open("r", encoding="utf-8-sig", newline="")))
    out: List[Dict[str, str]] = []
    seen = set()
    for r in rows:
        ref = (
            _chat_ref(_safe(r.get("chat_ref")))
            or _chat_ref(_safe(r.get("chat_username")))
            or _chat_ref(_safe(r.get("chat")))
            or _chat_ref(_safe(r.get("url")))
        )
        if not _is_chat_ref(ref):
            continue
        key = ref.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "chat_ref": ref,
                "chat_title": _safe(r.get("chat")) or _safe(r.get("title")) or ref,
                "source": _safe(r.get("source")) or "share_targets",
                "score": _safe(r.get("score")),
                "hits": _safe(r.get("hits")),
            }
        )
        if int(limit) > 0 and len(out) >= int(limit):
            break
    return out


def _posted_today(conn) -> int:
    day = datetime.now().date().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(1) AS cnt
        FROM events
        WHERE event_type = ?
          AND substr(COALESCE(occurred_at, ''), 1, 10) = ?
        """,
        (EVENT_POSTED, day),
    ).fetchone()
    try:
        return int((row["cnt"] if row is not None else 0) or 0)
    except Exception:
        return 0


def _already_shared_recently(conn, chat_ref: str, project_url: str, cooldown_days: int) -> bool:
    cutoff = (datetime.now() - timedelta(days=max(1, int(cooldown_days)))).isoformat(timespec="seconds")
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE event_type = ?
          AND occurred_at >= ?
          AND json_valid(details_json)
          AND lower(COALESCE(json_extract(details_json, '$.chat_ref'), '')) = ?
          AND lower(COALESCE(json_extract(details_json, '$.project_url'), '')) = ?
        LIMIT 1
        """,
        (EVENT_POSTED, cutoff, _safe(chat_ref).lower(), _safe(project_url).lower()),
    ).fetchone()
    return row is not None


def _offer_line(offer_title: str) -> str:
    title = _safe(offer_title) or "API QA smoke/regression checks with actionable findings"
    variants = [
        f"Offer: {title}.",
        f"Current offer: {title}.",
        f"I am currently offering: {title}.",
    ]
    return random.choice(variants)


def _read_template(path: Path) -> str:
    if path.exists():
        text = path.read_text(encoding="utf-8-sig").strip()
        if text:
            return text
    return (
        "Hi everyone! I build practical QA/API automation and can take paid short tasks.\n\n"
        "If useful for your team, here is my Upwork project:\n"
        "{project_url}\n\n"
        "{offer_line}\n\n"
        "I work remote worldwide and can start quickly."
    )


def _build_text(
    *,
    template_text: str,
    candidate_name: str,
    project_url: str,
    offer_title: str,
) -> str:
    vars_map = {
        "candidate_name": candidate_name,
        "project_url": project_url,
        "offer_title": _safe(offer_title),
        "offer_line": _offer_line(offer_title),
    }
    return render_template(template_text, vars_map).strip()


def bool_env(name: str, default: bool = False) -> bool:
    raw = _safe(os.getenv(name)).lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _upsert_target_lead(conn, chat_ref: str, chat_title: str) -> str:
    uname = chat_ref.lstrip("@")
    url = f"https://t.me/{uname}" if uname else ""
    lead = LeadUpsert(
        platform="telegram_source",
        lead_type="channel",
        contact=f"tg_chat:{uname.lower()}",
        url=url,
        company=_safe(chat_title) or chat_ref,
        job_title="public share target",
        location="Remote",
        source="telegram_public_share_batch",
        created_at=_now_iso(),
        raw={"chat_ref": chat_ref, "chat_title": chat_title},
    )
    lead_id, _ = upsert_lead_with_flag(conn, lead)
    return lead_id


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _safe(args.db):
        db_path = resolve_path(ROOT, _safe(args.db))

    in_csv = resolve_path(ROOT, args.csv)
    if not in_csv.exists():
        print(f"[tg-public-share] missing input: {in_csv}")
        return 2

    template_path = resolve_path(ROOT, args.template)
    template_text = _read_template(template_path)

    conn = db_connect(db_path)
    init_db(conn)
    try:
        profile = load_profile(conn)
        candidate_name = normalize_person_name(_safe(profile.get("candidate.name"))) or "Candidate Name"

        posted_before = _posted_today(conn)
        daily_cap = max(1, int(args.daily_cap))
        remaining = max(0, daily_cap - posted_before)
        if remaining <= 0:
            print(f"[tg-public-share] daily cap reached: {posted_before}/{daily_cap}")
            return 0

        requested_limit = max(1, int(args.limit))
        effective_limit = min(requested_limit, remaining)
        if effective_limit < requested_limit:
            print(
                f"[tg-public-share] strict cap active: requested={requested_limit}, "
                f"allowed_now={effective_limit}, already_posted_today={posted_before}"
            )

        targets = _load_targets(in_csv, limit=effective_limit)
        if not targets:
            print("[tg-public-share] no eligible targets.")
            return 0

        if float(args.max_delay_sec) < float(args.min_delay_sec):
            args.min_delay_sec, args.max_delay_sec = args.max_delay_sec, args.min_delay_sec
        if float(args.long_break_max_sec) < float(args.long_break_min_sec):
            args.long_break_min_sec, args.long_break_max_sec = args.long_break_max_sec, args.long_break_min_sec

        sent = 0
        skipped = 0
        failed = 0
        paced = 0

        client = None
        if not args.dry_run:
            auth = load_telethon_auth(ROOT)
            client = make_telethon_client(auth)

        if client is not None:
            async with client:
                for idx, t in enumerate(targets, start=1):
                    if posted_before + sent >= daily_cap:
                        print(f"[tg-public-share] daily cap reached during run: {posted_before + sent}/{daily_cap}")
                        break

                    chat_ref = _safe(t["chat_ref"])
                    chat_title = _safe(t["chat_title"])
                    lead_id = _upsert_target_lead(conn, chat_ref, chat_title)

                    if _already_shared_recently(conn, chat_ref, args.project_url, int(args.cooldown_days)):
                        skipped += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type=EVENT_SKIPPED,
                            occurred_at=_now_iso(),
                            details={
                                "reason": "cooldown_recent_share",
                                "chat_ref": chat_ref,
                                "project_url": args.project_url,
                                "cooldown_days": int(args.cooldown_days),
                            },
                        )
                        conn.commit()
                        print(f"[tg-public-share] {idx}/{len(targets)} skip cooldown {chat_ref}")
                        continue

                    text = _build_text(
                        template_text=template_text,
                        candidate_name=candidate_name,
                        project_url=args.project_url,
                        offer_title=args.offer_title,
                    )
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type=EVENT_STARTED,
                        occurred_at=_now_iso(),
                        details={"chat_ref": chat_ref, "chat_title": chat_title, "project_url": args.project_url},
                    )
                    conn.commit()

                    try:
                        ent = await client.get_entity(chat_ref)
                        if isinstance(ent, types.User):
                            skipped += 1
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type=EVENT_MANUAL,
                                occurred_at=_now_iso(),
                                details={"reason": "target_is_user_not_chat", "chat_ref": chat_ref},
                            )
                            conn.commit()
                            print(f"[tg-public-share] {idx}/{len(targets)} skip user target {chat_ref}")
                            continue
                        if isinstance(ent, types.Channel):
                            is_broadcast = bool(getattr(ent, "broadcast", False))
                            is_megagroup = bool(getattr(ent, "megagroup", False))
                            is_creator = bool(getattr(ent, "creator", False))
                            if is_broadcast and (not is_megagroup) and (not is_creator):
                                skipped += 1
                                add_event(
                                    conn,
                                    lead_id=lead_id,
                                    event_type=EVENT_MANUAL,
                                    occurred_at=_now_iso(),
                                    details={
                                        "reason": "broadcast_channel_readonly",
                                        "chat_ref": chat_ref,
                                        "chat_title": chat_title,
                                    },
                                )
                                conn.commit()
                                print(f"[tg-public-share] {idx}/{len(targets)} skip readonly channel {chat_ref}")
                                continue

                        await client.send_message(ent, text, link_preview=True)
                        sent += 1
                        paced += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type=EVENT_POSTED,
                            occurred_at=_now_iso(),
                            details={
                                "chat_ref": chat_ref,
                                "chat_title": chat_title,
                                "project_url": args.project_url,
                                "offer_title": args.offer_title,
                                "source": t.get("source", ""),
                                "score": t.get("score", ""),
                                "hits": t.get("hits", ""),
                            },
                        )
                        conn.commit()
                        print(f"[tg-public-share] {idx}/{len(targets)} posted {chat_ref}")
                        wait_sec = random.uniform(float(args.min_delay_sec), float(args.max_delay_sec))
                        print(f"[tg-public-share] wait {wait_sec:.1f}s")
                        await asyncio.sleep(wait_sec)
                        if int(args.long_break_every) > 0 and (paced % int(args.long_break_every) == 0) and idx < len(targets):
                            long_wait = random.uniform(float(args.long_break_min_sec), float(args.long_break_max_sec))
                            print(f"[tg-public-share] long break {long_wait:.1f}s")
                            await asyncio.sleep(long_wait)
                    except FloodWaitError as e:
                        sec = int(getattr(e, "seconds", 0) or 0)
                        failed += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type=EVENT_FAILED,
                            status="error",
                            occurred_at=_now_iso(),
                            details={"reason": f"flood_wait_{sec}s", "chat_ref": chat_ref},
                        )
                        conn.commit()
                        print(f"[tg-public-share] flood-wait {sec}s on {chat_ref}")
                        if 0 < sec <= 120:
                            await asyncio.sleep(sec + random.uniform(1.0, 3.0))
                        else:
                            break
                    except RPCError as e:
                        err_name = e.__class__.__name__
                        if err_name in WRITE_FORBIDDEN_ERRORS:
                            skipped += 1
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type=EVENT_MANUAL,
                                occurred_at=_now_iso(),
                                details={"reason": f"write_forbidden:{err_name}", "chat_ref": chat_ref},
                            )
                            conn.commit()
                            print(f"[tg-public-share] write-forbidden {chat_ref}: {err_name}")
                        else:
                            failed += 1
                            add_event(
                                conn,
                                lead_id=lead_id,
                                event_type=EVENT_FAILED,
                                status="error",
                                occurred_at=_now_iso(),
                                details={"reason": f"rpc_error:{err_name}", "chat_ref": chat_ref},
                            )
                            conn.commit()
                            print(f"[tg-public-share] rpc error on {chat_ref}: {err_name}")
                    except Exception as e:
                        failed += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type=EVENT_FAILED,
                            status="error",
                            occurred_at=_now_iso(),
                            details={"reason": _safe(e), "chat_ref": chat_ref},
                        )
                        conn.commit()
                        print(f"[tg-public-share] failed on {chat_ref}: {_safe(e)}")
                    await asyncio.sleep(random.uniform(2.0, 6.0))
        else:
            for idx, t in enumerate(targets, start=1):
                if posted_before + sent >= daily_cap:
                    print(f"[tg-public-share] daily cap reached during run: {posted_before + sent}/{daily_cap}")
                    break
                chat_ref = _safe(t["chat_ref"])
                chat_title = _safe(t["chat_title"])
                lead_id = _upsert_target_lead(conn, chat_ref, chat_title)

                if _already_shared_recently(conn, chat_ref, args.project_url, int(args.cooldown_days)):
                    skipped += 1
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type=EVENT_SKIPPED,
                        occurred_at=_now_iso(),
                        details={
                            "reason": "cooldown_recent_share",
                            "chat_ref": chat_ref,
                            "project_url": args.project_url,
                            "cooldown_days": int(args.cooldown_days),
                        },
                    )
                    conn.commit()
                    print(f"[tg-public-share] {idx}/{len(targets)} skip cooldown {chat_ref}")
                    continue

                text = _build_text(
                    template_text=template_text,
                    candidate_name=candidate_name,
                    project_url=args.project_url,
                    offer_title=args.offer_title,
                )
                skipped += 1
                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type=EVENT_MANUAL,
                    occurred_at=_now_iso(),
                    details={
                        "reason": "dry_run",
                        "chat_ref": chat_ref,
                        "chat_title": chat_title,
                        "preview": text[:220],
                        "project_url": args.project_url,
                    },
                )
                conn.commit()
                print(f"[tg-public-share] {idx}/{len(targets)} dry-run {chat_ref}")

        print(
            f"[tg-public-share] done: posted={sent} skipped={skipped} failed={failed} "
            f"daily={posted_before + sent}/{daily_cap}"
        )
        if args.telegram and bool_env("TELEGRAM_REPORT", True):
            send_telegram_message(
                "\n".join(
                    [
                        "AIJobSearcher: Telegram public share",
                        f"Targets loaded: {len(targets)}",
                        f"Posted: {sent}",
                        f"Skipped: {skipped}",
                        f"Failed: {failed}",
                        f"Daily: {posted_before + sent}/{daily_cap}",
                        f"Project URL: {args.project_url}",
                    ]
                )
            )
        return 0
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Post Upwork project link to public Telegram job chats/channels with pacing.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--csv", required=True, help="CSV with chat_ref/chat_username/chat/title columns.")
    ap.add_argument("--project-url", required=True, help="Upwork Project Catalog URL.")
    ap.add_argument("--offer-title", default="API QA smoke and regression tests with actionable findings")
    ap.add_argument("--template", default="templates/tg_public_share_upwork_en.txt")
    ap.add_argument("--limit", type=int, default=8)
    ap.add_argument("--daily-cap", type=int, default=DEFAULT_DAILY_CAP)
    ap.add_argument("--cooldown-days", type=int, default=14)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--min-delay-sec", type=float, default=28.0)
    ap.add_argument("--max-delay-sec", type=float, default=65.0)
    ap.add_argument("--long-break-every", type=int, default=3)
    ap.add_argument("--long-break-min-sec", type=float, default=120.0)
    ap.add_argument("--long-break-max-sec", type=float, default=260.0)
    ap.add_argument("--telegram", action="store_true", help="Send summary to report bot.")
    ap.add_argument("--timeout-seconds", type=int, default=3600)
    args = ap.parse_args()
    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[tg-public-share] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())

