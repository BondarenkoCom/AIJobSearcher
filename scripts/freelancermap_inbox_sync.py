import argparse
import asyncio
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from playwright.async_api import Page, async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, int_env  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402

SYSTEM_CONVERSATION_TYPES = {"welcome_message", "hard_spam_warning"}


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _text(v: Any) -> str:
    return str(v or "").strip()


def _lower(v: Any) -> str:
    return _text(v).lower()


def _safe_json_loads(raw: str) -> Dict[str, Any]:
    try:
        obj = json.loads(raw)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = _lower(v)
    return s in {"1", "true", "yes", "y", "on"}


def _dt_from_block(v: Any) -> str:
    if isinstance(v, dict):
        return _text(v.get("dateTime") or v.get("dateAsString"))
    return _text(v)


def _is_verification_gate_url(url: str) -> bool:
    u = _lower(url)
    return ("/user-verification" in u) or ("/upgrade" in u and "freelancermap.com" in u)


def _normalize_title_for_like(title: str) -> str:
    t = _text(title)
    if not t:
        return ""
    t = re.sub(r"\s+", " ", t)
    return t[:72]


def _dump_json_excerpt(v: Any, max_len: int = 6000) -> str:
    try:
        txt = json.dumps(v, ensure_ascii=False, sort_keys=True)
    except Exception:
        txt = "{}"
    return txt[:max_len]


def _extract_payload_from_scripts(raw_scripts: Sequence[str]) -> Dict[str, Any]:
    for raw in raw_scripts:
        txt = _text(raw)
        if not txt:
            continue
        obj = _safe_json_loads(txt)
        if obj and "initialConversations" in obj:
            return obj
    return {}


def _event_exists_with_reply_signature(conn, lead_id: str, reply_signature: str) -> bool:
    pattern = '%"reply_signature": "' + reply_signature.replace('"', '\\"') + '"%'
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE lead_id = ?
          AND event_type = 'fm_reply_received'
          AND details_json LIKE ?
        LIMIT 1
        """,
        (lead_id, pattern),
    ).fetchone()
    return row is not None


def _latest_event_details(conn, lead_id: str, event_type: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT details_json
        FROM events
        WHERE lead_id = ? AND event_type = ?
        ORDER BY occurred_at DESC, event_id DESC
        LIMIT 1
        """,
        (lead_id, event_type),
    ).fetchone()
    if not row:
        return {}
    raw = _text(row["details_json"])
    return _safe_json_loads(raw) if raw else {}


def _match_project_lead(conn, *, title: str, company: str) -> Optional[Dict[str, str]]:
    t = _text(title)
    c = _text(company)
    if t:
        row = conn.execute(
            """
            SELECT lead_id, job_title, company, url
            FROM leads
            WHERE platform = 'freelancermap.com'
              AND lead_type = 'project'
              AND lower(job_title) = lower(?)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (t,),
        ).fetchone()
        if row:
            return {
                "lead_id": _text(row["lead_id"]),
                "job_title": _text(row["job_title"]),
                "company": _text(row["company"]),
                "url": _text(row["url"]),
            }

    if c and t:
        title_like = "%" + _normalize_title_for_like(t).lower() + "%"
        row = conn.execute(
            """
            SELECT lead_id, job_title, company, url
            FROM leads
            WHERE platform = 'freelancermap.com'
              AND lead_type = 'project'
              AND lower(company) = lower(?)
              AND lower(job_title) LIKE ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (c, title_like),
        ).fetchone()
        if row:
            return {
                "lead_id": _text(row["lead_id"]),
                "job_title": _text(row["job_title"]),
                "company": _text(row["company"]),
                "url": _text(row["url"]),
            }

    if c:
        row = conn.execute(
            """
            SELECT lead_id, job_title, company, url
            FROM leads
            WHERE platform = 'freelancermap.com'
              AND lead_type = 'project'
              AND lower(company) = lower(?)
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (c,),
        ).fetchone()
        if row:
            return {
                "lead_id": _text(row["lead_id"]),
                "job_title": _text(row["job_title"]),
                "company": _text(row["company"]),
                "url": _text(row["url"]),
            }

    return None


@dataclass
class Conversation:
    conversation_id: int
    title: str
    conversation_type: str
    io_type: str
    my_side: str
    participant_side: str
    has_read: bool
    replied: bool
    has_ever_replied: bool
    state: str
    created_at: str
    last_activity_at: str
    contact_name: str
    contact_company: str
    contact_email: str
    contact_user_id: str
    raw_excerpt: str

    def is_system(self) -> bool:
        if self.conversation_id < 0:
            return True
        return self.conversation_type in SYSTEM_CONVERSATION_TYPES

    def looks_like_reply(self) -> bool:
        if self.is_system():
            return False
        if self.replied or self.has_ever_replied:
            return True
        if self.io_type.startswith("received"):
            return True
        if self.my_side == "recipient":
            return True
        if self.participant_side == "sender":
            return True
        if (not self.has_read) and (not self.io_type.startswith("sent")):
            return True
        return False

    def seen_signature(self) -> str:
        return "|".join(
            [
                str(self.conversation_id),
                self.io_type,
                self.my_side,
                self.participant_side,
                "1" if self.has_read else "0",
                "1" if self.replied else "0",
                "1" if self.has_ever_replied else "0",
                self.state,
                self.last_activity_at,
            ]
        )

    def reply_signature(self) -> str:
        return "|".join(
            [
                str(self.conversation_id),
                self.io_type,
                "1" if self.replied else "0",
                "1" if self.has_ever_replied else "0",
                self.last_activity_at,
            ]
        )


def _conversation_from_item(item: Dict[str, Any]) -> Conversation:
    c = item.get("contact") if isinstance(item.get("contact"), dict) else {}
    ci = c.get("contactInformation") if isinstance(c.get("contactInformation"), dict) else {}
    created = _dt_from_block(item.get("created"))
    sort_dt = _dt_from_block(item.get("sortDate"))
    last_activity = sort_dt or created
    raw_excerpt = {
        "id": item.get("id"),
        "title": item.get("title"),
        "ioType": item.get("ioType"),
        "conversationType": item.get("conversationType"),
        "mySide": item.get("mySide"),
        "participantSide": c.get("participantSide"),
        "hasRead": item.get("hasRead"),
        "replied": item.get("replied"),
        "hasEverReplied": item.get("hasEverReplied"),
        "conversationState": item.get("conversationState"),
        "sortDate": item.get("sortDate"),
        "created": item.get("created"),
        "contactInformation": ci,
    }
    return Conversation(
        conversation_id=int(item.get("id") or 0),
        title=_text(item.get("title")),
        conversation_type=_text(item.get("conversationType")).lower(),
        io_type=_text(item.get("ioType")).lower(),
        my_side=_text(item.get("mySide")).lower(),
        participant_side=_text(c.get("participantSide")).lower(),
        has_read=_bool(item.get("hasRead")),
        replied=_bool(item.get("replied")),
        has_ever_replied=_bool(item.get("hasEverReplied")),
        state=_text(item.get("conversationState")).lower(),
        created_at=created,
        last_activity_at=last_activity,
        contact_name=_text(ci.get("fullName")),
        contact_company=_text(ci.get("company")),
        contact_email=_text(ci.get("email")).lower(),
        contact_user_id=_text(ci.get("userId")),
        raw_excerpt=_dump_json_excerpt(raw_excerpt),
    )


def _extract_conversations(payload: Dict[str, Any], *, include_system: bool, limit: int) -> List[Conversation]:
    ic = payload.get("initialConversations")
    if not isinstance(ic, dict):
        return []
    items = ic.get("items")
    if not isinstance(items, list):
        return []

    out: List[Conversation] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        conv = _conversation_from_item(item)
        if (not include_system) and conv.is_system():
            continue
        out.append(conv)
        if limit > 0 and len(out) >= limit:
            break
    return out


async def _dump_debug(root: Path, page: Page, tag: str) -> None:
    debug_dir = root / "data" / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html = debug_dir / f"freelancermap_inbox_{tag}_{stamp}.html"
    png = debug_dir / f"freelancermap_inbox_{tag}_{stamp}.png"
    try:
        html.write_text(await page.content(), encoding="utf-8", errors="replace")
    except Exception:
        pass
    try:
        await page.screenshot(path=str(png), full_page=True)
    except Exception:
        pass


async def _is_logged_in(page: Page) -> bool:
    try:
        if await page.locator("a[href*='my_account']").count() > 0:
            return True
    except Exception:
        pass
    return False


async def _ensure_session(page: Page, *, email: str, password: str, timeout_ms: int) -> Tuple[bool, str]:
    try:
        await page.goto("https://www.freelancermap.com/my_account.html", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass

    if await _is_logged_in(page):
        return True, "ok"

    try:
        await page.goto("https://www.freelancermap.com/it-projects.html", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass

    for sel in [
        "button:has-text('Accept all cookies')",
        "button#onetrust-accept-btn-handler",
    ]:
        try:
            b = page.locator(sel).first
            if await b.count() > 0 and await b.is_visible():
                await b.click(timeout=1500)
                await page.wait_for_timeout(250)
                break
        except Exception:
            continue

    opened = False
    for sel in [
        "button[data-testid='login-button']",
        "button:has-text('Log in')",
    ]:
        try:
            b = page.locator(sel).first
            if await b.count() > 0:
                await b.click(timeout=3000)
                opened = True
                await page.wait_for_timeout(300)
                break
        except Exception:
            continue
    if not opened:
        return False, "login_button_not_found"

    email_input = page.locator("input[placeholder*='Email address or username'], input[name='loginEmail']").first
    password_input = page.locator("input[type='password']").first
    submit_btn = page.locator(
        "button[data-testid='next-button'], "
        "form button[type='submit'], "
        "button[type='submit']:not([data-testid='login-button'])"
    ).first

    if await email_input.count() == 0 or await password_input.count() == 0:
        return False, "login_form_not_found"

    await email_input.fill(email)
    await password_input.fill(password)
    clicked = False
    try:
        if await submit_btn.count() > 0:
            await submit_btn.click(timeout=4000, force=True)
            clicked = True
    except Exception:
        clicked = False
    if not clicked:
        try:
            await password_input.press("Enter")
        except Exception:
            pass
    await page.wait_for_timeout(1800)

    try:
        await page.goto("https://www.freelancermap.com/my_account.html", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    if await _is_logged_in(page):
        return True, "ok"
    return False, "login_failed_or_checkpoint"


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "captured_at",
        "conversation_id",
        "status",
        "conversation_type",
        "io_type",
        "my_side",
        "participant_side",
        "has_read",
        "replied",
        "has_ever_replied",
        "title",
        "contact_name",
        "contact_company",
        "contact_email",
        "last_activity_at",
        "project_lead_id",
        "project_company",
        "project_title",
        "inbox_url",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    email = _text(os.getenv("FREELANCERMAP_EMAIL"))
    password = _text(os.getenv("FREELANCERMAP_PASSWORD"))
    if not email or not password:
        print("[fm-inbox] missing FREELANCERMAP_EMAIL/FREELANCERMAP_PASSWORD")
        return 2

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _text(args.db):
        db_path = resolve_path(ROOT, _text(args.db))
    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    headless = bool_env("PLAYWRIGHT_HEADLESS", True)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(
        os.getenv("PLAYWRIGHT_USER_DATA_DIR_FREELANCERMAP")
        or (ROOT / "data" / "profiles" / "freelancermap")
    )

    launch_args: List[str] = ["--lang=en-US"]
    if _text(args.window_position):
        launch_args.append(f"--window-position={_text(args.window_position)}")
    if _text(args.window_size):
        launch_args.append(f"--window-size={_text(args.window_size)}")
    else:
        launch_args.append("--start-maximized")

    closer = SafeCloser()
    conn = None
    try:
        conn = db_connect(db_path)
        init_db(conn)

        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=launch_args,
        )
        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        ok_login, login_reason = await _ensure_session(
            page,
            email=email,
            password=password,
            timeout_ms=args.step_timeout_ms,
        )
        if not ok_login:
            print(f"[fm-inbox] login failed: {login_reason}")
            await _dump_debug(ROOT, page, "login_failed")
            return 2

        await page.goto("https://www.freelancermap.com/app/pobox/main", wait_until="domcontentloaded", timeout=args.step_timeout_ms)
        await page.wait_for_timeout(900)
        if _is_verification_gate_url(page.url):
            print("[fm-inbox] blocked by verification/paywall")
            await _dump_debug(ROOT, page, "verification_gate")
            return 2

        scripts = page.locator("script.js-react-on-rails-component")
        raw_scripts: List[str] = []
        for i in range(await scripts.count()):
            try:
                raw_scripts.append(await scripts.nth(i).inner_text())
            except Exception:
                continue

        payload = _extract_payload_from_scripts(raw_scripts)
        if not payload:
            print("[fm-inbox] failed to extract inbox payload")
            await _dump_debug(ROOT, page, "payload_missing")
            return 1

        unread_messages = int(payload.get("unreadMessages") or 0)
        conversations = _extract_conversations(
            payload,
            include_system=bool(args.include_system),
            limit=max(0, int(args.limit)),
        )
        if not conversations:
            print("[fm-inbox] no conversations found in payload")

        rows: List[Dict[str, Any]] = []
        captured_at = _now_iso()
        total = len(conversations)
        replied = 0
        pending = 0
        system = 0
        mapped = 0
        seen_updated = 0
        reply_events_conv = 0
        reply_events_project = 0

        for conv in conversations:
            status = "system" if conv.is_system() else ("replied" if conv.looks_like_reply() else "pending")
            if status == "system":
                system += 1
            elif status == "replied":
                replied += 1
            else:
                pending += 1

            conv_lead = LeadUpsert(
                platform="freelancermap.com",
                lead_type="conversation",
                contact=f"conversation:{conv.conversation_id}",
                url="https://www.freelancermap.com/app/pobox/main",
                company=conv.contact_company or "freelancermap.com",
                job_title=conv.title or f"Conversation {conv.conversation_id}",
                location="",
                source="freelancermap_inbox_sync",
                created_at=captured_at,
                raw={
                    "conversation_id": conv.conversation_id,
                    "conversation_type": conv.conversation_type,
                    "io_type": conv.io_type,
                    "my_side": conv.my_side,
                    "participant_side": conv.participant_side,
                    "has_read": conv.has_read,
                    "replied": conv.replied,
                    "has_ever_replied": conv.has_ever_replied,
                    "state": conv.state,
                    "created_at": conv.created_at,
                    "last_activity_at": conv.last_activity_at,
                    "contact_name": conv.contact_name,
                    "contact_company": conv.contact_company,
                    "contact_email": conv.contact_email,
                    "contact_user_id": conv.contact_user_id,
                    "raw_excerpt": conv.raw_excerpt,
                },
            )
            conv_lead_id, _ = upsert_lead_with_flag(conn, conv_lead)

            seen_signature = conv.seen_signature()
            last_seen = _latest_event_details(conn, conv_lead_id, "fm_inbox_seen")
            if _text(last_seen.get("seen_signature")) != seen_signature:
                seen_updated += 1
                if not args.dry_run:
                    add_event(
                        conn,
                        lead_id=conv_lead_id,
                        event_type="fm_inbox_seen",
                        status="ok",
                        occurred_at=captured_at,
                        details={
                            "conversation_id": conv.conversation_id,
                            "seen_signature": seen_signature,
                            "status": status,
                            "title": conv.title,
                            "contact_company": conv.contact_company,
                            "contact_name": conv.contact_name,
                            "has_read": conv.has_read,
                            "replied": conv.replied,
                            "has_ever_replied": conv.has_ever_replied,
                            "last_activity_at": conv.last_activity_at,
                        },
                    )

            matched = _match_project_lead(conn, title=conv.title, company=conv.contact_company)
            project_lead_id = ""
            project_company = ""
            project_title = ""
            if matched:
                mapped += 1
                project_lead_id = matched["lead_id"]
                project_company = matched["company"]
                project_title = matched["job_title"]

            if conv.looks_like_reply():
                sig = conv.reply_signature()
                if not _event_exists_with_reply_signature(conn, conv_lead_id, sig):
                    reply_events_conv += 1
                    if not args.dry_run:
                        add_event(
                            conn,
                            lead_id=conv_lead_id,
                            event_type="fm_reply_received",
                            status="ok",
                            occurred_at=captured_at,
                            details={
                                "conversation_id": conv.conversation_id,
                                "reply_signature": sig,
                                "status": status,
                                "title": conv.title,
                                "contact_company": conv.contact_company,
                                "contact_name": conv.contact_name,
                                "contact_email": conv.contact_email,
                                "last_activity_at": conv.last_activity_at,
                            },
                        )

                if project_lead_id:
                    project_sig = f"{sig}|project:{project_lead_id}"
                    if not _event_exists_with_reply_signature(conn, project_lead_id, project_sig):
                        reply_events_project += 1
                        if not args.dry_run:
                            add_event(
                                conn,
                                lead_id=project_lead_id,
                                event_type="fm_reply_received",
                                status="ok",
                                occurred_at=captured_at,
                                details={
                                    "conversation_id": conv.conversation_id,
                                    "reply_signature": project_sig,
                                    "status": status,
                                    "inbox_title": conv.title,
                                    "contact_company": conv.contact_company,
                                    "contact_name": conv.contact_name,
                                    "contact_email": conv.contact_email,
                                    "last_activity_at": conv.last_activity_at,
                                    "source": "freelancermap_inbox_sync",
                                },
                            )

            rows.append(
                {
                    "captured_at": captured_at,
                    "conversation_id": conv.conversation_id,
                    "status": status,
                    "conversation_type": conv.conversation_type,
                    "io_type": conv.io_type,
                    "my_side": conv.my_side,
                    "participant_side": conv.participant_side,
                    "has_read": int(conv.has_read),
                    "replied": int(conv.replied),
                    "has_ever_replied": int(conv.has_ever_replied),
                    "title": conv.title,
                    "contact_name": conv.contact_name,
                    "contact_company": conv.contact_company,
                    "contact_email": conv.contact_email,
                    "last_activity_at": conv.last_activity_at,
                    "project_lead_id": project_lead_id,
                    "project_company": project_company,
                    "project_title": project_title,
                    "inbox_url": "https://www.freelancermap.com/app/pobox/main",
                }
            )

        if not args.dry_run:
            conn.commit()

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        out_csv = out_dir / f"freelancermap_inbox_{stamp}.csv"
        _write_csv(out_csv, rows)

        print(
            "[fm-inbox] done: "
            f"total={total} replied={replied} pending={pending} system={system} "
            f"mapped={mapped} seen_updates={seen_updated} "
            f"new_reply_events(conv={reply_events_conv},project={reply_events_project}) "
            f"unread_badge={unread_messages}"
        )
        print(f"[fm-inbox] csv: {out_csv}")
        print(f"[fm-inbox] db: {db_path} dry_run={bool(args.dry_run)}")

        if args.telegram and bool_env("TELEGRAM_REPORT", True):
            lines = [
                "AIJobSearcher: Freelancermap inbox sync",
                f"Conversations: {total} (replied={replied}, pending={pending}, system={system})",
                f"Unread badge: {unread_messages}",
                f"New reply events: conv={reply_events_conv}, project={reply_events_project}",
                f"CSV: {out_csv}",
            ]
            send_telegram_message("\n".join(lines))

        return 0
    except KeyboardInterrupt:
        print("[fm-inbox] interrupted")
        return 130
    except Exception as e:
        print(f"[fm-inbox] error: {e}")
        return 1
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Sync freelancermap inbox and record reply status in activity.sqlite."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--limit", type=int, default=120, help="Max conversations to process")
    ap.add_argument("--include-system", action="store_true", help="Include system messages (welcome/spam-warning)")
    ap.add_argument("--dry-run", action="store_true", help="Do not write events to DB")
    ap.add_argument("--telegram", action="store_true", help="Send short summary to Telegram")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000)
    ap.add_argument("--window-position", default="", help="Browser position, e.g. -1920,0")
    ap.add_argument("--window-size", default="", help="Browser size, e.g. 1920,1080")
    args = ap.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
