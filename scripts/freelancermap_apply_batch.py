import argparse
import asyncio
import json
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import Page, async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import add_event, connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file, render_template  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, int_env  # noqa: E402
from src.notify import notify  # noqa: E402
from src.profile_store import load_profile, normalize_person_name  # noqa: E402


DEFAULT_TEMPLATE = """Hi {company} team,

I am applying for "{job_title}".
I am a QA Engineer with 5+ years of experience in manual and automation testing, strong API testing (REST/GraphQL), and C#/.NET automation (NUnit/RestSharp), with CI-based regression pipelines.

Key match:
- QA strategy, regression and release verification
- API/auth/security-focused testing
- Automation and practical CI workflows

My CV is attached in my freelancermap profile.
I am available to start immediately and can work remotely.

Best regards,
{candidate_name}
{candidate_email}
{candidate_phone}
LinkedIn: {candidate_linkedin}
"""


CONTACT_EVENTS = (
    "email_sent",
    "li_connect_sent",
    "li_dm_sent",
    "li_comment_posted",
    "li_apply_submitted",
    "external_apply_submitted",
    "fm_apply_submitted",
)

LANGUAGE_HARD_RE = re.compile(
    r"\b(dutch|french|german|italian|spanish|portuguese|polish)\b.{0,32}\b(required|must|mandatory|at least)\b",
    re.IGNORECASE | re.DOTALL,
)
QA_TITLE_RE = re.compile(
    r"\b(qa|quality\s+assurance|tester|testing|test\s*automation|sdet|test\s*engineer|quality\s*engineer)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_json(raw_json: Optional[str]) -> Dict[str, Any]:
    if not raw_json:
        return {}
    try:
        v = json.loads(raw_json)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _parse_set_csv(value: str) -> Set[str]:
    return {x.strip().lower() for x in (value or "").split(",") if x.strip()}


def _extract_emails(raw: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    v = raw.get("emails")
    if isinstance(v, list):
        for it in v:
            s = str(it or "").strip().lower()
            if s and "@" in s:
                out.append(s)
    elif isinstance(v, str):
        for it in re.split(r"[,\s;]+", v):
            s = str(it or "").strip().lower()
            if s and "@" in s:
                out.append(s)
    seen: Set[str] = set()
    uniq: List[str] = []
    for e in out:
        if e in seen:
            continue
        seen.add(e)
        uniq.append(e)
    return uniq


def _is_verification_gate_url(url: str) -> bool:
    u = (url or "").lower()
    return ("/user-verification" in u) or ("/upgrade" in u and "freelancermap.com" in u)


def _fetch_jobs_to_apply(
    conn,
    *,
    limit: int,
    include_attempted: bool,
    min_score: int,
    remote_modes: Set[str],
    engagements: Set[str],
) -> List[Dict[str, Any]]:
    where = [
        "l.platform = 'freelancermap.com'",
        "l.lead_type = 'project'",
        "EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type IN ('freelance_project_collected', 'collected'))",
        "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'fm_apply_submitted')",
    ]
    if not include_attempted:
        where.extend(
            [
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'fm_apply_needs_manual')",
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'fm_apply_failed')",
            ]
        )

    sample = max(int(limit) * 8, int(limit))
    sql = (
        "SELECT l.lead_id, l.contact, l.url, l.company, l.job_title, l.location, l.raw_json, l.created_at "
        "FROM leads l "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY l.created_at DESC "
        "LIMIT ?"
    )
    rows = conn.execute(sql, (sample,)).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = _parse_json(r["raw_json"])
        score = 0
        try:
            score = int(raw.get("score") or 0)
        except Exception:
            score = 0
        if score < min_score:
            continue

        mode = str(raw.get("remote_mode") or "unknown").strip().lower()
        engagement = str(raw.get("engagement") or "unknown").strip().lower()
        if remote_modes and mode not in remote_modes:
            continue
        if engagements and engagement not in engagements:
            continue

        out.append(
            {
                "lead_id": str(r["lead_id"]),
                "project_id": str(r["contact"] or ""),
                "url": str(r["url"] or ""),
                "company": str(r["company"] or ""),
                "job_title": str(r["job_title"] or ""),
                "location": str(r["location"] or ""),
                "score": score,
                "remote_mode": mode,
                "engagement": engagement,
                "created_at": str(r["created_at"] or ""),
                "raw": raw,
                "emails": _extract_emails(raw),
            }
        )
        if len(out) >= limit:
            break
    return out


def _cross_method_dedupe_reason(conn, job: Dict[str, Any]) -> str:
    lead_id = str(job.get("lead_id") or "")
    row = conn.execute(
        f"""
        SELECT e.event_type AS event_type
        FROM events e
        WHERE e.lead_id = ? AND e.event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
        LIMIT 1
        """,
        (lead_id, *CONTACT_EVENTS),
    ).fetchone()
    if row:
        return f"lead_already_contacted:{row['event_type']}"

    emails = [str(e).strip().lower() for e in (job.get("emails") or []) if str(e).strip()]
    if emails:
        ph = ",".join(["?"] * len(emails))
        row = conn.execute(
            f"""
            SELECT l.contact AS contact, e.event_type AS event_type
            FROM events e
            JOIN leads l ON l.lead_id = e.lead_id
            WHERE lower(l.contact) IN ({ph})
              AND e.event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
            LIMIT 1
            """,
            (*emails, *CONTACT_EVENTS),
        ).fetchone()
        if row:
            return f"email_already_contacted:{row['contact']}:{row['event_type']}"

    company = str(job.get("company") or "").strip().lower()
    if company:
        row = conn.execute(
            f"""
            SELECT l.contact AS contact, e.event_type AS event_type
            FROM events e
            JOIN leads l ON l.lead_id = e.lead_id
            WHERE lower(l.company) = ?
              AND e.event_type IN ({",".join(["?"] * len(CONTACT_EVENTS))})
            LIMIT 1
            """,
            (company, *CONTACT_EVENTS),
        ).fetchone()
        if row:
            return f"company_already_contacted:{company}:{row['event_type']}"

    return ""


def _load_template(root: Path, path_text: str) -> str:
    p = resolve_path(root, path_text)
    if p.exists():
        txt = p.read_text(encoding="utf-8", errors="replace").strip()
        if txt:
            return txt
    return DEFAULT_TEMPLATE.strip()


def _build_message(
    *,
    job: Dict[str, Any],
    cfg: Dict[str, Any],
    profile: Dict[str, str],
    template_text: str,
) -> str:
    candidate_name = normalize_person_name(str(cfg_get(cfg, "candidate.name", profile.get("candidate.name", ""))))
    vars_map = {
        "job_title": str(job.get("job_title") or ""),
        "company": str(job.get("company") or ""),
        "location": str(job.get("location") or ""),
        "project_url": str(job.get("url") or ""),
        "candidate_name": candidate_name,
        "candidate_email": str(cfg_get(cfg, "candidate.email", profile.get("candidate.email", ""))),
        "candidate_phone": str(cfg_get(cfg, "candidate.phone", profile.get("candidate.phone", ""))),
        "candidate_linkedin": str(cfg_get(cfg, "candidate.linkedin", profile.get("candidate.linkedin", ""))),
        "base_location": str(cfg_get(cfg, "candidate.base_location", profile.get("candidate.base_location", ""))),
        "timezone": str(cfg_get(cfg, "candidate.timezone", profile.get("candidate.timezone", ""))),
    }
    msg = render_template(template_text, vars_map).strip()
    return re.sub(r"\n{3,}", "\n\n", msg)


async def _dump_debug(root: Path, page: Page, tag: str) -> None:
    debug_dir = root / "data" / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html = debug_dir / f"freelancermap_{tag}_{stamp}.html"
    png = debug_dir / f"freelancermap_{tag}_{stamp}.png"
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


async def _open_apply_form(page: Page) -> Tuple[bool, str]:
    if _is_verification_gate_url(page.url):
        return False, "paywall_verification_required"
    for _ in range(2):
        try:
            btn = page.locator("[data-testid='contact-button']").first
            if await btn.count() > 0:
                await btn.click(timeout=2500)
                await page.wait_for_timeout(500)
        except Exception:
            pass

        if _is_verification_gate_url(page.url):
            return False, "paywall_verification_required"

        cover = page.locator("textarea#cover-letter, textarea.form-control:not(.g-recaptcha-response)").first
        try:
            if await cover.count() > 0 and await cover.is_visible():
                return True, "ok"
        except Exception:
            pass

    body_text = ""
    try:
        body_text = (await page.locator("body").inner_text()).lower()
    except Exception:
        pass

    if "account verification" in body_text and "payment method" in body_text:
        return False, "paywall_verification_required"
    if "activate your profile" in body_text or "activate profile" in body_text:
        return False, "profile_inactive"
    if "log in" in body_text and "email address or username" in body_text:
        return False, "not_logged_in"
    return False, "application_form_not_found"


async def _set_checkbox_by_selector(page: Page, selector: str, value: bool) -> bool:
    loc = page.locator(selector).first
    if await loc.count() == 0:
        return False
    try:
        checked = await loc.is_checked()
    except Exception:
        return False
    if checked != value:
        await loc.click()
        await page.wait_for_timeout(120)
    return True


async def _apply_once(
    page: Page,
    *,
    job: Dict[str, Any],
    message: str,
    submit: bool,
    send_email_flag: bool,
    send_phone_flag: bool,
    attach_cv: bool,
    timeout_ms: int,
) -> Tuple[str, str]:
    url = str(job.get("url") or "")
    if not url:
        return "failed", "missing_url"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        return "failed", "open_project_timeout"
    if _is_verification_gate_url(page.url):
        return "needs_manual", "paywall_verification_required"

    ok_form, reason = await _open_apply_form(page)
    if not ok_form:
        return "needs_manual", reason

    cover = page.locator("textarea#cover-letter, textarea.form-control:not(.g-recaptcha-response)").first
    if await cover.count() == 0:
        return "needs_manual", "cover_letter_not_found"
    await cover.fill(message)

    if attach_cv:
        await _set_checkbox_by_selector(page, "input[type='checkbox'][id*='singleCheckbox']", True)
    await _set_checkbox_by_selector(page, "input[type='checkbox'][name='sendEmail']", bool(send_email_flag))
    await _set_checkbox_by_selector(page, "input[type='checkbox'][name='sendPhone']", bool(send_phone_flag))

    if not submit:
        return "dry_run", "ready_no_submit"

    send_btn = page.get_by_role("button", name=re.compile(r"send application", re.IGNORECASE)).first
    if await send_btn.count() == 0:
        send_btn = page.locator("button:has-text('Send application')").first
    if await send_btn.count() == 0:
        return "needs_manual", "send_button_not_found"

    await send_btn.click(timeout=4000)
    await page.wait_for_timeout(2500)
    if _is_verification_gate_url(page.url):
        return "needs_manual", "paywall_verification_required"

    body = ""
    try:
        body = (await page.locator("body").inner_text()).lower()
    except Exception:
        body = ""
    if "account verification" in body and "payment method" in body:
        return "needs_manual", "paywall_verification_required"

    if re.search(r"(application has been sent|application sent|successfully sent|thank you for your application)", body):
        return "submitted", "ok"
    if "captcha" in body or "recaptcha" in body:
        return "needs_manual", "captcha"
    if "applications remaining this month" in body:
        return "needs_manual", "submit_uncertain_check_inbox"
    return "needs_manual", "submit_result_unclear"


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    email = (os.getenv("FREELANCERMAP_EMAIL") or "").strip()
    password = (os.getenv("FREELANCERMAP_PASSWORD") or "").strip()
    if not email or not password:
        print("[fm-apply] missing FREELANCERMAP_EMAIL/FREELANCERMAP_PASSWORD in .env/.env.accounts")
        return 2

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    remote_modes = _parse_set_csv(args.remote_modes)
    engagements = _parse_set_csv(args.engagements)
    strict_language = bool(args.strict_language)
    strict_qa_title = not bool(args.no_strict_qa_title)

    template_text = _load_template(ROOT, args.template)

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(
        os.getenv("PLAYWRIGHT_USER_DATA_DIR_FREELANCERMAP")
        or (ROOT / "data" / "profiles" / "freelancermap")
    )

    launch_args: List[str] = ["--lang=en-US"]
    if args.window_position.strip():
        launch_args.append(f"--window-position={args.window_position.strip()}")
    if args.window_size.strip():
        launch_args.append(f"--window-size={args.window_size.strip()}")
    else:
        launch_args.append("--start-maximized")

    closer = SafeCloser()
    db_conn = None
    try:
        db_conn = db_connect(db_path)
        init_db(db_conn)
        try:
            profile = load_profile(db_conn)
        except Exception:
            profile = {}

        jobs = _fetch_jobs_to_apply(
            db_conn,
            limit=args.limit,
            include_attempted=args.include_attempted,
            min_score=args.min_score,
            remote_modes=remote_modes,
            engagements=engagements,
        )
        if not jobs:
            print("[fm-apply] nothing to do (no eligible projects in DB).")
            return 0
        print(f"[fm-apply] projects queued: {len(jobs)}")

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
            print(f"[fm-apply] login failed: {login_reason}")
            await _dump_debug(ROOT, page, "login_failed")
            notify(ROOT, cfg, kind="attention")
            return 2

        submitted = 0
        dry_run_count = 0
        manual = 0
        failed = 0
        skipped = 0

        for idx, job in enumerate(jobs, start=1):
            lead_id = str(job["lead_id"])
            title = str(job.get("job_title") or "")
            company = str(job.get("company") or "")
            url = str(job.get("url") or "")
            score = int(job.get("score") or 0)
            print(f"[fm-apply] {idx}/{len(jobs)} score={score} {company} | {title} | {url}")

            dedupe_reason = _cross_method_dedupe_reason(db_conn, job)
            if dedupe_reason:
                skipped += 1
                print(f"[fm-apply] skipped: {dedupe_reason}")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="fm_apply_skipped",
                    status="skip",
                    occurred_at=_now_iso(),
                    details={"reason": dedupe_reason},
                )
                db_conn.commit()
                continue

            if strict_qa_title and not QA_TITLE_RE.search(title):
                skipped += 1
                print("[fm-apply] skipped: strict_qa_title_filter")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="fm_apply_skipped",
                    status="skip",
                    occurred_at=_now_iso(),
                    details={"reason": "strict_qa_title_filter"},
                )
                db_conn.commit()
                continue

            text_blob = str((job.get("raw") or {}).get("text") or "")
            if strict_language and LANGUAGE_HARD_RE.search(text_blob):
                skipped += 1
                print("[fm-apply] skipped: strict_language_filter")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="fm_apply_skipped",
                    status="skip",
                    occurred_at=_now_iso(),
                    details={"reason": "strict_language_filter"},
                )
                db_conn.commit()
                continue

            add_event(
                db_conn,
                lead_id=lead_id,
                event_type="fm_apply_started",
                status="ok",
                occurred_at=_now_iso(),
                details={"url": url, "score": score},
            )
            db_conn.commit()

            msg = _build_message(job=job, cfg=cfg, profile=profile, template_text=template_text)
            try:
                result, details = await _apply_once(
                    page,
                    job=job,
                    message=msg,
                    submit=not args.no_submit,
                    send_email_flag=not args.no_send_email,
                    send_phone_flag=not args.no_send_phone,
                    attach_cv=not args.no_attach_cv,
                    timeout_ms=args.step_timeout_ms,
                )
            except PlaywrightTimeoutError:
                result, details = ("failed", "playwright_timeout")
            except Exception as e:
                result, details = ("failed", f"exception:{type(e).__name__}")

            if result == "submitted":
                submitted += 1
                ev = "fm_apply_submitted"
                st = "ok"
            elif result == "dry_run":
                dry_run_count += 1
                ev = "fm_apply_dry_run"
                st = "ok"
            elif result == "needs_manual":
                manual += 1
                ev = "fm_apply_needs_manual"
                st = "manual"
                notify(ROOT, cfg, kind="attention")
                await _dump_debug(ROOT, page, "needs_manual")
            else:
                failed += 1
                ev = "fm_apply_failed"
                st = "failed"
                notify(ROOT, cfg, kind="error")
                await _dump_debug(ROOT, page, "failed")

            add_event(
                db_conn,
                lead_id=lead_id,
                event_type=ev,
                status=st,
                occurred_at=_now_iso(),
                details={"details": details, "url": url},
            )
            db_conn.commit()

            print(f"[fm-apply] result={result} details={details}")

            if result == "needs_manual" and args.pause_on_manual:
                print("[fm-apply] pause_on_manual: resolve in browser, then press Enter...")
                try:
                    input()
                except KeyboardInterrupt:
                    return 130

            if idx < len(jobs):
                await page.wait_for_timeout(random.randint(args.min_delay_ms, args.max_delay_ms))
                if args.long_break_every > 0 and idx % args.long_break_every == 0:
                    await page.wait_for_timeout(random.randint(args.long_break_min_ms, args.long_break_max_ms))

        print(
            "[fm-apply] done: "
            f"submitted={submitted} dry_run={dry_run_count} manual={manual} failed={failed} skipped={skipped}"
        )
        notify(ROOT, cfg, kind="done")
        return 0
    except KeyboardInterrupt:
        print("[fm-apply] interrupted")
        return 130
    except Exception as e:
        print(f"[fm-apply] error: {e}")
        try:
            notify(ROOT, cfg, kind="error")
        except Exception:
            pass
        return 1
    finally:
        try:
            if db_conn is not None:
                db_conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Apply to freelancermap projects from activity.sqlite with strict dedupe and human pacing."
    )
    ap.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--limit", type=int, default=10, help="Max projects per run")
    ap.add_argument("--min-score", type=int, default=10, help="Minimum fit score from raw_json")
    ap.add_argument("--remote-modes", default="remote,hybrid", help="Comma-separated: remote,hybrid,on_site,unknown")
    ap.add_argument("--engagements", default="long,gig", help="Comma-separated: long,gig,unknown")
    ap.add_argument("--include-attempted", action="store_true", help="Retry previously manual/failed projects")
    ap.add_argument("--strict-language", action="store_true", help="Skip projects with hard non-English language requirements")
    ap.add_argument("--no-strict-qa-title", action="store_true", help="Allow non-QA titles (not recommended)")
    ap.add_argument("--template", default="templates/freelancermap_apply_en.txt", help="Message template path")
    ap.add_argument("--no-submit", action="store_true", help="Fill everything but do not click 'Send application'")
    ap.add_argument("--no-attach-cv", action="store_true", help="Do not auto-check CV attachment")
    ap.add_argument("--no-send-email", action="store_true", help="Uncheck 'Send email address'")
    ap.add_argument("--no-send-phone", action="store_true", help="Uncheck 'Send phone number'")
    ap.add_argument("--pause-on-manual", action="store_true", help="Pause terminal when manual step is needed")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Navigation/step timeout")
    ap.add_argument("--window-position", default="", help="Browser window position, e.g. -1920,0 for left monitor")
    ap.add_argument("--window-size", default="", help="Browser window size, e.g. 1920,1080")
    ap.add_argument("--min-delay-ms", type=int, default=20_000, help="Min delay between applications")
    ap.add_argument("--max-delay-ms", type=int, default=65_000, help="Max delay between applications")
    ap.add_argument("--long-break-every", type=int, default=4, help="Long break after every N jobs (0 = off)")
    ap.add_argument("--long-break-min-ms", type=int, default=90_000, help="Long break min ms")
    ap.add_argument("--long-break-max-ms", type=int, default=180_000, help="Long break max ms")
    args = ap.parse_args()
    return asyncio.run(run(args))


if __name__ == "__main__":
    raise SystemExit(main())
