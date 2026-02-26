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


DEFAULT_TEMPLATE = """Hi,

I am interested in your project "{job_title}".
I am a QA Engineer with 5+ years of experience in manual and automation testing, API testing (REST/GraphQL), and C#/.NET automation (NUnit/RestSharp), including CI-based regression pipelines.

Why I fit:
- Strong QA strategy: exploratory, regression, release verification
- API and security-focused testing (auth/session/data isolation risks)
- Practical automation and reliable bug reproduction reports

I can start quickly and work in English with clear daily communication.

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
    "wa_apply_submitted",
)

QA_TITLE_RE = re.compile(
    r"\b(qa|quality\s+assurance|tester|testing|test\s*automation|sdet|test\s*engineer|quality\s*engineer)\b",
    re.IGNORECASE,
)

SUCCESS_RE = re.compile(
    r"(proposal sent|bid sent|successfully submitted|your proposal has been|application sent|thank you)",
    re.IGNORECASE,
)
PROFILE_REVIEW_RE = re.compile(r"(profile is in review|reviewing your profile)", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _text(v: Any) -> str:
    return str(v or "").strip()


def _parse_json(raw_json: Optional[str]) -> Dict[str, Any]:
    if not raw_json:
        return {}
    try:
        obj = json.loads(raw_json)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _parse_set_csv(value: str) -> Set[str]:
    return {x.strip().lower() for x in (value or "").split(",") if x.strip()}


def _extract_emails(raw: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    v = raw.get("emails")
    if isinstance(v, list):
        for it in v:
            s = _text(it).lower()
            if s and "@" in s:
                out.append(s)
    elif isinstance(v, str):
        for it in re.split(r"[,\s;]+", v):
            s = _text(it).lower()
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


def _slug_from_url(url: str) -> str:
    m = re.search(r"/job/([^/?#]+)", _text(url))
    return _text(m.group(1) if m else "")


def _load_target_slugs(path_text: str) -> Set[str]:
    ptxt = _text(path_text)
    if not ptxt:
        return set()
    p = resolve_path(ROOT, ptxt)
    if not p.exists():
        print(f"[workana-apply] targets file not found: {p}")
        return set()
    out: Set[str] = set()
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return set()
    for ln in lines:
        s = _text(ln)
        if not s:
            continue
        slug = _slug_from_url(s)
        if not slug:
            # allow plain slug line
            slug = re.sub(r"[^a-zA-Z0-9_-]+", "", s)
        slug = _text(slug).lower()
        if slug:
            out.add(slug)
    print(f"[workana-apply] targets loaded: {len(out)} from {p}")
    return out


def _fetch_jobs_to_apply(
    conn,
    *,
    limit: int,
    include_attempted: bool,
    min_score: int,
    remote_modes: Set[str],
    engagements: Set[str],
    target_slugs: Set[str],
) -> List[Dict[str, Any]]:
    where = [
        "l.platform = 'workana.com'",
        "l.lead_type = 'project'",
        "EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type IN ('workana_project_collected', 'collected'))",
        "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'wa_apply_submitted')",
    ]
    if not include_attempted:
        where.extend(
            [
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'wa_apply_needs_manual')",
                "NOT EXISTS(SELECT 1 FROM events e WHERE e.lead_id = l.lead_id AND e.event_type = 'wa_apply_failed')",
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

        mode = _text(raw.get("remote_mode")).lower() or "unknown"
        engagement = _text(raw.get("engagement")).lower() or "unknown"
        if remote_modes and mode not in remote_modes:
            continue
        if engagements and engagement not in engagements:
            continue

        slug = _text(r["contact"])
        url = _text(r["url"])
        if not slug:
            slug = _slug_from_url(url)
        if not slug:
            continue
        if target_slugs and (_text(slug).lower() not in target_slugs):
            continue

        out.append(
            {
                "lead_id": _text(r["lead_id"]),
                "slug": slug,
                "url": url,
                "company": _text(r["company"]),
                "job_title": _text(r["job_title"]),
                "location": _text(r["location"]),
                "score": score,
                "remote_mode": mode,
                "engagement": engagement,
                "created_at": _text(r["created_at"]),
                "raw": raw,
                "emails": _extract_emails(raw),
            }
        )
        if len(out) >= limit:
            break
    return out


def _cross_method_dedupe_reason(conn, job: Dict[str, Any]) -> str:
    lead_id = _text(job.get("lead_id"))
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

    company = _text(job.get("company")).lower()
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
    candidate_name = normalize_person_name(_text(cfg_get(cfg, "candidate.name", profile.get("candidate.name", ""))))
    vars_map = {
        "job_title": _text(job.get("job_title")),
        "company": _text(job.get("company")),
        "location": _text(job.get("location")),
        "project_url": _text(job.get("url")),
        "candidate_name": candidate_name,
        "candidate_email": _text(cfg_get(cfg, "candidate.email", profile.get("candidate.email", ""))),
        "candidate_phone": _text(cfg_get(cfg, "candidate.phone", profile.get("candidate.phone", ""))),
        "candidate_linkedin": _text(cfg_get(cfg, "candidate.linkedin", profile.get("candidate.linkedin", ""))),
        "base_location": _text(cfg_get(cfg, "candidate.base_location", profile.get("candidate.base_location", ""))),
        "timezone": _text(cfg_get(cfg, "candidate.timezone", profile.get("candidate.timezone", ""))),
    }
    msg = render_template(template_text, vars_map).strip()
    return re.sub(r"\n{3,}", "\n\n", msg)


async def _dump_debug(root: Path, page: Page, tag: str) -> None:
    debug_dir = root / "data" / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    html = debug_dir / f"workana_{tag}_{stamp}.html"
    png = debug_dir / f"workana_{tag}_{stamp}.png"
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
        if "login" in (page.url or "").lower():
            return False
    except Exception:
        pass
    try:
        if await page.locator("button:has-text('Find work')").count() > 0:
            return True
    except Exception:
        pass
    try:
        if await page.locator("a[href*='/users/messages'], a[href*='/my_projects?type_company=worker'], a[href*='/logout']").count() > 0:
            return True
    except Exception:
        pass
    try:
        has_pwd = await page.locator("input[type='password']").count() > 0
        has_email = (
            await page.locator("input[name='email'], input[type='email'], input[autocomplete='email']").count() > 0
        )
        if has_pwd and has_email:
            return False
    except Exception:
        pass
    return False


async def _ensure_session(page: Page, *, email: str, password: str, timeout_ms: int) -> Tuple[bool, str]:
    try:
        await page.goto("https://www.workana.com/dashboard", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    if await _is_logged_in(page):
        return True, "ok"

    if not email or not password:
        return False, "missing_credentials_and_not_logged_in"

    try:
        await page.goto("https://www.workana.com/login", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        return False, "login_page_timeout"

    email_input = page.locator("input[name='email'], input[type='email'], input[autocomplete='email']").first
    password_input = page.locator("input[name='password'], input[type='password']").first
    submit = page.locator("button[type='submit'], button:has-text('Login'), button:has-text('Log in')").first

    if await email_input.count() == 0 or await password_input.count() == 0:
        return False, "login_form_not_found"

    await email_input.fill(email)
    await password_input.fill(password)

    clicked = False
    try:
        if await submit.count() > 0:
            await submit.click(timeout=5000)
            clicked = True
    except Exception:
        clicked = False
    if not clicked:
        try:
            await password_input.press("Enter")
        except Exception:
            pass
    await page.wait_for_timeout(2200)

    try:
        await page.goto("https://www.workana.com/dashboard", wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        pass
    if await _is_logged_in(page):
        return True, "ok"
    return False, "login_failed_or_checkpoint"


async def _check_bid_permission(page: Page, slug: str) -> Dict[str, Any]:
    safe_slug = _text(slug)
    if not safe_slug:
        return {"status": 0, "snippet": "missing_slug"}
    try:
        return await page.evaluate(
            """
            async (slug) => {
              const token = (window.Workana && window.Workana.ajaxCSRFToken) || "";
              const url = `/workers/permissions/make_bid_action?projectSlug=${encodeURIComponent(slug)}`;
              try {
                const r = await fetch(url, {
                  method: "GET",
                  credentials: "include",
                  headers: {
                    "X-Csrf-Token": token,
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/plain, */*"
                  }
                });
                const ct = r.headers.get("content-type") || "";
                const text = await r.text();
                return {status: r.status, contentType: ct, snippet: text.slice(0, 260)};
              } catch (e) {
                return {status: 0, contentType: "", snippet: String(e)};
              }
            }
            """,
            safe_slug,
        )
    except Exception as e:
        return {"status": 0, "snippet": f"permission_exception:{type(e).__name__}"}


async def _fill_first_visible_textarea(page: Page, value: str) -> bool:
    selectors = [
        "form textarea[name*='proposal' i]",
        "form textarea[name*='cover' i]",
        "form textarea[name*='message' i]",
        "form textarea",
        "textarea",
    ]
    for sel in selectors:
        loc = page.locator(sel).first
        try:
            if await loc.count() > 0 and await loc.is_visible():
                await loc.fill(value)
                return True
        except Exception:
            continue
    return False


async def _apply_once(page: Page, *, job: Dict[str, Any], message: str, submit: bool, timeout_ms: int) -> Tuple[str, str]:
    url = _text(job.get("url"))
    if not url:
        return "failed", "missing_url"

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        return "failed", "open_project_timeout"

    place_btn = page.locator("button:has-text('Place a bid'), a:has-text('Place a bid')").first
    if await place_btn.count() == 0:
        return "needs_manual", "place_bid_button_not_found"
    try:
        await place_btn.click(timeout=4000)
    except Exception:
        return "needs_manual", "place_bid_click_failed"
    await page.wait_for_timeout(900)

    body = ""
    try:
        body = (await page.locator("body").inner_text()).lower()
    except Exception:
        body = ""

    if PROFILE_REVIEW_RE.search(body):
        return "needs_manual", "profile_in_review"
    if "priority moderation" in body:
        return "needs_manual", "priority_moderation_paywall"
    if "verify" in body and "profile" in body:
        return "needs_manual", "verification_required"
    if "captcha" in body or "recaptcha" in body:
        return "needs_manual", "captcha"

    filled = await _fill_first_visible_textarea(page, message)
    if not filled:
        return "needs_manual", "proposal_field_not_found"

    if not submit:
        return "dry_run", "ready_no_submit"

    send_btn = page.locator(
        "button:has-text('Send proposal'), button:has-text('Place a bid'), "
        "button:has-text('Submit bid'), button[type='submit']"
    ).first
    if await send_btn.count() == 0:
        return "needs_manual", "submit_button_not_found"
    try:
        await send_btn.click(timeout=5000)
    except Exception:
        return "needs_manual", "submit_click_failed"

    await page.wait_for_timeout(2500)
    try:
        body = (await page.locator("body").inner_text()).lower()
    except Exception:
        body = ""

    if SUCCESS_RE.search(body):
        return "submitted", "ok"
    if PROFILE_REVIEW_RE.search(body):
        return "needs_manual", "profile_in_review"
    if "credits" in body or "connects" in body:
        return "needs_manual", "insufficient_credits_or_limits"
    if "captcha" in body:
        return "needs_manual", "captcha"
    return "needs_manual", "submit_result_unclear"


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    email = _text(os.getenv("WORKANA_EMAIL"))
    password = _text(os.getenv("WORKANA_PASSWORD"))

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _text(args.db):
        db_path = resolve_path(ROOT, _text(args.db))

    remote_modes = _parse_set_csv(args.remote_modes)
    engagements = _parse_set_csv(args.engagements)
    strict_qa_title = not bool(args.no_strict_qa_title)
    template_text = _load_template(ROOT, args.template)
    target_slugs = _load_target_slugs(args.targets_file)

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(
        os.getenv("PLAYWRIGHT_USER_DATA_DIR_WORKANA")
        or (ROOT / "data" / "profiles" / "workana")
    )

    launch_args: List[str] = ["--lang=en-US"]
    if _text(args.window_position):
        launch_args.append(f"--window-position={_text(args.window_position)}")
    if _text(args.window_size):
        launch_args.append(f"--window-size={_text(args.window_size)}")
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
            target_slugs=target_slugs,
        )
        if not jobs:
            print("[workana-apply] nothing to do (no eligible projects in DB).")
            return 0
        print(f"[workana-apply] projects queued: {len(jobs)}")

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
            print(f"[workana-apply] login failed: {login_reason}")
            await _dump_debug(ROOT, page, "login_failed")
            notify(ROOT, cfg, kind="attention")
            return 2

        submitted = 0
        dry_run_count = 0
        manual = 0
        failed = 0
        skipped = 0

        for idx, job in enumerate(jobs, start=1):
            lead_id = _text(job.get("lead_id"))
            slug = _text(job.get("slug"))
            title = _text(job.get("job_title"))
            company = _text(job.get("company"))
            url = _text(job.get("url"))
            score = int(job.get("score") or 0)
            print(f"[workana-apply] {idx}/{len(jobs)} score={score} {company} | {title} | {url}")

            dedupe_reason = _cross_method_dedupe_reason(db_conn, job)
            if dedupe_reason:
                skipped += 1
                print(f"[workana-apply] skipped: {dedupe_reason}")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="wa_apply_skipped",
                    status="skip",
                    occurred_at=_now_iso(),
                    details={"reason": dedupe_reason},
                )
                db_conn.commit()
                continue

            if strict_qa_title and (not QA_TITLE_RE.search(title)):
                skipped += 1
                print("[workana-apply] skipped: strict_qa_title_filter")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="wa_apply_skipped",
                    status="skip",
                    occurred_at=_now_iso(),
                    details={"reason": "strict_qa_title_filter"},
                )
                db_conn.commit()
                continue

            add_event(
                db_conn,
                lead_id=lead_id,
                event_type="wa_apply_started",
                status="ok",
                occurred_at=_now_iso(),
                details={"url": url, "slug": slug, "score": score},
            )
            db_conn.commit()

            perm = await _check_bid_permission(page, slug)
            perm_status = int(perm.get("status") or 0)
            perm_snippet = _text(perm.get("snippet"))[:260]
            if perm_status != 200:
                manual += 1
                print(f"[workana-apply] manual: permission_status={perm_status}")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="wa_apply_needs_manual",
                    status="manual",
                    occurred_at=_now_iso(),
                    details={
                        "details": f"permission_status_{perm_status}",
                        "url": url,
                        "slug": slug,
                        "permission_snippet": perm_snippet,
                    },
                )
                db_conn.commit()
                notify(ROOT, cfg, kind="attention")
                await _dump_debug(ROOT, page, "permission_blocked")
                continue

            msg = _build_message(job=job, cfg=cfg, profile=profile, template_text=template_text)
            try:
                result, details = await _apply_once(
                    page,
                    job=job,
                    message=msg,
                    submit=not args.no_submit,
                    timeout_ms=args.step_timeout_ms,
                )
            except PlaywrightTimeoutError:
                result, details = ("failed", "playwright_timeout")
            except Exception as e:
                result, details = ("failed", f"exception:{type(e).__name__}")

            if result == "submitted":
                submitted += 1
                ev = "wa_apply_submitted"
                st = "ok"
            elif result == "dry_run":
                dry_run_count += 1
                ev = "wa_apply_dry_run"
                st = "ok"
            elif result == "needs_manual":
                manual += 1
                ev = "wa_apply_needs_manual"
                st = "manual"
                notify(ROOT, cfg, kind="attention")
                await _dump_debug(ROOT, page, "needs_manual")
            else:
                failed += 1
                ev = "wa_apply_failed"
                st = "failed"
                notify(ROOT, cfg, kind="error")
                await _dump_debug(ROOT, page, "failed")

            add_event(
                db_conn,
                lead_id=lead_id,
                event_type=ev,
                status=st,
                occurred_at=_now_iso(),
                details={"details": details, "url": url, "slug": slug},
            )
            db_conn.commit()

            print(f"[workana-apply] result={result} details={details}")

            if result == "needs_manual" and args.pause_on_manual:
                print("[workana-apply] pause_on_manual: resolve in browser, then press Enter...")
                try:
                    input()
                except KeyboardInterrupt:
                    return 130

            if idx < len(jobs):
                await page.wait_for_timeout(random.randint(args.min_delay_ms, args.max_delay_ms))
                if args.long_break_every > 0 and idx % args.long_break_every == 0:
                    await page.wait_for_timeout(random.randint(args.long_break_min_ms, args.long_break_max_ms))

        print(
            "[workana-apply] done: "
            f"submitted={submitted} dry_run={dry_run_count} manual={manual} failed={failed} skipped={skipped}"
        )
        notify(ROOT, cfg, kind="done")
        return 0
    except KeyboardInterrupt:
        print("[workana-apply] interrupted")
        return 130
    except Exception as e:
        print(f"[workana-apply] error: {e}")
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
        description="Apply to Workana projects from activity.sqlite with strict dedupe and human pacing."
    )
    ap.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--limit", type=int, default=10, help="Max projects per run")
    ap.add_argument("--min-score", type=int, default=9, help="Minimum fit score from raw_json")
    ap.add_argument("--remote-modes", default="remote,hybrid", help="Comma-separated: remote,hybrid,on_site,unknown")
    ap.add_argument("--engagements", default="long,gig", help="Comma-separated: long,gig,unknown")
    ap.add_argument("--include-attempted", action="store_true", help="Retry previously manual/failed projects")
    ap.add_argument("--targets-file", default="", help="Optional file with Workana project URLs/slugs (one per line)")
    ap.add_argument("--no-strict-qa-title", action="store_true", help="Allow non-QA titles (not recommended)")
    ap.add_argument("--template", default="templates/workana_apply_en.txt", help="Message template path")
    ap.add_argument("--no-submit", action="store_true", help="Fill everything but do not submit")
    ap.add_argument("--pause-on-manual", action="store_true", help="Pause terminal when manual step is needed")
    ap.add_argument("--step-timeout-ms", type=int, default=35_000, help="Navigation/step timeout")
    ap.add_argument("--window-position", default="", help="Browser window position, e.g. -1920,0")
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
