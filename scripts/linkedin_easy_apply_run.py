import argparse
import asyncio
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_easy_apply import (  # noqa: E402
    candidate_from_cfg,
    continue_easy_apply_current,
    extract_filled_answers,
    run_easy_apply_once,
)
from src.linkedin_playwright import (  # noqa: E402
    SafeCloser,
    bool_env,
    dump_debug,
    ensure_linkedin_session,
    goto_guarded,
    int_env,
    is_checkpoint_url,
)
from src.notify import notify  # noqa: E402
from src.profile_store import load_profile, upsert_answer  # noqa: E402


@dataclass
class Candidate:
    first_name: str
    last_name: str
    phone_country: str
    phone_number: str
    email: str


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _split_name(full_name: str) -> Tuple[str, str]:
    parts = [p for p in (full_name or "").strip().split() if p]
    if not parts:
        return ("", "")
    if len(parts) == 1:
        return (parts[0], "")
    return (parts[0], " ".join(parts[1:]))


def _candidate_from_cfg(cfg: Dict[str, object]) -> Candidate:
    name = str(cfg_get(cfg, "candidate.name", "")).strip()
    first, last = _split_name(name)
    phone = str(cfg_get(cfg, "candidate.phone", "")).strip()
    phone_country = "Vietnam (+84)"
    phone_number = re.sub(r"[^0-9]", "", phone)
    if phone_number.startswith("84") and len(phone_number) >= 10:
        phone_number = phone_number[2:]
    email = str(cfg_get(cfg, "candidate.email", "")).strip()
    return Candidate(
        first_name=first,
        last_name=last,
        phone_country=phone_country,
        phone_number=phone_number,
        email=email,
    )


async def _fill_if_present(scope, label: str, value: str) -> bool:
    if not value:
        return False
    try:
        loc = scope.get_by_label(label, exact=False).first
        if not await loc.is_visible(timeout=1500):
            return False
        await loc.fill(value)
        return True
    except Exception:
        return False


async def _select_phone_country_if_present(scope, value: str) -> bool:
    if not value:
        return False
    try:
        sel = scope.get_by_label("Phone country code", exact=False).first
        if not await sel.is_visible(timeout=1500):
            return False
        try:
            await sel.select_option(label=value)
            return True
        except Exception:
            pass
        try:
            await sel.click()
            opt = scope.get_by_role("option", name=re.compile(re.escape(value), re.IGNORECASE)).first
            if await opt.is_visible(timeout=1500):
                await opt.click()
                return True
        except Exception:
            pass
        return False
    except Exception:
        return False


async def _attach_resume_if_possible(scope, resume_path: Path) -> bool:
    if not resume_path.exists():
        return False

    try:
        inputs = scope.locator("input[type='file']")
        n = await inputs.count()
        if n <= 0:
            return False

        best_idx = 0
        best_score = -1
        for i in range(n):
            inp = inputs.nth(i)
            try:
                meta = await inp.evaluate(
                    """(el) => {
                      const id = el.id || '';
                      const name = el.getAttribute('name') || '';
                      const aria = el.getAttribute('aria-label') || '';
                      const lab = id ? document.querySelector('label[for=\"' + id + '\"]') : null;
                      const labelText = lab ? (lab.innerText || '').trim() : '';
                      const box = el.closest('section,div') || el.parentElement;
                      const boxText = box ? (box.innerText || '').replace(/\\s+/g,' ').trim().slice(0,200) : '';
                      return {id,name,aria,labelText,boxText};
                    }"""
                )
                hay = " ".join(
                    [
                        str(meta.get("id") or ""),
                        str(meta.get("name") or ""),
                        str(meta.get("aria") or ""),
                        str(meta.get("labelText") or ""),
                        str(meta.get("boxText") or ""),
                    ]
                ).lower()
                score = 0
                if "resume" in hay:
                    score += 3
                if "cv" in hay:
                    score += 2
                if "cover" in hay:
                    score -= 2
                if score > best_score:
                    best_score = score
                    best_idx = i
            except Exception:
                continue

        await inputs.nth(best_idx).set_input_files(str(resume_path))
        return True
    except Exception:
        return False


async def _find_primary_button(scope):
    names = [
        r"Submit application",
        r"Submit",
        r"Review",
        r"Next",
        r"Continue",
        r"Done",
    ]
    for pat in names:
        try:
            btn = scope.get_by_role("button", name=re.compile(pat, re.IGNORECASE)).first
            if await btn.is_visible(timeout=800):
                try:
                    if await btn.is_enabled():
                        return btn
                except Exception:
                    return btn
        except Exception:
            continue
    return None


async def _scope_text(scope) -> str:
    """
    Best-effort: return text for both Locator and Page scopes.
    - Locator: inner_text()
    - Page: evaluate(document.body.innerText)
    """
    try:
        return await scope.inner_text(timeout=1500)
    except TypeError:
        try:
            return await scope.evaluate("() => (document.body?.innerText || '')")
        except Exception:
            return ""
    except Exception:
        try:
            return await scope.evaluate("() => (document.body?.innerText || '')")
        except Exception:
            return ""


async def _detect_submitted(scope) -> bool:
    low = (await _scope_text(scope) or "").lower()
    return (
        "application submitted" in low
        or "your application was sent" in low
        or "application sent" in low
        or ("submitted" in low and "application" in low)
    )


async def _page_detect_submitted(page) -> bool:
    """Fallback detection when the dialog disappears after submit."""
    try:
        text = await page.evaluate("() => (document.body?.innerText || '')")
    except Exception:
        return False
    low = (text or "").lower()

    if re.search(
        r"\\bapplied\\s+\\d+\\s*(?:m|h|d|w|mo|minute|minutes|hour|hours|day|days|week|weeks|month|months)\\s+ago\\b",
        low,
        re.IGNORECASE,
    ):
        return True
    if "application submitted" in low:
        return True
    if "application status" in low and "submitted" in low:
        return True
    if "your application was sent" in low:
        return True
    return False


async def _wait_for_apply_ui(page, timeout_ms: int = 15_000) -> bool:
    """
    Easy Apply UI can be a modal without role=dialog (LinkedIn SDUI) or a full apply page.
    We treat the UI as "present" if any common form field or action button becomes visible.
    """
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000.0)
    while asyncio.get_running_loop().time() < deadline:
        if is_checkpoint_url(page.url):
            return False
        try:
            loc = page.get_by_label("First name", exact=False).first
            if await loc.is_visible(timeout=500):
                return True
        except Exception:
            pass
        try:
            loc = page.get_by_text("Contact info", exact=False).first
            if await loc.is_visible(timeout=500):
                return True
        except Exception:
            pass
        try:
            btn = page.get_by_role("button", name=re.compile(r"(Next|Review|Submit application|Submit)", re.IGNORECASE)).first
            if await btn.is_visible(timeout=500):
                return True
        except Exception:
            pass
        await page.wait_for_timeout(400)
    return False


async def _run_easy_apply_once(
    *,
    root: Path,
    page,
    job_url: str,
    candidate: Candidate,
    resume_path: Path,
    max_steps: int,
    submit: bool,
) -> Tuple[str, str]:
    """
    Returns (result, details):
      result: submitted | needs_manual | failed
    """
    ok = await goto_guarded(root=root, page=page, url=job_url, timeout_ms=30_000, tag_on_fail="apply_job_open_failed")
    if not ok:
        return ("failed", "job_open_failed_or_checkpoint")

    if await _page_detect_submitted(page):
        return ("submitted", "already_applied_detected_on_job_page")

    await page.wait_for_timeout(1500)
    easy_a = page.locator("a[href*='openSDUIApplyFlow=true'], a[href*='/apply/?openSDUIApplyFlow=true']").first
    easy_btn = page.get_by_role("button", name=re.compile(r"easy apply", re.IGNORECASE)).first
    clicked = False
    apply_page = page
    popup_task = None
    try:
        popup_task = asyncio.create_task(page.wait_for_event("popup"))
    except Exception:
        popup_task = None
    try:
        if await easy_a.is_visible(timeout=2000):
            await easy_a.click()
            clicked = True
    except Exception:
        pass
    if not clicked:
        try:
            if await easy_btn.is_visible(timeout=1500):
                await easy_btn.click()
                clicked = True
        except Exception:
            pass
    if not clicked:
        await dump_debug(root, page, "apply_no_easy_apply")
        return ("needs_manual", "no_easy_apply")

    if popup_task is not None:
        try:
            apply_page = await asyncio.wait_for(popup_task, timeout=3.0)
            apply_page.set_default_timeout(30_000)
            apply_page.set_default_navigation_timeout(30_000)
            try:
                await apply_page.wait_for_load_state("domcontentloaded", timeout=15_000)
            except Exception:
                pass
        except Exception:
            try:
                popup_task.cancel()
            except Exception:
                pass

    if not await _wait_for_apply_ui(apply_page, timeout_ms=18_000):
        await dump_debug(root, apply_page, "apply_ui_not_found")
        return ("needs_manual", "apply_ui_not_found")

    try:
        if await apply_page.locator("*[role='dialog']").count() > 0:
            scope = apply_page.locator("*[role='dialog']").first
        else:
            scope = apply_page
    except Exception:
        scope = apply_page

    page = apply_page

    for step in range(max_steps):
        if is_checkpoint_url(page.url):
            await dump_debug(root, page, "apply_checkpoint")
            return ("failed", "checkpoint")

        if await _page_detect_submitted(page):
            return ("submitted", "page_detected_submitted")

        try:
            if scope is not page and await page.locator("*[role='dialog']").count() == 0:
                if await _page_detect_submitted(page):
                    return ("submitted", "dialog_closed_page_detected_submitted")
                await page.wait_for_timeout(2500)
                if await page.locator("*[role='dialog']").count() == 0:
                    if await _page_detect_submitted(page):
                        return ("submitted", "dialog_closed_page_detected_submitted_2")
                    await dump_debug(root, page, f"apply_dialog_gone_s{step+1}")
                    return ("needs_manual", "dialog_gone_no_success_text")
                scope = page.locator("*[role='dialog']").first
        except Exception:
            pass

        await _fill_if_present(scope, "First name", candidate.first_name)
        await _fill_if_present(scope, "Last name", candidate.last_name)
        await _select_phone_country_if_present(scope, candidate.phone_country)
        await _fill_if_present(scope, "Mobile phone number", candidate.phone_number)
        await _fill_if_present(scope, "Email address", candidate.email)

        await _attach_resume_if_possible(scope, resume_path)

        if await _detect_submitted(scope):
            return ("submitted", "detected_submitted_text")

        btn = await _find_primary_button(scope)
        if btn is None:
            await page.wait_for_timeout(2500)
            btn = await _find_primary_button(scope)
        if btn is None:
            await dump_debug(root, page, f"apply_no_button_s{step+1}")
            return ("needs_manual", "no_next_or_submit_button")

        name = ""
        try:
            name = (await btn.inner_text()).strip()
        except Exception:
            pass

        if re.search(r"submit", name or "", re.IGNORECASE) and not submit:
            await dump_debug(root, page, f"apply_reached_submit_s{step+1}")
            return ("needs_manual", "reached_submit_but_submit_disabled")

        try:
            if not await btn.is_enabled():
                await dump_debug(root, page, f"apply_button_disabled_s{step+1}")
                return ("needs_manual", "primary_button_disabled")
        except Exception:
            pass

        await page.wait_for_timeout(random.randint(600, 1200))
        try:
            await btn.scroll_into_view_if_needed()
        except Exception:
            pass
        await btn.click()
        await page.wait_for_timeout(random.randint(1400, 2600))

        if scope is not page and await page.locator("*[role='dialog']").count() == 0 and await _page_detect_submitted(page):
            return ("submitted", "page_detected_submitted_after_click")

    await dump_debug(root, page, "apply_max_steps")
    return ("needs_manual", "max_steps_reached")


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    candidate = None

    resume_path = resolve_path(ROOT, args.resume)
    if not args.resume.strip():
        attach = cfg_get(cfg, "email.attachments", [])
        if isinstance(attach, list) and attach:
            resume_path = resolve_path(ROOT, str(attach[0]))

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    closer = SafeCloser()
    db_conn = None
    try:
        db_conn = db_connect(db_path)
        init_db(db_conn)

        try:
            profile = load_profile(db_conn)
        except Exception:
            profile = {}

        candidate = candidate_from_cfg(cfg, profile=profile)

        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=["--start-maximized", "--lang=en-US"],
        )

        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        ok = await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page, timeout_ms=args.step_timeout_ms)
        if not ok:
            print("[li-apply] not logged in. Run scripts/linkedin_login.py first.")
            await dump_debug(ROOT, page, "apply_not_logged_in")
            return 2

        job_url = args.job_url.strip()
        if not job_url:
            print("[li-apply] missing --job-url")
            return 1

        lead_id = None
        try:
            lead_id, inserted = upsert_lead_with_flag(
                db_conn,
                LeadUpsert(
                    platform="linkedin",
                    lead_type="job",
                    contact=job_url,
                    url=job_url,
                    company="",
                    job_title="",
                    location="",
                    source="linkedin_easy_apply",
                    created_at=_now_iso(),
                    raw={"job_url": job_url},
                ),
            )
            if inserted:
                add_event(db_conn, lead_id=lead_id, event_type="collected", status="ok", occurred_at=_now_iso(), details={"source": "linkedin_easy_apply"})
            add_event(db_conn, lead_id=lead_id, event_type="li_apply_started", status="ok", occurred_at=_now_iso(), details={})
            db_conn.commit()
        except Exception:
            lead_id = None

        result, details = await run_easy_apply_once(
            root=ROOT,
            page=page,
            job_url=job_url,
            candidate=candidate,
            resume_path=resume_path,
            max_steps=args.max_steps,
            submit=not args.no_submit,
            db_conn=db_conn,
            profile=profile,
        )

        print(f"[li-apply] result={result} details={details}")

        if db_conn is not None and lead_id is not None:
            try:
                ev = "li_apply_submitted" if result == "submitted" else ("li_apply_needs_manual" if result == "needs_manual" else "li_apply_failed")
                add_event(db_conn, lead_id=lead_id, event_type=ev, status="ok", occurred_at=_now_iso(), details={"details": details})
                db_conn.commit()
            except Exception:
                pass

        if result == "needs_manual" and args.pause_on_manual and db_conn is not None:
            try:
                notify(ROOT, cfg, kind="attention")
            except Exception:
                pass

            missing = []
            reason = ""
            try:
                payload = json.loads(details) if (details or "").strip().startswith("{") else {}
                reason = str(payload.get("reason") or "")
                missing = payload.get("missing") or []
            except Exception:
                missing = []
                reason = ""

            if not missing and "required" not in reason.lower():
                print("[li-apply] pause-on-manual: no missing required questions detected; skipping pause.")
            else:
                if missing:
                    print("[li-apply] Missing required questions:")
                    for m in missing[:50]:
                        q = (m.get("question") or "").strip()
                        if q:
                            print(f"  - {q}")

                prompt = "[li-apply] Fill the answers in the open Easy Apply modal, then press Enter here to continue... "
                try:
                    await asyncio.to_thread(input, prompt)
                except Exception:
                    loop = asyncio.get_running_loop()
                    await loop.run_in_executor(None, input, prompt)

                try:
                    if await page.locator("*[role='dialog']").count() > 0:
                        scope = page.locator("*[role='dialog']").first
                    else:
                        scope = page

                    learned = await extract_filled_answers(scope)
                    for item in learned:
                        q_raw = (item.get("question") or "").strip()
                        ans = (item.get("answer") or "").strip()
                        if q_raw and ans:
                            upsert_answer(db_conn, q_raw=q_raw, answer=ans, status="confirmed")
                    db_conn.commit()

                    result2, details2 = await continue_easy_apply_current(
                        root=ROOT,
                        page=page,
                        candidate=candidate,
                        resume_path=resume_path,
                        max_steps=max(8, args.max_steps),
                        submit=not args.no_submit,
                        db_conn=db_conn,
                        profile=profile,
                    )
                    print(f"[li-apply] continued result={result2} details={details2}")

                    if lead_id is not None:
                        ev2 = "li_apply_submitted" if result2 == "submitted" else ("li_apply_needs_manual" if result2 == "needs_manual" else "li_apply_failed")
                        add_event(db_conn, lead_id=lead_id, event_type=ev2, status="ok", occurred_at=_now_iso(), details={"details": details2, "continued": True})
                        db_conn.commit()

                    result, details = result2, details2
                except Exception as e:
                    print(f"[li-apply] learn/continue failed: {e}")

        try:
            if result == "submitted":
                notify(ROOT, cfg, kind="done")
            elif result == "needs_manual":
                notify(ROOT, cfg, kind="attention")
            else:
                notify(ROOT, cfg, kind="error")
        except Exception:
            pass

        if args.keep_open:
            print("[li-apply] keep-open enabled; press Ctrl+C in this terminal to stop.")
            while True:
                await asyncio.sleep(5)

        return 0 if result == "submitted" else (3 if result == "needs_manual" else 4)
    except KeyboardInterrupt:
        print("[li-apply] interrupted; closing browser...")
        return 130
    except asyncio.TimeoutError:
        print("[li-apply] hard timeout hit; closing browser...")
        try:
            notify(ROOT, cfg, kind="timeout")
        except Exception:
            pass
        return 6
    except Exception as e:
        print(f"[li-apply] error: {e}")
        try:
            notify(ROOT, cfg, kind="error")
        except Exception:
            pass
        try:
            if closer.ctx and closer.ctx.pages:
                await dump_debug(ROOT, closer.ctx.pages[0], "apply_error")
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
    ap = argparse.ArgumentParser(description="LinkedIn Easy Apply runner (autofill + optional submit)")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override activity DB path")
    ap.add_argument("--job-url", required=True, help="LinkedIn job URL, e.g. https://www.linkedin.com/jobs/view/<id>/")
    ap.add_argument("--resume", default="", help="Resume PDF path (default: first email attachment from config)")
    ap.add_argument("--max-steps", type=int, default=8, help="Max modal steps to attempt")
    ap.add_argument("--no-submit", action="store_true", help="Stop on Submit (do not click submit)")
    ap.add_argument("--keep-open", action="store_true", help="Keep browser open after finishing (for debugging)")
    ap.add_argument("--pause-on-manual", action="store_true", help="Pause on missing required questions, learn answers, then continue")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step Playwright timeout")
    ap.add_argument("--timeout-seconds", type=int, default=3600, help="Overall timeout")
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-apply] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())
