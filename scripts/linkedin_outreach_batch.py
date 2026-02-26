import argparse
import asyncio
import csv
import json
import os
import random
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import add_event, connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import (  # noqa: E402
    SafeCloser,
    bool_env,
    dump_debug,
    ensure_linkedin_session,
    int_env,
    is_checkpoint_url,
)
from src.profile_store import load_profile, normalize_person_name  # noqa: E402

STRICT_PERSONAL_OUTREACH_DAILY_MAX = 15


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        path = (p.path or "").rstrip("/")
        netloc = (p.netloc or "").lower()
        if "linkedin.com" in netloc and (
            path.startswith("/jobs/view/")
            or path.startswith("/feed/update/")
            or path.startswith("/posts/")
            or path.startswith("/in/")
            or path.startswith("/company/")
        ):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _is_profile_url(url: str) -> bool:
    try:
        p = urlparse((url or "").strip())
        return "linkedin.com" in (p.netloc or "").lower() and (p.path or "").startswith("/in/")
    except Exception:
        return False


def _is_people_search_url(url: str) -> bool:
    try:
        p = urlparse((url or "").strip())
        path = (p.path or "").lower()
        return "linkedin.com" in (p.netloc or "").lower() and "/search/results/people" in path
    except Exception:
        return False


@dataclass
class Target:
    lead_id: str
    profile_url: str
    post_or_job_url: str
    job_title: str
    author_name: str
    action: str  # dm|connect


def _load_lead_maps(conn) -> Tuple[Dict[str, str], Dict[str, str]]:
    by_post_url: Dict[str, str] = {}
    by_author_url: Dict[str, str] = {}
    rows = conn.execute(
        """
        SELECT lead_id, url, raw_json
        FROM leads
        WHERE platform='linkedin' AND lead_type='post'
        """
    ).fetchall()
    for r in rows:
        lead_id = str(r["lead_id"] or "").strip()
        post_url = _canonical_url(str(r["url"] or ""))
        raw = {}
        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}
        author_url = _canonical_url(str(raw.get("author_url") or ""))
        if lead_id and post_url:
            by_post_url[post_url] = lead_id
        if lead_id and author_url:
            by_author_url[author_url] = lead_id
    return by_post_url, by_author_url


def _already_contacted(conn, lead_id: str, profile_url: str) -> bool:
    """
    Prevent duplicates by lead_id OR by canonical profile_url stored in event details_json.
    This avoids DM'ing the same recruiter multiple times across different leads.
    """
    prof = _canonical_url(profile_url)
    p_slash = (prof + "/%") if prof else ""
    p_q = (prof + "?%") if prof else ""
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE event_type IN ('li_dm_sent', 'li_connect_sent', 'li_comment_posted')
          AND (
            lead_id = ?
            OR (
              json_valid(details_json)
              AND (
                json_extract(details_json, '$.profile_url') = ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
              )
            )
          )
        LIMIT 1
        """,
        (lead_id, prof, p_slash, p_q),
    ).fetchone()
    return row is not None


def _personal_outreach_sent_today(conn) -> int:
    today = datetime.now().date().isoformat()
    row = conn.execute(
        """
        SELECT COUNT(1) AS cnt
        FROM events
        WHERE event_type IN ('li_dm_sent', 'li_connect_sent')
          AND substr(COALESCE(occurred_at, ''), 1, 10) = ?
        """,
        (today,),
    ).fetchone()
    try:
        return int((row["cnt"] if row is not None else 0) or 0)
    except Exception:
        return 0


def _first_name(full_name: str) -> str:
    parts = [p for p in (full_name or "").strip().split() if p]
    return parts[0] if parts else "there"


def _role_pitch(job_title: str) -> str:
    t = (job_title or "").lower()
    if any(k in t for k in ("playwright", "selenium", "cypress", "ui")):
        return "I can quickly stabilize flaky UI automation and regression coverage."
    if any(k in t for k in ("api", "backend", "rest", "graphql")):
        return "I can tighten API/auth regression checks and release confidence."
    if any(k in t for k in ("mobile", "ios", "android", "appium")):
        return "I can improve mobile test coverage and release reliability."
    if any(k in t for k in ("performance", "load", "jmeter", "gatling")):
        return "I can run practical performance checks and triage bottlenecks fast."
    return "I can help with practical QA automation and API-focused validation."


def _connect_note(candidate_name: str, recipient_name: str, job_title: str = "") -> str:
    """
    LinkedIn connection note must be short (~300 chars). Keep it human and specific,
    but don't overfit to untrusted post text.
    """
    cand = normalize_person_name(candidate_name) or "Candidate"
    rec = _first_name(recipient_name)
    role = (job_title or "a QA role").strip()
    pitch = _role_pitch(role)
    # Keep this deliberately compact to avoid truncation and "spammy" phrasing.
    text = (
        f"Hi {rec} - I saw your post about {role}. "
        f"I'm {cand}, QA Engineer (manual + automation): API/auth testing + C#/.NET automation. "
        f"{pitch} Open to remote worldwide. Happy to connect."
    )
    return text[:295]


def _dm_text(candidate_name: str, recipient_name: str, job_title: str) -> str:
    cand = normalize_person_name(candidate_name) or "Candidate Name"
    rec = _first_name(recipient_name)
    role = (job_title or "the QA role").strip()
    pitch = _role_pitch(role)
    return (
        f"Hi {rec}, thanks for sharing the {role} opening. "
        f"I'm {cand}, a QA Engineer (manual + automation) with 5+ years in web products/startups. "
        "Strong in API/auth testing (REST/GraphQL) and C#/.NET automation (NUnit/RestSharp), plus CI checks. "
        f"{pitch} "
        "Remote from Vietnam (UTC+7), can start immediately. "
        "If you're still hiring, I'm happy to share my CV and a quick intro."
    )


def _dm_subject(job_title: str) -> str:
    role = re.sub(r"\s+", " ", (job_title or "").strip())
    if role:
        return f"QA Engineer | {role}"[:120]
    return "QA Engineer | Remote opportunity"


def _load_targets(csv_path: Path, *, conn, limit: int) -> List[Target]:
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    by_post_url, by_author_url = _load_lead_maps(conn)
    out: List[Target] = []
    seen_profile: set[str] = set()

    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rd = csv.DictReader(f)
        for row in rd:
            post_url = _canonical_url((row.get("post_or_job_url") or "").strip())
            author_url = _canonical_url((row.get("author_url") or "").strip())
            profile_url = author_url or post_url
            if not profile_url:
                continue
            if profile_url in seen_profile:
                continue
            seen_profile.add(profile_url)

            lead_id = ""
            if author_url:
                lead_id = by_author_url.get(author_url, "")
            if not lead_id and post_url:
                lead_id = by_post_url.get(post_url, "")
            if not lead_id:
                # Keep deterministic pseudo lead id fallback for event consistency.
                lead_id = f"post_{abs(hash(profile_url))}"

            out.append(
                Target(
                    lead_id=lead_id,
                    profile_url=profile_url,
                    post_or_job_url=post_url,
                    job_title=(row.get("job_title") or "").strip(),
                    author_name=(row.get("author_name") or "").strip(),
                    action=((row.get("action") or "connect").strip().lower()),
                )
            )
            if len(out) >= int(limit):
                break
    return out


async def _click_first_visible(candidates: List, *, timeout_ms: int = 1800) -> bool:
    async def _robust_click(loc) -> bool:
        try:
            await loc.scroll_into_view_if_needed(timeout=timeout_ms)
        except Exception:
            pass

        # 1) Normal click (actionability checks)
        try:
            await loc.click(timeout=timeout_ms)
            return True
        except Exception:
            pass

        # 2) Forced click (bypass pointer interception)
        try:
            await loc.click(timeout=timeout_ms, force=True)
            return True
        except Exception:
            pass

        # 3) DOM click fallbacks (works even when Playwright deems it "not clickable")
        try:
            await loc.dispatch_event("click")
            return True
        except Exception:
            pass
        try:
            ok = await loc.evaluate("(el) => { el.click(); return true; }")
            return bool(ok)
        except Exception:
            return False

    for c in candidates:
        try:
            if await c.is_visible(timeout=timeout_ms):
                if await _robust_click(c):
                    return True
        except Exception:
            continue
    return False


async def _is_message_compose_open(page) -> bool:
    markers = [
        page.locator("div.msg-overlay-conversation-bubble--is-active:visible").last,
        page.locator("div.msg-overlay-conversation-bubble:visible").last,
        page.locator("div.msg-overlay-conversation-bubble__content-wrapper:visible").first,
        page.locator("form.msg-form:visible").last,
        page.locator("div.msg-form__msg-content-container:visible").last,
        page.locator("div.msg-form__contenteditable[contenteditable='true']:visible").first,
        page.locator("div[role='textbox'][aria-label*='Write a message']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true']:visible").first,
        page.locator("input[name='subject']:visible").first,
    ]
    for m in markers:
        try:
            if await m.is_visible(timeout=450):
                return True
        except Exception:
            continue
    return False


async def _open_more_menu(scope) -> bool:
    return await _click_first_visible(
        [
            scope.locator("button[aria-label='More actions']:visible").first,
            scope.locator("button[aria-label^='More actions']:visible").first,
            scope.locator("button[aria-label*='More actions']:visible").first,
            scope.locator("button:has-text('More'):visible").first,
            scope.get_by_role("button", name=re.compile(r"^More$", re.IGNORECASE)).first,
            scope.get_by_role("button", name=re.compile(r"More actions", re.IGNORECASE)).first,
            scope.locator("button[aria-label*='More']:visible").first,
        ],
        timeout_ms=1200,
    )


async def _profile_action_scope(page):
    """
    Try to scope clicks to the profile top card (prevents clicking sidebar suggestions).
    Fallback to main.
    """
    candidates = [
        page.locator("main section").filter(has=page.locator("main h1:visible")).first,
        page.locator("main").first,
    ]
    for c in candidates:
        try:
            if await c.is_visible(timeout=800):
                return c
        except Exception:
            continue
    return page.locator("main").first


async def _active_compose_scope(page):
    candidates = [
        page.locator("div.msg-overlay-conversation-bubble--is-active:visible").last,
        page.locator("div.msg-overlay-conversation-bubble:visible").last,
        page.locator("div.msg-overlay-conversation-bubble__content-wrapper:visible").last,
    ]
    for c in candidates:
        try:
            if await c.is_visible(timeout=500):
                return c
        except Exception:
            continue
    return page


async def _is_paid_inmail_required(scope) -> bool:
    try:
        credits = scope.locator("section.msg-inmail-credits-display:visible").first
        if not await credits.is_visible(timeout=250):
            return False
        scope_text = ((await scope.inner_text(timeout=700)) or "").lower()
        credits_text = ((await credits.inner_text(timeout=700)) or "").lower()
        combined = f"{scope_text}\n{credits_text}"
        if "free message" in combined:
            return False
        if re.search(r"use\s*\d+\s*of\s*\d+\s*inmail", combined):
            return True
        if "inmail credit" in combined or "inmail credits" in combined:
            return True
    except Exception:
        return False
    return False


async def _wait_send_enabled(page, scope, *, timeout_ms: int = 3500):
    end_ts = datetime.now().timestamp() + max(0.5, timeout_ms / 1000.0)
    btn = scope.locator("button.msg-form__send-btn:visible").first
    while datetime.now().timestamp() < end_ts:
        try:
            if await btn.is_visible(timeout=400):
                disabled_attr = await btn.get_attribute("disabled")
                cls = (await btn.get_attribute("class")) or ""
                if disabled_attr is None and "artdeco-button--disabled" not in cls:
                    return btn
        except Exception:
            pass
        await page.wait_for_timeout(200)
    return None


async def _fill_dm_subject_if_present(scope, subject: str) -> None:
    subject_locators = [
        scope.locator("input[name='subject']:visible").first,
        scope.locator("input[placeholder*='Subject']:visible").first,
    ]
    for loc in subject_locators:
        try:
            if await loc.is_visible(timeout=500):
                await loc.fill(subject)
                return
        except Exception:
            continue


async def _fill_dm_body(page, scope, text: str) -> bool:
    box_candidates = [
        scope.locator("div.msg-form__contenteditable[contenteditable='true']:visible").first,
        scope.locator("div[role='textbox'][contenteditable='true']:visible").first,
        scope.locator("div[role='textbox'][aria-label*='Write a message']:visible").first,
        scope.locator("textarea[name='message']:visible").first,
        scope.locator("textarea:visible").first,
    ]
    box = None
    for b in box_candidates:
        try:
            if await b.is_visible(timeout=900):
                box = b
                break
        except Exception:
            continue
    if box is None:
        return False

    try:
        await box.click(timeout=1200)
        try:
            await box.fill(text)
        except Exception:
            await page.keyboard.press("Control+A")
            await page.keyboard.press("Backspace")
            await page.keyboard.type(text, delay=9)
        try:
            val = (await box.input_value(timeout=250)) if "textarea" in (await box.evaluate("el => el.tagName.toLowerCase()")) else ""
            if isinstance(val, str) and val.strip():
                return True
        except Exception:
            pass
        try:
            wrote = await box.evaluate(
                """(el, t) => {
                    const tag = (el.tagName || '').toLowerCase();
                    if (tag === 'textarea' || tag === 'input') {
                        el.value = t;
                        el.dispatchEvent(new Event('input', { bubbles: true }));
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                        return !!(el.value || '').trim();
                    }
                    el.focus();
                    el.textContent = '';
                    const p = document.createElement('p');
                    p.textContent = t;
                    el.appendChild(p);
                    el.dispatchEvent(new InputEvent('input', { bubbles: true, data: t, inputType: 'insertText' }));
                    return (el.innerText || '').trim().length > 0;
                }""",
                text,
            )
            return bool(wrote)
        except Exception:
            return True
    except Exception:
        return False


async def _compose_recipient_matches(page, scope, expected_profile_url: str) -> bool:
    """
    Best-effort safety: if we can detect the recipient profile link inside the composer,
    ensure it matches the expected target profile URL.
    """
    expected = _canonical_url(expected_profile_url)
    if not expected:
        return True

    candidates = [
        scope.locator("a.msg-compose__profile-link:visible").first,
        scope.locator("div.artdeco-entity-lockup__title a[href^='/in/']:visible").first,
        scope.locator("div.msg-overlay-conversation-bubble__header a[href^='/in/']:visible").first,
    ]
    href = ""
    for c in candidates:
        try:
            if await c.is_visible(timeout=450):
                href = (await c.get_attribute("href")) or ""
                if href:
                    break
        except Exception:
            continue

    if not href:
        # Cannot validate; play it safe and refuse to send.
        return False

    try:
        full = await page.evaluate(
            "(h) => { try { return new URL(h, location.origin).toString(); } catch { return h; } }",
            href,
        )
    except Exception:
        full = href
    actual = _canonical_url(str(full or ""))
    return bool(actual and actual == expected)


async def _click_text_action(page, labels: List[str], *, timeout_ms: int = 2000) -> bool:
    """Click action by visible text, then climb to nearest clickable parent."""
    # 1) Fast role-based pass first.
    for label in labels:
        try:
            btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)).first
            if await btn.is_visible(timeout=400):
                await btn.scroll_into_view_if_needed(timeout=700)
                await btn.click(timeout=700)
                return True
        except Exception:
            pass
        try:
            link = page.get_by_role("link", name=re.compile(rf"^{re.escape(label)}$", re.IGNORECASE)).first
            if await link.is_visible(timeout=400):
                await link.scroll_into_view_if_needed(timeout=700)
                await link.click(timeout=700)
                return True
        except Exception:
            pass

    # 2) Text-to-parent fallback (works with deeply nested LinkedIn spans).
    deadline = datetime.now().timestamp() + max(0.5, timeout_ms / 1000.0)
    norm_labels = [x.strip().lower() for x in labels if x.strip()]
    while datetime.now().timestamp() < deadline:
        try:
            clicked = await page.evaluate(
                """(wanted) => {
                    const norm = (s) => (s || '').replace(/\\s+/g, ' ').trim().toLowerCase();
                    const visible = (el) => {
                        if (!el || !(el instanceof Element)) return false;
                        const r = el.getBoundingClientRect();
                        const style = window.getComputedStyle(el);
                        return r.width > 0 && r.height > 0 && style.visibility !== 'hidden' && style.display !== 'none';
                    };
                    const inGlobalNav = (el) => {
                        return !!(
                            el.closest('header') ||
                            el.closest('nav') ||
                            el.closest('[data-testid=\"primary-nav\"]') ||
                            el.closest('[data-view-name^=\"navigation-\"]')
                        );
                    };
                    const all = Array.from(document.querySelectorAll('button,a,[role=\"button\"],span,div'));
                    for (const el of all) {
                        const t = norm(el.textContent);
                        if (!t || !wanted.includes(t)) continue;
                        const clickable = el.closest('button,a,[role=\"button\"]') || el;
                        if (inGlobalNav(clickable)) continue;
                        if (!visible(clickable)) continue;
                        clickable.scrollIntoView({block: 'center', inline: 'center'});
                        clickable.dispatchEvent(new MouseEvent('click', {bubbles: true, cancelable: true, view: window}));
                        return true;
                    }
                    return false;
                }""",
                norm_labels,
            )
            if clicked:
                return True
        except Exception:
            pass
        await page.wait_for_timeout(220)
    return False


async def _send_dm(page, text: str, subject: str, *, expected_profile_url: str) -> Tuple[bool, str]:
    top = await _profile_action_scope(page)

    # Prefer "compose" href navigation when available (more reliable than clicking).
    try:
        a = top.locator("a[href*='/messaging/compose']:visible").first
        if await a.is_visible(timeout=350):
            href = (await a.get_attribute("href")) or ""
            if href:
                try:
                    full = await page.evaluate(
                        "(h) => { try { return new URL(h, location.origin).toString(); } catch { return h; } }",
                        href,
                    )
                except Exception:
                    full = href
                await page.goto(str(full), wait_until="domcontentloaded")
    except Exception:
        pass

    opened = await _click_first_visible(
        [
            top.locator("button[aria-label^='Message ']:visible").first,
            top.locator("button[aria-label*=' Message ']:visible").first,
            top.locator("button:has-text('Message'):visible").first,
            top.locator("a:has-text('Message'):visible").first,
            top.locator("[role='button']:has-text('Message'):visible").first,
        ],
        timeout_ms=1300,
    )
    if not opened:
        # Some profiles hide "Message" under the "More" menu.
        if await _open_more_menu(top):
            await page.wait_for_timeout(350)
            opened = await _click_first_visible(
                [
                    page.locator("div[role='menu']:visible").get_by_role(
                        "menuitem", name=re.compile(r"^Message$", re.IGNORECASE)
                    ).first,
                    page.get_by_role("menuitem", name=re.compile(r"^Message$", re.IGNORECASE)).first,
                    page.get_by_role("button", name=re.compile(r"^Message$", re.IGNORECASE)).first,
                    page.locator("span:has-text('Message')").first,
                ],
                timeout_ms=1400,
            )
        if not opened:
            return (False, "message_button_not_found")

    await page.wait_for_timeout(900)
    current_url = _canonical_url(page.url)
    expected_url = _canonical_url(expected_profile_url)
    if current_url and expected_url and current_url != expected_url and "/messaging/" not in current_url:
        return (False, "message_opened_wrong_page")
    opened_ok = False
    for _ in range(8):
        if await _is_message_compose_open(page):
            opened_ok = True
            break
        await page.wait_for_timeout(300)
    if not opened_ok:
        # Retry opening once more for flaky profile cards / delayed drawers.
        reopened = await _click_first_visible(
            [
                top.locator("button[aria-label^='Message ']:visible").first,
                top.locator("button[aria-label*=' Message ']:visible").first,
                top.locator("button:has-text('Message'):visible").first,
            ],
            timeout_ms=1400,
        )
        if reopened:
            await page.wait_for_timeout(900)
            for _ in range(6):
                if await _is_message_compose_open(page):
                    opened_ok = True
                    break
                await page.wait_for_timeout(250)
    if not opened_ok:
        return (False, "message_compose_not_opened")

    scope = await _active_compose_scope(page)

    # Do not spend InMail credits automatically.
    if await _is_paid_inmail_required(scope):
        return (False, "inmail_requires_credits")

    if not await _compose_recipient_matches(page, scope, expected_profile_url):
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass
        return (False, "recipient_mismatch")

    await _fill_dm_subject_if_present(scope, subject)
    filled = await _fill_dm_body(page, scope, text)
    if not filled:
        return (False, "message_fill_failed")

    send_button = await _wait_send_enabled(page, scope, timeout_ms=5200)
    if send_button is not None:
        try:
            await send_button.click(timeout=1200)
            await page.wait_for_timeout(900)
            return (True, "dm_sent")
        except Exception:
            pass

    sent = await _click_first_visible(
        [
            scope.locator("button.msg-form__send-btn:not([disabled]):not(.artdeco-button--disabled)").first,
            scope.locator("form.msg-form button[type='submit']:not([disabled])").first,
            scope.get_by_role("button", name=re.compile(r"^Send$", re.IGNORECASE)).first,
            scope.locator("button:has-text('Send')").first,
            scope.locator("button[aria-label*='Send']").first,
        ],
        timeout_ms=1500,
    )
    if not sent:
        return (False, "message_send_button_not_found")

    await page.wait_for_timeout(900)
    return (True, "dm_sent")


async def _open_connect_modal(page) -> Tuple[bool, str]:
    top = await _profile_action_scope(page)
    direct_ok = await _click_first_visible(
        [
            top.get_by_role("button", name=re.compile(r"^Connect$", re.IGNORECASE)).first,
            top.locator("button[aria-label^='Connect ']:visible").first,
            top.locator("button:has-text('Connect'):visible").first,
            top.locator("xpath=//span[normalize-space()='Connect']/ancestor::*[self::button or self::a][1]").first,
        ],
        timeout_ms=900,
    )
    if not direct_ok:
        direct_ok = await _click_text_action(page, ["Connect"], timeout_ms=1500)
    if direct_ok:
        return (True, "connect_opened_direct")

    more_clicked = await _click_first_visible(
        [
            top.get_by_role("button", name=re.compile(r"^More$", re.IGNORECASE)).first,
            top.get_by_role("button", name=re.compile(r"More actions", re.IGNORECASE)).first,
            top.locator("button[aria-label*='More']:visible").first,
        ],
        timeout_ms=900,
    )
    if not more_clicked:
        return (False, "connect_no_more_actions")

    await page.wait_for_timeout(500)
    menu_ok = await _click_first_visible(
        [
            page.locator("div[role='menu']:visible").get_by_role("menuitem", name=re.compile(r"Connect", re.IGNORECASE)).first,
            page.get_by_role("menuitem", name=re.compile(r"Connect", re.IGNORECASE)).first,
            page.get_by_role("button", name=re.compile(r"Connect", re.IGNORECASE)).first,
            page.locator("span:has-text('Connect')").first,
        ],
        timeout_ms=1400,
    )
    if not menu_ok:
        return (False, "connect_menu_item_not_found")
    return (True, "connect_opened_menu")


async def _find_connect_dialog(page):
    """
    LinkedIn can render multiple dialogs. Pick the one that looks like an invite/connect flow.
    """
    dialogs = page.locator("[role='dialog']:visible")
    try:
        n = await dialogs.count()
    except Exception:
        n = 0
    for i in range(n - 1, -1, -1):
        d = dialogs.nth(i)
        try:
            t = ((await d.inner_text(timeout=700)) or "").lower()
        except Exception:
            continue
        if "add a note" in t or "send invitation" in t or ("invite" in t and "send" in t) or ("connect" in t and "send" in t):
            return d
    return dialogs.last if n else page


async def _send_connect(page, note: str) -> Tuple[bool, str]:
    opened, reason = await _open_connect_modal(page)
    if not opened:
        return (False, reason)

    await page.wait_for_timeout(700)
    dialog = await _find_connect_dialog(page)
    # If Add a note exists, use it.
    added_note = await _click_first_visible(
        [
            dialog.get_by_role("button", name=re.compile(r"Add a note", re.IGNORECASE)).first,
            dialog.locator("button:has-text('Add a note')").first,
        ],
        timeout_ms=1200,
    )
    if added_note:
        await page.wait_for_timeout(500)
        note_box_candidates = [
            dialog.locator("textarea#custom-message").first,
            dialog.locator("textarea[name='message']").first,
            dialog.locator("textarea").first,
        ]
        for tb in note_box_candidates:
            try:
                if await tb.is_visible(timeout=1000):
                    await tb.fill(note[:295])
                    break
            except Exception:
                continue

    # Some flows require email verification; do not loop.
    try:
        email_like = dialog.locator(
            "input[type='email'], input[name*='email' i], input[id*='email' i], input[placeholder*='email' i], input[aria-label*='email' i]"
        ).first
        if await email_like.is_visible(timeout=350):
            return (False, "connect_email_required")
    except Exception:
        pass

    sent = await _click_first_visible(
        [
            dialog.get_by_role("button", name=re.compile(r"^(Send|Send now|Send invitation|Send without a note|Done|Next)$", re.IGNORECASE)).last,
            dialog.locator("button:has-text('Send')").last,
            dialog.locator("button:has-text('Send invitation')").last,
            dialog.locator("button:has-text('Done')").last,
            dialog.locator("button:has-text('Next')").last,
            dialog.locator("button[aria-label*='Send']").last,
            dialog.locator("button[aria-label*='Invite']").last,
            dialog.locator("button:has-text('Connect')").last,
            dialog.locator("button.artdeco-button--primary:visible").last,
        ],
        timeout_ms=1600,
    )
    if not sent:
        return (False, "connect_send_not_found")

    await page.wait_for_timeout(800)
    return (True, "connect_sent")


async def _send_follow(page) -> Tuple[bool, str]:
    top = await _profile_action_scope(page)
    ok = await _click_first_visible(
        [
            top.get_by_role("button", name=re.compile(r"^Follow$", re.IGNORECASE)).first,
            top.locator("button[aria-label^='Follow ']:visible").first,
            top.locator("button:has-text('Follow'):visible").first,
            top.locator("xpath=//span[normalize-space()='Follow']/ancestor::*[self::button or self::a][1]").first,
            top.locator("span:has-text('Follow'):visible").first,
        ],
        timeout_ms=900,
    )
    if not ok:
        ok = await _click_text_action(page, ["Follow"], timeout_ms=1200)
    if not ok:
        return (False, "follow_not_found")
    await page.wait_for_timeout(500)
    return (True, "follow_sent")


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))

    headless = args.headless or bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo_ms = args.slow_mo_ms if args.slow_mo_ms >= 0 else int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    csv_path = resolve_path(ROOT, args.csv)
    closer = SafeCloser()
    conn = None
    try:
        conn = db_connect(db_path)
        init_db(conn)
        profile = load_profile(conn)
        candidate_name = normalize_person_name(profile.get("candidate.name") or "") or "Candidate Name"

        sent_today_before = _personal_outreach_sent_today(conn)
        remaining_today = max(0, STRICT_PERSONAL_OUTREACH_DAILY_MAX - sent_today_before)
        if remaining_today <= 0:
            print(
                f"[li-outreach] daily personal outreach cap reached: "
                f"{sent_today_before}/{STRICT_PERSONAL_OUTREACH_DAILY_MAX}. Stop."
            )
            return 0

        effective_limit = max(0, min(int(args.limit), remaining_today))
        if effective_limit < int(args.limit):
            print(
                f"[li-outreach] strict cap active: requested={args.limit}, "
                f"allowed_now={effective_limit}, already_sent_today={sent_today_before}"
            )

        targets = _load_targets(csv_path, conn=conn, limit=effective_limit)
        if not targets:
            print("[li-outreach] no targets loaded.")
            return 0

        if args.max_delay_sec < args.min_delay_sec:
            args.min_delay_sec, args.max_delay_sec = args.max_delay_sec, args.min_delay_sec
        if args.long_break_max_sec < args.long_break_min_sec:
            args.long_break_min_sec, args.long_break_max_sec = args.long_break_max_sec, args.long_break_min_sec

        print(f"[li-outreach] loaded targets: {len(targets)}")
        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo_ms,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=["--start-maximized", "--lang=en-US"],
        )
        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        if not await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page):
            print("[li-outreach] not logged in (li_at missing). Run scripts/linkedin_login.py first.")
            return 2

        sent_dm = 0
        sent_connect = 0
        skipped = 0
        failed = 0
        paced_actions = 0

        for idx, t in enumerate(targets, start=1):
            if sent_today_before + sent_dm + sent_connect >= STRICT_PERSONAL_OUTREACH_DAILY_MAX:
                print(
                    f"[li-outreach] strict daily cap reached during run: "
                    f"{sent_today_before + sent_dm + sent_connect}/{STRICT_PERSONAL_OUTREACH_DAILY_MAX}. Stop."
                )
                break
            print(f"[li-outreach] {idx}/{len(targets)} -> {t.profile_url}")
            if _already_contacted(conn, t.lead_id, t.profile_url):
                print("[li-outreach] skip: already contacted")
                skipped += 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_skipped",
                    occurred_at=_now_iso(),
                    details={"reason": "already_contacted", "profile_url": t.profile_url, "job_url": t.post_or_job_url},
                )
                conn.commit()
                continue

            add_event(
                conn,
                lead_id=t.lead_id,
                event_type="li_outreach_started",
                occurred_at=_now_iso(),
                details={"profile_url": t.profile_url, "job_url": t.post_or_job_url, "preferred_action": t.action},
            )
            conn.commit()

            try:
                await page.goto(t.profile_url, wait_until="domcontentloaded", timeout=args.step_timeout_ms)
            except Exception:
                await dump_debug(ROOT, page, "outreach_open_failed")
                failed += 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_failed",
                    occurred_at=_now_iso(),
                    details={"reason": "open_failed", "profile_url": t.profile_url},
                )
                conn.commit()
                continue

            expected_url = _canonical_url(t.profile_url)
            actual_url = _canonical_url(page.url)

            # Guard: if LinkedIn redirects us elsewhere, do NOT click Message/Follow/Connect.
            if expected_url and actual_url and actual_url != expected_url:
                await dump_debug(ROOT, page, "outreach_unexpected_redirect")
                failed += 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "unexpected_redirect", "profile_url": t.profile_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            if not _is_profile_url(page.url):
                await dump_debug(ROOT, page, "outreach_wrong_page")
                failed += 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "manual_wrong_page", "profile_url": t.profile_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            if is_checkpoint_url(page.url):
                await dump_debug(ROOT, page, "outreach_checkpoint")
                failed += 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_failed",
                    occurred_at=_now_iso(),
                    details={"reason": "checkpoint", "profile_url": t.profile_url},
                )
                conn.commit()
                continue

            await page.wait_for_timeout(random.randint(700, 1300))

            dm_text = _dm_text(candidate_name, t.author_name, t.job_title)
            dm_subject = _dm_subject(t.job_title)
            note = _connect_note(candidate_name, t.author_name, t.job_title)

            outcome = ""
            details = ""
            dm_reason = ""
            connect_reason = ""
            follow_reason = ""
            # Prefer connect-first for most post-based outreach (many profiles require paid InMail for DM).
            action_order = ["connect", "dm"] if t.action == "connect" else ["dm", "connect"]
            for act in action_order:
                if act == "dm":
                    ok, why = await _send_dm(page, dm_text, dm_subject, expected_profile_url=t.profile_url)
                    if ok:
                        outcome = "li_dm_sent"
                        details = why
                        sent_dm += 1
                        dm_reason = why
                        break
                    dm_reason = why
                    details = why
                else:
                    ok, why = await _send_connect(page, note)
                    if ok:
                        outcome = "li_connect_sent"
                        details = why
                        sent_connect += 1
                        connect_reason = why
                        break
                    connect_reason = why
                    details = why

            # Always try Follow as well to increase network reach.
            follow_ok, follow_why = await _send_follow(page)
            follow_reason = follow_why
            if follow_ok:
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_follow_sent",
                    occurred_at=_now_iso(),
                    details={"result": follow_why, "profile_url": t.profile_url, "actual_url": page.url},
                )
                conn.commit()

            if not outcome:
                failed += 1
                await dump_debug(ROOT, page, "outreach_manual_needed")
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_outreach_needs_manual",
                    occurred_at=_now_iso(),
                    details={
                        "reason": details or ("follow_only" if follow_ok else "no_action_succeeded"),
                        "profile_url": t.profile_url,
                        "actual_url": page.url,
                        "dm_reason": dm_reason,
                        "connect_reason": connect_reason,
                        "follow_reason": follow_reason,
                    },
                )
            else:
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type=outcome,
                    occurred_at=_now_iso(),
                    details={
                        "result": details,
                        "profile_url": t.profile_url,
                        "actual_url": page.url,
                        "job_url": t.post_or_job_url,
                        "preferred_action": t.action,
                        "dm_reason": dm_reason,
                        "connect_reason": connect_reason,
                        "follow_reason": follow_reason,
                    },
                )
            conn.commit()
            paced_actions += 1
            await page.wait_for_timeout(int(random.uniform(args.min_delay_sec, args.max_delay_sec) * 1000))
            if (
                args.long_break_every > 0
                and paced_actions % int(args.long_break_every) == 0
                and idx < len(targets)
            ):
                await page.wait_for_timeout(int(random.uniform(args.long_break_min_sec, args.long_break_max_sec) * 1000))

        print(
            f"[li-outreach] done: dm_sent={sent_dm} connect_sent={sent_connect} "
            f"skipped={skipped} failed={failed} "
            f"daily_personal={sent_today_before + sent_dm + sent_connect}/{STRICT_PERSONAL_OUTREACH_DAILY_MAX}"
        )
        return 0
    except PlaywrightTimeoutError:
        print("[li-outreach] timeout.")
        return 4
    except Exception as e:
        print(f"[li-outreach] error: {e}")
        return 1
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="LinkedIn outreach batch: DM or Connect with Send click.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--csv", default="data/out/linkedin_post_targets_top10.csv")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--slow-mo-ms", type=int, default=-1)
    ap.add_argument("--step-timeout-ms", type=int, default=25_000)
    ap.add_argument("--min-delay-sec", type=float, default=18.0)
    ap.add_argument("--max-delay-sec", type=float, default=45.0)
    ap.add_argument("--long-break-every", type=int, default=5)
    ap.add_argument("--long-break-min-sec", type=float, default=120.0)
    ap.add_argument("--long-break-max-sec", type=float, default=260.0)
    ap.add_argument("--timeout-seconds", type=int, default=3600)
    args = ap.parse_args()
    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-outreach] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())



