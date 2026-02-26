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
from typing import Any, Dict, List, Optional, Tuple
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
from src.telegram_notify import send_telegram_message  # noqa: E402


COMMENT_BTN_RE = re.compile(r"^Comment$", re.IGNORECASE)
POST_BTN_RE = re.compile(r"^(Post|Comment|Reply)$", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        netloc = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        if "linkedin.com" in netloc and (path.startswith("/feed/update/") or path.startswith("/posts/") or path.startswith("/in/")):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _is_post_url(url: str) -> bool:
    try:
        p = urlparse((url or "").strip())
        if "linkedin.com" not in (p.netloc or "").lower():
            return False
        path = (p.path or "").lower()
        return path.startswith("/feed/update/") or path.startswith("/posts/")
    except Exception:
        return False


def _canonical_profile_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        if "linkedin.com" not in (p.netloc or "").lower():
            return u
        path = (p.path or "").rstrip("/")
        if path.startswith("/in/"):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _first_name(name: str) -> str:
    parts = [p for p in (name or "").strip().split() if p]
    return parts[0] if parts else "there"


def _clean_role_hint(text: str) -> str:
    t = re.sub(r"\s+", " ", (text or "").strip())
    # Normalize common smart punctuation, then drop non-ascii (emojis etc) so our regexes work.
    t = (
        t.replace("\u2019", "'")
        .replace("\u201c", '"')
        .replace("\u201d", '"')
        .replace("\u2013", "-")
        .replace("\u2014", "-")
    )
    t = t.encode("ascii", "ignore").decode("ascii")
    t = re.sub(r"\s+", " ", t).strip()

    # Cheap cleanup of common noise in post snippets.
    t = re.sub(r"\bwe('re| are)?\s+hiring\b[:\s-]*", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"^hiring[:\s-]*", "", t, flags=re.IGNORECASE).strip()
    t = re.sub(r"\s+Job by\s+.+$", "", t, flags=re.IGNORECASE).strip()
    t = t.strip(" -|:;")

    # Keep it short to avoid spammy comment walls.
    t = t[:90]
    # If it doesn't even look like a QA role title, fall back to generic wording.
    if not re.search(r"\b(qa|quality|sdet|test|tester|automation)\b", t, flags=re.IGNORECASE):
        return ""
    return t


def _pick_comment_text(
    *,
    candidate_name: str,
    author_name: str,
    job_title: str,
    emails: List[str],
) -> str:
    cand = normalize_person_name(candidate_name) or "Candidate"
    rec = _first_name(author_name)
    role = _clean_role_hint(job_title) or "the QA role"

    # If the post already includes a hiring email, prefer asking them to confirm it (no DM promise).
    if emails:
        e = emails[0]
        variants = [
            f"Hi {rec}, thanks for sharing. Is {e} the right email to send my CV for the {role} opening? "
            f"I'm {cand}, QA (manual + automation): API/auth testing + C#/.NET automation. Open to remote worldwide.",
            f"Hi {rec} - should I send my CV to {e} re: {role}? "
            f"QA Automation Engineer here (API testing, C#/.NET automation, CI). Open to remote worldwide.",
        ]
        return random.choice(variants)[:600]

    variants = [
        f"Hi {rec}, thanks for sharing the {role} opening. "
        f"I'm {cand}, QA (manual + automation): API/auth testing (REST/GraphQL) + C#/.NET automation. "
        "Open to remote worldwide. What's the best email or apply link to send my CV?",
        f"Hi {rec} - I'm {cand}, QA Automation Engineer (API testing + C#/.NET automation, CI). "
        "Open to remote worldwide. If you're open to it, I can share my CV/details in DM.",
        f"Hi {rec}, interested in the {role} role. "
        f"QA Engineer here (manual + automation), strong API testing + C#/.NET automation. "
        "Open to remote worldwide. Please share the best way to apply.",
    ]
    return random.choice(variants)[:600]


def _already_contacted_any(conn, *, lead_id: str, profile_url: str) -> bool:
    """
    Skip if we already contacted this person by any LinkedIn outreach method:
      - DM sent
      - Connect sent
      - Comment posted
    Also skips if we already commented for the specific lead_id.
    """
    prof = _canonical_profile_url(profile_url)
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
              ? != ''
              AND json_valid(details_json)
              AND (
                json_extract(details_json, '$.profile_url') = ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
              )
            )
          )
        LIMIT 1
        """,
        (lead_id, prof, prof, p_slash, p_q),
    ).fetchone()
    return row is not None


def _already_commented_post(conn, *, post_url: str) -> bool:
    pu = _canonical_url(post_url)
    if not pu:
        return False
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE event_type = 'li_comment_posted'
          AND json_valid(details_json)
          AND json_extract(details_json, '$.post_url') = ?
        LIMIT 1
        """,
        (pu,),
    ).fetchone()
    return row is not None


def _already_emailed_any(conn, emails: List[str]) -> bool:
    # Dedupe emails we actually sent (event_type=email_sent) to avoid double-touch.
    for e in emails or []:
        em = (e or "").strip().lower()
        if not em:
            continue
        row = conn.execute(
            """
            SELECT 1
            FROM events e
            JOIN leads l ON l.lead_id = e.lead_id
            WHERE e.event_type = 'email_sent'
              AND l.platform = 'email'
              AND l.contact = ?
            LIMIT 1
            """,
            (em,),
        ).fetchone()
        if row is not None:
            return True
    return False


@dataclass
class CommentTarget:
    lead_id: str
    post_url: str
    author_url: str
    author_name: str
    job_title: str
    snippet: str
    emails: List[str]
    score: int
    status: str


def _parse_raw(raw_json: str) -> Dict[str, Any]:
    try:
        v = json.loads(raw_json or "{}")
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _split_emails(value: str) -> List[str]:
    out: List[str] = []
    for part in (value or "").split(";"):
        e = (part or "").strip().lower()
        if e:
            out.append(e)
    # Prefer non-generic addresses first.
    out = sorted(set(out))
    return out[:5]


def _fetch_targets(conn, *, limit: int, min_score: int) -> List[CommentTarget]:
    rows = conn.execute(
        """
        SELECT lead_id, url, job_title, raw_json, created_at
        FROM leads
        WHERE platform='linkedin' AND lead_type='post'
        ORDER BY created_at DESC
        LIMIT 2500
        """
    ).fetchall()

    out: List[CommentTarget] = []
    seen_posts: set[str] = set()
    for r in rows:
        lead_id = str(r["lead_id"] or "").strip()
        raw = _parse_raw(str(r["raw_json"] or "{}"))
        triage = raw.get("triage") or {}
        flags = triage.get("flags") or {}
        hard_reasons = list(flags.get("hard_reasons") or [])

        status = str(triage.get("status") or "review").strip()
        score = int(triage.get("score") or 0)

        # Keep only relevant, remote QA-ish posts.
        if status not in {"fit", "review"}:
            continue
        if score < int(min_score):
            continue
        if hard_reasons:
            continue
        if not bool(flags.get("qa")):
            continue
        if not bool(flags.get("remote")):
            continue

        post_url = _canonical_url(str(raw.get("post_url") or r["url"] or ""))
        if not post_url or not _is_post_url(post_url):
            continue
        if post_url in seen_posts:
            continue
        if _already_commented_post(conn, post_url=post_url):
            continue
        seen_posts.add(post_url)

        author_url = _canonical_profile_url(str(raw.get("author_url") or ""))
        author_name = str(raw.get("author_name") or "").strip()
        snippet = str(raw.get("snippet") or "").strip()
        emails = _split_emails(str(raw.get("emails") or ""))
        job_title = str(r["job_title"] or "").strip() or str(raw.get("job_title") or "").strip()

        # If we can't even identify a profile, we can still comment by post URL.
        # Dedupe is then only by lead_id.
        if emails and _already_emailed_any(conn, emails):
            continue
        if author_url and _already_contacted_any(conn, lead_id=lead_id, profile_url=author_url):
            continue

        out.append(
            CommentTarget(
                lead_id=lead_id,
                post_url=post_url,
                author_url=author_url,
                author_name=author_name,
                job_title=job_title or snippet[:120],
                snippet=snippet,
                emails=emails,
                score=score,
                status=status,
            )
        )
        if len(out) >= int(limit):
            break
    return out


async def _robust_click(loc, *, timeout_ms: int) -> bool:
    try:
        await loc.scroll_into_view_if_needed(timeout=timeout_ms)
    except Exception:
        pass
    try:
        await loc.click(timeout=timeout_ms)
        return True
    except Exception:
        pass
    try:
        await loc.click(timeout=timeout_ms, force=True)
        return True
    except Exception:
        pass
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


async def _click_comment(page, *, timeout_ms: int) -> bool:
    candidates = [
        page.get_by_role("button", name=COMMENT_BTN_RE).first,
        page.locator("xpath=//span[normalize-space()='Comment']/ancestor::button[1]").first,
        page.locator("button[aria-label^='Comment']:visible").first,
        page.locator("button[aria-label*='Comment']:visible").first,
        page.locator("button:has-text('Comment'):visible").first,
    ]
    for c in candidates:
        try:
            if await c.is_visible(timeout=800):
                return await _robust_click(c, timeout_ms=timeout_ms)
        except Exception:
            continue
    return False


async def _find_comment_box(page):
    candidates = [
        page.locator("div.comments-comment-box__editor div[role='textbox'][contenteditable='true']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true'][aria-label*='Add a comment']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true'][data-placeholder*='Add a comment']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true'][aria-label*='Write a comment']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true'][data-placeholder*='Write a comment']:visible").first,
        page.locator("div[role='textbox'][contenteditable='true'][data-placeholder]:visible").first,
    ]
    for c in candidates:
        try:
            if await c.is_visible(timeout=900):
                return c
        except Exception:
            continue
    return None


async def _comments_disabled_reason(page) -> str:
    """
    LinkedIn can disable comments for non-connections ("Only connections can comment...")
    or disable comments entirely. Detect those states so we skip cleanly.
    """
    blockers = [
        page.locator("div.comments-disabled-comments-block:visible").first,
        page.locator("div:has-text('Only connections can comment on this post'):visible").first,
        page.locator("div:has-text('Comments are turned off'):visible").first,
        page.locator("div:has-text('Comments have been turned off'):visible").first,
    ]
    for b in blockers:
        try:
            if await b.is_visible(timeout=350):
                try:
                    txt = ((await b.inner_text(timeout=700)) or "").strip()
                except Exception:
                    txt = ""
                low = (txt or "").lower()
                if "only connections can comment" in low:
                    return "comments_connections_only"
                if "turned off" in low or "disabled" in low:
                    return "comments_disabled"
                return "comments_blocked"
        except Exception:
            continue
    return ""


async def _fill_comment(page, box, text: str) -> bool:
    try:
        await box.click(timeout=1500)
    except Exception:
        pass
    try:
        # Prefer keyboard typing (more human-like; triggers input handlers reliably).
        await page.keyboard.type(text, delay=random.randint(12, 28))
        # Basic validation: innerText should now contain something.
        try:
            inner = (await box.inner_text(timeout=800)) or ""
            if inner.strip():
                return True
        except Exception:
            return True
    except Exception:
        pass
    try:
        wrote = await box.evaluate(
            """(el, t) => {
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
        return False


async def _click_post_comment(page, box, *, timeout_ms: int) -> bool:
    # Prefer clicking a submit button near the comment box to avoid "Post" elsewhere.
    scopes = []
    try:
        scopes.append(box.locator("xpath=ancestor::form[1]"))
    except Exception:
        pass
    try:
        scopes.append(box.locator("xpath=ancestor::*[contains(@class,'comments-comment-box')][1]"))
    except Exception:
        pass
    scopes.append(page)

    for scope in scopes:
        try:
            btn = scope.get_by_role("button", name=POST_BTN_RE).filter(has_not=scope.locator("[disabled]")).last
            if await btn.is_visible(timeout=600):
                if await _robust_click(btn, timeout_ms=timeout_ms):
                    return True
        except Exception:
            pass
        try:
            btn2 = scope.locator("button:has-text('Post'):visible").last
            if await btn2.is_visible(timeout=600):
                if await _robust_click(btn2, timeout_ms=timeout_ms):
                    return True
        except Exception:
            pass

    return False


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    conn = db_connect(db_path)
    init_db(conn)
    profile = load_profile(conn)
    candidate_name = normalize_person_name(profile.get("candidate.name") or "") or str(cfg_get(cfg, "candidate.name", "Candidate Name"))

    targets = _fetch_targets(conn, limit=args.limit, min_score=args.min_score)
    print(f"[li-comment] targets={len(targets)} db={db_path}")
    if not targets:
        if bool_env("TELEGRAM_REPORT", True):
            send_telegram_message(f"AIJobSearcher: LinkedIn comments report\nTargets: 0\nDB: {db_path}")
        conn.close()
        return 0

    headless = args.headless or bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo_ms = args.slow_mo_ms if args.slow_mo_ms >= 0 else int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    closer = SafeCloser()
    try:
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
            print("[li-comment] not logged in (li_at missing). Run scripts/linkedin_login.py first.")
            return 2

        posted = 0
        skipped = 0
        failed = 0
        posted_urls: List[str] = []
        skipped_reasons: Dict[str, int] = {}
        failed_reasons: Dict[str, int] = {}

        for idx, t in enumerate(targets, 1):
            print(f"[li-comment] {idx}/{len(targets)} {t.post_url}")
            if t.author_url and _already_contacted_any(conn, lead_id=t.lead_id, profile_url=t.author_url):
                skipped += 1
                print(f"[li-comment] skip already_contacted profile={t.author_url}")
                skipped_reasons["already_contacted"] = skipped_reasons.get("already_contacted", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_skipped",
                    occurred_at=_now_iso(),
                    details={"reason": "already_contacted", "post_url": t.post_url, "profile_url": t.author_url},
                )
                conn.commit()
                continue

            add_event(
                conn,
                lead_id=t.lead_id,
                event_type="li_comment_started",
                occurred_at=_now_iso(),
                details={
                    "post_url": t.post_url,
                    "profile_url": t.author_url,
                    "score": t.score,
                    "status": t.status,
                },
            )
            conn.commit()

            try:
                await page.goto(t.post_url, wait_until="domcontentloaded", timeout=args.step_timeout_ms)
            except Exception:
                failed += 1
                await dump_debug(ROOT, page, "comment_open_failed")
                failed_reasons["open_failed"] = failed_reasons.get("open_failed", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_failed",
                    occurred_at=_now_iso(),
                    details={"reason": "open_failed", "post_url": t.post_url, "profile_url": t.author_url},
                )
                conn.commit()
                continue

            if is_checkpoint_url(page.url):
                failed += 1
                await dump_debug(ROOT, page, "comment_checkpoint")
                failed_reasons["checkpoint"] = failed_reasons.get("checkpoint", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_failed",
                    occurred_at=_now_iso(),
                    details={"reason": "checkpoint", "post_url": t.post_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            if not _is_post_url(page.url):
                failed += 1
                await dump_debug(ROOT, page, "comment_wrong_page")
                failed_reasons["unexpected_redirect"] = failed_reasons.get("unexpected_redirect", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "unexpected_redirect", "post_url": t.post_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            # Human-ish pacing.
            await page.wait_for_timeout(random.randint(800, 1600))

            clicked = await _click_comment(page, timeout_ms=1800)
            if not clicked:
                failed += 1
                await dump_debug(ROOT, page, "comment_btn_missing")
                failed_reasons["comment_button_not_found"] = failed_reasons.get("comment_button_not_found", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "comment_button_not_found", "post_url": t.post_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            await page.wait_for_timeout(random.randint(500, 1100))
            disabled_reason = await _comments_disabled_reason(page)
            if disabled_reason:
                skipped += 1
                print(f"[li-comment] skip {disabled_reason}")
                skipped_reasons[disabled_reason] = skipped_reasons.get(disabled_reason, 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_skipped",
                    occurred_at=_now_iso(),
                    details={
                        "reason": disabled_reason,
                        "post_url": t.post_url,
                        "profile_url": t.author_url,
                        "actual_url": page.url,
                    },
                )
                conn.commit()
                continue
            box = await _find_comment_box(page)
            if box is None:
                failed += 1
                await dump_debug(ROOT, page, "comment_box_missing")
                failed_reasons["comment_box_not_found"] = failed_reasons.get("comment_box_not_found", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "comment_box_not_found", "post_url": t.post_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            comment_text = _pick_comment_text(
                candidate_name=candidate_name,
                author_name=t.author_name,
                job_title=t.job_title,
                emails=t.emails,
            )

            ok_fill = await _fill_comment(page, box, comment_text)
            if not ok_fill:
                failed += 1
                await dump_debug(ROOT, page, "comment_fill_failed")
                failed_reasons["comment_fill_failed"] = failed_reasons.get("comment_fill_failed", 0) + 1
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_needs_manual",
                    occurred_at=_now_iso(),
                    details={"reason": "comment_fill_failed", "post_url": t.post_url, "actual_url": page.url},
                )
                conn.commit()
                continue

            await page.wait_for_timeout(random.randint(600, 1300))

            if args.dry_run:
                posted += 1
                posted_urls.append(t.post_url)
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_dry_run",
                    occurred_at=_now_iso(),
                    details={
                        "post_url": t.post_url,
                        "profile_url": t.author_url,
                        "comment": comment_text[:600],
                    },
                )
                conn.commit()
            else:
                ok_post = await _click_post_comment(page, box, timeout_ms=2500)
                if not ok_post:
                    failed += 1
                    await dump_debug(ROOT, page, "comment_post_btn_missing")
                    failed_reasons["comment_post_button_not_found"] = failed_reasons.get("comment_post_button_not_found", 0) + 1
                    add_event(
                        conn,
                        lead_id=t.lead_id,
                        event_type="li_comment_needs_manual",
                        occurred_at=_now_iso(),
                        details={
                            "reason": "comment_post_button_not_found",
                            "post_url": t.post_url,
                            "profile_url": t.author_url,
                        },
                    )
                    conn.commit()
                    continue

                posted += 1
                posted_urls.append(t.post_url)
                add_event(
                    conn,
                    lead_id=t.lead_id,
                    event_type="li_comment_posted",
                    occurred_at=_now_iso(),
                    details={
                        "post_url": t.post_url,
                        "profile_url": t.author_url,
                        "comment": comment_text[:600],
                        "emails_in_post": t.emails,
                    },
                )
                conn.commit()

            # Human rhythm delays.
            if idx < len(targets):
                delay = random.uniform(args.min_delay_sec, args.max_delay_sec)
                print(f"[li-comment] sleep {delay:.1f}s")
                await page.wait_for_timeout(int(delay * 1000))
                if args.long_break_every > 0 and posted > 0 and posted % args.long_break_every == 0:
                    long_delay = random.uniform(args.long_break_min_sec, args.long_break_max_sec)
                    print(f"[li-comment] long break {long_delay:.1f}s")
                    await page.wait_for_timeout(int(long_delay * 1000))

        print(f"[li-comment] done: posted={posted} skipped={skipped} failed={failed}")
        if bool_env("TELEGRAM_REPORT", True):
            lines = []
            lines.append("AIJobSearcher: LinkedIn comments report")
            lines.append(f"posted={posted} skipped={skipped} failed={failed}")
            if skipped_reasons:
                parts = [f"{k}={v}" for k, v in sorted(skipped_reasons.items(), key=lambda kv: (-kv[1], kv[0]))]
                lines.append("skipped_reasons: " + ", ".join(parts)[:800])
            if failed_reasons:
                parts = [f"{k}={v}" for k, v in sorted(failed_reasons.items(), key=lambda kv: (-kv[1], kv[0]))]
                lines.append("failed_reasons: " + ", ".join(parts)[:800])
            if posted_urls:
                lines.append("posted_urls:")
                for u in posted_urls[:10]:
                    lines.append(f"- {u}")
            send_telegram_message("\n".join(lines))
        return 0

    except PlaywrightTimeoutError:
        print("[li-comment] timeout.")
        return 4
    finally:
        try:
            conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Comment on LinkedIn post leads (public comments) with dedupe + pacing.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--min-score", type=int, default=3)
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--slow-mo-ms", type=int, default=-1)
    ap.add_argument("--step-timeout-ms", type=int, default=30_000)
    ap.add_argument("--min-delay-sec", type=float, default=18.0)
    ap.add_argument("--max-delay-sec", type=float, default=45.0)
    ap.add_argument("--long-break-every", type=int, default=5)
    ap.add_argument("--long-break-min-sec", type=float, default=120.0)
    ap.add_argument("--long-break-max-sec", type=float, default=260.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--timeout-seconds", type=int, default=3600)
    args = ap.parse_args()
    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-comment] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())



