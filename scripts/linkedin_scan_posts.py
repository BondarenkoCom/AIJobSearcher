import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set
from urllib.parse import urlparse, urlunparse

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, dump_debug, ensure_linkedin_session, int_env, is_checkpoint_url  # noqa: E402


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
HARD_BLOCK_PATTERNS = [
    (re.compile(r"\bw2\s+only\b", re.IGNORECASE), "w2_only"),
    (re.compile(r"\bus\s+citizen[s]?\s+only\b", re.IGNORECASE), "us_citizens_only"),
    (re.compile(r"\bgreen\s*card\s+holder[s]?\s+only\b", re.IGNORECASE), "green_card_only"),
    (re.compile(r"\bus\s+citizens?\s*&\s*green\s*card\b", re.IGNORECASE), "us_gc_only"),
    (re.compile(r"\bauthorized\s+to\s+work\s+in\s+the\s+us\b", re.IGNORECASE), "us_work_auth_required"),
    (re.compile(r"\bno\s+c2c\b", re.IGNORECASE), "no_c2c"),
    (re.compile(r"\bon[-\s]?site\s+only\b", re.IGNORECASE), "onsite_only"),
]
HIRING_RE = re.compile(
    r"\b(we[' ]?re hiring|hiring|looking for|open role|open position|vacancy|job opening|interested candidates)\b",
    re.IGNORECASE,
)
QA_RE = re.compile(r"\b(qa|quality assurance|sdet|test automation|software testing|tester)\b", re.IGNORECASE)
REMOTE_RE = re.compile(
    r"\b(remote|distributed|work from anywhere|global remote|work from home|work-from-home|wfh|home[-\s]?based)\b",
    re.IGNORECASE,
)
DM_RE = re.compile(r"\b(dm me|message me|inbox me|reach out|connect with me)\b", re.IGNORECASE)
STARTUP_RE = re.compile(r"\b(startup|seed|series a|series b|founding|0\s*to\s*1|build from scratch)\b", re.IGNORECASE)


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _slug(s: str) -> str:
    t = re.sub(r"[^a-z0-9]+", "_", (s or "").lower()).strip("_")
    return (t or "query")[:48]


def _safe_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8", errors="replace")


async def _dump_debug(page, tag: str) -> None:
    html_path, png_path = await dump_debug(ROOT, page, f"posts_{tag}")
    if html_path:
        print(f"[li-posts] debug html: {html_path}")
    if png_path:
        print(f"[li-posts] debug png:  {png_path}")


def _search_url(query: str, *, sort: str = "top") -> str:
    from urllib.parse import quote_plus

    q = quote_plus(query.strip())
    url = f"https://www.linkedin.com/search/results/content/?keywords={q}&origin=GLOBAL_SEARCH_HEADER"
    if (sort or "").strip().lower() in {"latest", "date", "recent"}:
        url += "&sortBy=DATE_POSTED"
    return url


async def _extract_visible_posts(page) -> List[Dict[str, str]]:
    js = """
    () => {
      const out = [];
      const seen = new Set();

      const anchors = Array.from(document.querySelectorAll('a[href]'));
      const postAnchors = anchors.filter(a => {
        const h = a.getAttribute('href') || '';
        return (
          h.includes('/feed/update/') ||
          h.includes('/posts/') ||
          h.includes('urn:li:activity:')
        );
      });

      function abs(href) {
        try { return new URL(href, location.origin).toString(); } catch (e) { return href; }
      }

      for (const a of postAnchors) {
        const postUrl = abs(a.getAttribute('href') || '');
        if (!postUrl) continue;

        // Best-effort container: LI or DIV around the anchor.
        const container = a.closest('li, div') || a.parentElement;
        const text = (container?.innerText || '').replace(/\\s+/g, ' ').trim();
        const snippet = text.slice(0, 600);

        // Author/profile link often exists in the same result card, but not always
        // in the immediate anchor parent. Climb a few ancestors to increase hit rate.
        let authorUrl = '';
        let authorName = '';
        let ctx = container;
        for (let i = 0; i < 7 && ctx && !authorUrl; i++) {
          const profileA = ctx.querySelector ? ctx.querySelector('a[href*=\"/in/\"], a[href*=\"/company/\"]') : null;
          if (profileA) {
            authorUrl = abs(profileA.getAttribute('href') || '');
            authorName = (profileA.innerText || '').replace(/\\s+/g, ' ').trim();
            break;
          }
          ctx = ctx.parentElement;
        }

        const key = postUrl + '|' + authorUrl;
        if (seen.has(key)) continue;
        seen.add(key);
        out.push({ post_url: postUrl, author_url: authorUrl, author_name: authorName, snippet });
      }

      // Newer LinkedIn content search often renders cards where the main text
      // is in ".update-components-entity__description-container".
      const descNodes = Array.from(document.querySelectorAll('.update-components-entity__description-container'));
      for (const d of descNodes) {
        let container = d.closest('.fie-impression-container, li');
        if (!container) {
          let p = d.parentElement;
          for (let i = 0; i < 7 && p; i++) {
            if (p.querySelectorAll && p.querySelectorAll('a[href]').length >= 2) {
              container = p;
              break;
            }
            p = p.parentElement;
          }
        }
        if (!container) container = d.parentElement || d;
        if (!container) continue;

        const allLinks = Array.from(container.querySelectorAll('a[href]'))
          .map((a) => abs(a.getAttribute('href') || ''))
          .filter(Boolean);
        const uniqLinks = Array.from(new Set(allLinks));
        const profileLinks = uniqLinks.filter((h) => h.includes('/in/') || h.includes('/company/'));
        const contentLinks = uniqLinks
          .filter((h) => !(h.includes('/in/') || h.includes('/company/')))
          .filter((h) => h.includes('/feed/update/') || h.includes('/posts/') || h.includes('urn:li:activity:'));

        let postUrl = '';
        const preferred = contentLinks.find((h) => h.includes('/feed/update/') || h.includes('/posts/'));
        postUrl = preferred || contentLinks[0] || '';
        if (!postUrl) continue;
        let authorUrl = profileLinks[0] || '';
        if (!authorUrl) {
          const profileA = container.querySelector('a[href*=\"/in/\"], a[href*=\"/company/\"]');
          if (profileA) authorUrl = abs(profileA.getAttribute('href') || '');
        }
        const key = postUrl + '|' + authorUrl;
        if (seen.has(key)) continue;
        seen.add(key);
        let authorName = '';
        const profileA = container.querySelector('a[href*="/in/"], a[href*="/company/"]');
        if (profileA) {
          authorName = (profileA.innerText || '').replace(/\\s+/g, ' ').trim();
        }

        const snippet = ((d.innerText || container.innerText || '').replace(/\\s+/g, ' ').trim()).slice(0, 600);
        out.push({ post_url: postUrl, author_url: authorUrl, author_name: authorName, snippet });
      }

      return out;
    }
    """
    return await page.evaluate(js)


def _extract_emails(text: str) -> str:
    if not text:
        return ""
    emails = sorted({m.group(0).lower() for m in EMAIL_RE.finditer(text)})
    return ";".join(emails)


def _guess_title(snippet: str) -> str:
    if not snippet:
        return "LinkedIn post lead"
    first = re.split(r"[.\n]", snippet, maxsplit=1)[0].strip()
    if not first:
        return "LinkedIn post lead"
    return first[:140]


def _classify_post(snippet: str, *, query: str = "") -> Dict[str, object]:
    txt = (snippet or "").strip()
    hard_reasons: List[str] = []
    for rx, code in HARD_BLOCK_PATTERNS:
        if rx.search(txt):
            hard_reasons.append(code)

    hiring = bool(HIRING_RE.search(txt))
    qa = bool(QA_RE.search(txt))
    remote = bool(REMOTE_RE.search(txt))
    q = (query or "").lower()
    if (not remote) and ("remote" in q):
        remote = True
    dm_open = bool(DM_RE.search(txt))
    startup = bool(STARTUP_RE.search(txt))

    score = 0
    if qa:
        score += 2
    if hiring:
        score += 2
    if remote:
        score += 1
    if startup:
        score += 1

    if hard_reasons:
        status = "skip_hard"
        action = "skip"
    elif qa and hiring:
        status = "fit"
        action = "dm" if dm_open else "connect"
    elif qa or hiring:
        status = "review"
        action = "dm" if dm_open else "connect"
    else:
        status = "weak"
        action = "skip"

    return {
        "status": status,
        "action": action,
        "score": score,
        "flags": {
            "hiring": hiring,
            "qa": qa,
            "remote": remote,
            "dm_open": dm_open,
            "startup_hint": startup,
            "hard_reasons": hard_reasons,
        },
    }


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        if "linkedin.com" not in (p.netloc or "").lower():
            return u
        path = (p.path or "").rstrip("/")
        if path.startswith("/jobs/view/") or path.startswith("/feed/update/") or path.startswith("/posts/"):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _open_db_from_config(config_path: str):
    cfg_path = resolve_path(ROOT, config_path)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    conn = db_connect(db_path)
    init_db(conn)
    return conn, db_path


def _persist_posts(conn, *, query: str, rows: List[Dict[str, str]]) -> Dict[str, int]:
    stats = {"inserted": 0, "updated": 0, "fit": 0, "review": 0, "skip_hard": 0, "weak": 0}
    for row in rows:
        post_url = _canonical_url((row.get("post_url") or "").strip())
        author_url = (row.get("author_url") or "").strip()
        author_name = (row.get("author_name") or "").strip()
        snippet = (row.get("snippet") or "").strip()
        triage = _classify_post(snippet, query=query)
        status = str(triage.get("status") or "review")
        action = str(triage.get("action") or "connect")

        contact = author_url or post_url
        lead = LeadUpsert(
            platform="linkedin",
            lead_type="post",
            contact=contact,
            url=post_url,
            company=author_name,
            job_title=_guess_title(snippet),
            location="",
            source=f"linkedin_content:{query}",
            raw={
                "query": query,
                "post_url": post_url,
                "author_name": author_name,
                "author_url": author_url,
                "emails": row.get("emails", ""),
                "snippet": snippet,
                "triage": triage,
            },
        )
        lead_id, inserted = upsert_lead_with_flag(conn, lead)
        try:
            conn.execute(
                "UPDATE leads SET raw_json = ? WHERE lead_id = ?",
                (json.dumps(lead.raw or {}, ensure_ascii=False), lead_id),
            )
        except Exception:
            pass
        if inserted:
            stats["inserted"] += 1
        else:
            stats["updated"] += 1
        stats[status] = stats.get(status, 0) + 1

        add_event(
            conn,
            lead_id=lead_id,
            event_type="li_post_triage",
            status=status,
            details={
                "query": query,
                "action": action,
                "score": triage.get("score", 0),
                "flags": triage.get("flags", {}),
                "post_url": post_url,
                "author_url": author_url,
            },
        )
    conn.commit()
    return stats


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    closer = SafeCloser()
    db_conn = None
    try:
        if args.write_db:
            db_conn, db_path = _open_db_from_config(args.config)
            print(f"[li-posts] db: {db_path}")

        closer.pw = await async_playwright().start()
        chromium_args = ["--start-maximized"]
        extra_headers = None
        if args.force_en:
            chromium_args.append("--lang=en-US")
            extra_headers = {"Accept-Language": "en-US,en;q=0.9"}
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US" if args.force_en else None,
            extra_http_headers=extra_headers,
            timezone_id=args.timezone_id or None,
            args=chromium_args,
        )
        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        if not await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page):
            print("[li-posts] not logged in (li_at cookie missing). Run scripts/linkedin_login.py first.")
            return 2

        url = _search_url(args.query, sort=args.sort)
        print(f"[li-posts] open: {url}")
        await page.goto(url, wait_until="domcontentloaded")
        if is_checkpoint_url(page.url):
            await _dump_debug(page, "checkpoint")
            print("[li-posts] blocked by checkpoint/captcha; stopping.")
            return 4

        try:
            await page.wait_for_timeout(1500)
        except Exception:
            pass

        collected: List[Dict[str, str]] = []
        seen_posts: Set[str] = set()

        for i in range(args.scrolls):
            batch = await _extract_visible_posts(page)
            for item in batch:
                post_url = (item.get("post_url") or "").strip()
                author_url = (item.get("author_url") or "").strip()
                key = f"{post_url}|{author_url}"
                if not post_url or key in seen_posts:
                    continue
                seen_posts.add(key)

                snippet = (item.get("snippet") or "").strip()
                emails = _extract_emails(snippet)
                collected.append(
                    {
                        "captured_at": datetime.now().isoformat(timespec="seconds"),
                        "query": args.query,
                        "post_url": post_url,
                        "author_name": (item.get("author_name") or "").strip(),
                        "author_url": author_url,
                        "emails": emails,
                        "snippet": snippet,
                    }
                )

            print(f"[li-posts] scroll {i+1}/{args.scrolls}: posts={len(collected)}")
            if len(collected) >= args.limit:
                break

            await page.mouse.wheel(0, args.scroll_px)
            await page.wait_for_timeout(args.scroll_wait_ms)

        if not collected:
            await _dump_debug(page, "no_results")
            print("[li-posts] no posts collected (see debug dump).")
            return 3

        out_dir = (ROOT / args.out_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        out_csv = out_dir / f"linkedin_posts_{_slug(args.query)}_{stamp}.csv"

        collected = collected[: args.limit]

        import csv

        with out_csv.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=["captured_at", "query", "post_url", "author_name", "author_url", "emails", "snippet"],
            )
            w.writeheader()
            w.writerows(collected)

        print(f"[li-posts] wrote {out_csv} (rows={len(collected)})")
        if db_conn is not None:
            db_stats = _persist_posts(db_conn, query=args.query, rows=collected)
            print(
                "[li-posts] db upsert:"
                f" inserted={db_stats.get('inserted', 0)}"
                f" updated={db_stats.get('updated', 0)}"
                f" fit={db_stats.get('fit', 0)}"
                f" review={db_stats.get('review', 0)}"
                f" skip_hard={db_stats.get('skip_hard', 0)}"
                f" weak={db_stats.get('weak', 0)}"
            )
        return 0
    except PlaywrightTimeoutError:
        print("[li-posts] timeout; closing browser...")
        return 4
    except KeyboardInterrupt:
        print("[li-posts] interrupted; closing browser...")
        return 130
    except Exception as e:
        print(f"[li-posts] error: {e}")
        try:
            if closer.ctx and closer.ctx.pages:
                await _dump_debug(closer.ctx.pages[0], "error")
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
    ap = argparse.ArgumentParser(description="Scan LinkedIn posts via content search (requires logged-in session)")
    ap.add_argument("--config", default="config/config.yaml", help="Config path (for activity DB path)")
    ap.add_argument("--write-db", action="store_true", help="Also upsert post leads and triage into activity DB")
    ap.add_argument("--query", required=True, help="Search query, e.g. 'hiring QA remote'")
    ap.add_argument("--sort", default="top", help="Sort: top (default) or latest")
    ap.add_argument("--limit", type=int, default=50, help="Max posts to save")
    ap.add_argument("--scrolls", type=int, default=12, help="How many scroll steps to perform")
    ap.add_argument("--scroll-px", type=int, default=1600, help="Pixels per scroll")
    ap.add_argument("--scroll-wait-ms", type=int, default=1800, help="Wait after each scroll")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step timeout")
    ap.add_argument("--timeout-seconds", type=int, default=180, help="Overall timeout (kills browser if stuck)")
    ap.add_argument("--out-dir", default="data/out", help="Output dir")
    ap.add_argument("--force-en", action="store_true", help="Try to force en-US locale for the browser context")
    ap.add_argument("--timezone-id", default="", help="Optional tz id, e.g. 'Asia/Ho_Chi_Minh'")
    args = ap.parse_args()

    args.force_en = True

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-posts] hard timeout hit; exiting.")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
