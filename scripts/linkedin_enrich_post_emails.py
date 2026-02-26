import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple
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


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        if "linkedin.com" not in (p.netloc or "").lower():
            return u
        path = (p.path or "").rstrip("/")
        if path.startswith("/feed/update/") or path.startswith("/posts/") or path.startswith("/jobs/view/"):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _split_emails(value: str) -> List[str]:
    if not value:
        return []
    out: List[str] = []
    for part in value.split(";"):
        e = (part or "").strip().lower()
        if e:
            out.append(e)
    return sorted(set(out))


def _extract_emails_from_text(text: str) -> List[str]:
    if not text:
        return []
    emails = {m.group(0).lower() for m in EMAIL_RE.finditer(text)}
    return sorted(emails)


def _open_db(config_path: str):
    cfg_path = resolve_path(ROOT, config_path)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    conn = db_connect(db_path)
    init_db(conn)
    return conn, db_path


def _load_post_leads(conn, *, limit: int) -> List[Dict[str, str]]:
    sql = """
    SELECT lead_id, url, company, job_title, raw_json
    FROM leads
    WHERE platform='linkedin' AND lead_type='post'
    ORDER BY created_at DESC
    """
    params: Tuple[object, ...] = ()
    if limit > 0:
        sql += " LIMIT ?"
        params = (limit,)
    rows = conn.execute(sql, params).fetchall()
    out: List[Dict[str, str]] = []
    for r in rows:
        raw: Dict[str, object] = {}
        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}
        post_url = _canonical_url(str(raw.get("post_url") or r["url"] or ""))
        out.append(
            {
                "lead_id": str(r["lead_id"] or ""),
                "post_url": post_url,
                "company": str(r["company"] or ""),
                "job_title": str(r["job_title"] or ""),
                "raw_json": str(r["raw_json"] or "{}"),
            }
        )
    return out


async def _expand_more(page) -> None:
    js = """
    () => {
      let clicked = 0;
      const textMatch = (s) => {
        const t = (s || '').toLowerCase().trim();
        return t === 'more' || t.includes('see more') || t.includes('...more');
      };
      const nodes = Array.from(document.querySelectorAll('button, span, a[role="button"], div[role="button"]'));
      for (const el of nodes) {
        if (clicked >= 10) break;
        const rect = el.getBoundingClientRect();
        if (rect.width < 1 || rect.height < 1) continue;
        if (!textMatch(el.innerText || el.textContent)) continue;
        try {
          el.click();
          clicked += 1;
        } catch (_) {}
      }
      return clicked;
    }
    """
    try:
        await page.evaluate(js)
    except Exception:
        return


async def _extract_from_page(page) -> Dict[str, object]:
    js = """
    () => {
      const bodyText = (document.body?.innerText || '').replace(/\\u00a0/g, ' ');
      const rx = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\\.[A-Z]{2,}/gi;
      const txt = bodyText.match(rx) || [];
      const mailto = Array.from(document.querySelectorAll('a[href^="mailto:"]'))
        .map(a => (a.getAttribute('href') || '').replace(/^mailto:/i, '').split('?')[0].trim().toLowerCase())
        .filter(Boolean);
      const all = Array.from(new Set([...txt.map(x => x.toLowerCase()), ...mailto]));
      const h1 = (document.querySelector('h1')?.innerText || '').trim();
      return {
        emails: all,
        title: h1 || (document.title || '').trim(),
        excerpt: bodyText.slice(0, 800)
      };
    }
    """
    data = await page.evaluate(js)
    if not isinstance(data, dict):
        return {"emails": [], "title": "", "excerpt": ""}
    emails = [str(x).strip().lower() for x in (data.get("emails") or []) if str(x).strip()]
    emails = sorted(set(emails))
    return {
        "emails": emails,
        "title": str(data.get("title") or "").strip(),
        "excerpt": str(data.get("excerpt") or "").strip(),
    }


def _merge_raw(raw_json: str, *, emails: List[str], page_title: str, excerpt: str) -> str:
    try:
        raw = json.loads(raw_json or "{}")
        if not isinstance(raw, dict):
            raw = {}
    except Exception:
        raw = {}

    prev = _split_emails(str(raw.get("emails") or ""))
    merged = sorted(set(prev).union(set(emails)))
    raw["emails"] = ";".join(merged)
    raw["email_scan"] = {
        "updated_at": _now_iso(),
        "emails_count": len(merged),
        "page_title": page_title,
    }
    if excerpt:
        raw["snippet"] = excerpt[:600]
    return json.dumps(raw, ensure_ascii=False, sort_keys=True)


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    conn, db_path = _open_db(args.config)
    print(f"[post-email] db: {db_path}")
    leads = _load_post_leads(conn, limit=args.limit)
    print(f"[post-email] leads to scan: {len(leads)}")
    if not leads:
        return 0

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    closer = SafeCloser()
    stats = {
        "scanned": 0,
        "updated": 0,
        "with_email": 0,
        "new_email_rows": 0,
        "checkpoint": 0,
        "failed": 0,
    }
    found_rows: List[Dict[str, str]] = []
    seen_email_pairs: set[Tuple[str, str]] = set()

    try:
        closer.pw = await async_playwright().start()
        chromium_args = ["--start-maximized", "--lang=en-US"]
        extra_headers = {"Accept-Language": "en-US,en;q=0.9"}
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US",
            extra_http_headers=extra_headers,
            args=chromium_args,
        )
        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        if not await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page):
            print("[post-email] not logged in (li_at missing). Run scripts/linkedin_login.py first.")
            return 2

        for idx, lead in enumerate(leads, 1):
            lead_id = lead["lead_id"]
            post_url = lead["post_url"]
            if not post_url:
                stats["failed"] += 1
                continue

            print(f"[post-email] {idx}/{len(leads)} {post_url}")
            try:
                await page.goto(post_url, wait_until="domcontentloaded", timeout=args.step_timeout_ms)
                if is_checkpoint_url(page.url):
                    stats["checkpoint"] += 1
                    await dump_debug(ROOT, page, "post_email_checkpoint")
                    continue
                await page.wait_for_timeout(1200)
                await _expand_more(page)
                await page.wait_for_timeout(400)
                extracted = await _extract_from_page(page)
            except PlaywrightTimeoutError:
                stats["failed"] += 1
                await dump_debug(ROOT, page, "post_email_timeout")
                continue
            except Exception:
                stats["failed"] += 1
                await dump_debug(ROOT, page, "post_email_error")
                continue

            stats["scanned"] += 1
            found = [e for e in extracted.get("emails", []) if "@" in e]
            if not found:
                fallback = _extract_emails_from_text(str(extracted.get("excerpt") or ""))
                found = fallback
            if found:
                stats["with_email"] += 1

            raw_new = _merge_raw(
                lead["raw_json"],
                emails=found,
                page_title=str(extracted.get("title") or ""),
                excerpt=str(extracted.get("excerpt") or ""),
            )

            with conn:
                conn.execute("UPDATE leads SET raw_json = ? WHERE lead_id = ?", (raw_new, lead_id))
                if found:
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="li_post_email_found",
                        status="ok",
                        details={
                            "emails": found,
                            "post_url": post_url,
                            "emails_count": len(found),
                        },
                    )
                else:
                    add_event(
                        conn,
                        lead_id=lead_id,
                        event_type="li_post_email_scan",
                        status="none",
                        details={"post_url": post_url, "emails_count": 0},
                    )
                conn.commit()
            stats["updated"] += 1

            for email in found:
                pair = (lead_id, email)
                if pair in seen_email_pairs:
                    continue
                seen_email_pairs.add(pair)
                found_rows.append(
                    {
                        "lead_id": lead_id,
                        "email": email,
                        "company": lead["company"],
                        "job_title": lead["job_title"],
                        "post_url": post_url,
                    }
                )
                stats["new_email_rows"] += 1

            await page.wait_for_timeout(args.delay_ms)

        out_dir = (ROOT / args.out_dir).resolve()
        out_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
        out_json = out_dir / f"linkedin_post_email_enrich_{stamp}.json"
        out_payload = {"stats": stats, "rows": found_rows}
        out_json.write_text(json.dumps(out_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[post-email] report: {out_json}")
        print(
            "[post-email] done:"
            f" scanned={stats['scanned']}"
            f" updated={stats['updated']}"
            f" with_email={stats['with_email']}"
            f" email_rows={stats['new_email_rows']}"
            f" checkpoint={stats['checkpoint']}"
            f" failed={stats['failed']}"
        )
        return 0
    finally:
        try:
            conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Enrich saved LinkedIn post leads with emails extracted from post pages")
    ap.add_argument("--config", default="config/config.yaml", help="Config path")
    ap.add_argument("--limit", type=int, default=0, help="How many saved post leads to scan (0 = all)")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-page timeout")
    ap.add_argument("--delay-ms", type=int, default=850, help="Delay between post scans")
    ap.add_argument("--out-dir", default="data/out", help="Output dir for report")
    ap.add_argument("--timeout-seconds", type=int, default=3600, help="Hard timeout")
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[post-email] hard timeout hit; exiting.")
        return 4


if __name__ == "__main__":
    raise SystemExit(main())
