import argparse
import asyncio
import csv
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, dump_debug, ensure_linkedin_session, goto_guarded, int_env  # noqa: E402


VN_RE = re.compile(r"vietnam|viet nam|ho chi minh|hcmc|hanoi|da nang|saigon", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _split_doc_title(doc_title: str) -> Tuple[str, str, str]:
    """
    LinkedIn job pages often have a stable document.title even when the DOM changes.

    Common format:
      "{job_title} | {workplace} | {company} | LinkedIn"

    Returns: (job_title, workplace, company)
    """
    t = (doc_title or "").strip()
    if not t:
        return ("", "", "")

    parts = [p.strip() for p in t.split(" | ")]

    if len(parts) >= 4 and parts[-1].lower().startswith("linkedin"):
        company = parts[-2].strip()
        workplace = parts[-3].strip()
        job_title = " | ".join(parts[:-3]).strip()
        return (job_title, workplace, company)

    if len(parts) >= 3 and parts[-1].lower().startswith("linkedin"):
        company = parts[-2].strip()
        job_title = " | ".join(parts[:-2]).strip()
        return (job_title, "", company)

    return (t, "", "")


def _canonical_job_url(href: str) -> str:
    if not href:
        return ""
    m = re.search(r"/jobs/view/([0-9]+)", href)
    if not m:
        return ""
    job_id = m.group(1)
    return f"https://www.linkedin.com/jobs/view/{job_id}/"


def _jobs_search_url(query: str, location: str) -> str:
    q = quote_plus((query or "").strip())
    loc = quote_plus((location or "").strip())
    return f"https://www.linkedin.com/jobs/search/?keywords={q}&location={loc}"


async def _scroll_results(page, px: int) -> str:
    """
    LinkedIn Jobs uses a scrollable results list container (two-pane layout).
    Scrolling the window often does nothing, so we try to scroll the list element.
    """
    js = """
    (px) => {
      const candidates = [
        document.querySelector('div.scaffold-layout__list > div'),
        document.querySelector('div.scaffold-layout__list'),
        document.querySelector('.jobs-search-results-list'),
        document.querySelector('.scaffold-layout__list'),
      ].filter(Boolean);

      let el = null;
      for (const c of candidates) {
        const cs = getComputedStyle(c);
        if ((cs.overflowY === 'auto' || cs.overflowY === 'scroll') && c.scrollHeight > (c.clientHeight + 50)) {
          el = c;
          break;
        }
      }
      if (!el) el = candidates[0] || null;

      if (el) {
        el.scrollBy(0, px);
        return 'list';
      }
      window.scrollBy(0, px);
      return 'window';
    }
    """
    try:
        return str(await page.evaluate(js, px))
    except Exception:
        try:
            await page.mouse.wheel(0, px)
        except Exception:
            pass
        return "wheel"


async def _extract_job_links(page) -> List[str]:
    js = """
    () => {
      const out = [];
      // Prefer explicit anchors when present.
      const as = Array.from(document.querySelectorAll('a[href]'));
      for (const a of as) {
        const h = a.getAttribute('href') || '';
        if (!h.includes('/jobs/view/')) continue;
        out.push(h);
      }
      // Some list items are virtualized; capture job IDs from attributes as a fallback.
      const lis = Array.from(document.querySelectorAll('li[data-occludable-job-id]'));
      for (const li of lis) {
        const id = li.getAttribute('data-occludable-job-id') || '';
        if (!id) continue;
        out.push('/jobs/view/' + id + '/');
      }
      return out;
    }
    """
    hrefs = await page.evaluate(js)
    if not isinstance(hrefs, list):
        return []
    urls: List[str] = []
    for h in hrefs:
        if not isinstance(h, str):
            continue
        u = _canonical_job_url(h)
        if u:
            urls.append(u)
    seen: Set[str] = set()
    uniq: List[str] = []
    for u in urls:
        if u in seen:
            continue
        seen.add(u)
        uniq.append(u)
    return uniq


async def _extract_job_detail(page) -> Dict[str, str]:
    try:
        doc_title = await page.title()
    except Exception:
        doc_title = ""

    title_from_doc, workplace, company_from_doc = _split_doc_title(doc_title)

    company = ""
    try:
        company = await page.evaluate(
            "() => (document.querySelector('a[href*=\"/company/\"]')?.innerText || '').trim()"
        )
    except Exception:
        company = ""
    if not company:
        company = company_from_doc

    location = ""
    snippet = ""
    text = ""
    try:
        text = await page.evaluate("() => (document.body?.innerText || '')")
    except Exception:
        text = ""

    if text:
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

        if not workplace:
            for ln in lines:
                if ln in {"Remote", "Hybrid", "On-site"}:
                    workplace = ln
                    break
        bullet = "\u00b7"
        for ln in lines:
            lnl = ln.lower()
            if bullet in ln and ("applicant" in lnl or "reposted" in lnl or "posted" in lnl):
                location = ln.split(bullet)[0].strip()
                break

        for i, ln in enumerate(lines):
            if ln.lower() == "about the job":
                snippet = " ".join(lines[i + 1 : i + 6]).strip()[:280]
                break

    return {
        "title": title_from_doc,
        "workplace": workplace,
        "company": company,
        "location": location,
        "snippet": snippet,
    }


async def _extract_apply(page) -> Tuple[str, str]:
    """
    Returns (application_route, application_url)
    application_route: platform | external | unknown
    """
    easy_a = page.locator("a[href*='openSDUIApplyFlow=true'], a[href*='/apply/?openSDUIApplyFlow=true']").first
    try:
        if await easy_a.is_visible(timeout=1_500):
            href = (await easy_a.get_attribute("href")) or ""
            if href.startswith("/"):
                href = "https://www.linkedin.com" + href
            return ("platform", href)
    except Exception:
        pass

    easy_btn = page.get_by_role("button", name=re.compile(r"easy apply", re.IGNORECASE))
    try:
        if await easy_btn.first.is_visible(timeout=1_500):
            return ("platform", "")
    except Exception:
        pass

    try:
        ext = page.locator("a[href^='http']").filter(has_text=re.compile(r"\\bapply\\b", re.IGNORECASE)).first
        if await ext.is_visible(timeout=1_500):
            href = (await ext.get_attribute("href")) or ""
            if href and "linkedin.com" not in href:
                return ("external", href)
    except Exception:
        pass

    return ("unknown", "")


def _is_remote(location: str) -> bool:
    return "remote" in (location or "").lower()


def _is_vietnam(location: str) -> bool:
    return bool(VN_RE.search(location or ""))


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    closer = SafeCloser()
    db_conn = None
    try:
        db_conn = db_connect(db_path)
        init_db(db_conn)

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

        ok = await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page, timeout_ms=args.step_timeout_ms)
        if not ok:
            print("[li-jobs] not logged in / checkpoint. Run scripts/linkedin_login.py first.")
            return 2

        base_search_url = _jobs_search_url(args.query, args.location)
        print(f"[li-jobs] base search: {base_search_url}")

        page_size = 25
        candidates: List[str] = []
        seen: Set[str] = set()
        stale_pages = 0
        for i in range(args.scrolls):
            start = i * page_size
            search_url = base_search_url if start == 0 else (base_search_url + f"&start={start}")
            print(f"[li-jobs] page {i+1}/{args.scrolls}: {search_url}")

            ok = await goto_guarded(
                root=ROOT,
                page=page,
                url=search_url,
                timeout_ms=args.step_timeout_ms,
                tag_on_fail="jobs_search_failed",
            )
            if not ok:
                print("[li-jobs] search page failed (checkpoint/captcha?)")
                if candidates:
                    print(f"[li-jobs] proceeding with collected candidates (n={len(candidates)})")
                    break
                return 3

            await page.wait_for_timeout(1200)
            try:
                await page.wait_for_selector("li.scaffold-layout__list-item, li[data-occludable-job-id]", timeout=10_000)
            except Exception:
                pass

            links = await _extract_job_links(page)
            new_on_page = 0
            for u in links:
                if u in seen:
                    continue
                seen.add(u)
                candidates.append(u)
                new_on_page += 1

                try:
                    lead_id, inserted = upsert_lead_with_flag(
                        db_conn,
                        LeadUpsert(
                            platform="linkedin",
                            lead_type="job",
                            contact=u,
                            url=u,
                            company="",
                            job_title="",
                            location="",
                            source=f"linkedin_jobs:{args.query}",
                            created_at=_now_iso(),
                            raw=None,
                        ),
                    )
                    if inserted:
                        add_event(
                            db_conn,
                            lead_id=lead_id,
                            event_type="li_job_candidate",
                            status="ok",
                            occurred_at=_now_iso(),
                            details={"query": args.query},
                        )
                    db_conn.commit()
                except Exception:
                    pass

            print(f"[li-jobs] candidates={len(candidates)} (+{new_on_page})")
            if len(candidates) >= args.limit * args.candidate_multiplier:
                break
            if new_on_page == 0:
                stale_pages += 1
                if stale_pages >= 2:
                    print("[li-jobs] no new candidates on multiple pages; stopping pagination early")
                    break
            else:
                stale_pages = 0

            await page.wait_for_timeout(args.scroll_wait_ms)

        if not candidates:
            await dump_debug(ROOT, page, "jobs_no_candidates")
            print("[li-jobs] no candidates collected (see debug dump).")
            return 4

        detail = await closer.ctx.new_page()
        detail.set_default_timeout(args.step_timeout_ms)
        detail.set_default_navigation_timeout(args.step_timeout_ms)

        out_rows: List[Dict[str, str]] = []
        collected_urls: Set[str] = set()
        filtered_out: Dict[str, int] = {"vietnam": 0, "not_remote": 0}

        for idx, job_url in enumerate(candidates):
            if len(out_rows) >= args.limit:
                break
            if job_url in collected_urls:
                continue
            if not args.include_seen:
                try:
                    if db_conn.execute(
                        """
                        SELECT 1
                        FROM leads l
                        JOIN events e ON e.lead_id = l.lead_id
                        WHERE l.platform = 'linkedin'
                          AND l.lead_type = 'job'
                          AND l.contact = ?
                          AND e.event_type IN ('collected', 'li_job_filtered_out')
                        LIMIT 1
                        """,
                        (job_url.lower(),),
                    ).fetchone():
                        continue
                except Exception:
                    pass

            ok = await goto_guarded(root=ROOT, page=detail, url=job_url, timeout_ms=args.step_timeout_ms, tag_on_fail="job_open_failed")
            if not ok:
                print("[li-jobs] blocked (checkpoint/captcha) while opening job; stopping.")
                break

            await detail.wait_for_timeout(random.randint(900, 1600))

            data = await _extract_job_detail(detail)
            title = data.get("title", "")
            workplace = data.get("workplace", "")
            company = data.get("company", "")
            location = data.get("location", "")
            snippet = data.get("snippet", "")

            remote = _is_remote(workplace) or _is_remote(location)
            vietnam = _is_vietnam(location)

            captured_at = _now_iso()
            row = {
                "captured_at": captured_at,
                "query": args.query,
                "job_title": title,
                "company": company,
                "location": location,
                "workplace": workplace,
                "remote": "1" if remote else "0",
                "job_url": job_url,
                "apply_type": "",
                "apply_url": "",
                "snippet": snippet,
            }

            loc_store = location
            if workplace:
                if location and workplace.lower() not in location.lower():
                    loc_store = f"{location} ({workplace})"
                elif not location:
                    loc_store = workplace

            reject_reason = ""
            if args.exclude_vietnam and vietnam:
                reject_reason = "vietnam"
            elif args.remote_only and not remote:
                if not (args.allow_vietnam and vietnam):
                    reject_reason = "not_remote"

            if reject_reason:
                try:
                    lead_id, _inserted = upsert_lead_with_flag(
                        db_conn,
                        LeadUpsert(
                            platform="linkedin",
                            lead_type="job",
                            contact=job_url,
                            url=job_url,
                            company=company,
                            job_title=title,
                            location=loc_store,
                            source=f"linkedin_jobs:{args.query}",
                            created_at=captured_at,
                            raw={**row, "filtered_out": reject_reason},
                        ),
                    )
                    add_event(
                        db_conn,
                        lead_id=lead_id,
                        event_type="li_job_filtered_out",
                        status="ok",
                        occurred_at=captured_at,
                        details={"reason": reject_reason},
                    )
                    db_conn.commit()
                except Exception:
                    pass
                filtered_out[reject_reason] = filtered_out.get(reject_reason, 0) + 1
                continue

            apply_type, apply_url = await _extract_apply(detail)
            row["apply_type"] = apply_type
            row["apply_url"] = apply_url

            try:
                lead_id, inserted = upsert_lead_with_flag(
                    db_conn,
                    LeadUpsert(
                        platform="linkedin",
                        lead_type="job",
                        contact=job_url,  # contact is a stable identifier for jobs
                        url=job_url,
                        company=company,
                        job_title=title,
                        location=loc_store,
                        source=f"linkedin_jobs:{args.query}",
                        created_at=captured_at,
                        raw=row,
                    ),
                )
                row["lead_id"] = lead_id
                if not db_conn.execute(
                    "SELECT 1 FROM events WHERE lead_id = ? AND event_type = 'collected' LIMIT 1",
                    (lead_id,),
                ).fetchone():
                    add_event(
                        db_conn,
                        lead_id=lead_id,
                        event_type="collected",
                        status="ok",
                        occurred_at=captured_at,
                        details={"source": "linkedin_jobs"},
                    )
                db_conn.commit()
            except Exception:
                row["lead_id"] = ""

            out_rows.append(row)
            collected_urls.add(job_url)

            await detail.wait_for_timeout(random.randint(args.min_job_delay_ms, args.max_job_delay_ms))
            if args.long_break_every > 0 and len(out_rows) % args.long_break_every == 0 and len(out_rows) < args.limit:
                await detail.wait_for_timeout(random.randint(args.long_break_min_ms, args.long_break_max_ms))

            if (idx + 1) % 10 == 0:
                print(
                    f"[li-jobs] processed {idx+1}/{len(candidates)} -> kept {len(out_rows)}"
                    f" (filtered: vietnam={filtered_out.get('vietnam',0)} not_remote={filtered_out.get('not_remote',0)})"
                )

        if not out_rows:
            await dump_debug(ROOT, detail, "jobs_no_results")
            print("[li-jobs] no jobs matched filters (see debug dump).")
            return 5

        if args.write_csv:
            stamp = datetime.now().strftime("%Y-%m-%d")
            out_csv = out_dir / f"linkedin_jobs_{stamp}.csv"
            fieldnames = [
                "captured_at",
                "query",
                "job_title",
                "company",
                "location",
                "workplace",
                "remote",
                "job_url",
                "apply_type",
                "apply_url",
                "snippet",
                "lead_id",
            ]
            with out_csv.open("w", encoding="utf-8", newline="") as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                w.writeheader()
                w.writerows(out_rows)

            print(f"[li-jobs] wrote {out_csv} (rows={len(out_rows)})")
        else:
            print(
                f"[li-jobs] CSV export disabled; wrote leads to DB only (kept={len(out_rows)}) "
                f"(filtered: vietnam={filtered_out.get('vietnam',0)} not_remote={filtered_out.get('not_remote',0)})"
            )
        return 0
    except PlaywrightTimeoutError:
        print("[li-jobs] timeout; closing browser...")
        return 6
    except asyncio.TimeoutError:
        print("[li-jobs] hard timeout hit; closing browser...")
        return 6
    except KeyboardInterrupt:
        print("[li-jobs] interrupted; closing browser...")
        return 130
    except Exception as e:
        print(f"[li-jobs] error: {e}")
        try:
            if closer.ctx and closer.ctx.pages:
                await dump_debug(ROOT, closer.ctx.pages[0], "jobs_error")
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
    ap = argparse.ArgumentParser(description="LinkedIn Jobs scanner (Playwright, logged-in session required)")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override activity DB path")
    ap.add_argument("--query", default="QA Engineer", help="Job search keywords")
    ap.add_argument("--location", default="Remote", help="Location string for LinkedIn search")
    ap.add_argument("--limit", type=int, default=50, help="How many jobs to keep (after filtering)")
    ap.add_argument("--remote-only", action="store_true", help="Keep only jobs with 'Remote' in location")
    ap.add_argument("--allow-vietnam", action="store_true", help="If remote-only, still keep Vietnam jobs")
    ap.add_argument("--exclude-vietnam", action="store_true", help="Exclude jobs located in Vietnam (even if remote)")
    ap.add_argument("--candidate-multiplier", type=int, default=4, help="Collect this many candidates per kept job")
    ap.add_argument("--scrolls", type=int, default=14, help="How many scroll steps on results page")
    ap.add_argument("--scroll-px", type=int, default=1600, help="Pixels per scroll")
    ap.add_argument("--scroll-wait-ms", type=int, default=1500, help="Wait after each scroll")
    ap.add_argument("--include-seen", action="store_true", help="Also process jobs already collected in DB (default: skip)")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step timeout")
    ap.add_argument("--timeout-seconds", type=int, default=240, help="Overall timeout (kills browser if stuck)")
    ap.add_argument("--out-dir", default="data/out", help="Output dir for CSV")
    ap.add_argument("--write-csv", action="store_true", help="Also write a CSV snapshot (default: DB only)")
    ap.add_argument("--force-en", action="store_true", help="Try to force en-US locale for the browser context")
    ap.add_argument("--timezone-id", default="", help="Optional tz id, e.g. 'Asia/Ho_Chi_Minh'")

    ap.add_argument("--min-job-delay-ms", type=int, default=700, help="Min delay between job page opens")
    ap.add_argument("--max-job-delay-ms", type=int, default=1800, help="Max delay between job page opens")
    ap.add_argument("--long-break-every", type=int, default=8, help="Long break every N kept jobs")
    ap.add_argument("--long-break-min-ms", type=int, default=6000, help="Min long break")
    ap.add_argument("--long-break-max-ms", type=int, default=12000, help="Max long break")

    args = ap.parse_args()

    args.force_en = True

    args.remote_only = True

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-jobs] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())
