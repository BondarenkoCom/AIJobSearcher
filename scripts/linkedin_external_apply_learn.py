import argparse
import asyncio
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

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
from src.linkedin_playwright import SafeCloser, bool_env, dump_debug, ensure_linkedin_session, int_env  # noqa: E402
from src.profile_store import load_profile, normalize_question  # noqa: E402


ATS_HOST_HINTS = {
    "workable.com": "workable",
    "greenhouse.io": "greenhouse",
    "lever.co": "lever",
    "ashbyhq.com": "ashby",
    "smartrecruiters.com": "smartrecruiters",
}


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


def _linkedin_job_id(url: str) -> str:
    m = re.search(r"/jobs/view/(\d+)", str(url or ""), re.IGNORECASE)
    return m.group(1) if m else ""


def _canonical_linkedin_url(url: str) -> str:
    jid = _linkedin_job_id(url)
    if jid:
        return f"https://www.linkedin.com/jobs/view/{jid}/"
    return str(url or "").strip()


def _domain(url: str) -> str:
    try:
        return (urlparse(url).netloc or "").lower()
    except Exception:
        return ""


def _platform_from_url(url: str) -> str:
    host = _domain(url)
    for hint, platform in ATS_HOST_HINTS.items():
        if hint in host:
            return platform
    return "external_unknown"


def _is_linkedin_url(url: str) -> bool:
    return "linkedin.com" in _domain(url)


def _fetch_jobs_for_learning(conn, *, limit: int) -> List[Dict[str, str]]:
    rows = conn.execute(
        """
        SELECT
          l.lead_id, l.contact, l.company, l.job_title, l.raw_json, MAX(e.occurred_at) AS last_manual
        FROM leads l
        JOIN events e ON e.lead_id = l.lead_id
        WHERE l.platform = 'linkedin'
          AND l.lead_type = 'job'
          AND e.event_type = 'li_apply_needs_manual'
          AND NOT EXISTS(
            SELECT 1 FROM events es
            WHERE es.lead_id = l.lead_id AND es.event_type = 'li_apply_submitted'
          )
        GROUP BY l.lead_id, l.contact, l.company, l.job_title, l.raw_json
        ORDER BY last_manual DESC
        LIMIT ?
        """,
        (int(limit * 10),),
    ).fetchall()

    out: List[Dict[str, str]] = []
    seen_jid: set[str] = set()
    for r in rows:
        job_url = _canonical_linkedin_url(str(r["contact"] or ""))
        jid = _linkedin_job_id(job_url)
        if jid and jid in seen_jid:
            continue
        if jid:
            seen_jid.add(jid)
        raw = _parse_json(r["raw_json"])
        raw_apply_url = str(raw.get("apply_url") or "")
        out.append(
            {
                "lead_id": str(r["lead_id"]),
                "job_url": job_url,
                "company": str(r["company"] or ""),
                "job_title": str(r["job_title"] or ""),
                "apply_url": "" if _is_linkedin_url(raw_apply_url) else raw_apply_url,
                "apply_type": str(raw.get("apply_type") or ""),
            }
        )
        if len(out) >= limit:
            break
    return out


async def _extract_external_apply_url(page) -> str:
    # 1) direct external links with apply-like semantics.
    try:
        links = await page.evaluate(
            """() => {
              const out = [];
              const nodes = Array.from(document.querySelectorAll('a[href]'));
              for (const a of nodes) {
                const href = (a.href || '').trim();
                if (!href || !/^https?:/i.test(href)) continue;
                if (/linkedin\\.com/i.test(href)) continue;
                const cs = window.getComputedStyle(a);
                const r = a.getBoundingClientRect();
                const visible = cs && cs.display !== 'none' && cs.visibility !== 'hidden' && r.width > 0 && r.height > 0;
                if (!visible) continue;
                out.push({
                  href,
                  text: (a.innerText || '').replace(/\\s+/g, ' ').trim(),
                  aria: (a.getAttribute('aria-label') || '').trim(),
                  cls: (a.className || '').toString(),
                  y: r.y || 0
                });
              }
              return out;
            }"""
        )
    except Exception:
        links = []

    best_url = ""
    best_score = -10_000
    for ln in links or []:
        href = str(ln.get("href") or "").strip()
        if not href:
            continue
        blob = " ".join(
            [
                href.lower(),
                str(ln.get("text") or "").lower(),
                str(ln.get("aria") or "").lower(),
                str(ln.get("cls") or "").lower(),
            ]
        )
        score = 0
        if "apply" in blob:
            score += 30
        if any(d in href.lower() for d in ATS_HOST_HINTS):
            score += 60
        if any(k in blob for k in ["job", "career", "position"]):
            score += 10
        if float(ln.get("y") or 9999) < 1200:
            score += 5
        if score > best_score:
            best_score = score
            best_url = href

    if best_url:
        return best_url

    # 2) try clicking plain Apply button/link (not Easy Apply) and capture popup/tab.
    candidates = [
        page.get_by_role("link", name=re.compile(r"^apply", re.IGNORECASE)).first,
        page.get_by_role("button", name=re.compile(r"^apply", re.IGNORECASE)).first,
    ]
    for loc in candidates:
        try:
            if not await loc.is_visible(timeout=800):
                continue
            try:
                label = (await loc.inner_text(timeout=300)).strip().lower()
            except Exception:
                label = ""
            if "easy apply" in label:
                continue

            # Capture popup if opened.
            try:
                async with page.expect_popup(timeout=3000) as pop:
                    await loc.click()
                p = await pop.value
                try:
                    await p.wait_for_load_state("domcontentloaded", timeout=10_000)
                except Exception:
                    pass
                ext = str(p.url or "").strip()
                await p.close()
                if ext and "linkedin.com" not in ext:
                    return ext
            except Exception:
                pass

            # Same-tab redirect path.
            await page.wait_for_timeout(1200)
            if page.url and "linkedin.com" not in page.url:
                return str(page.url)
        except Exception:
            continue
    return ""


async def _profile_external_form(page, *, profile: Dict[str, str]) -> Dict[str, Any]:
    # Open cookie banner and landing CTA if present so actual form fields become visible.
    for btn_name in ["Accept all", "Accept All", "I agree", "Agree"]:
        try:
            b = page.get_by_role("button", name=re.compile(re.escape(btn_name), re.IGNORECASE)).first
            if await b.is_visible(timeout=700):
                await b.click()
                await page.wait_for_timeout(500)
                break
        except Exception:
            continue

    for btn_rx in [
        r"Apply for this job",
        r"Apply now",
        r"Start application",
        r"Continue application",
    ]:
        try:
            b = page.get_by_role("button", name=re.compile(btn_rx, re.IGNORECASE)).first
            if await b.is_visible(timeout=900):
                await b.click()
                await page.wait_for_timeout(1000)
                break
        except Exception:
            continue

    for link_rx in [r"Apply for this job", r"Apply now", r"Start application"]:
        try:
            a = page.get_by_role("link", name=re.compile(link_rx, re.IGNORECASE)).first
            if await a.is_visible(timeout=900):
                await a.click()
                await page.wait_for_timeout(1000)
                break
        except Exception:
            continue

    title = ""
    try:
        title = await page.title()
    except Exception:
        pass

    try:
        controls = await page.evaluate(
            """() => {
              const vis = (el) => {
                const cs = window.getComputedStyle(el);
                const r = el.getBoundingClientRect();
                return cs.display !== 'none' && cs.visibility !== 'hidden' && r.width > 0 && r.height > 0;
              };
              const out = [];
              const all = Array.from(document.querySelectorAll('input, textarea, select'));
              for (const el of all) {
                if (!vis(el)) continue;
                const tag = (el.tagName || '').toLowerCase();
                const type = (el.getAttribute('type') || '').toLowerCase();
                if (tag === 'input' && ['hidden', 'button', 'submit'].includes(type)) continue;
                const id = el.id || '';
                const required = !!el.required || ((el.getAttribute('aria-required') || '').toLowerCase() === 'true');
                const name = (el.getAttribute('name') || '').trim();
                const placeholder = (el.getAttribute('placeholder') || '').trim();
                const aria = (el.getAttribute('aria-label') || '').trim();
                const accept = (el.getAttribute('accept') || '').trim();
                let label = '';
                try {
                  if (el.labels && el.labels.length) label = (el.labels[0].innerText || '').trim();
                } catch (e) {}
                if (!label && id) {
                  const lab = document.querySelector('label[for=\"' + id + '\"]');
                  if (lab) label = (lab.innerText || '').trim();
                }
                let legend = '';
                try {
                  const fs = el.closest('fieldset');
                  if (fs) {
                    const lg = fs.querySelector('legend');
                    if (lg) legend = (lg.innerText || '').trim();
                  }
                } catch (e) {}
                const question = (legend || label || aria || placeholder || name || '').replace(/\\s+/g, ' ').trim();
                out.push({tag, type, name, required, question, accept});
              }
              return out;
            }"""
        )
    except Exception:
        controls = []

    req = [c for c in controls if bool(c.get("required"))]
    unknown_required: List[str] = []
    auto_ready_required = 0

    def guess_answer(question: str, field_type: str, name: str) -> str:
        q = normalize_question(question)
        n = normalize_question(name)
        cand_name = str(profile.get("candidate.name") or "").strip()
        first = cand_name.split(" ")[0] if cand_name else ""
        last = " ".join(cand_name.split(" ")[1:]) if len(cand_name.split(" ")) > 1 else ""
        email = str(profile.get("candidate.email") or "").strip()
        phone = re.sub(r"[^0-9]", "", str(profile.get("candidate.phone") or "").strip())
        linkedin = str(profile.get("candidate.linkedin") or "").strip()
        location = str(profile.get("candidate.location") or "").strip()

        if "email" in q or n == "email":
            return email
        if "first name" in q or n in {"first_name", "firstname"}:
            return first
        if "last name" in q or n in {"last_name", "lastname"}:
            return last
        if q in {"name", "full name"} or n in {"name", "full_name"}:
            return cand_name
        if "phone" in q or "mobile" in q or "phone" in n:
            return phone
        if "linkedin" in q or "linkedin" in n:
            return linkedin
        if "location" in q or "city" in q:
            return location
        if "how many years" in q:
            return "5"
        if "salary" in q or "compensation" in q:
            return ""
        if field_type in {"radio", "checkbox"}:
            if "authorized to work" in q and "us" in q:
                return ""
            if "sponsorship" in q:
                return ""
            if "remote" in q and "comfortable" in q:
                return "yes"
            if "background check" in q:
                return "yes"
        return ""

    for c in req:
        q = str(c.get("question") or "")
        t = str(c.get("type") or "")
        n = str(c.get("name") or "")
        a = guess_answer(q, t, n)
        if a:
            auto_ready_required += 1
        else:
            if q:
                unknown_required.append(q)

    return {
        "title": title,
        "final_url": page.url,
        "controls_total": len(controls),
        "required_total": len(req),
        "required_autofillable": auto_ready_required,
        "required_unknown_total": len(unknown_required),
        "required_unknown_sample": unknown_required[:12],
        "has_file_input": any(str(c.get("type") or "").lower() == "file" for c in controls),
        "has_image_file_requirement": any(
            ("image/" in str(c.get("accept") or "").lower() or ".jpg" in str(c.get("accept") or "").lower())
            and bool(c.get("required"))
            for c in controls
        ),
    }


def _update_lead_apply_raw(conn, *, lead_id: str, apply_url: str, platform: str) -> None:
    row = conn.execute("SELECT raw_json FROM leads WHERE lead_id = ? LIMIT 1", (lead_id,)).fetchone()
    raw = _parse_json(row["raw_json"] if row else "")
    raw["apply_type"] = "external"
    raw["apply_url"] = apply_url
    raw["external_platform"] = platform
    conn.execute("UPDATE leads SET raw_json = ? WHERE lead_id = ?", (json.dumps(raw, ensure_ascii=False), lead_id))


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    closer = SafeCloser()
    db_conn = None
    try:
        db_conn = db_connect(db_path)
        init_db(db_conn)
        profile = load_profile(db_conn)

        jobs = _fetch_jobs_for_learning(db_conn, limit=args.limit)
        if not jobs:
            print("[ext-learn] no candidate linkedin jobs found.")
            return 0
        print(f"[ext-learn] jobs to inspect: {len(jobs)}")

        headless = bool_env("PLAYWRIGHT_HEADLESS", True)
        slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
        user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=["--lang=en-US"],
        )
        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        ok = await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page, timeout_ms=args.step_timeout_ms)
        if not ok:
            print("[ext-learn] not logged in to LinkedIn.")
            await dump_debug(ROOT, page, "ext_learn_not_logged_in")
            return 2

        discovered = 0
        profiled = 0
        unknown_heavy = 0

        for idx, item in enumerate(jobs, start=1):
            lead_id = item["lead_id"]
            job_url = item["job_url"]
            print(f"[ext-learn] {idx}/{len(jobs)} {item.get('company','')} | {item.get('job_title','')} | {job_url}")

            add_event(
                db_conn,
                lead_id=lead_id,
                event_type="external_learn_started",
                occurred_at=_now_iso(),
                details={"job_url": job_url},
            )

            ext_url = (item.get("apply_url") or "").strip()
            if ext_url and _is_linkedin_url(ext_url):
                ext_url = ""
            if not ext_url:
                try:
                    await page.goto(job_url, wait_until="domcontentloaded", timeout=args.step_timeout_ms)
                    await page.wait_for_timeout(1400)
                    ext_url = await _extract_external_apply_url(page)
                except Exception:
                    await dump_debug(ROOT, page, "ext_learn_linkedin_open_failed")
                    ext_url = ""

            if not ext_url:
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="external_learn_no_external_url",
                    occurred_at=_now_iso(),
                    details={"job_url": job_url},
                )
                db_conn.commit()
                continue

            platform = _platform_from_url(ext_url)
            discovered += 1
            _update_lead_apply_raw(db_conn, lead_id=lead_id, apply_url=ext_url, platform=platform)
            add_event(
                db_conn,
                lead_id=lead_id,
                event_type="external_apply_discovered",
                occurred_at=_now_iso(),
                details={"job_url": job_url, "external_url": ext_url, "platform": platform},
            )
            db_conn.commit()

            ext_page = await closer.ctx.new_page()
            ext_page.set_default_timeout(args.step_timeout_ms)
            ext_page.set_default_navigation_timeout(args.step_timeout_ms)
            try:
                await ext_page.goto(ext_url, wait_until="domcontentloaded", timeout=args.step_timeout_ms)
                await ext_page.wait_for_timeout(1500)
                profile_data = await _profile_external_form(ext_page, profile=profile)
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="external_apply_profiled",
                    occurred_at=_now_iso(),
                    details={
                        "job_url": job_url,
                        "external_url": ext_url,
                        "platform": platform,
                        **profile_data,
                    },
                )
                db_conn.commit()
                profiled += 1
                if int(profile_data.get("required_unknown_total") or 0) >= 3:
                    unknown_heavy += 1
                print(
                    "[ext-learn]   -> profiled"
                    f" platform={platform} required={profile_data.get('required_total',0)}"
                    f" unknown={profile_data.get('required_unknown_total',0)}"
                )
            except PlaywrightTimeoutError:
                await dump_debug(ROOT, ext_page, "ext_learn_external_timeout")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="external_apply_profile_failed",
                    occurred_at=_now_iso(),
                    details={"job_url": job_url, "external_url": ext_url, "reason": "timeout"},
                )
                db_conn.commit()
            except Exception as e:
                await dump_debug(ROOT, ext_page, "ext_learn_external_error")
                add_event(
                    db_conn,
                    lead_id=lead_id,
                    event_type="external_apply_profile_failed",
                    occurred_at=_now_iso(),
                    details={
                        "job_url": job_url,
                        "external_url": ext_url,
                        "reason": f"exception:{type(e).__name__}",
                    },
                )
                db_conn.commit()
            finally:
                try:
                    await ext_page.close()
                except Exception:
                    pass

        print(
            f"[ext-learn] done: inspected={len(jobs)} discovered_external={discovered} "
            f"profiled={profiled} unknown_heavy={unknown_heavy}"
        )
        return 0
    except Exception as e:
        print(f"[ext-learn] error: {e}")
        return 1
    finally:
        try:
            if db_conn is not None:
                db_conn.close()
        except Exception:
            pass
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Learn external Apply flows from LinkedIn jobs (headless profiling).")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--limit", type=int, default=10, help="How many LinkedIn jobs to inspect")
    ap.add_argument("--step-timeout-ms", type=int, default=30000, help="Per-page timeout")
    ap.add_argument("--timeout-seconds", type=int, default=3600, help="Overall timeout")
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[ext-learn] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())
