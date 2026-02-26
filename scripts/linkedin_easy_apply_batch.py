import argparse
import asyncio
import json
import os
import random
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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
from src.linkedin_easy_apply import candidate_from_cfg, run_easy_apply_once  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, dump_debug, ensure_linkedin_session, int_env, is_checkpoint_url  # noqa: E402
from src.notify import notify  # noqa: E402
from src.profile_store import load_profile  # noqa: E402


QA_TITLE_RE = re.compile(
    r"\b(qa|quality\s*assurance|sdet|test\s*automation|automation\s*engineer|test\s*engineer|software\s*tester)\b",
    re.IGNORECASE,
)
VIETNAM_RE = re.compile(
    r"\b(vietnam|viet\s*nam|ho\s*chi\s*minh|hcmc|hanoi|da\s*nang|saigon)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _parse_raw(raw_json: Optional[str]) -> Dict[str, Any]:
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


def _canonical_linkedin_job_url(url: str) -> str:
    jid = _linkedin_job_id(url)
    if jid:
        return f"https://www.linkedin.com/jobs/view/{jid}/"
    return str(url or "").strip()


def _load_target_urls(path_text: str) -> set[str]:
    ptxt = str(path_text or "").strip()
    if not ptxt:
        return set()
    p = resolve_path(ROOT, ptxt)
    if not p.exists():
        print(f"[li-apply-batch] targets file not found: {p}")
        return set()
    out: set[str] = set()
    try:
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return set()
    for line in lines:
        u = _canonical_linkedin_job_url(line.strip())
        if _linkedin_job_id(u):
            out.add(u)
    print(f"[li-apply-batch] targets loaded: {len(out)} from {p}")
    return out


def _fetch_jobs_to_apply(
    conn,
    *,
    limit: int,
    easy_only: bool,
    include_attempted: bool,
    target_urls: set[str],
    require_qa_title: bool,
    exclude_vietnam: bool,
) -> List[Dict[str, Any]]:
    where = [
        "l.platform = 'linkedin'",
        "l.lead_type = 'job'",
        "EXISTS(SELECT 1 FROM events ec WHERE ec.lead_id = l.lead_id AND ec.event_type = 'collected')",
        "NOT EXISTS(SELECT 1 FROM events ea WHERE ea.lead_id = l.lead_id AND ea.event_type = 'li_apply_submitted')",
    ]
    if not include_attempted:
        where.extend(
            [
                "NOT EXISTS(SELECT 1 FROM events em WHERE em.lead_id = l.lead_id AND em.event_type = 'li_apply_needs_manual')",
                "NOT EXISTS(SELECT 1 FROM events ef WHERE ef.lead_id = l.lead_id AND ef.event_type = 'li_apply_failed')",
            ]
        )

    sql = (
        "SELECT l.lead_id, l.contact, l.company, l.job_title, l.location, l.raw_json, l.created_at "
        "FROM leads l "
        f"WHERE {' AND '.join(where)} "
        "ORDER BY l.created_at DESC "
        "LIMIT ?"
    )
    sample_limit = max(int(limit) * 10, int(limit))
    rows = conn.execute(sql, (sample_limit,)).fetchall()
    submitted_rows = conn.execute(
        """
        SELECT l.contact
        FROM events e
        JOIN leads l ON l.lead_id = e.lead_id
        WHERE l.platform = 'linkedin' AND l.lead_type = 'job' AND e.event_type = 'li_apply_submitted'
        """
    ).fetchall()
    submitted_job_ids = {jid for jid in (_linkedin_job_id(str(r["contact"] or "")) for r in submitted_rows) if jid}

    out: List[Dict[str, Any]] = []
    seen_job_ids: set[str] = set()
    for r in rows:
        raw = _parse_raw(r["raw_json"])
        apply_type = str(raw.get("apply_type") or "").strip().lower()
        raw_url = str(r["contact"])
        job_url = _canonical_linkedin_job_url(raw_url)
        job_id = _linkedin_job_id(job_url)
        if job_id and (job_id in submitted_job_ids or job_id in seen_job_ids):
            continue
        if target_urls and job_url not in target_urls:
            continue
        title = str(r["job_title"] or "")
        location = str(r["location"] or "")
        if require_qa_title and (not QA_TITLE_RE.search(title)):
            continue
        if exclude_vietnam:
            txt = " ".join(
                [
                    title,
                    location,
                    str(raw.get("workplace") or ""),
                    str(raw.get("snippet") or ""),
                ]
            )
            if VIETNAM_RE.search(txt):
                continue
        if easy_only and apply_type and apply_type not in {"easy_apply", "unknown"}:
            continue
        if job_id:
            seen_job_ids.add(job_id)
        out.append(
            {
                "lead_id": str(r["lead_id"]),
                "job_url": job_url,
                "company": str(r["company"] or ""),
                "title": title,
                "location": location,
                "apply_type": apply_type,
                "created_at": str(r["created_at"] or ""),
            }
        )
        if len(out) >= int(limit):
            break
    return out


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    candidate = None
    target_urls = _load_target_urls(args.targets_file)

    resume_path = resolve_path(ROOT, args.resume) if args.resume.strip() else None
    if resume_path is None or not resume_path.exists():
        attach = cfg_get(cfg, "email.attachments", [])
        if isinstance(attach, list) and attach:
            rp = resolve_path(ROOT, str(attach[0]))
            resume_path = rp if rp.exists() else None
    if resume_path is None:
        resume_path = resolve_path(ROOT, "Docs/ARTEM_BONDARENKO_CV_2026.pdf")

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

        jobs = _fetch_jobs_to_apply(
            db_conn,
            limit=args.limit,
            easy_only=not args.include_external,
            include_attempted=args.include_attempted,
            target_urls=target_urls,
            require_qa_title=bool(args.require_qa_title),
            exclude_vietnam=bool(args.exclude_vietnam),
        )
        if not jobs:
            print("[li-apply-batch] nothing to do (no eligible jobs in DB).")
            return 0

        print(f"[li-apply-batch] jobs to apply: {len(jobs)}")

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
            print("[li-apply-batch] not logged in. Run scripts/linkedin_login.py first.")
            await dump_debug(ROOT, page, "apply_batch_not_logged_in")
            notify(ROOT, cfg, kind="attention")
            return 2

        submitted = 0
        manual = 0
        failed = 0

        for idx, item in enumerate(jobs, start=1):
            lead_id = item["lead_id"]
            job_url = item["job_url"]
            title = item.get("title") or ""
            company = item.get("company") or ""
            print(f"[li-apply-batch] {idx}/{len(jobs)} -> {company} | {title} | {job_url}")

            try:
                add_event(db_conn, lead_id=lead_id, event_type="li_apply_started", status="ok", occurred_at=_now_iso(), details={"job_url": job_url})
                db_conn.commit()
            except Exception:
                pass

            try:
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
            except PlaywrightTimeoutError:
                await dump_debug(ROOT, page, "apply_batch_step_timeout")
                notify(ROOT, cfg, kind="timeout")
                result, details = ("failed", "playwright_timeout")
            except Exception as e:
                await dump_debug(ROOT, page, "apply_batch_exception")
                notify(ROOT, cfg, kind="error")
                result, details = ("failed", f"exception:{type(e).__name__}")

            ev = "li_apply_submitted" if result == "submitted" else ("li_apply_needs_manual" if result == "needs_manual" else "li_apply_failed")
            try:
                add_event(db_conn, lead_id=lead_id, event_type=ev, status="ok", occurred_at=_now_iso(), details={"details": details})
                db_conn.commit()
            except Exception:
                pass

            if result == "submitted":
                submitted += 1
            elif result == "needs_manual":
                manual += 1
                notify(ROOT, cfg, kind="attention")
                print(f"[li-apply-batch] needs manual: {details}")
                if args.pause_on_manual:
                    print("[li-apply-batch] Paused for manual action in the browser. Press Enter to continue...")
                    try:
                        input()
                    except KeyboardInterrupt:
                        return 130
            else:
                failed += 1
                notify(ROOT, cfg, kind="error")
                print(f"[li-apply-batch] failed: {details}")
                if args.stop_on_fail:
                    print("[li-apply-batch] stop_on_fail enabled; stopping.")
                    break

            await page.wait_for_timeout(random.randint(args.min_delay_ms, args.max_delay_ms))
            if args.long_break_every > 0 and idx % args.long_break_every == 0 and idx < len(jobs):
                await page.wait_for_timeout(random.randint(args.long_break_min_ms, args.long_break_max_ms))

            if is_checkpoint_url(page.url):
                notify(ROOT, cfg, kind="attention")
                await dump_debug(ROOT, page, "apply_batch_checkpoint")
                print("[li-apply-batch] checkpoint/captcha detected; stopping.")
                break

        print(f"[li-apply-batch] done: submitted={submitted} manual={manual} failed={failed}")
        notify(ROOT, cfg, kind="done")
        return 0
    except PlaywrightTimeoutError:
        print("[li-apply-batch] timeout; closing browser...")
        try:
            notify(ROOT, cfg, kind="timeout")
        except Exception:
            pass
        return 6
    except asyncio.TimeoutError:
        print("[li-apply-batch] hard timeout hit; closing browser...")
        try:
            notify(ROOT, cfg, kind="timeout")
        except Exception:
            pass
        return 6
    except KeyboardInterrupt:
        print("[li-apply-batch] interrupted; closing browser...")
        return 130
    except Exception as e:
        print(f"[li-apply-batch] error: {e}")
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
    ap = argparse.ArgumentParser(description="Batch LinkedIn Easy Apply from activity.sqlite (Playwright)")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--limit", type=int, default=25, help="Max jobs to attempt per run")
    ap.add_argument("--max-steps", type=int, default=8, help="Max modal steps per job")
    ap.add_argument("--no-submit", action="store_true", help="Stop on Submit (do not click submit)")
    ap.add_argument("--include-external", action="store_true", help="Also try non-easy-apply jobs (default: skip)")
    ap.add_argument("--include-attempted", action="store_true", help="Also include jobs we already tried (needs_manual/failed)")
    ap.add_argument("--targets-file", default="", help="Optional file with LinkedIn job URLs (one per line) to restrict apply scope")
    ap.add_argument("--require-qa-title", action="store_true", help="Skip non-QA titles")
    ap.add_argument("--exclude-vietnam", action="store_true", help="Skip jobs with Vietnam location markers")
    ap.add_argument("--pause-on-manual", action="store_true", help="Pause and wait for Enter when manual input is needed")
    ap.add_argument("--stop-on-fail", action="store_true", help="Stop the batch when a job fails")
    ap.add_argument("--resume", default="", help="Resume PDF path (default: first email attachment from config)")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step Playwright timeout")
    ap.add_argument("--timeout-seconds", type=int, default=3600, help="Overall timeout")

    ap.add_argument("--min-delay-ms", type=int, default=4500, help="Min delay between applications")
    ap.add_argument("--max-delay-ms", type=int, default=9500, help="Max delay between applications")
    ap.add_argument("--long-break-every", type=int, default=7, help="Long break every N applications")
    ap.add_argument("--long-break-min-ms", type=int, default=75_000, help="Min long break")
    ap.add_argument("--long-break-max-ms", type=int, default=150_000, help="Max long break")

    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-apply-batch] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())
