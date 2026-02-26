import argparse
import csv
import json
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.ats_scanner import collect_jobs_from_ats, scan_companies_for_ats  # noqa: E402
from src.ats_output import write_ats_sources  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.filtering import filter_and_score  # noqa: E402
from src.models import CompanyLead, Job  # noqa: E402
from src.utils import dedupe_ats_sources, dedupe_jobs, normalize_text  # noqa: E402


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def today_midnight_iso() -> str:
    return f"{date.today().isoformat()}T00:00:00"


def is_remote_job(job: Job) -> bool:
    t = normalize_text(" ".join([job.title, job.location, job.description]))
    return any(k in t for k in ("remote", "worldwide", "anywhere", "work from anywhere", "distributed"))


@dataclass
class RowOut:
    ats_type: str
    company: str
    title: str
    location: str
    url: str
    score: int
    source: str


def _write_csv(path: Path, rows: List[RowOut]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=["ats_type", "score", "company", "title", "location", "url", "source"],
        )
        wr.writeheader()
        for r in rows:
            wr.writerow(
                {
                    "ats_type": r.ats_type,
                    "score": r.score,
                    "company": r.company,
                    "title": r.title,
                    "location": r.location,
                    "url": r.url,
                    "source": r.source,
                }
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scan startup websites for ATS boards (Greenhouse/Lever) and collect jobs.")
    p.add_argument("--config", default="config/config.yaml")
    p.add_argument("--limit-companies", type=int, default=60, help="How many startup companies to scan for ATS boards.")
    p.add_argument("--hiring-only", action="store_true", default=True, help="Use only YC startups marked as hiring.")
    p.add_argument("--include-non-hiring", action="store_true", help="Include non-hiring startups too.")
    p.add_argument("--seed", type=int, default=20260213)
    p.add_argument("--timeout-sec", type=float, default=8.0)
    p.add_argument("--max-links", type=int, default=2)
    p.add_argument("--sleep-sec", type=float, default=0.2)
    p.add_argument("--min-score", type=int, default=2, help="Min keyword score for QA shortlist.")
    p.add_argument("--jobs-limit", type=int, default=200, help="Max jobs to insert/export after filtering.")
    p.add_argument("--out", default="", help="Optional CSV output path.")
    return p.parse_args()


def _load_startups(conn, *, hiring_only: bool) -> List[Dict[str, Any]]:
    if hiring_only:
        rows = conn.execute(
            """
            SELECT lead_id, company, url, location, source, raw_json
            FROM leads
            WHERE platform = 'startup_scan' AND lead_type = 'startup_company'
              AND COALESCE(json_extract(raw_json,'$.is_hiring'), 0) = 1
              AND url != ''
            """,
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT lead_id, company, url, location, source, raw_json
            FROM leads
            WHERE platform = 'startup_scan' AND lead_type = 'startup_company'
              AND url != ''
            """,
        ).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        raw = {}
        try:
            raw = json.loads(r["raw_json"] or "{}")
        except Exception:
            raw = {}
        out.append(
            {
                "company": (r["company"] or "").strip(),
                "website": (r["url"] or "").strip(),
                "location": (r["location"] or "").strip(),
                "source": (r["source"] or "").strip(),
                "emails": (raw.get("emails") or "").strip(),
            }
        )
    return out


def main() -> int:
    args = parse_args()
    cfg = load_config(str(resolve_path(ROOT, args.config)))

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    conn = db_connect(db_path)
    init_db(conn)

    hiring_only = bool(args.hiring_only) and not bool(args.include_non_hiring)
    startups = _load_startups(conn, hiring_only=hiring_only)
    print(f"[ats-startups] startups in db (hiring_only={hiring_only}): {len(startups)}")
    if not startups:
        return 2

    random.seed(int(args.seed))
    random.shuffle(startups)
    startups = startups[: max(1, int(args.limit_companies))]

    leads: List[CompanyLead] = []
    for s in startups:
        website = (s.get("website") or "").strip()
        if not website:
            continue
        leads.append(
            CompanyLead(
                name=(s.get("company") or "").strip(),
                website=website,
                location=(s.get("location") or "").strip(),
                email=(s.get("emails") or "").split(";")[0].strip() if (s.get("emails") or "").strip() else "",
                source=(s.get("source") or "").strip(),
                url=website,
                notes="yc_startup",
                raw=s,
            )
        )

    ats_cfg = cfg_get(cfg, "ats", {}) or {}
    enabled_types = ats_cfg.get("types", {"greenhouse": True, "lever": True}) or {"greenhouse": True, "lever": True}
    enabled_types = {k: bool(v) for k, v in dict(enabled_types).items()}
    # Force only greenhouse+lever (they have public APIs in our code).
    for k in list(enabled_types.keys()):
        if k not in ("greenhouse", "lever"):
            enabled_types[k] = False

    sources = scan_companies_for_ats(
        leads,
        max_companies=len(leads),
        max_links_per_company=int(args.max_links),
        timeout_sec=float(args.timeout_sec),
        sleep_sec=float(args.sleep_sec),
        user_agent=str(cfg_get(cfg, "ats.user_agent", "Mozilla/5.0")),
        start_index=0,
        progress_every=10,
        scan_links=True,
        scan_common_paths=True,
    )
    sources = dedupe_ats_sources(sources)
    by_type: Dict[str, int] = {}
    for s in sources:
        by_type[s.ats_type] = by_type.get(s.ats_type, 0) + 1
    print(f"[ats-startups] ATS sources found: {len(sources)} types={by_type}")

    # Persist ATS sources for debugging/reuse.
    out_dir = resolve_path(ROOT, cfg_get(cfg, "output.out_dir", "data/out"))
    sources_path = out_dir / f"ats_startups_sources_{now_stamp()}.csv"
    try:
        write_ats_sources(sources, sources_path)
        print(f"[ats-startups] sources csv -> {sources_path}")
    except Exception:
        pass

    jobs = collect_jobs_from_ats(
        sources,
        enabled_types=enabled_types,
        timeout_sec=float(cfg_get(cfg, "ats.jobs_timeout_sec", 20)),
        sleep_sec=float(args.sleep_sec),
    )
    jobs = dedupe_jobs(jobs)
    print(f"[ats-startups] jobs fetched from ATS APIs: {len(jobs)}")

    # Filter to remote-only + QA relevance via config keywords.
    include_keywords = cfg_get(cfg, "profile.keywords.include", []) or []
    exclude_keywords = cfg_get(cfg, "profile.keywords.exclude", []) or []
    _all, shortlisted = filter_and_score(
        jobs,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        locations=[],
        min_score=0,
        max_results=0,
    )

    final: List[Tuple[Job, int]] = []
    for j, score in shortlisted:
        if not is_remote_job(j):
            continue
        if int(score) < int(args.min_score):
            continue
        final.append((j, int(score)))
        if len(final) >= int(args.jobs_limit):
            break

    # Persist to leads DB.
    inserted = 0
    updated = 0
    rows_out: List[RowOut] = []

    with conn:
        for j, score in final:
            ats_type = ""
            src = (j.source or "").strip()
            if src.startswith("greenhouse:"):
                ats_type = "greenhouse"
            elif src.startswith("lever:"):
                ats_type = "lever"

            raw = dict(j.raw or {})
            raw["score"] = int(score)
            raw["ats_type"] = ats_type

            lid, ins = upsert_lead_with_flag(
                conn,
                LeadUpsert(
                    platform="ats",
                    lead_type="job",
                    contact=j.url,
                    url=j.url,
                    company=j.company,
                    job_title=j.title,
                    location=j.location,
                    source=j.source,
                    raw=raw,
                ),
            )
            if ins:
                inserted += 1
                add_event(conn, lead_id=lid, event_type="collected", status="ok", occurred_at=today_midnight_iso())
            else:
                updated += 1

            rows_out.append(
                RowOut(
                    ats_type=ats_type,
                    company=j.company,
                    title=j.title,
                    location=j.location,
                    url=j.url,
                    score=int(score),
                    source=j.source,
                )
            )

    out_dir = resolve_path(ROOT, cfg_get(cfg, "output.out_dir", "data/out"))
    out_path = resolve_path(ROOT, args.out) if args.out.strip() else (out_dir / f"ats_startups_jobs_{now_stamp()}.csv")
    _write_csv(out_path, rows_out)

    print(f"[ats-startups] final remote QA jobs: {len(final)} (inserted={inserted} updated={updated})")
    print(f"[ats-startups] db: {db_path}")
    print(f"[ats-startups] csv -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
