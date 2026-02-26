import argparse
import csv
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.collectors.http_json import collect_from_http_json  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.filtering import filter_and_score  # noqa: E402
from src.models import Job  # noqa: E402
from src.utils import dedupe_jobs, normalize_text  # noqa: E402


CONTRACT_HINT_RE = re.compile(
    r"(?i)(?:\\bcontract\\b|\\bfreelance\\b|\\bconsultant\\b|\\btemporary\\b|\\btemp\\b|\\bpart[- ]?time\\b|\\b1099\\b|\\bc2c\\b|\\bw2\\b)"
)
MONTHS_HINT_RE = re.compile(r"(?i)\\b(3|6|12)\\s*(?:months|mos)\\b|\\blong[- ]?term\\b|\\b6\\+\\s*months\\b")


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def today_midnight_iso() -> str:
    return f"{date.today().isoformat()}T00:00:00"


def _safe_list(v: Any) -> List[str]:
    if isinstance(v, list):
        return [str(x) for x in v if x is not None]
    return []


def _raw_job_type(raw: Dict[str, Any]) -> str:
    jt2 = raw.get("jobType")
    if isinstance(jt2, list):
        parts = [str(x).strip() for x in jt2 if str(x).strip()]
        if parts:
            return ",".join(parts)

    jt = raw.get("job_type")
    if isinstance(jt, str) and jt.strip():
        return jt.strip()

    jts = _safe_list(raw.get("job_types"))
    if jts:
        return ",".join([x.strip() for x in jts if str(x).strip()])

    tags = _safe_list(raw.get("tags"))
    if tags:
        return "tags:" + ",".join([x.strip() for x in tags if str(x).strip()])

    return ""


def contract_hints(job: Job) -> List[str]:
    raw = job.raw or {}
    hints: List[str] = []

    jt = normalize_text(_raw_job_type(raw))
    if "contract" in jt or "freelance" in jt:
        hints.append(f"raw:{jt[:60]}")

    text = normalize_text(" ".join([job.title, job.location, job.description]))
    if CONTRACT_HINT_RE.search(text):
        hints.append("text:contract_like")
    if MONTHS_HINT_RE.search(text):
        hints.append("text:months_or_long_term")
    return hints


@dataclass
class RowOut:
    source: str
    title: str
    company: str
    location: str
    url: str
    score: int
    contract_hints: str
    raw_job_type: str


def _write_csv(path: Path, rows: Iterable[RowOut]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=[
                "source",
                "score",
                "contract_hints",
                "raw_job_type",
                "company",
                "title",
                "location",
                "url",
            ],
        )
        wr.writeheader()
        for r in rows:
            wr.writerow(
                {
                    "source": r.source,
                    "score": r.score,
                    "contract_hints": r.contract_hints,
                    "raw_job_type": r.raw_job_type,
                    "company": r.company,
                    "title": r.title,
                    "location": r.location,
                    "url": r.url,
                }
            )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scan public job board APIs and store shortlisted jobs in SQLite.")
    p.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    p.add_argument("--limit", type=int, default=200, help="Max shortlisted jobs to insert (default: 200).")
    p.add_argument(
        "--min-score",
        type=int,
        default=-1,
        help="Override min_score from config (default: use config). Try 2 to broaden results.",
    )
    p.add_argument("--contracts-only", action="store_true", help="Only export/insert contract-ish jobs.")
    p.add_argument(
        "--out",
        default="",
        help="Optional CSV output path (default: data/out/contracts_scan_<timestamp>.csv).",
    )
    return p.parse_args()


def is_remote(job: Job) -> bool:
    raw = job.raw or {}
    if "remote" in raw:
        return bool(raw.get("remote") is True)

    if (job.source or "").lower().startswith("remoteok"):
        return True

    if (job.source or "").lower().startswith("jobicy"):
        return True

    if (job.source or "").lower().startswith("remotive"):
        return True

    t = normalize_text(" ".join([job.location, job.description]))
    return any(k in t for k in ("remote", "worldwide", "anywhere", "work from anywhere"))


def main() -> int:
    args = parse_args()
    cfg = load_config(str(resolve_path(ROOT, args.config)))

    http_sources = cfg_get(cfg, "sources.http_json", []) or []
    jobs: List[Job] = []
    per_source_counts: Dict[str, int] = {}

    for src in http_sources:
        if not isinstance(src, dict) or not src.get("enabled", False):
            continue
        name = str(src.get("name") or "http_json")
        items = collect_from_http_json(src)
        per_source_counts[name] = len(items)
        jobs.extend(items)

    jobs = dedupe_jobs(jobs)

    include_keywords = cfg_get(cfg, "profile.keywords.include", []) or []
    exclude_title_keywords = cfg_get(cfg, "profile.keywords.exclude", []) or []
    locations: List[str] = []
    min_score_cfg = int(cfg_get(cfg, "filters.min_score", 1))
    min_score = int(args.min_score) if int(args.min_score) >= 0 else min_score_cfg
    max_results = int(cfg_get(cfg, "filters.max_results", 200))

    _all, shortlisted = filter_and_score(
        jobs,
        include_keywords=include_keywords,
        exclude_keywords=[],
        locations=locations,
        min_score=min_score,
        max_results=min(max_results, int(args.limit)),
    )

    def _title_is_excluded(j: Job) -> bool:
        t = normalize_text(j.title or "")
        return any(normalize_text(kw) in t for kw in exclude_title_keywords)

    shortlisted = [(j, s) for (j, s) in shortlisted if is_remote(j) and not _title_is_excluded(j)]

    shortlisted2: List[Tuple[Job, int]] = []
    for j, score in shortlisted:
        hints = contract_hints(j)
        if args.contracts_only and not hints:
            continue
        shortlisted2.append((j, score))

    out_dir = resolve_path(ROOT, cfg_get(cfg, "output.out_dir", "data/out"))
    out_path = resolve_path(ROOT, args.out) if args.out else (out_dir / f"contracts_scan_{now_stamp()}.csv")

    rows_out: List[RowOut] = []
    for j, score in shortlisted2:
        raw = j.raw or {}
        rows_out.append(
            RowOut(
                source=j.source,
                title=j.title,
                company=j.company,
                location=j.location,
                url=j.url,
                score=int(score),
                contract_hints=";".join(contract_hints(j)),
                raw_job_type=_raw_job_type(raw),
            )
        )
    _write_csv(out_path, rows_out)

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    conn = db_connect(db_path)
    init_db(conn)

    inserted = 0
    updated = 0
    inserted_by_source: Dict[str, int] = {}
    contracts = 0

    with conn:
        for j, score in shortlisted2:
            hints = contract_hints(j)
            if hints:
                contracts += 1

            raw = dict(j.raw or {})
            raw["score"] = int(score)
            raw["contract_hints"] = hints
            raw["raw_job_type"] = _raw_job_type(raw)

            lid, ins = upsert_lead_with_flag(
                conn,
                LeadUpsert(
                    platform="job_board",
                    lead_type="job",
                    contact=j.url,
                    url=j.url,
                    company=j.company,
                    job_title=j.title,
                    location=j.location,
                    source=j.source,
                    created_at=None,
                    raw=raw,
                ),
            )
            if ins:
                inserted += 1
                inserted_by_source[j.source] = inserted_by_source.get(j.source, 0) + 1
                add_event(conn, lead_id=lid, event_type="collected", status="ok", occurred_at=today_midnight_iso())
            else:
                updated += 1

    print(f"[web_scan] sources enabled: {len([s for s in http_sources if isinstance(s, dict) and s.get('enabled')])}")
    for k, v in sorted(per_source_counts.items()):
        print(f"[web_scan] collected from {k}: {v}")
    print(f"[web_scan] deduped jobs: {len(jobs)}")
    print(f"[web_scan] shortlisted: {len(shortlisted2)} (contracts-ish: {contracts})")
    print(f"[web_scan] db: {db_path}")
    print(f"[web_scan] leads upserted: inserted={inserted} updated={updated}")
    if inserted_by_source:
        for k, v in sorted(inserted_by_source.items(), key=lambda x: (-x[1], x[0])):
            print(f"[web_scan] inserted {k}: {v}")
    print(f"[web_scan] csv -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
