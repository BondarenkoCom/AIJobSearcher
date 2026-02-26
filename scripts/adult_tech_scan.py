import argparse
import csv
import json
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
import urllib3

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )
}
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

QA_RE = re.compile(
    r"\b(qa|quality assurance|test automation|automation testing|software tester|sdet|quality engineer|test engineer)\b",
    re.IGNORECASE,
)
TOOL_RE = re.compile(r"\b(playwright|selenium|cypress|appium|postman|api testing|rest api)\b", re.IGNORECASE)
REMOTE_RE = re.compile(r"\b(remote|worldwide|anywhere|distributed|hybrid)\b", re.IGNORECASE)


@dataclass
class SourceJob:
    source: str
    company: str
    title: str
    location: str
    url: str
    employment: str
    posted_at: str
    description: str
    raw: Dict[str, Any]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _today_midnight_iso() -> str:
    return f"{date.today().isoformat()}T00:00:00"


def _text(v: Any) -> str:
    return str(v or "").strip()


def _strip_html(s: str) -> str:
    if not s:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"</p>", "\n", s, flags=re.IGNORECASE)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _canonical_url(url: str) -> str:
    u = _text(url)
    if not u:
        return ""
    # Keep simple and stable for dedupe.
    u = u.split("#", 1)[0].strip()
    return u


def _qa_score(title: str, description: str) -> int:
    title_t = _text(title)
    desc_t = _text(description)
    score = 0
    if QA_RE.search(title_t):
        score += 8
    if QA_RE.search(desc_t):
        score += 2
    if TOOL_RE.search(f"{title_t}\n{desc_t}"):
        score += 2
    # Avoid false positives where no explicit QA/test-engineering signal exists.
    if (score > 0) and (not QA_RE.search(title_t)) and (not TOOL_RE.search(f"{title_t}\n{desc_t}")):
        score = 0
    return min(10, score)


def _remote_mode(location: str, description: str) -> str:
    txt = f"{_text(location)}\n{_text(description)}"
    if re.search(r"\bon[-\s]?site\b", txt, re.IGNORECASE):
        return "on_site"
    if re.search(r"\bhybrid\b", txt, re.IGNORECASE):
        return "hybrid"
    if REMOTE_RE.search(txt):
        return "remote"
    return "unknown"


def _fetch_aylo(timeout_sec: float) -> List[SourceJob]:
    url = "https://boards-api.greenhouse.io/v1/boards/aylo/jobs"
    r = requests.get(url, headers=UA, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json() if r.text else {}
    jobs = data.get("jobs") or []
    out: List[SourceJob] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        loc = ""
        if isinstance(j.get("location"), dict):
            loc = _text(j["location"].get("name"))
        out.append(
            SourceJob(
                source="adult_api:aylo_greenhouse",
                company="Aylo",
                title=_text(j.get("title")),
                location=loc,
                url=_text(j.get("absolute_url")),
                employment="",
                posted_at=_text(j.get("updated_at") or j.get("internal_job_id")),
                description="",
                raw=j,
            )
        )
    return out


def _fetch_multimedia(timeout_sec: float) -> List[SourceJob]:
    url = "https://apply.workable.com/api/v1/widget/accounts/multimediallc?details=true"
    r = requests.get(url, headers=UA, timeout=timeout_sec)
    r.raise_for_status()
    data = r.json() if r.text else {}
    jobs = data.get("jobs") or []
    out: List[SourceJob] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        state = _text(j.get("state"))
        country = _text(j.get("country"))
        loc = ", ".join([x for x in [state, country] if x]) or _text(j.get("location"))
        out.append(
            SourceJob(
                source="adult_api:chaturbate_workable",
                company=_text(j.get("department")) or "Multi Media, LLC (Chaturbate)",
                title=_text(j.get("title")),
                location=loc,
                url=_text(j.get("url")),
                employment=_text(j.get("employment_type")),
                posted_at=_text(j.get("created_at") or j.get("updated_at")),
                description=_strip_html(_text(j.get("description"))),
                raw=j,
            )
        )
    return out


def _fetch_docler(timeout_sec: float) -> List[SourceJob]:
    """
    Docler serves career data through Next.js JSON:
      /career/_next/data/<build_id>/en/jobs.json
    """
    base = "https://doclerholding.com"
    jobs_page = f"{base}/career/jobs"
    r = requests.get(jobs_page, headers=UA, timeout=timeout_sec, verify=False)
    r.raise_for_status()
    html = r.text

    m = re.search(r"/career/_next/static/([^/]+)/_buildManifest\\.js", html)
    if not m:
        return []
    build_id = _text(m.group(1))
    if not build_id:
        return []

    data_url = f"{base}/career/_next/data/{build_id}/en/jobs.json"
    rr = requests.get(data_url, headers=UA, timeout=timeout_sec, verify=False)
    if rr.status_code != 200:
        return []
    data = rr.json() if rr.text else {}

    jobs: List[Dict[str, Any]] = []
    try:
        q = data["pageProps"]["dehydratedState"]["queries"]
        if isinstance(q, list) and q:
            d = q[0]["state"]["data"]["data"]
            if isinstance(d, list):
                jobs = [x for x in d if isinstance(x, dict)]
    except Exception:
        jobs = []

    out: List[SourceJob] = []
    for j in jobs:
        out.append(
            SourceJob(
                source="adult_api:docler_next",
                company="Docler Holding",
                title=_text(j.get("title") or j.get("name")),
                location=_text(j.get("location") or j.get("city")),
                url=_text(j.get("url") or j.get("slug")),
                employment=_text(j.get("employmentType") or j.get("type")),
                posted_at=_text(j.get("createdAt") or j.get("updatedAt")),
                description=_strip_html(_text(j.get("description"))),
                raw=j,
            )
        )
    return out


def _dedupe_jobs(rows: Iterable[SourceJob]) -> List[SourceJob]:
    out: List[SourceJob] = []
    seen = set()
    for r in rows:
        key = (_canonical_url(r.url), _text(r.title).lower(), _text(r.company).lower())
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def _write_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "source",
        "company",
        "title",
        "location",
        "employment",
        "remote_mode",
        "qa_score",
        "url",
        "posted_at",
        "description_snippet",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Scan non-LinkedIn adult/creator-tech sources (Greenhouse/Workable/Next.js) for QA/test roles."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--write-db", action="store_true", help="Upsert to activity.sqlite")
    ap.add_argument("--include-non-qa", action="store_true", help="Keep all jobs, not only QA/test roles")
    ap.add_argument("--timeout-sec", type=float, default=25.0)
    ap.add_argument("--out", default="", help="Output CSV path")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _text(args.db):
        db_path = resolve_path(ROOT, _text(args.db))

    all_rows: List[SourceJob] = []
    source_stats: Dict[str, int] = {}

    for name, fetcher in [
        ("aylo_greenhouse", _fetch_aylo),
        ("chaturbate_workable", _fetch_multimedia),
        ("docler_next", _fetch_docler),
    ]:
        try:
            rows = fetcher(float(args.timeout_sec))
            source_stats[name] = len(rows)
            all_rows.extend(rows)
        except Exception as e:
            source_stats[name] = -1
            print(f"[adult-scan] source {name} failed: {type(e).__name__}: {str(e)[:200]}")

    all_rows = _dedupe_jobs(all_rows)
    enriched: List[Dict[str, Any]] = []
    for r in all_rows:
        qa = _qa_score(r.title, r.description)
        remote_mode = _remote_mode(r.location, r.description)
        row = {
            "source": r.source,
            "company": r.company,
            "title": r.title,
            "location": r.location,
            "employment": r.employment,
            "remote_mode": remote_mode,
            "qa_score": qa,
            "url": _canonical_url(r.url),
            "posted_at": r.posted_at,
            "description_snippet": _text(r.description)[:300],
            "raw": r.raw,
        }
        if (not args.include_non_qa) and qa <= 0:
            continue
        enriched.append(row)

    enriched.sort(key=lambda x: (int(x.get("qa_score") or 0), x.get("source", "")), reverse=True)

    out_path = (
        resolve_path(ROOT, args.out)
        if _text(args.out)
        else (ROOT / "data" / "out" / f"adult_tech_scan_{_ts()}.csv")
    )
    _write_csv(out_path, enriched)

    inserted = 0
    updated = 0
    if args.write_db:
        conn = db_connect(db_path)
        init_db(conn)
        with conn:
            for r in enriched:
                raw = dict(r.get("raw") or {})
                raw.update(
                    {
                        "qa_score": int(r.get("qa_score") or 0),
                        "remote_mode": r.get("remote_mode"),
                        "employment": r.get("employment"),
                        "sector": "adult_creator_tech",
                        "contact_method": "platform_apply",
                    }
                )
                lid, ins = upsert_lead_with_flag(
                    conn,
                    LeadUpsert(
                        platform="adult_tech",
                        lead_type="job",
                        contact=_text(r.get("url")),
                        url=_text(r.get("url")),
                        company=_text(r.get("company")),
                        job_title=_text(r.get("title")),
                        location=_text(r.get("location")),
                        source=_text(r.get("source")),
                        created_at=None,
                        raw=raw,
                    ),
                )
                if ins:
                    inserted += 1
                    add_event(
                        conn,
                        lead_id=lid,
                        event_type="adult_tech_collected",
                        status="ok",
                        occurred_at=_today_midnight_iso(),
                    )
                else:
                    updated += 1
        conn.close()

    print("[adult-scan] source stats:")
    for k, v in source_stats.items():
        print(f"- {k}: {v}")
    print(f"[adult-scan] qa_filtered_rows={len(enriched)}")
    print(f"[adult-scan] csv -> {out_path}")
    if args.write_db:
        print(f"[adult-scan] db -> {db_path}")
        print(f"[adult-scan] upserted inserted={inserted} updated={updated}")
    for i, r in enumerate(enriched[:15], 1):
        print(
            f"{i:02d}. {r.get('company')} | {r.get('title')} | "
            f"qa={r.get('qa_score')} | {r.get('remote_mode')} | {r.get('url')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
