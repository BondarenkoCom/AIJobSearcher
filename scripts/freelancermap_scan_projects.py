import argparse
import csv
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from pypdf import PdfReader

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import bool_env  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402


EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)

QA_RE = re.compile(
    r"\b(qa|quality\s+assurance|tester|testing|test\s+automation|automation\s+test|sdet|performance\s+test|manual\s+test|api\s+test)\b",
    re.IGNORECASE,
)
QA_TITLE_RE = re.compile(
    r"\b(qa|quality\s+assurance|tester|testing|test\s*automation|sdet|test\s*engineer|quality\s*engineer)\b",
    re.IGNORECASE,
)
REMOTE_NEG_RE = re.compile(r"\b(on[-\s]?site|onsite)\b", re.IGNORECASE)
HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
REMOTE_POS_RE = re.compile(r"\bremote\b", re.IGNORECASE)

SLUG_HINT_RE = re.compile(r"(qa|test|testing|sdet|quality|automation|selenium|playwright|cypress|appium)", re.IGNORECASE)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ua_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_resume_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return ""

    parts: List[str] = []
    for p in reader.pages:
        try:
            parts.append(p.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def _resume_skill_tokens(text: str) -> List[str]:
    """
    Extract a small, stable set of tokens we will use to rank projects.
    We intentionally keep this conservative to avoid overfitting.
    """
    t = (text or "").lower()
    candidates = [
        "c#",
        ".net",
        "nunit",
        "restsharp",
        "rest",
        "graphql",
        "api",
        "postman",
        "sql",
        "jira",
        "ci",
        "selenium",
        "playwright",
        "cypress",
        "appium",
        "swagger",
        "auth",
        "authorization",
        "security",
        "regression",
        "exploratory",
    ]
    out: List[str] = []
    for tok in candidates:
        if tok in t:
            out.append(tok)
    seen: set[str] = set()
    uniq: List[str] = []
    for tok in out:
        if tok in seen:
            continue
        seen.add(tok)
        uniq.append(tok)
    return uniq


def _parse_sitemap_urls(xml_bytes: bytes) -> List[Tuple[str, str]]:
    xml_bytes = (xml_bytes or b"").lstrip()
    root = ET.fromstring(xml_bytes)
    out: List[Tuple[str, str]] = []
    for url_el in root.findall(".//{*}url"):
        loc = url_el.findtext("{*}loc") or ""
        lastmod = url_el.findtext("{*}lastmod") or ""
        loc = loc.strip()
        if not loc:
            continue
        out.append((loc, lastmod.strip()))
    return out


def _fetch_sitemap(session: requests.Session, url: str, timeout: Tuple[float, float]) -> List[Tuple[str, str]]:
    r = session.get(url, headers=_ua_headers(), timeout=timeout)
    r.raise_for_status()
    return _parse_sitemap_urls(r.content)


def _extract_project_json(html: str) -> Optional[Dict[str, Any]]:
    soup = BeautifulSoup(html or "", "html.parser")
    scripts = soup.find_all("script", class_=re.compile(r"js-react-on-rails-component"))
    for sc in scripts:
        txt = (sc.string or sc.get_text() or "").strip()
        if not txt or '"project"' not in txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        proj = data.get("project")
        if isinstance(proj, dict) and proj.get("title"):
            return proj
    return None


def _remote_mode(proj: Dict[str, Any]) -> str:
    ct = proj.get("contractType") or {}
    if isinstance(ct, dict):
        rp = ct.get("remoteInPercent")
        try:
            val = int(rp)
            if val >= 100:
                return "remote"
            if val <= 0:
                return "on_site"
            return "hybrid"
        except Exception:
            pass
    title = str(proj.get("title") or "")
    desc = str(proj.get("description") or "")
    txt = (title + "\n" + desc).lower()
    if REMOTE_NEG_RE.search(txt):
        return "on_site"
    if HYBRID_RE.search(txt):
        return "hybrid"
    if REMOTE_POS_RE.search(txt):
        return "remote"
    return "unknown"


def _engagement_type(proj: Dict[str, Any]) -> str:
    ct = proj.get("contractType") or {}
    if isinstance(ct, dict) and str(ct.get("contractType") or "").lower() in {"permanent", "permanent_position"}:
        return "long"

    dur = proj.get("durationInMonths")
    try:
        d = int(dur)
        return "gig" if d <= 2 else "long"
    except Exception:
        pass

    txt = (str(proj.get("title") or "") + "\n" + str(proj.get("description") or "")).lower()
    m = re.search(r"\b(\d+)\s*(month|months|week|weeks|day|days)\b", txt)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if unit.startswith("day"):
            return "gig"
        if unit.startswith("week"):
            return "long" if n >= 4 else "gig"
        if unit.startswith("month"):
            return "gig" if n <= 2 else "long"
    return "unknown"


def _extract_emails(text: str) -> List[str]:
    if not text:
        return []
    emails = sorted({m.group(0).lower() for m in EMAIL_RE.finditer(text)})
    return emails[:10]


def _project_text_for_match(proj: Dict[str, Any]) -> str:
    parts: List[str] = []
    parts.append(str(proj.get("title") or ""))
    parts.append(str(proj.get("description") or ""))
    skills = proj.get("skills") or {}
    if isinstance(skills, dict):
        enabled = skills.get("enabled") or []
        if isinstance(enabled, list):
            for s in enabled:
                if not isinstance(s, dict):
                    continue
                parts.append(str(s.get("localizedName") or s.get("nameEn") or s.get("nameDe") or ""))
    return "\n".join([p for p in parts if p]).strip()


def _score_project(proj: Dict[str, Any], *, resume_tokens: List[str]) -> Tuple[int, List[str]]:
    txt = _project_text_for_match(proj)
    low = txt.lower()
    score = 0
    reasons: List[str] = []

    if QA_RE.search(str(proj.get("title") or "")):
        score += 8
        reasons.append("qa_in_title")
    elif QA_RE.search(low):
        score += 4
        reasons.append("qa_in_text")

    rm = _remote_mode(proj)
    if rm == "remote":
        score += 3
        reasons.append("remote")
    elif rm == "hybrid":
        score += 1
        reasons.append("hybrid")
    elif rm == "on_site":
        score -= 3
        reasons.append("on_site")

    hits: List[str] = []
    for tok in resume_tokens:
        if tok and tok in low:
            hits.append(tok)
    if hits:
        score += min(8, len(set(hits)))
        reasons.append("resume_hits:" + ",".join(sorted(set(hits))[:8]))

    emails = _extract_emails(str(proj.get("description") or ""))
    if emails:
        score += 2
        reasons.append("has_email")

    if re.search(r"\b(eu nationals only|us only|uk only|germany only|belgium based|must be based)\b", low):
        score -= 4
        reasons.append("geo_restricted")

    return score, reasons


def _is_qa_title(title: str) -> bool:
    return bool(QA_TITLE_RE.search(title or ""))


@dataclass(frozen=True)
class ScannedProject:
    platform: str
    project_id: str
    title: str
    url: str
    company: str
    location: str
    remote_mode: str
    engagement: str
    duration_months: Optional[int]
    workload_pct: Optional[int]
    created_at: str
    updated_at: str
    expires_at: str
    emails: List[str]
    score: int
    reasons: List[str]
    raw: Dict[str, Any]


def _scan_one(session: requests.Session, url: str, *, resume_tokens: List[str], timeout: Tuple[float, float]) -> Optional[ScannedProject]:
    try:
        r = session.get(url, headers=_ua_headers(), timeout=timeout)
        if r.status_code >= 400:
            return None
        proj = _extract_project_json(r.text)
        if not proj:
            return None

        title = str(proj.get("title") or "")
        if not _is_qa_title(title):
            return None
        txt = _project_text_for_match(proj)

        score, reasons = _score_project(proj, resume_tokens=resume_tokens)

        pid = str(proj.get("id") or "").strip()
        title = str(proj.get("title") or "").strip()
        if not pid or not title:
            return None

        company = str(proj.get("company") or "").strip()
        city = str(proj.get("city") or "").strip()
        country = ""
        c = proj.get("country") or {}
        if isinstance(c, dict):
            country = str(c.get("localizedName") or c.get("nameEn") or c.get("iso2") or "").strip()
        location = ", ".join([p for p in [city, country] if p])

        rm = _remote_mode(proj)
        engagement = _engagement_type(proj)

        dur_m = None
        try:
            if proj.get("durationInMonths") is not None:
                dur_m = int(proj.get("durationInMonths"))
        except Exception:
            dur_m = None

        workload = None
        try:
            if proj.get("workload") is not None:
                workload = int(proj.get("workload"))
        except Exception:
            workload = None

        created = str(proj.get("created") or "").strip()
        updated = str(proj.get("updated") or "").strip()
        expires = str(proj.get("expires") or "").strip()

        emails = _extract_emails(str(proj.get("description") or ""))

        raw = {
            "platform_domain": "freelancermap.com",
            "project_id": pid,
            "remote_mode": rm,
            "engagement": engagement,
            "score": score,
            "reasons": reasons,
            "emails": emails,
            "company_url": ("https://www.freelancermap.com" + str(proj.get("companyUrl") or "")) if proj.get("companyUrl") else "",
            "poster_api": ("https://www.freelancermap.com" + str(proj.get("poster") or "")) if proj.get("poster") else "",
            "duration_in_months": dur_m,
            "workload_pct": workload,
            "extension_possible": bool(proj.get("extensionPossible")) if proj.get("extensionPossible") is not None else None,
            "start_text": str(proj.get("startText") or "").strip(),
            "skills": [
                str(s.get("localizedName") or s.get("nameEn") or s.get("nameDe") or "").strip()
                for s in ((proj.get("skills") or {}).get("enabled") or [])
                if isinstance(s, dict)
            ],
            "text": txt[:8000],  # cap for DB sanity
        }

        return ScannedProject(
            platform="freelancermap.com",
            project_id=pid,
            title=title,
            url=url,
            company=company or "freelancermap.com",
            location=location,
            remote_mode=rm,
            engagement=engagement,
            duration_months=dur_m,
            workload_pct=workload,
            created_at=created,
            updated_at=updated,
            expires_at=expires,
            emails=emails,
            score=score,
            reasons=reasons,
            raw=raw,
        )
    except Exception:
        return None


def _write_csv(path: Path, rows: Iterable[ScannedProject]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "captured_at",
                "platform",
                "score",
                "remote_mode",
                "engagement",
                "duration_months",
                "workload_pct",
                "title",
                "company",
                "location",
                "url",
                "contact_emails",
                "contact_method",
                "reasons",
            ],
        )
        w.writeheader()
        now = _now_iso()
        for r in rows:
            w.writerow(
                {
                    "captured_at": now,
                    "platform": r.platform,
                    "score": r.score,
                    "remote_mode": r.remote_mode,
                    "engagement": r.engagement,
                    "duration_months": r.duration_months if r.duration_months is not None else "",
                    "workload_pct": r.workload_pct if r.workload_pct is not None else "",
                    "title": r.title,
                    "company": r.company,
                    "location": r.location,
                    "url": r.url,
                    "contact_emails": ";".join(r.emails),
                    "contact_method": ("email" if r.emails else "apply_via_platform"),
                    "reasons": ",".join(r.reasons),
                }
            )


def main() -> int:
    ap = argparse.ArgumentParser(description="Scan freelancermap.com public projects via sitemap and filter for QA fits.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path (else taken from config)")
    ap.add_argument("--resume-pdf", default="Docs/ARTEM_BONDARENKO_CV_2026.pdf")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--limit", type=int, default=120, help="Max project URLs to fetch after prefilter")
    ap.add_argument("--no-slug-prefilter", action="store_true", help="Scan all sitemap URLs (slower), not only QA-ish slug hints")
    ap.add_argument("--min-score", type=int, default=8)
    ap.add_argument("--max-workers", type=int, default=8)
    ap.add_argument("--timeout-sec", type=float, default=12.0)
    ap.add_argument("--require-remote", action="store_true", help="Keep only remote (100%) projects")
    ap.add_argument("--include-hybrid", action="store_true", help="If require-remote is set, also keep hybrid")
    ap.add_argument("--write-db", action="store_true", help="Upsert into activity.sqlite")
    ap.add_argument("--dry-run", action="store_true", help="Do not write to DB; only output CSVs")
    ap.add_argument("--telegram", action="store_true", help="Send a short Telegram summary (respects TELEGRAM_REPORT=0)")
    args = ap.parse_args()

    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    resume_path = resolve_path(ROOT, args.resume_pdf)
    resume_text = _extract_resume_text(resume_path) if resume_path.exists() else ""
    resume_tokens = _resume_skill_tokens(resume_text)
    if resume_tokens:
        print(f"[freelancermap] resume tokens: {', '.join(resume_tokens)}")

    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    timeout = (min(10.0, args.timeout_sec), args.timeout_sec)

    projects_idx = "https://www.freelancermap.com/sitemaps/projects-0.xml"
    urls = _fetch_sitemap(session, projects_idx, timeout=timeout)
    if args.no_slug_prefilter:
        cand_urls = [u for (u, _lm) in urls]
    else:
        cand_urls = [u for (u, _lm) in urls if SLUG_HINT_RE.search(u)]
    if args.limit > 0:
        cand_urls = cand_urls[: int(args.limit)]
    print(f"[freelancermap] sitemap urls={len(urls)} candidates={len(cand_urls)}")

    scanned: List[ScannedProject] = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.max_workers))) as ex:
        futs = [
            ex.submit(_scan_one, session, u, resume_tokens=resume_tokens, timeout=timeout)
            for u in cand_urls
        ]
        for fut in as_completed(futs):
            v = fut.result()
            if not v:
                continue
            if v.score < int(args.min_score):
                continue
            if args.require_remote:
                if v.remote_mode == "remote":
                    pass
                elif args.include_hybrid and v.remote_mode == "hybrid":
                    pass
                else:
                    continue
            scanned.append(v)
            time.sleep(0.05)

    scanned.sort(key=lambda x: (x.score, x.duration_months or 0), reverse=True)
    print(f"[freelancermap] scanned_fit={len(scanned)} (min_score={args.min_score})")

    long_rows = [r for r in scanned if r.engagement == "long"]
    gig_rows = [r for r in scanned if r.engagement != "long"]

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_long = out_dir / f"freelancermap_long_{stamp}.csv"
    csv_gig = out_dir / f"freelancermap_gigs_{stamp}.csv"
    _write_csv(csv_long, long_rows)
    _write_csv(csv_gig, gig_rows)
    print(f"[freelancermap] wrote: {csv_long} (rows={len(long_rows)})")
    print(f"[freelancermap] wrote: {csv_gig} (rows={len(gig_rows)})")

    inserted = 0
    updated = 0
    if args.write_db and (not args.dry_run):
        conn = db_connect(db_path)
        init_db(conn)
        with conn:
            for r in scanned:
                lead = LeadUpsert(
                    platform=r.platform,
                    lead_type="project",
                    contact=r.project_id,
                    url=r.url,
                    company=r.company,
                    job_title=r.title,
                    location=r.location,
                    source="public_scan:freelancermap_sitemap",
                    created_at=_now_iso(),
                    raw=r.raw,
                )
                lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                if was_inserted:
                    inserted += 1
                else:
                    updated += 1

                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="freelance_project_collected",
                    status="ok",
                    details={
                        "platform": r.platform,
                        "project_id": r.project_id,
                        "url": r.url,
                        "remote_mode": r.remote_mode,
                        "engagement": r.engagement,
                        "duration_months": r.duration_months,
                        "workload_pct": r.workload_pct,
                        "emails": r.emails,
                        "score": r.score,
                    },
                )
            conn.commit()
        try:
            conn.close()
        except Exception:
            pass

    print(f"[freelancermap] db: {db_path} inserted={inserted} updated={updated}")

    if args.telegram and bool_env("TELEGRAM_REPORT", True):
        lines = [
            "AIJobSearcher: Freelancermap public scan",
            f"Fit projects: {len(scanned)} (long={len(long_rows)} gigs={len(gig_rows)})",
            f"DB: {db_path}",
            f"CSV long: {csv_long}",
            f"CSV gigs: {csv_gig}",
        ]
        send_telegram_message("\n".join(lines))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())


