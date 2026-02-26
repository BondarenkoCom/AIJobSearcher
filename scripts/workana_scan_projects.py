import argparse
import csv
import html
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlencode

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
    r"\b(qa|quality\s+assurance|tester|testing|test\s*automation|automation\s*test|sdet|quality\s*engineer)\b",
    re.IGNORECASE,
)
LONG_HINT_RE = re.compile(
    r"\b(long[-\s]?term|ongoing|full[-\s]?time|monthly|retainer|12\s*months?|6\s*months?)\b",
    re.IGNORECASE,
)
GIG_HINT_RE = re.compile(
    r"\b(one[-\s]?time|quick|small task|fix bug|single task|few hours|urgent)\b",
    re.IGNORECASE,
)
ONSITE_RE = re.compile(r"\b(on[-\s]?site|onsite|relocate|office[-\s]?based)\b", re.IGNORECASE)
HYBRID_RE = re.compile(r"\bhybrid\b", re.IGNORECASE)
GEO_RESTRICT_RE = re.compile(
    r"\b(only candidates from|must be based in|residents only|us only|eu only|uk only|germany only)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ua_headers() -> Dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }


def _extract_resume_text(pdf_path: Path) -> str:
    try:
        reader = PdfReader(str(pdf_path))
    except Exception:
        return ""
    parts: List[str] = []
    for page in reader.pages:
        try:
            parts.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(parts)


def _resume_skill_tokens(text: str) -> List[str]:
    t = (text or "").lower()
    candidates = [
        "qa",
        "c#",
        ".net",
        "nunit",
        "restsharp",
        "rest",
        "graphql",
        "api",
        "postman",
        "sql",
        "selenium",
        "playwright",
        "jira",
        "jenkins",
        "bitbucket",
        "gitlab",
        "docker",
        "security",
        "regression",
        "exploratory",
    ]
    out: List[str] = []
    seen: Set[str] = set()
    for tok in candidates:
        if tok in t and tok not in seen:
            seen.add(tok)
            out.append(tok)
    return out


def _extract_emails(text: str) -> List[str]:
    if not text:
        return []
    out = sorted({m.group(0).lower() for m in EMAIL_RE.finditer(text)})
    return out[:10]


def _parse_results_initials(html_text: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_text or "", "html.parser")
    search_tag = soup.find("search")
    if not search_tag:
        return {}
    raw = (
        search_tag.get(":results-initials")
        or search_tag.get("results-initials")
        or search_tag.get("data-results-initials")
        or ""
    )
    if not raw:
        return {}
    try:
        return json.loads(html.unescape(raw))
    except Exception:
        return {}


def _extract_title_and_url(title_html: str, slug: str) -> Tuple[str, str]:
    title = ""
    url = ""
    if title_html:
        frag = BeautifulSoup(title_html, "html.parser")
        a = frag.find("a")
        if a is not None:
            href = str(a.get("href") or "").strip()
            if href:
                url = ("https://www.workana.com" + href) if href.startswith("/") else href
            span = a.find("span")
            if span is not None:
                title = str(span.get("title") or "").strip()
            if not title:
                title = a.get_text(" ", strip=True)
        if not title:
            title = frag.get_text(" ", strip=True)
    if not url and slug:
        url = f"https://www.workana.com/job/{slug}"
    return title.strip(), url.strip()


def _extract_country(country_html: str) -> str:
    if not country_html:
        return ""
    frag = BeautifulSoup(country_html, "html.parser")
    a = frag.find("a")
    if a is None:
        txt = frag.get_text(" ", strip=True)
        return txt.strip()
    return a.get_text(" ", strip=True).strip()


def _extract_skills(item: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    skills = item.get("skills")
    if not isinstance(skills, list):
        return out
    for s in skills:
        if isinstance(s, dict):
            t = str(s.get("anchorText") or "").strip()
            if t:
                out.append(t)
    seen: Set[str] = set()
    uniq: List[str] = []
    for s in out:
        k = s.lower()
        if k in seen:
            continue
        seen.add(k)
        uniq.append(s)
    return uniq


def _project_text(*parts: Sequence[str]) -> str:
    flat: List[str] = []
    for p in parts:
        if isinstance(p, str):
            if p.strip():
                flat.append(p.strip())
        else:
            for x in p:
                sx = str(x or "").strip()
                if sx:
                    flat.append(sx)
    return "\n".join(flat).strip()


def _remote_mode(text: str) -> str:
    low = (text or "").lower()
    if ONSITE_RE.search(low):
        return "on_site"
    if HYBRID_RE.search(low):
        return "hybrid"
    # On Workana, freelance projects are remote by default unless explicitly restricted.
    return "remote"


def _engagement_type(item: Dict[str, Any], text: str) -> str:
    if bool(item.get("isHourly")):
        return "long"

    low = (text or "").lower()
    if LONG_HINT_RE.search(low):
        return "long"
    if GIG_HINT_RE.search(low):
        return "gig"

    # If no signal, fixed-price freelance tasks are treated as gig by default.
    return "gig"


def _score_project(
    *,
    title: str,
    text: str,
    resume_tokens: List[str],
    remote_mode: str,
    engagement: str,
    emails: List[str],
) -> Tuple[int, List[str]]:
    low = (text or "").lower()
    score = 0
    reasons: List[str] = []

    if QA_RE.search(title or ""):
        score += 8
        reasons.append("qa_in_title")
    elif QA_RE.search(low):
        score += 4
        reasons.append("qa_in_text")
    else:
        score -= 6
        reasons.append("not_qa_like")

    if remote_mode == "remote":
        score += 2
        reasons.append("remote")
    elif remote_mode == "hybrid":
        score += 1
        reasons.append("hybrid")
    elif remote_mode == "on_site":
        score -= 3
        reasons.append("on_site")

    if engagement == "long":
        score += 1
        reasons.append("long")
    elif engagement == "gig":
        score += 1
        reasons.append("gig")

    hits: List[str] = []
    for tok in resume_tokens:
        if tok and tok in low:
            hits.append(tok)
    if hits:
        uniq = sorted(set(hits))
        score += min(8, len(uniq))
        reasons.append("resume_hits:" + ",".join(uniq[:8]))

    if GEO_RESTRICT_RE.search(low):
        score -= 4
        reasons.append("geo_restricted")

    if emails:
        score += 2
        reasons.append("has_email")

    return score, reasons


@dataclass(frozen=True)
class ScannedProject:
    platform: str
    slug: str
    title: str
    url: str
    company: str
    location: str
    remote_mode: str
    engagement: str
    is_hourly: bool
    budget: str
    posted_date: str
    published_date: str
    total_bids: str
    emails: List[str]
    score: int
    reasons: List[str]
    raw: Dict[str, Any]


def _scan_search_page(
    session: requests.Session,
    *,
    url: str,
    timeout: Tuple[float, float],
    resume_tokens: List[str],
    strict_qa_title: bool,
) -> Tuple[List[ScannedProject], int]:
    r = session.get(url, headers=_ua_headers(), timeout=timeout)
    r.raise_for_status()

    data = _parse_results_initials(r.text)
    results = data.get("results")
    if not isinstance(results, list):
        return [], 0

    pages = 0
    pagination = data.get("pagination")
    if isinstance(pagination, dict):
        try:
            pages = int(pagination.get("pages") or 0)
        except Exception:
            pages = 0

    out: List[ScannedProject] = []
    for item in results:
        if not isinstance(item, dict):
            continue

        slug = str(item.get("slug") or "").strip()
        title_html = str(item.get("title") or "")
        title, project_url = _extract_title_and_url(title_html, slug)
        if not slug or not title or not project_url:
            continue
        if strict_qa_title and (not QA_RE.search(title)):
            continue

        author = str(item.get("authorName") or "").strip() or "workana_client"
        description = str(item.get("description") or "").strip()
        skills = _extract_skills(item)
        country = _extract_country(str(item.get("country") or ""))
        budget = str(item.get("budget") or "").strip()
        posted_date = str(item.get("postedDate") or "").strip()
        published_date = str(item.get("publishedDate") or "").strip()
        total_bids = str(item.get("totalBids") or "").strip()
        is_hourly = bool(item.get("isHourly"))

        text = _project_text(title, description, skills, budget)
        remote_mode = _remote_mode(text)
        engagement = _engagement_type(item, text)
        emails = _extract_emails(description)
        score, reasons = _score_project(
            title=title,
            text=text,
            resume_tokens=resume_tokens,
            remote_mode=remote_mode,
            engagement=engagement,
            emails=emails,
        )

        raw = {
            "platform_domain": "workana.com",
            "slug": slug,
            "author_name": author,
            "budget": budget,
            "is_hourly": is_hourly,
            "posted_date": posted_date,
            "published_date": published_date,
            "total_bids": total_bids,
            "skills": skills,
            "description": description[:16000],
            "emails": emails,
            "remote_mode": remote_mode,
            "engagement": engagement,
            "score": score,
            "reasons": reasons,
            "permission_endpoint": f"/workers/permissions/make_bid_action?projectSlug={slug}",
        }

        out.append(
            ScannedProject(
                platform="workana.com",
                slug=slug,
                title=title,
                url=project_url,
                company=author,
                location=country,
                remote_mode=remote_mode,
                engagement=engagement,
                is_hourly=is_hourly,
                budget=budget,
                posted_date=posted_date,
                published_date=published_date,
                total_bids=total_bids,
                emails=emails,
                score=score,
                reasons=reasons,
                raw=raw,
            )
        )

    return out, pages


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
                "hourly",
                "title",
                "company",
                "location",
                "budget",
                "posted_date",
                "total_bids",
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
                    "hourly": int(r.is_hourly),
                    "title": r.title,
                    "company": r.company,
                    "location": r.location,
                    "budget": r.budget,
                    "posted_date": r.posted_date,
                    "total_bids": r.total_bids,
                    "url": r.url,
                    "contact_emails": ";".join(r.emails),
                    "contact_method": ("email" if r.emails else "apply_via_platform"),
                    "reasons": ",".join(r.reasons),
                }
            )


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Scan Workana jobs via public HTML+embedded JSON and save QA-fit projects."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--resume-pdf", default="Docs/ARTEM_BONDARENKO_CV_2026.pdf")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--category", default="it-programming")
    ap.add_argument("--language", default="en")
    ap.add_argument("--skills", default="qa-automation,c-1,api,rest-api,selenium-webdriver,playwright")
    ap.add_argument("--query", default="qa", help="Text query for Workana (query=...)")
    ap.add_argument("--pages", type=int, default=6, help="Max pages to fetch")
    ap.add_argument("--limit", type=int, default=160, help="Max projects kept after filtering")
    ap.add_argument("--sleep-sec", type=float, default=1.2, help="Delay between page fetches")
    ap.add_argument("--min-score", type=int, default=7)
    ap.add_argument("--strict-qa-title", action="store_true", help="Require explicit QA keywords in title")
    ap.add_argument("--require-remote", action="store_true", help="Keep only remote")
    ap.add_argument("--include-hybrid", action="store_true", help="If require-remote, include hybrid too")
    ap.add_argument("--write-db", action="store_true", help="Upsert into activity.sqlite")
    ap.add_argument("--dry-run", action="store_true", help="No DB writes")
    ap.add_argument("--telegram", action="store_true", help="Send summary to Telegram")
    args = ap.parse_args()

    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if str(args.db).strip():
        db_path = resolve_path(ROOT, str(args.db).strip())

    resume_path = resolve_path(ROOT, args.resume_pdf)
    resume_text = _extract_resume_text(resume_path) if resume_path.exists() else ""
    resume_tokens = _resume_skill_tokens(resume_text)
    if resume_tokens:
        print(f"[workana-scan] resume tokens: {', '.join(resume_tokens)}")

    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    skills = ",".join([s.strip() for s in str(args.skills).split(",") if s.strip()])
    params: Dict[str, str] = {
        "category": str(args.category).strip(),
        "language": str(args.language).strip(),
    }
    if skills:
        params["skills"] = skills
    if str(args.query).strip():
        params["query"] = str(args.query).strip()

    strict_qa_title = bool(args.strict_qa_title)
    timeout = (8.0, 16.0)
    session = requests.Session()

    max_pages = max(1, int(args.pages))
    total_pages_hint = 0
    all_rows: List[ScannedProject] = []
    seen_slugs: Set[str] = set()

    for page in range(1, max_pages + 1):
        qp = dict(params)
        qp["page"] = str(page)
        url = "https://www.workana.com/jobs?" + urlencode(qp)
        try:
            rows, pages_hint = _scan_search_page(
                session,
                url=url,
                timeout=timeout,
                resume_tokens=resume_tokens,
                strict_qa_title=strict_qa_title,
            )
        except Exception as e:
            print(f"[workana-scan] page={page} fetch error: {e}")
            continue

        if pages_hint > 0:
            total_pages_hint = max(total_pages_hint, pages_hint)

        added = 0
        for r in rows:
            if r.slug in seen_slugs:
                continue
            seen_slugs.add(r.slug)
            all_rows.append(r)
            added += 1

        print(
            f"[workana-scan] page={page} fetched={len(rows)} added={added} "
            f"total_unique={len(all_rows)} pages_hint={pages_hint or '?'}"
        )

        if total_pages_hint and page >= total_pages_hint:
            break
        if page < max_pages:
            time.sleep(max(0.0, float(args.sleep_sec)))

    filtered: List[ScannedProject] = []
    for r in all_rows:
        if r.score < int(args.min_score):
            continue
        if args.require_remote:
            if r.remote_mode == "remote":
                pass
            elif args.include_hybrid and r.remote_mode == "hybrid":
                pass
            else:
                continue
        filtered.append(r)

    filtered.sort(key=lambda x: (x.score, x.is_hourly, x.total_bids), reverse=True)
    if args.limit > 0:
        filtered = filtered[: int(args.limit)]

    long_rows = [r for r in filtered if r.engagement == "long"]
    gig_rows = [r for r in filtered if r.engagement != "long"]

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_long = out_dir / f"workana_long_{stamp}.csv"
    csv_gig = out_dir / f"workana_gigs_{stamp}.csv"
    _write_csv(csv_long, long_rows)
    _write_csv(csv_gig, gig_rows)

    print(
        f"[workana-scan] done: total={len(filtered)} long={len(long_rows)} "
        f"gigs={len(gig_rows)} pages_hint={total_pages_hint or '?'}"
    )
    print(f"[workana-scan] wrote: {csv_long}")
    print(f"[workana-scan] wrote: {csv_gig}")

    inserted = 0
    updated = 0
    if args.write_db and (not args.dry_run):
        conn = db_connect(db_path)
        init_db(conn)
        with conn:
            for r in filtered:
                lead = LeadUpsert(
                    platform=r.platform,
                    lead_type="project",
                    contact=r.slug,
                    url=r.url,
                    company=r.company,
                    job_title=r.title,
                    location=r.location,
                    source="public_scan:workana_jobs",
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
                    event_type="workana_project_collected",
                    status="ok",
                    details={
                        "platform": r.platform,
                        "slug": r.slug,
                        "url": r.url,
                        "remote_mode": r.remote_mode,
                        "engagement": r.engagement,
                        "budget": r.budget,
                        "is_hourly": r.is_hourly,
                        "score": r.score,
                        "emails": r.emails,
                    },
                )
            conn.commit()
        try:
            conn.close()
        except Exception:
            pass

    print(f"[workana-scan] db: {db_path} inserted={inserted} updated={updated}")

    if args.telegram and bool_env("TELEGRAM_REPORT", True):
        lines = [
            "AIJobSearcher: Workana public scan",
            f"Fit projects: {len(filtered)} (long={len(long_rows)} gigs={len(gig_rows)})",
            f"DB: {db_path}",
            f"CSV long: {csv_long}",
            f"CSV gigs: {csv_gig}",
        ]
        send_telegram_message("\n".join(lines))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
