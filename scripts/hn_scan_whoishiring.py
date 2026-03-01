import argparse
import csv
import html as htmllib
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.filtering import filter_and_score  # noqa: E402
from src.models import Job  # noqa: E402
from src.utils import dedupe_jobs, normalize_text  # noqa: E402


ALGOLIA_SEARCH = "https://hn.algolia.com/api/v1/search"
ALGOLIA_ITEM = "https://hn.algolia.com/api/v1/items/{id}"


EMAIL_RE = re.compile(r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b")
URL_RE = re.compile(r'(?i)\bhttps?://[^\s<>"]+')

CONTRACT_HINT_RE = re.compile(
    r"(?i)(?:\bcontract\b|\bfreelance\b|\bconsultant\b|\btemporary\b|\btemp\b|\b1099\b|\bc2c\b|\bw2\b)"
)


def now_stamp() -> str:
    return datetime.now().strftime("%Y-%m-%d_%H%M%S")


def today_midnight_iso() -> str:
    return f"{date.today().isoformat()}T00:00:00"


def _strip_html(text: str) -> str:
    s = htmllib.unescape(text or "")
    s = s.replace("<p>", "\n").replace("</p>", "\n")
    s = s.replace("<br>", "\n").replace("<br/>", "\n").replace("<br />", "\n")
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"[ \t\r\f\v]+", " ", s)
    s = re.sub(r"\\n\\s*\\n\\s*", "\n", s)
    return s.strip()


def _first_line(text: str) -> str:
    for line in (text or "").splitlines():
        line = line.strip()
        if line:
            return line
    return ""


def _split_pipes(line: str) -> List[str]:
    parts = [p.strip() for p in (line or "").split("|")]
    return [p for p in parts if p]


ROLE_PART_RE = re.compile(
    r"(?i)(?:qa engineer|qa automation|quality assurance engineer|quality assurance|quality engineer|\bsdet\b|test engineer|testing engineer|test automation engineer|test automation|software tester|software test)"
)
LOC_PART_RE = re.compile(r"(?i)(?:\bremote\b|onsite|on-site|hybrid|worldwide|global|anywhere|work from anywhere)")
TYPE_PART_RE = re.compile(r"(?i)(?:full[- ]?time|part[- ]?time|contract|intern|internship|temporary|freelance)")


def _pick_best_part(parts: List[str], rx: re.Pattern, *, skip: Optional[str] = None) -> str:
    best = ""
    best_score = 0
    for p in parts:
        if skip and p == skip:
            continue
        score = len(rx.findall(p or ""))
        if score > best_score:
            best_score = score
            best = p
    return best


def parse_header(line1: str) -> Tuple[str, str, str]:
    """
    HN headers are usually pipe-delimited but ordering varies:
      Company | Role | Location | Type ...
      Company | Location | Role | Type ...
    We pick best role + best location heuristically.
    """
    parts = _split_pipes(line1)
    company = parts[0] if parts else ""
    tail = parts[1:] if len(parts) > 1 else []

    title = _pick_best_part(tail, ROLE_PART_RE)
    location = _pick_best_part(tail, LOC_PART_RE, skip=title)

    if not title:
        for p in tail:
            if TYPE_PART_RE.search(p or ""):
                continue
            title = p
            break

    return company, title, location


def _extract_emails(text: str) -> List[str]:
    emails = [m.group(0).strip() for m in EMAIL_RE.finditer(text or "")]
    out: List[str] = []
    seen = set()
    for e in emails:
        el = e.lower()
        if el in seen:
            continue
        seen.add(el)
        out.append(e)
    return out


def _clean_url(u: str) -> str:
    u = (u or "").strip()
    u = u.rstrip(").,;]")
    return u


def _extract_urls(text: str) -> List[str]:
    urls = [_clean_url(m.group(0)) for m in URL_RE.finditer(text or "")]
    out: List[str] = []
    seen = set()
    for u in urls:
        ul = u.lower()
        if ul in seen:
            continue
        seen.add(ul)
        out.append(u)
    return out


def is_remote_text(text: str) -> bool:
    t = normalize_text(text)
    return any(k in t for k in ("remote", "distributed", "anywhere", "work from anywhere", "worldwide"))


def contract_hints(text: str) -> bool:
    return bool(CONTRACT_HINT_RE.search(text or ""))


@dataclass
class RowOut:
    thread: str
    score: int
    remote: str
    contract: str
    company: str
    title: str
    location: str
    contact: str
    url: str


def _write_csv(path: Path, rows: Iterable[RowOut]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        wr = csv.DictWriter(
            f,
            fieldnames=[
                "thread",
                "score",
                "remote",
                "contract",
                "company",
                "title",
                "location",
                "contact",
                "url",
            ],
        )
        wr.writeheader()
        for r in rows:
            wr.writerow(
                {
                    "thread": r.thread,
                    "score": r.score,
                    "remote": r.remote,
                    "contract": r.contract,
                    "company": r.company,
                    "title": r.title,
                    "location": r.location,
                    "contact": r.contact,
                    "url": r.url,
                }
            )


def _thread_title_for(month: str) -> str:
    dt = datetime.strptime(month + "-01", "%Y-%m-%d")
    return f"Ask HN: Who is hiring? ({dt.strftime('%B %Y')})"


def _previous_month(month: str) -> str:
    dt = datetime.strptime(month + "-01", "%Y-%m-%d")
    year = dt.year
    month_num = dt.month - 1
    if month_num <= 0:
        year -= 1
        month_num = 12
    return f"{year:04d}-{month_num:02d}"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Scan Hacker News 'Who is hiring' thread via Algolia API.")
    p.add_argument("--config", default="config/config.yaml", help="Path to config YAML.")
    p.add_argument("--month", default=date.today().strftime("%Y-%m"), help="Thread month (YYYY-MM).")
    p.add_argument("--limit", type=int, default=200, help="Max shortlisted items to insert/export.")
    p.add_argument("--min-score", type=int, default=2, help="Min keyword score for shortlist.")
    p.add_argument("--contracts-only", action="store_true", help="Keep only items mentioning contract-like terms.")
    p.add_argument("--out", default="", help="Optional CSV output path.")
    return p.parse_args()


def _find_thread_id(title: str) -> Optional[str]:
    r = requests.get(ALGOLIA_SEARCH, params={"query": title, "tags": "story"}, timeout=25)
    r.raise_for_status()
    data = r.json()
    hits = data.get("hits") or []
    if not hits:
        return None
    for h in hits:
        if (h.get("title") or "").strip() == title and h.get("objectID"):
            return str(h["objectID"])
    if hits[0].get("objectID"):
        return str(hits[0]["objectID"])
    return None


def main() -> int:
    args = parse_args()
    cfg = load_config(str(resolve_path(ROOT, args.config)))

    requested_month = args.month
    thread_title = _thread_title_for(requested_month)
    thread_id = _find_thread_id(thread_title)
    if not thread_id:
        fallback_month = _previous_month(requested_month)
        fallback_title = _thread_title_for(fallback_month)
        fallback_id = _find_thread_id(fallback_title)
        if fallback_id:
            print(f"[hn] thread not found for {requested_month}, fallback -> {fallback_month}")
            args.month = fallback_month
            thread_title = fallback_title
            thread_id = fallback_id
    if not thread_id:
        print(f"[hn] thread not found: {thread_title}")
        return 2

    r = requests.get(ALGOLIA_ITEM.format(id=thread_id), timeout=40)
    r.raise_for_status()
    data = r.json()

    children = data.get("children") or []
    print(f"[hn] thread: {thread_title} id={thread_id} children={len(children)}")

    jobs: List[Job] = []
    meta: List[Dict[str, Any]] = []

    for c in children:
        if not isinstance(c, dict) or c.get("type") != "comment":
            continue
        cid = str(c.get("id") or "")
        raw_html = c.get("text") or ""
        text = _strip_html(raw_html)
        if not text:
            continue

        line1 = _first_line(text)
        company, title, location = parse_header(line1)

        m_role = ROLE_PART_RE.search(line1) or ROLE_PART_RE.search(text)
        if m_role:
            title = m_role.group(0).strip()

        title = title or line1[:140]

        emails = _extract_emails(text)
        urls = _extract_urls(text)
        contact = emails[0] if emails else (urls[0] if urls else f"https://news.ycombinator.com/item?id={cid}")

        jobs.append(
            Job(
                title=title,
                company=company or "Unknown",
                location=location,
                url=f"https://news.ycombinator.com/item?id={cid}",
                description=text,
                contact_email=emails[0] if emails else "",
                source=f"hn_whoishiring:{args.month}",
                raw={
                    "thread_id": thread_id,
                    "comment_id": cid,
                    "line1": line1,
                    "parsed_title": title,
                    "parsed_location": location,
                    "emails": emails,
                    "urls": urls,
                    "text": text,
                },
            )
        )
        meta.append({"comment_id": cid, "contact": contact})

    jobs = dedupe_jobs(jobs)

    include_keywords = cfg_get(cfg, "profile.keywords.include", []) or []
    exclude_keywords: List[str] = []

    candidates: List[Job] = []
    for j in jobs:
        raw = j.raw or {}
        header = f"{raw.get('line1','')} {j.title} {j.location}".strip()
        remote_blob = f"{header}\n{j.description}".strip()
        if not is_remote_text(remote_blob):
            continue
        candidates.append(j)

    _all, shortlisted = filter_and_score(
        candidates,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
        locations=[],
        min_score=0,
        max_results=0,
    )

    shortlisted2: List[Tuple[Job, int]] = []
    for j, score in shortlisted:
        if int(score) < int(args.min_score):
            continue
        if args.contracts_only and not contract_hints(j.description or ""):
            continue
        shortlisted2.append((j, score))
        if len(shortlisted2) >= int(args.limit):
            break

    out_dir = resolve_path(ROOT, cfg_get(cfg, "output.out_dir", "data/out"))
    out_path = resolve_path(ROOT, args.out) if args.out else (out_dir / f"hn_whoishiring_{args.month}_{now_stamp()}.csv")

    rows_out: List[RowOut] = []
    for j, score in shortlisted2:
        desc = j.description or ""
        raw = j.raw or {}
        contact = (raw.get("emails") or [""])[0] or (raw.get("urls") or [""])[0] or j.url
        rows_out.append(
            RowOut(
                thread=args.month,
                score=int(score),
                remote="yes",
                contract="yes" if contract_hints(desc) else "no",
                company=j.company,
                title=j.title,
                location=j.location,
                contact=contact,
                url=j.url,
            )
        )
    _write_csv(out_path, rows_out)

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    conn = db_connect(db_path)
    init_db(conn)

    inserted = 0
    updated = 0
    contracts = 0

    with conn:
        for j, score in shortlisted2:
            raw = dict(j.raw or {})
            raw["score"] = int(score)
            raw["remote"] = True
            raw["contract_like"] = bool(contract_hints(j.description))
            contact = (raw.get("emails") or [""])[0] or (raw.get("urls") or [""])[0] or j.url
            lid, ins = upsert_lead_with_flag(
                conn,
                LeadUpsert(
                    platform="hn",
                    lead_type="post",
                    contact=contact,
                    url=j.url,
                    company=j.company,
                    job_title=j.title,
                    location=j.location,
                    source=j.source,
                    raw=raw,
                ),
            )
            if raw.get("contract_like"):
                contracts += 1
            if ins:
                inserted += 1
                add_event(conn, lead_id=lid, event_type="collected", status="ok", occurred_at=today_midnight_iso())
            else:
                updated += 1

    print(f"[hn] shortlisted remote: {len(shortlisted2)} (contracts-like: {contracts})")
    print(f"[hn] db: {db_path} inserted={inserted} updated={updated}")
    print(f"[hn] csv -> {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
