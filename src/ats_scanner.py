import re
import time
from typing import Dict, Iterable, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from .models import ATSSource, CompanyLead, Job


ATS_PATTERNS = {
    "greenhouse": re.compile(r"https?://boards\.greenhouse\.io/([A-Za-z0-9_-]+)", re.I),
    "lever": re.compile(r"https?://jobs\.lever\.co/([A-Za-z0-9_-]+)", re.I),
    "workable": re.compile(r"https?://apply\.workable\.com/([A-Za-z0-9_-]+)", re.I),
    "ashby": re.compile(r"https?://jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)", re.I),
    "smartrecruiters": re.compile(r"https?://(?:www\.)?smartrecruiters\.com/([A-Za-z0-9_-]+)", re.I),
}


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _normalize_url(url: str) -> str:
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("http://") or url.startswith("https://"):
        return url
    if url.startswith("www."):
        return "https://" + url
    return url


def _same_domain(a: str, b: str) -> bool:
    try:
        return urlparse(a).netloc == urlparse(b).netloc
    except Exception:
        return False


def _extract_candidate_links(html: str, base_url: str, max_links: int) -> List[str]:
    if max_links <= 0:
        return []
    soup = BeautifulSoup(html, "html.parser")
    keywords = ["career", "careers", "jobs", "join", "work-with-us", "workwithus", "recruit"]
    links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        text = (a.get_text(" ", strip=True) or "").lower()
        href_lower = href.lower()
        if not any(k in href_lower or k in text for k in keywords):
            continue
        full = _normalize_url(urljoin(base_url, href))
        if not full:
            continue
        if not _same_domain(full, base_url) and "greenhouse.io" not in full and "lever.co" not in full and "workable.com" not in full and "ashbyhq.com" not in full and "smartrecruiters.com" not in full:
            continue
        if full not in links:
            links.append(full)
        if len(links) >= max_links:
            break
    return links


def _find_ats_in_text(text: str) -> List[Tuple[str, str]]:
    found: List[Tuple[str, str]] = []
    for ats_type, pattern in ATS_PATTERNS.items():
        for match in pattern.findall(text):
            if match:
                found.append((ats_type, match))
    return found


def _build_ats_source(lead: CompanyLead, ats_type: str, slug: str, source_url: str) -> ATSSource:
    if ats_type == "greenhouse":
        board_url = f"https://boards.greenhouse.io/{slug}"
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    elif ats_type == "lever":
        board_url = f"https://jobs.lever.co/{slug}"
        api_url = f"https://api.lever.co/v0/postings/{slug}"
    elif ats_type == "workable":
        board_url = f"https://apply.workable.com/{slug}/"
        api_url = ""
    elif ats_type == "ashby":
        board_url = f"https://jobs.ashbyhq.com/{slug}"
        api_url = ""
    elif ats_type == "smartrecruiters":
        board_url = f"https://www.smartrecruiters.com/{slug}"
        api_url = ""
    else:
        board_url = ""
        api_url = ""

    return ATSSource(
        company=lead.name,
        website=lead.website,
        ats_type=ats_type,
        board_url=board_url,
        api_url=api_url,
        source_url=source_url,
    )


def scan_company_for_ats(
    lead: CompanyLead,
    *,
    session: requests.Session,
    max_links: int = 3,
    timeout_sec: float = 15.0,
    sleep_sec: float = 0.3,
    scan_links: bool = True,
    scan_common_paths: bool = True,
) -> List[ATSSource]:
    ats_sources: List[ATSSource] = []
    website = _normalize_url(lead.website)
    if not website:
        return ats_sources

    def fetch(url: str) -> Optional[str]:
        try:
            resp = session.get(url, timeout=timeout_sec)
            if resp.status_code >= 400:
                return None
            return resp.text
        except Exception:
            return None

    # 1) Try homepage
    home_html = fetch(website)
    if home_html:
        for ats_type, slug in _find_ats_in_text(home_html):
            ats_sources.append(_build_ats_source(lead, ats_type, slug, website))
        if ats_sources:
            return ats_sources
        candidate_links = _extract_candidate_links(home_html, website, max_links=max_links) if scan_links else []
    else:
        candidate_links = []

    # 2) Try candidate career links
    if scan_links:
        for link in candidate_links:
            html = fetch(link)
            if not html:
                continue
            for ats_type, slug in _find_ats_in_text(html):
                ats_sources.append(_build_ats_source(lead, ats_type, slug, link))
            if ats_sources:
                return ats_sources
            time.sleep(sleep_sec)

    # 3) Try common paths
    if scan_common_paths:
        common_paths = ["/careers", "/career", "/jobs", "/join-us", "/work-with-us"]
        for path in common_paths:
            url = website.rstrip("/") + path
            html = fetch(url)
            if not html:
                continue
            for ats_type, slug in _find_ats_in_text(html):
                ats_sources.append(_build_ats_source(lead, ats_type, slug, url))
            if ats_sources:
                return ats_sources
            time.sleep(sleep_sec)

    return ats_sources


def scan_companies_for_ats(
    leads: Iterable[CompanyLead],
    *,
    max_companies: int = 200,
    max_links_per_company: int = 3,
    timeout_sec: float = 15.0,
    sleep_sec: float = 0.3,
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    start_index: int = 0,
    progress_every: int = 10,
    scan_links: bool = True,
    scan_common_paths: bool = True,
) -> List[ATSSource]:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    leads_list = list(leads)
    total = min(max_companies, len(leads_list))

    ats_sources: List[ATSSource] = []
    count = 0
    for idx, lead in enumerate(leads_list):
        if count >= max_companies:
            break
        if not lead.website:
            continue
        if progress_every and (idx % progress_every == 0):
            label = lead.name or lead.website
            print(f"[ats] {start_index + idx + 1}/{start_index + total} {label}")
        found = scan_company_for_ats(
            lead,
            session=session,
            max_links=max_links_per_company,
            timeout_sec=timeout_sec,
            sleep_sec=sleep_sec,
            scan_links=scan_links,
            scan_common_paths=scan_common_paths,
        )
        ats_sources.extend(found)
        count += 1
        time.sleep(sleep_sec)

    return ats_sources


def collect_jobs_from_greenhouse(source: ATSSource, timeout_sec: float = 20.0) -> List[Job]:
    if not source.api_url:
        return []
    try:
        resp = requests.get(source.api_url, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    items = data.get("jobs", data if isinstance(data, list) else [])
    jobs: List[Job] = []
    for item in items:
        title = item.get("title") or item.get("name") or ""
        url = item.get("absolute_url") or item.get("url") or ""
        loc = ""
        location = item.get("location")
        if isinstance(location, dict):
            loc = location.get("name") or ""
        job = Job(
            title=title,
            company=source.company or source.board_url,
            location=loc,
            url=url,
            description="",
            contact_email="",
            source=f"greenhouse:{source.company or ''}",
            raw=item,
        )
        if job.title and job.url:
            jobs.append(job)
    return jobs


def collect_jobs_from_lever(source: ATSSource, timeout_sec: float = 20.0) -> List[Job]:
    if not source.api_url:
        return []
    try:
        resp = requests.get(source.api_url, timeout=timeout_sec)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    if not isinstance(data, list):
        return []

    jobs: List[Job] = []
    for item in data:
        title = item.get("text") or ""
        url = item.get("hostedUrl") or item.get("applyUrl") or ""
        categories = item.get("categories") or {}
        loc = categories.get("location") or ""
        job = Job(
            title=title,
            company=source.company or source.board_url,
            location=loc,
            url=url,
            description="",
            contact_email="",
            source=f"lever:{source.company or ''}",
            raw=item,
        )
        if job.title and job.url:
            jobs.append(job)
    return jobs


def collect_jobs_from_ats(
    sources: Iterable[ATSSource],
    *,
    enabled_types: Optional[Dict[str, bool]] = None,
    timeout_sec: float = 20.0,
    sleep_sec: float = 0.2,
) -> List[Job]:
    enabled_types = enabled_types or {"greenhouse": True, "lever": True}
    jobs: List[Job] = []
    for src in sources:
        if not enabled_types.get(src.ats_type, False):
            continue
        if src.ats_type == "greenhouse":
            jobs.extend(collect_jobs_from_greenhouse(src, timeout_sec=timeout_sec))
        elif src.ats_type == "lever":
            jobs.extend(collect_jobs_from_lever(src, timeout_sec=timeout_sec))
        time.sleep(sleep_sec)
    return jobs
