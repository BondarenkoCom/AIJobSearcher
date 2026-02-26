import re
import time
import xml.etree.ElementTree as ET
from typing import Iterable, List, Set

import requests
from bs4 import BeautifulSoup

from ..models import CompanyLead


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")


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


def _extract_links_from_listing(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "").strip()
        if not href:
            continue
        if "/doanh-nghiep/" not in href:
            continue
        if href.endswith("/doanh-nghiep/"):
            continue
        if "/page/" in href or href.endswith("/feed/"):
            continue
        links.append(_normalize_url(href))
    return links


def _parse_rss_links(xml_text: str) -> List[str]:
    links: List[str] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return links
    for item in root.findall(".//item"):
        link = item.findtext("link")
        if link:
            links.append(link.strip())
    return links


def _extract_company_from_title(title: str) -> str:
    if not title:
        return ""
    if "-" in title:
        return title.split("-")[0].strip()
    return title.strip()


def _extract_website(soup: BeautifulSoup) -> str:
    for desc in soup.select(".elementor-image-box-description"):
        text = _clean_text(desc.get_text(strip=True))
        if text.startswith("http") or text.startswith("www."):
            return _normalize_url(text)
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if not href:
            continue
        if "danhbaict.vn" in href or "vinasa.org.vn" in href:
            continue
        if href.startswith("http") or href.startswith("www."):
            return _normalize_url(href)
    return ""


def _extract_email(text: str) -> str:
    for email in _EMAIL_RE.findall(text):
        if email.lower().endswith("@vinasa.org.vn"):
            continue
        return email
    return ""


def _fetch_company_details(url: str, timeout_sec: float) -> CompanyLead:
    try:
        resp = requests.get(url, timeout=timeout_sec)
        resp.raise_for_status()
    except Exception:
        return CompanyLead(url=url)

    soup = BeautifulSoup(resp.text, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    name = _extract_company_from_title(title)
    website = _extract_website(soup)
    text = _clean_text(soup.get_text(" ", strip=True))
    email = _extract_email(text)

    return CompanyLead(
        name=name,
        website=website,
        email=email,
        source="danhbaict",
        url=url,
        notes="",
        raw={"title": title},
    )


def collect_danhbaict(
    list_url: str = "https://danhbaict.vn/doanh-nghiep/",
    feed_url: str = "",
    max_pages: int = 3,
    max_items: int = 200,
    fetch_details: bool = True,
    sleep_sec: float = 0.5,
    timeout_sec: float = 20.0,
) -> List[CompanyLead]:
    links: List[str] = []

    if feed_url:
        try:
            resp = requests.get(feed_url, timeout=timeout_sec)
            resp.raise_for_status()
            links = _parse_rss_links(resp.text)
        except Exception as exc:
            print(f"[danhbaict] feed failed: {exc}")
            links = []

    if not links and list_url:
        seen: Set[str] = set()
        for page in range(1, max_pages + 1):
            url = list_url.rstrip("/") + ("/" if page == 1 else f"/page/{page}/")
            try:
                resp = requests.get(url, timeout=timeout_sec)
                resp.raise_for_status()
            except Exception as exc:
                print(f"[danhbaict] list page failed: {exc}")
                break
            page_links = _extract_links_from_listing(resp.text)
            for link in page_links:
                if link not in seen:
                    seen.add(link)
                    links.append(link)
            if not page_links:
                break
            time.sleep(sleep_sec)

    if max_items > 0:
        links = links[:max_items]

    leads: List[CompanyLead] = []
    if fetch_details:
        for link in links:
            lead = _fetch_company_details(link, timeout_sec=timeout_sec)
            if lead.name or lead.website:
                lead.source = "danhbaict"
                leads.append(lead)
            time.sleep(sleep_sec)
    else:
        for link in links:
            leads.append(CompanyLead(name="", source="danhbaict", url=link))

    return leads
