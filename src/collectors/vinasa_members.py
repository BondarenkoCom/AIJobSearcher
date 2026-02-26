import re
from typing import List

import requests
from bs4 import BeautifulSoup

from ..models import CompanyLead


_EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_WEBSITE_RE = re.compile(r'(https?://[^\s<>"\']+|www\.[^\s<>"\']+)')


def _clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def _first_email(text: str) -> str:
    for email in _EMAIL_RE.findall(text):
        if email.lower().endswith("@vinasa.org.vn"):
            continue
        return email
    return ""


def _first_website(text: str) -> str:
    match = _WEBSITE_RE.search(text)
    if not match:
        return ""
    url = match.group(1).rstrip(").,;")
    return url


def _parse_entry(text: str) -> dict:
    text = _clean_text(text)
    if not text or "Address" not in text:
        return {}
    if text.count("Address") > 1:
        return {}
    name_part, rest = text.split("Address", 1)
    name = name_part.replace(":", "").strip(" -")
    rest = rest.replace(":", " ", 1).strip()
    email = _first_email(text)
    website = _first_website(text)
    return {
        "name": name,
        "email": email,
        "website": website,
        "notes": rest,
    }


def collect_vinasa_members(url: str, location: str, source: str, timeout_sec: float = 20.0) -> List[CompanyLead]:
    try:
        resp = requests.get(url, timeout=timeout_sec)
        resp.raise_for_status()
    except Exception as exc:
        print(f"[vinasa] failed: {exc}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    tables = soup.select("table")
    if not tables:
        return []

    best = max(tables, key=lambda t: len(t.select("tr")))
    leads: List[CompanyLead] = []

    for row in best.select("tr"):
        cells = row.select("td,th")
        if not cells:
            continue
        text = _clean_text(cells[-1].get_text(" ", strip=True))
        parsed = _parse_entry(text)
        if not parsed.get("name"):
            continue
        lead = CompanyLead(
            name=parsed["name"],
            website=parsed.get("website", ""),
            email=parsed.get("email", ""),
            location=location,
            source=source,
            url=url,
            notes=parsed.get("notes", ""),
            raw={"text": text},
        )
        leads.append(lead)

    return leads
