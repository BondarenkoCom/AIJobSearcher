import re
from typing import Iterable, List, Set
from .models import Job, CompanyLead, ATSSource


_space_re = re.compile(r"\s+")
_non_alnum_re = re.compile(r"[^a-z0-9]")


def normalize_text(text: str) -> str:
    if not text:
        return ""
    t = text.lower()
    t = _space_re.sub(" ", t).strip()
    return t


def normalize_key(text: str) -> str:
    t = normalize_text(text)
    t = _non_alnum_re.sub("", t)
    return t


def dedupe_jobs(jobs: Iterable[Job]) -> List[Job]:
    seen: Set[str] = set()
    out: List[Job] = []
    for job in jobs:
        if job.url:
            key = f"url:{normalize_text(job.url)}"
        else:
            key = "|".join([
                normalize_key(job.title),
                normalize_key(job.company),
                normalize_key(job.location),
            ])
        if key in seen:
            continue
        seen.add(key)
        out.append(job)
    return out


def dedupe_companies(leads: Iterable[CompanyLead]) -> List[CompanyLead]:
    seen: Set[str] = set()
    out: List[CompanyLead] = []
    for lead in leads:
        key = "|".join([
            normalize_key(lead.name),
            normalize_text(lead.website or ""),
            normalize_text(lead.email or ""),
        ])
        if key in seen:
            continue
        seen.add(key)
        out.append(lead)
    return out


def dedupe_ats_sources(items: Iterable[ATSSource]) -> List[ATSSource]:
    seen: Set[str] = set()
    out: List[ATSSource] = []
    for item in items:
        key = "|".join([
            normalize_key(item.company),
            normalize_text(item.ats_type),
            normalize_text(item.api_url or item.board_url or ""),
        ])
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out
