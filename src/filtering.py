from typing import Iterable, List, Tuple
from .models import Job
from .utils import normalize_text


def _build_text(job: Job) -> str:
    return normalize_text(
        " ".join([job.title, job.company, job.location, job.description])
    )


def score_job(job: Job, include_keywords: List[str]) -> int:
    if not include_keywords:
        return 1
    text = _build_text(job)
    score = 0
    for kw in include_keywords:
        if normalize_text(kw) in text:
            score += 1
    return score


def is_excluded(job: Job, exclude_keywords: List[str]) -> bool:
    if not exclude_keywords:
        return False
    text = _build_text(job)
    for kw in exclude_keywords:
        if normalize_text(kw) in text:
            return True
    return False


def match_location(job: Job, locations: List[str]) -> bool:
    if not locations:
        return True
    text = normalize_text(" ".join([job.location, job.description]))
    for loc in locations:
        if normalize_text(loc) in text:
            return True
    return False


def filter_and_score(
    jobs: Iterable[Job],
    include_keywords: List[str],
    exclude_keywords: List[str],
    locations: List[str],
    min_score: int,
    max_results: int,
) -> Tuple[List[Tuple[Job, int]], List[Tuple[Job, int]]]:
    scored_all: List[Tuple[Job, int]] = []
    for job in jobs:
        if is_excluded(job, exclude_keywords):
            continue
        if not match_location(job, locations):
            continue
        score = score_job(job, include_keywords)
        scored_all.append((job, score))

    scored_all.sort(key=lambda x: x[1], reverse=True)
    shortlisted = [pair for pair in scored_all if pair[1] >= min_score]
    if max_results > 0:
        shortlisted = shortlisted[:max_results]
    return scored_all, shortlisted
