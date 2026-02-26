import csv
from pathlib import Path
from typing import Iterable, Tuple
from .models import Job


DEFAULT_FIELDS = [
    "title",
    "company",
    "location",
    "url",
    "description",
    "contact_email",
    "source",
    "score",
]


def write_scored_jobs(rows: Iterable[Tuple[Job, int]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=DEFAULT_FIELDS)
        writer.writeheader()
        for job, score in rows:
            writer.writerow(job.as_row(score=score))
