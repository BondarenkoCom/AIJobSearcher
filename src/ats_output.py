import csv
from pathlib import Path
from typing import Iterable
from .models import ATSSource


FIELDS = [
    "company",
    "website",
    "ats_type",
    "board_url",
    "api_url",
    "source_url",
]


def write_ats_sources(items: Iterable[ATSSource], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for item in items:
            writer.writerow(item.as_row())
