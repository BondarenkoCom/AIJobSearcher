import csv
from pathlib import Path
from typing import Iterable
from .models import CompanyLead


FIELDS = [
    "name",
    "website",
    "location",
    "email",
    "source",
    "url",
    "notes",
]


def write_company_leads(leads: Iterable[CompanyLead], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for lead in leads:
            writer.writerow(lead.as_row())
