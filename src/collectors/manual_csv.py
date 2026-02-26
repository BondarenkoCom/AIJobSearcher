import csv
from pathlib import Path
from typing import List
from ..models import Job


EXPECTED_FIELDS = {
    "title",
    "company",
    "location",
    "url",
    "description",
    "contact_email",
    "source",
}


def collect_from_csv(path: Path) -> List[Job]:
    if not path.exists():
        return []
    jobs: List[Job] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            title = (row.get("title") or "").strip()
            company = (row.get("company") or "").strip()
            if not title or not company:
                continue
            job = Job(
                title=title,
                company=company,
                location=(row.get("location") or "").strip(),
                url=(row.get("url") or "").strip(),
                description=(row.get("description") or "").strip(),
                contact_email=(row.get("contact_email") or "").strip(),
                source=(row.get("source") or "manual_csv").strip(),
                raw={k: v for k, v in row.items() if k not in EXPECTED_FIELDS},
            )
            jobs.append(job)
    return jobs
