from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class Job:
    title: str = ""
    company: str = ""
    location: str = ""
    url: str = ""
    description: str = ""
    contact_email: str = ""
    source: str = ""
    raw: Optional[Dict[str, Any]] = None

    def as_row(self, score: Optional[int] = None) -> Dict[str, Any]:
        row = {
            "title": self.title,
            "company": self.company,
            "location": self.location,
            "url": self.url,
            "description": self.description,
            "contact_email": self.contact_email,
            "source": self.source,
        }
        if score is not None:
            row["score"] = score
        return row


@dataclass
class CompanyLead:
    name: str = ""
    website: str = ""
    location: str = ""
    email: str = ""
    source: str = ""
    url: str = ""
    notes: str = ""
    raw: Optional[Dict[str, Any]] = None

    def as_row(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "website": self.website,
            "location": self.location,
            "email": self.email,
            "source": self.source,
            "url": self.url,
            "notes": self.notes,
        }


@dataclass
class ATSSource:
    company: str = ""
    website: str = ""
    ats_type: str = ""
    board_url: str = ""
    api_url: str = ""
    source_url: str = ""

    def as_row(self) -> Dict[str, Any]:
        return {
            "company": self.company,
            "website": self.website,
            "ats_type": self.ats_type,
            "board_url": self.board_url,
            "api_url": self.api_url,
            "source_url": self.source_url,
        }
