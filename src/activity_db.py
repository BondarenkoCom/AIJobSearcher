import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS leads (
  lead_id TEXT PRIMARY KEY,
  platform TEXT NOT NULL,
  lead_type TEXT NOT NULL,
  contact TEXT NOT NULL,
  url TEXT NOT NULL,
  company TEXT NOT NULL,
  job_title TEXT NOT NULL,
  location TEXT NOT NULL,
  source TEXT NOT NULL,
  created_at TEXT NOT NULL,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_leads_platform ON leads(platform);
CREATE INDEX IF NOT EXISTS idx_leads_contact ON leads(contact);
CREATE INDEX IF NOT EXISTS idx_leads_company ON leads(company);

CREATE TABLE IF NOT EXISTS events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  lead_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  status TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  details_json TEXT,
  FOREIGN KEY (lead_id) REFERENCES leads(lead_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_events_lead ON events(lead_id);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_time ON events(occurred_at);
-- Prevent accidental duplicate imports/runs.
CREATE UNIQUE INDEX IF NOT EXISTS uniq_events ON events(lead_id, event_type, occurred_at, COALESCE(details_json,''));

CREATE TABLE IF NOT EXISTS blocklist (
  contact TEXT PRIMARY KEY,
  reason TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_blocklist_time ON blocklist(created_at);

-- CV/profile "imprint" for fast autofill and Q/A learning.
CREATE TABLE IF NOT EXISTS profile_kv (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS documents (
  doc_id TEXT PRIMARY KEY,
  doc_type TEXT NOT NULL,
  content TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS answer_bank (
  q_norm TEXT PRIMARY KEY,
  q_raw TEXT NOT NULL,
  answer TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_answer_bank_updated_at ON answer_bank(updated_at);
"""


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _norm(s: str) -> str:
    return (s or "").strip()


def _norm_email(s: str) -> str:
    return _norm(s).lower()


def _lead_id(
    *,
    platform: str,
    lead_type: str,
    contact: str,
    url: str,
    company: str,
    job_title: str,
) -> str:
    # Stable ID across sources. Keep it simple and deterministic.
    key = "|".join(
        [
            _norm(platform).lower(),
            _norm(lead_type).lower(),
            _norm_email(contact),
            _norm(url),
            _norm(company).lower(),
            _norm(job_title).lower(),
        ]
    )
    return hashlib.sha1(key.encode("utf-8")).hexdigest()


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


@dataclass
class LeadUpsert:
    platform: str
    lead_type: str
    contact: str
    url: str = ""
    company: str = ""
    job_title: str = ""
    location: str = ""
    source: str = ""
    created_at: Optional[str] = None
    raw: Optional[Dict[str, Any]] = None


def upsert_lead(conn: sqlite3.Connection, lead: LeadUpsert) -> str:
    lid, _inserted = upsert_lead_with_flag(conn, lead)
    return lid


def upsert_lead_with_flag(conn: sqlite3.Connection, lead: LeadUpsert) -> Tuple[str, bool]:
    # Dedupe by (platform, lead_type, contact) first.
    # Some sources may initially insert a job lead with blank company/title,
    # then later re-import the same URL with richer fields. If our lead_id
    # includes company/title, we'd create duplicates. Prefer the existing row.
    contact_norm = _norm_email(lead.contact)
    existing = conn.execute(
        """
        SELECT lead_id
        FROM leads
        WHERE platform = ? AND lead_type = ? AND contact = ?
        ORDER BY (company != '') DESC, (job_title != '') DESC, created_at ASC
        LIMIT 1
        """,
        (_norm(lead.platform), _norm(lead.lead_type), contact_norm),
    ).fetchone()

    if existing and existing["lead_id"]:
        lid = str(existing["lead_id"])
        inserted = False
    else:
        lid = _lead_id(
            platform=lead.platform,
            lead_type=lead.lead_type,
            contact=lead.contact,
            url=lead.url,
            company=lead.company,
            job_title=lead.job_title,
        )
        inserted = None  # determined by insert below

    created_at = lead.created_at or _now_iso()
    raw_json = json.dumps(lead.raw or {}, ensure_ascii=False) if lead.raw is not None else None

    # Insert first; then update missing fields to the latest non-empty values.
    cur = conn.execute(
        """
        INSERT OR IGNORE INTO leads
        (lead_id, platform, lead_type, contact, url, company, job_title, location, source, created_at, raw_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            lid,
            _norm(lead.platform),
            _norm(lead.lead_type),
            contact_norm,
            _norm(lead.url),
            _norm(lead.company),
            _norm(lead.job_title),
            _norm(lead.location),
            _norm(lead.source),
            created_at,
            raw_json,
        ),
    )
    if inserted is None:
        inserted = bool(getattr(cur, "rowcount", 0) == 1)

    conn.execute(
        """
        UPDATE leads SET
          url = CASE WHEN url = '' THEN ? ELSE url END,
          company = CASE WHEN company = '' THEN ? ELSE company END,
          job_title = CASE WHEN job_title = '' THEN ? ELSE job_title END,
          location = CASE WHEN location = '' THEN ? ELSE location END,
          source = CASE WHEN source = '' THEN ? ELSE source END,
          raw_json = COALESCE(raw_json, ?)
        WHERE lead_id = ?
        """,
        (
            _norm(lead.url),
            _norm(lead.company),
            _norm(lead.job_title),
            _norm(lead.location),
            _norm(lead.source),
            raw_json,
            lid,
        ),
    )

    return lid, inserted


def add_event(
    conn: sqlite3.Connection,
    *,
    lead_id: str,
    event_type: str,
    status: str = "ok",
    occurred_at: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO events (lead_id, event_type, status, occurred_at, details_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            lead_id,
            _norm(event_type),
            _norm(status) or "ok",
            occurred_at or _now_iso(),
            json.dumps(details or {}, ensure_ascii=False, sort_keys=True) if details is not None else None,
        ),
    )


def is_blocked(conn: sqlite3.Connection, contact: str) -> bool:
    c = _norm_email(contact)
    if not c:
        return False
    row = conn.execute("SELECT 1 FROM blocklist WHERE contact = ? LIMIT 1", (c,)).fetchone()
    return row is not None


def add_to_blocklist(
    conn: sqlite3.Connection,
    *,
    contact: str,
    reason: str = "",
    created_at: Optional[str] = None,
) -> None:
    c = _norm_email(contact)
    if not c:
        return
    conn.execute(
        """
        INSERT OR IGNORE INTO blocklist (contact, reason, created_at)
        VALUES (?, ?, ?)
        """,
        (c, _norm(reason), created_at or _now_iso()),
    )


def count_rows(conn: sqlite3.Connection) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for table in ("leads", "events", "blocklist"):
        out[table] = int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
    return out


def get_blocklist_contacts(conn: sqlite3.Connection) -> Set[str]:
    rows = conn.execute("SELECT contact FROM blocklist").fetchall()
    return {_norm_email(r["contact"]) for r in rows if r["contact"]}


def get_last_event_by_contact(conn: sqlite3.Connection, event_type: str) -> Dict[str, datetime]:
    rows = conn.execute(
        """
        SELECT l.contact AS contact, MAX(e.occurred_at) AS ts
        FROM events e
        JOIN leads l ON l.lead_id = e.lead_id
        WHERE e.event_type = ?
        GROUP BY l.contact
        """,
        (_norm(event_type),),
    ).fetchall()
    out: Dict[str, datetime] = {}
    for r in rows:
        contact = _norm_email(r["contact"])
        ts = (r["ts"] or "").strip()
        if not contact or not ts:
            continue
        try:
            out[contact] = datetime.fromisoformat(ts)
        except Exception:
            # Ignore unparsable timestamps.
            continue
    return out


def get_event_counts_by_day(conn: sqlite3.Connection, event_type: str) -> Dict[date, int]:
    rows = conn.execute(
        """
        SELECT substr(occurred_at, 1, 10) AS day, COUNT(*) AS c
        FROM events
        WHERE event_type = ?
        GROUP BY day
        """,
        (_norm(event_type),),
    ).fetchall()
    out: Dict[date, int] = {}
    for r in rows:
        d = (r["day"] or "").strip()
        if not d:
            continue
        try:
            out[date.fromisoformat(d)] = int(r["c"] or 0)
        except Exception:
            continue
    return out
