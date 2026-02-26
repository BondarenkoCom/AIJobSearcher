import argparse
import csv
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import (  # noqa: E402
    LeadUpsert,
    add_event,
    add_to_blocklist,
    connect,
    count_rows,
    init_db,
    upsert_lead,
)
from src.config import resolve_path  # noqa: E402


def _norm(s: str) -> str:
    return (s or "").strip()


def _norm_email(s: str) -> str:
    return _norm(s).lower()


def _read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f))


def _iter_lead_csvs(leads_dir: Path) -> Iterable[Path]:
    for p in sorted(leads_dir.glob("*.csv")):
        yield p


def _iter_lead_txts(leads_dir: Path) -> Iterable[Path]:
    for p in sorted(leads_dir.glob("*.txt")):
        yield p


def _parse_pipe_line(line: str) -> Tuple[str, str, str, str]:
    parts = [p.strip() for p in line.split("|")]
    parts = [p for p in parts if p != ""]
    if not parts:
        return "", "", "", ""
    email = parts[0]
    title = parts[1] if len(parts) > 1 else ""
    location = parts[2] if len(parts) > 2 else ""
    company = " | ".join(parts[3:]).strip() if len(parts) > 3 else ""
    return email, title, location, company


def _import_blacklist(conn, blacklist_path: Path) -> int:
    if not blacklist_path.exists():
        return 0
    added = 0
    for line in blacklist_path.read_text(encoding="utf-8", errors="replace").splitlines():
        email = _norm_email(line)
        if not email or "@" not in email:
            continue
        add_to_blocklist(conn, contact=email, reason="import:blacklist.txt")
        added += 1
    return added


def _import_sent_log(conn, sent_log_path: Path) -> int:
    if not sent_log_path.exists():
        return 0
    rows = _read_csv(sent_log_path)
    imported = 0
    for r in rows:
        to_email = _norm_email(r.get("to_email", ""))
        if not to_email or "@" not in to_email:
            continue
        ts = _norm(r.get("timestamp", "")) or None
        lead_id = upsert_lead(
            conn,
            LeadUpsert(
                platform="email",
                lead_type="job",
                contact=to_email,
                url=_norm(r.get("job_url", "")),
                company=_norm(r.get("company", "")),
                job_title=_norm(r.get("job_title", "")),
                location=_norm(r.get("location", "")),
                source=_norm(r.get("source", "")) or "sent_log.csv",
                created_at=ts,
                raw=r,
            ),
        )
        add_event(
            conn,
            lead_id=lead_id,
            event_type="email_sent",
            status="ok",
            occurred_at=ts,
            details={"import": "sent_log.csv"},
        )
        imported += 1
    return imported


def _import_leads_csv(conn, path: Path) -> int:
    rows = _read_csv(path)
    imported = 0
    file_mtime = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    for r in rows:
        email = _norm_email(r.get("contact_email", ""))
        if not email or "@" not in email:
            continue
        lead_id = upsert_lead(
            conn,
            LeadUpsert(
                platform="email",
                lead_type="job",
                contact=email,
                url=_norm(r.get("url", "")),
                company=_norm(r.get("company", "")),
                job_title=_norm(r.get("title", "")),
                location=_norm(r.get("location", "")),
                source=_norm(r.get("source", "")) or path.name,
                created_at=file_mtime,
                raw=r,
            ),
        )
        add_event(conn, lead_id=lead_id, event_type="collected", status="ok", occurred_at=file_mtime, details={"file": path.name})

        # If the row says "sent" but it doesn't exist in sent_log, keep that as a separate marker.
        sent_at = _norm(r.get("sent_at", ""))
        sent_status = _norm(r.get("sent_status", "")).lower()
        if sent_at or sent_status == "sent":
            add_event(
                conn,
                lead_id=lead_id,
                event_type="source_marked_sent",
                status="unknown",
                occurred_at=sent_at or file_mtime,
                details={"file": path.name},
            )
        imported += 1
    return imported


def _import_leads_txt(conn, path: Path) -> int:
    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    imported = 0
    file_mtime = datetime.fromtimestamp(path.stat().st_mtime).isoformat(timespec="seconds")
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        email, title, location, company = _parse_pipe_line(line)
        email = _norm_email(email)
        if not email or "@" not in email:
            continue
        lead_id = upsert_lead(
            conn,
            LeadUpsert(
                platform="email",
                lead_type="job",
                contact=email,
                url="",
                company=_norm(company),
                job_title=_norm(title),
                location=_norm(location),
                source=path.name,
                created_at=file_mtime,
                raw={"line": line, "file": path.name},
            ),
        )
        add_event(conn, lead_id=lead_id, event_type="collected", status="ok", occurred_at=file_mtime, details={"file": path.name})
        imported += 1
    return imported


def main() -> int:
    ap = argparse.ArgumentParser(description="Create activity.sqlite and import current CSV/TXT tracking data")
    ap.add_argument("--db", default="data/out/activity.sqlite", help="SQLite DB path")
    ap.add_argument("--sent-log", default="data/out/sent_log.csv", help="sent_log.csv path")
    ap.add_argument("--blacklist", default="data/out/blacklist.txt", help="blacklist.txt path")
    ap.add_argument("--leads-dir", default="DatesFromAnothersSources", help="Directory with lead lists (csv/txt)")
    ap.add_argument("--reset", action="store_true", help="Drop existing tables and rebuild DB")
    args = ap.parse_args()

    db_path = resolve_path(ROOT, args.db)
    sent_log_path = resolve_path(ROOT, args.sent_log)
    blacklist_path = resolve_path(ROOT, args.blacklist)
    leads_dir = resolve_path(ROOT, args.leads_dir)

    if not leads_dir.exists():
        print(f"[migrate] leads dir missing: {leads_dir}")
        return 2

    conn = connect(db_path)
    try:
        if args.reset:
            conn.executescript(
                """
                DROP TABLE IF EXISTS events;
                DROP TABLE IF EXISTS leads;
                DROP TABLE IF EXISTS blocklist;
                """
            )
            conn.commit()

        init_db(conn)

        stats = Counter()

        stats["blacklist_lines"] += _import_blacklist(conn, blacklist_path)
        conn.commit()

        # Import "what we actually sent" first (authoritative).
        stats["sent_log_rows"] += _import_sent_log(conn, sent_log_path)
        conn.commit()

        # Import lead lists (CSV + TXT) so we can query + dedupe going forward.
        for p in _iter_lead_csvs(leads_dir):
            stats["leads_csv_rows"] += _import_leads_csv(conn, p)
        conn.commit()

        for p in _iter_lead_txts(leads_dir):
            stats["leads_txt_rows"] += _import_leads_txt(conn, p)
        conn.commit()

        counts = count_rows(conn)

        print(f"[migrate] db: {db_path}")
        print(f"[migrate] imported: blacklist_lines={stats['blacklist_lines']}")
        print(f"[migrate] imported: sent_log_rows={stats['sent_log_rows']}")
        print(f"[migrate] imported: leads_csv_rows={stats['leads_csv_rows']}")
        print(f"[migrate] imported: leads_txt_rows={stats['leads_txt_rows']}")
        print(f"[migrate] counts: leads={counts['leads']} events={counts['events']} blocklist={counts['blocklist']}")

        # Quick event type breakdown (top-level sanity check).
        rows = conn.execute("SELECT event_type, COUNT(*) AS c FROM events GROUP BY event_type ORDER BY c DESC").fetchall()
        for r in rows:
            print(f"[migrate] events: {r['event_type']}={r['c']}")

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
