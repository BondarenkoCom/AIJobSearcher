import argparse
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (ROOT / path)


def main() -> int:
    ap = argparse.ArgumentParser(description="Quick report from activity.sqlite")
    ap.add_argument("--db", default="data/out/activity.sqlite", help="SQLite DB path")
    ap.add_argument("--last", type=int, default=20, help="How many last email_sent events to print")
    args = ap.parse_args()

    db_path = _resolve(args.db)
    if not db_path.exists():
        print(f"[report] missing DB: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        counts = {}
        for t in ("leads", "events", "blocklist"):
            counts[t] = int(conn.execute(f"SELECT COUNT(*) AS c FROM {t}").fetchone()["c"])

        print(f"[report] db: {db_path}")
        print(f"[report] counts: leads={counts['leads']} events={counts['events']} blocklist={counts['blocklist']}")

        rows = conn.execute(
            "SELECT event_type, COUNT(*) AS c FROM events GROUP BY event_type ORDER BY c DESC"
        ).fetchall()
        by_type = Counter({r["event_type"]: int(r["c"]) for r in rows})
        print("[report] events by type:")
        for k, v in by_type.most_common():
            print(f"  - {k}: {v}")

        last = int(args.last or 0)
        if last > 0:
            print(f"[report] last {last} email_sent:")
            sent = conn.execute(
                """
                SELECT
                  e.occurred_at,
                  l.contact,
                  l.company,
                  l.job_title,
                  l.source,
                  l.url
                FROM events e
                JOIN leads l ON l.lead_id = e.lead_id
                WHERE e.event_type = 'email_sent'
                ORDER BY e.occurred_at DESC
                LIMIT ?
                """,
                (last,),
            ).fetchall()
            for r in sent:
                ts = r["occurred_at"]
                company = (r["company"] or "")[:40]
                title = (r["job_title"] or "")[:45]
                print(f"  {ts} | {r['contact']} | {company} | {title}")

        daily = conn.execute(
            """
            SELECT substr(occurred_at, 1, 10) AS day, COUNT(*) AS c
            FROM events
            WHERE event_type = 'email_sent'
            GROUP BY day
            ORDER BY day DESC
            LIMIT 14
            """
        ).fetchall()
        if daily:
            print("[report] email_sent by day (last 14 days):")
            for r in daily:
                print(f"  {r['day']}: {r['c']}")

        print(f"[report] generated_at: {datetime.now().isoformat(timespec='seconds')}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

