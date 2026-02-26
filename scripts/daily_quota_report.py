import argparse
import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import Dict

ROOT = Path(__file__).resolve().parents[1]


def _resolve(p: str) -> Path:
    path = Path(p)
    return path if path.is_absolute() else (ROOT / path)


def _count_leads_today(conn: sqlite3.Connection, platform: str, day: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(1) AS c
        FROM leads
        WHERE platform = ?
          AND substr(COALESCE(created_at, ''), 1, 10) = ?
        """,
        (platform, day),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def _count_events_today(conn: sqlite3.Connection, event_type: str, day: str) -> int:
    row = conn.execute(
        """
        SELECT COUNT(1) AS c
        FROM events
        WHERE event_type = ?
          AND substr(COALESCE(occurred_at, ''), 1, 10) = ?
        """,
        (event_type, day),
    ).fetchone()
    return int((row["c"] if row else 0) or 0)


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily quota report by source/platform.")
    ap.add_argument("--db", default="data/out/activity.sqlite")
    ap.add_argument("--date", default=date.today().isoformat(), help="YYYY-MM-DD")
    ap.add_argument("--quota-telegram", type=int, default=50)
    ap.add_argument("--quota-reddit", type=int, default=40)
    ap.add_argument("--quota-hn", type=int, default=20)
    ap.add_argument("--quota-job-board", type=int, default=30)
    ap.add_argument("--quota-linkedin", type=int, default=25)
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    db_path = _resolve(args.db)
    if not db_path.exists():
        print(f"[quota] missing DB: {db_path}")
        return 2

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        day = str(args.date).strip()
        quotas: Dict[str, int] = {
            "telegram": int(args.quota_telegram),
            "reddit": int(args.quota_reddit),
            "hn": int(args.quota_hn),
            "job_board": int(args.quota_job_board),
            "linkedin": int(args.quota_linkedin),
        }
        actual = {k: _count_leads_today(conn, k, day) for k in quotas.keys()}
        sends = {
            "email_sent": _count_events_today(conn, "email_sent", day),
        }

        print(f"[quota] day={day} db={db_path}")
        print("[quota] source leads:")
        for k in ("telegram", "reddit", "hn", "job_board", "linkedin"):
            q = quotas[k]
            a = actual[k]
            pct = (100.0 * a / q) if q > 0 else 0.0
            print(f"- {k}: {a}/{q} ({pct:.1f}%)")

        print("[quota] sends:")
        for k, v in sends.items():
            print(f"- {k}: {v}")

        payload = {
            "day": day,
            "db": str(db_path),
            "quotas": quotas,
            "actual": actual,
            "sends": sends,
        }
        out_json = str(args.out_json or "").strip()
        if out_json:
            p = _resolve(out_json)
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[quota] json={p}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
