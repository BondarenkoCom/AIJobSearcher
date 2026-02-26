import argparse
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def main() -> int:
    ap = argparse.ArgumentParser(description="Mark a lead/job as applied in activity.sqlite")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--job-url", default="", help="Job URL (will be used as contact for job leads)")
    ap.add_argument("--lead-id", default="", help="Lead id (optional if job-url provided)")
    ap.add_argument("--note", default="manual_confirmed", help="Note for the event details")
    ap.add_argument("--occurred-at", default="", help="ISO timestamp (default: now)")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    occurred_at = args.occurred_at.strip() or _now_iso()

    lead_id = args.lead_id.strip()
    job_url = args.job_url.strip()
    if not lead_id and not job_url:
        print("[mark-applied] provide --lead-id or --job-url")
        return 2

    conn = db_connect(db_path)
    try:
        init_db(conn)

        if not lead_id:
            lead_id, _inserted = upsert_lead_with_flag(
                conn,
                LeadUpsert(
                    platform="linkedin",
                    lead_type="job",
                    contact=job_url,
                    url=job_url,
                    company="",
                    job_title="",
                    location="",
                    source="manual_mark_applied",
                    created_at=occurred_at,
                    raw={"job_url": job_url},
                ),
            )

        add_event(
            conn,
            lead_id=lead_id,
            event_type="li_apply_submitted",
            status="ok",
            occurred_at=occurred_at,
            details={"source": "manual", "note": args.note},
        )
        conn.commit()
        print(f"[mark-applied] ok lead_id={lead_id}")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())

