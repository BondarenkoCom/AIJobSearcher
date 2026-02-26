import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import add_event, connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


APPLY_PARAMS_RE = re.compile(
    r"requestData=\{[^}]*'action':'([^']+)'[^}]*'nonce':'([^']+)'[^}]*'post_id':(\d+)",
    re.I,
)
APPLY_PARAMS_RE2 = re.compile(
    r'requestData=\{[^}]*"action":"([^"]+)"[^}]*"nonce":"([^"]+)"[^}]*"post_id":(\d+)',
    re.I,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def extract_apply_params(html: str) -> Optional[Tuple[str, str, int]]:
    m = APPLY_PARAMS_RE.search(html or "") or APPLY_PARAMS_RE2.search(html or "")
    if not m:
        return None
    return (m.group(1), m.group(2), int(m.group(3)))


def fetch_external_apply_url(*, job_url: str, action: str, nonce: str, post_id: int, timeout_sec: float) -> str:
    payload = {"action": action, "nonce": nonce, "post_id": int(post_id), "increment_clicks": True}
    r = requests.post(
        "https://jobicy.com/signals.php",
        data=payload,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=timeout_sec,
    )
    r.raise_for_status()
    j = r.json() if r.content else {}
    url = str((j or {}).get("url") or "").strip()
    return url


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Extract real external apply URLs for Jobicy leads and store into SQLite.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--limit", type=int, default=20)
    ap.add_argument("--timeout-sec", type=float, default=25.0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    conn = db_connect(db_path)
    init_db(conn)

    rows = conn.execute(
        """
        SELECT lead_id, contact, url, raw_json
        FROM leads
        WHERE platform = 'job_board'
          AND lead_type = 'job'
          AND source = 'jobicy_api'
          AND url LIKE 'https://jobicy.com/jobs/%'
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(args.limit),),
    ).fetchall()

    if not rows:
        print("[jobicy] no jobicy_api leads found in DB.")
        return 0

    updated = 0
    skipped = 0
    failed = 0
    for r in rows:
        lead_id = str(r["lead_id"])
        job_url = str(r["url"] or r["contact"] or "").strip()
        if not job_url:
            failed += 1
            continue

        raw: Dict[str, Any] = {}
        try:
            raw = json.loads(r["raw_json"] or "{}")
            if not isinstance(raw, dict):
                raw = {}
        except Exception:
            raw = {}

        if str(raw.get("apply_url") or "").strip():
            skipped += 1
            continue

        try:
            html = requests.get(job_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=float(args.timeout_sec)).text
            params = extract_apply_params(html)
            if not params:
                failed += 1
                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="jobicy_apply_url_failed",
                    status="no_params",
                    occurred_at=_now_iso(),
                    details={"job_url": job_url},
                )
                conn.commit()
                continue
            action, nonce, post_id = params
            ext = fetch_external_apply_url(
                job_url=job_url,
                action=action,
                nonce=nonce,
                post_id=post_id,
                timeout_sec=float(args.timeout_sec),
            )
            if not ext:
                failed += 1
                add_event(
                    conn,
                    lead_id=lead_id,
                    event_type="jobicy_apply_url_failed",
                    status="empty_url",
                    occurred_at=_now_iso(),
                    details={"job_url": job_url, "post_id": post_id},
                )
                conn.commit()
                continue

            raw["apply_type"] = "external"
            raw["apply_url"] = ext
            raw["apply_source"] = "jobicy"
            raw["jobicy_post_id"] = int(post_id)

            conn.execute(
                "UPDATE leads SET raw_json = ? WHERE lead_id = ?",
                (json.dumps(raw, ensure_ascii=False, sort_keys=True), lead_id),
            )
            add_event(
                conn,
                lead_id=lead_id,
                event_type="jobicy_apply_url_extracted",
                status="ok",
                occurred_at=_now_iso(),
                details={"job_url": job_url, "external_url": ext},
            )
            conn.commit()
            updated += 1
            print(f"[jobicy] {lead_id[:8]} external_url -> {ext}")
        except Exception as e:
            failed += 1
            add_event(
                conn,
                lead_id=lead_id,
                event_type="jobicy_apply_url_failed",
                status="exception",
                occurred_at=_now_iso(),
                details={"job_url": job_url, "error": str(e)[:300]},
            )
            conn.commit()

    print(f"[jobicy] db={db_path}")
    print(f"[jobicy] processed={len(rows)} updated={updated} skipped={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

