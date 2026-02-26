import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect  # noqa: E402
from src.activity_db import init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _canonical_profile_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        # LinkedIn profile canonicalization: drop query, trailing slash.
        from urllib.parse import urlparse, urlunparse

        p = urlparse(u)
        if "linkedin.com" not in (p.netloc or "").lower():
            return u
        path = (p.path or "").rstrip("/")
        if path.startswith("/in/"):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _parse_raw(raw_json: Optional[str]) -> Dict[str, Any]:
    if not raw_json:
        return {}
    try:
        v = json.loads(raw_json)
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _already_contacted(conn, lead_id: str, profile_url: str) -> bool:
    prof = _canonical_profile_url(profile_url)
    p_slash = (prof + "/%") if prof else ""
    p_q = (prof + "?%") if prof else ""
    row = conn.execute(
        """
        SELECT 1
        FROM events
        WHERE event_type IN ('li_dm_sent', 'li_connect_sent', 'li_comment_posted')
          AND (
            lead_id = ?
            OR (
              json_valid(details_json)
              AND (
                json_extract(details_json, '$.profile_url') = ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
                OR json_extract(details_json, '$.profile_url') LIKE ?
              )
            )
          )
        LIMIT 1
        """,
        (lead_id, prof, p_slash, p_q),
    ).fetchone()
    return row is not None


def _fetch_post_leads(conn, *, limit: int) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT lead_id, url, job_title, company, source, created_at, raw_json
        FROM leads
        WHERE platform='linkedin' AND lead_type='post'
        ORDER BY created_at DESC
        LIMIT 2000
        """
    ).fetchall()

    out: List[Dict[str, Any]] = []
    seen_profiles: set[str] = set()

    for r in rows:
        lead_id = str(r["lead_id"] or "").strip()
        job_title = str(r["job_title"] or "")
        raw = _parse_raw(r["raw_json"])
        triage = raw.get("triage") or {}
        status = str(triage.get("status") or "review").strip()
        action = str(triage.get("action") or "connect").strip()
        score = int(triage.get("score") or 0)
        flags = triage.get("flags") or {}
        # Safety: don't outreach non-QA roles. "review" can be set by generic "hiring" text.
        if not bool(flags.get("qa")):
            continue
        hard_reasons = []
        try:
            hard_reasons = list(flags.get("hard_reasons") or [])
        except Exception:
            hard_reasons = []

        # Keep only reasonable targets.
        if status not in {"fit", "review"}:
            continue
        if score < 2:
            continue
        if hard_reasons:
            continue

        author_url = str(raw.get("author_url") or "").strip()
        # Outreach targets: only people profiles. Company pages are follow-only and handled elsewhere.
        if "/in/" not in author_url:
            continue
        author_name = str(raw.get("author_name") or "").strip()
        snippet = str(raw.get("snippet") or "").strip()

        # Remote-only focus for now (skip hybrid/onsite/closed).
        combined = f"{job_title}\n{snippet}".lower()
        if "no longer accepting applications" in combined:
            continue
        if "hybrid" in combined:
            continue
        if "on-site" in combined or "onsite" in combined or "on site" in combined:
            continue
        # "Remote" is usually present in either job title or snippet for good targets.
        if "remote" not in combined:
            continue

        post_url = str(raw.get("post_url") or "").strip() or str(r["url"] or "").strip()
        source_query = str(raw.get("query") or "").strip() or str(r["source"] or "").strip()

        prof = _canonical_profile_url(author_url)
        if not prof:
            continue
        if prof in seen_profiles:
            continue
        if _already_contacted(conn, lead_id, prof):
            continue
        seen_profiles.add(prof)

        out.append(
            {
                "created_at": str(r["created_at"] or _now_iso()),
                "status": status,
                "action": action,
                "score": score,
                "job_title": job_title,
                "author_name": author_name,
                "author_url": author_url,
                "post_or_job_url": post_url,
                "source_query": source_query,
                "snippet": snippet,
            }
        )
        if len(out) >= int(limit):
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Export LinkedIn post leads (triaged) to outreach targets CSV.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--out", default="data/out/linkedin_post_targets_top30.csv")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    conn = db_connect(db_path)
    try:
        init_db(conn)
        rows = _fetch_post_leads(conn, limit=args.limit)
    finally:
        conn.close()

    out_path = resolve_path(ROOT, args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "created_at",
                "status",
                "action",
                "score",
                "job_title",
                "author_name",
                "author_url",
                "post_or_job_url",
                "source_query",
                "snippet",
            ],
        )
        w.writeheader()
        w.writerows(rows)

    print(f"[export-post-targets] wrote {out_path} (rows={len(rows)})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
