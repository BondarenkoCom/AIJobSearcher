import argparse
import json
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Sequence, Set, Tuple
from urllib.parse import urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402

QA_RE = re.compile(
    r"\b(qa|quality\s*assurance|sdet|tester|test\s*automation|automation\s*testing|api\s*testing|playwright|selenium)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _domain(url: str) -> str:
    try:
        return (urlparse(str(url or "").strip()).netloc or "").lower()
    except Exception:
        return ""


def _is_http(url: str) -> bool:
    u = str(url or "").strip().lower()
    return u.startswith("http://") or u.startswith("https://")


def _canonical_url(url: str) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        netloc = (p.netloc or "").lower()
        path = (p.path or "").rstrip("/")
        keep_query_hosts = ("greenhouse.io", "lever.co", "workable.com", "ashbyhq.com")
        keep_query = any(h in netloc for h in keep_query_hosts)
        return urlunparse((p.scheme or "https", p.netloc, path, "", (p.query if keep_query else ""), ""))
    except Exception:
        return u


def _is_skip_domain(url: str) -> bool:
    d = _domain(url)
    if not d:
        return True
    blocked = (
        "t.me",
        "telegram.me",
        "linkedin.com",
        "lnkd.in",
        "youtube.com",
        "youtu.be",
        "instagram.com",
        "facebook.com",
        "x.com",
        "twitter.com",
        "vk.com",
        "ok.ru",
        "whatsapp.com",
        "wa.me",
    )
    return any(b in d for b in blocked)


def _safe(v) -> str:
    return str(v or "").strip()


def _parse_json(raw_json: str) -> Dict:
    try:
        v = json.loads(raw_json or "{}")
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _extract_external_urls(lead_url: str, raw: Dict) -> List[str]:
    out: List[str] = []
    cand: List[str] = []
    if _safe(lead_url):
        cand.append(_safe(lead_url))
    urls = raw.get("urls")
    if isinstance(urls, list):
        for u in urls:
            cand.append(_safe(u))
    seen: Set[str] = set()
    for u in cand:
        if not _is_http(u):
            continue
        c = _canonical_url(u)
        if not c:
            continue
        if _is_skip_domain(c):
            continue
        if c.lower() in seen:
            continue
        seen.add(c.lower())
        out.append(c)
    return out


def _within_days(created_at: str, days: int) -> bool:
    if int(days) <= 0:
        return True
    s = _safe(created_at)
    if not s:
        return True
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return True
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt >= datetime.now(timezone.utc) - timedelta(days=int(days))


def _already_seeded(conn, ext_url: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM leads
        WHERE platform='telegram' AND lead_type='job' AND contact=?
        LIMIT 1
        """,
        (ext_url,),
    ).fetchone()
    return row is not None


def run(args: argparse.Namespace) -> int:
    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _safe(args.db):
        db_path = resolve_path(ROOT, _safe(args.db))

    conn = db_connect(db_path)
    init_db(conn)
    try:
        rows = conn.execute(
            """
            SELECT lead_id, company, job_title, contact, url, source, created_at, raw_json
            FROM leads
            WHERE platform='telegram' AND lead_type IN ('gig', 'project', 'post')
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(1, int(args.scan_limit)),),
        ).fetchall()

        seeded = 0
        skipped_existing = 0
        scanned = 0
        urls_total = 0

        for r in rows:
            created_at = _safe(r["created_at"])
            if not _within_days(created_at, int(args.days)):
                continue
            scanned += 1
            raw = _parse_json(_safe(r["raw_json"]))
            title_blob = " ".join(
                [
                    _safe(r["job_title"]),
                    _safe(r["company"]),
                    _safe(raw.get("text")),
                    _safe(raw.get("snippet")),
                ]
            )
            if (not args.allow_nonqa) and (not QA_RE.search(title_blob)):
                continue
            ext_urls = _extract_external_urls(_safe(r["url"]), raw)
            urls_total += len(ext_urls)
            if not ext_urls:
                continue

            for ext in ext_urls:
                if _already_seeded(conn, ext):
                    skipped_existing += 1
                    continue
                lead = LeadUpsert(
                    platform="telegram",
                    lead_type="job",
                    contact=ext,
                    url=ext,
                    company=_safe(r["company"]) or "Telegram lead",
                    job_title=_safe(r["job_title"]) or "External apply from Telegram",
                    location="Remote",
                    source=_safe(r["source"]) or "telegram_external_seed",
                    created_at=created_at or _now_iso(),
                    raw={
                        "source": "telegram_external_seed",
                        "apply_type": "external",
                        "apply_url": ext,
                        "origin_platform": "telegram",
                        "origin_lead_id": _safe(r["lead_id"]),
                        "origin_contact": _safe(r["contact"]),
                        "origin_post_url": _safe(r["url"]),
                    },
                )
                new_lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                if was_inserted:
                    seeded += 1
                    add_event(
                        conn,
                        lead_id=new_lead_id,
                        event_type="tg_external_seeded",
                        occurred_at=_now_iso(),
                        details={"origin_lead_id": _safe(r["lead_id"]), "external_url": ext},
                    )
                if int(args.max_new) > 0 and seeded >= int(args.max_new):
                    conn.commit()
                    print(
                        f"[tg-ext-seed] scanned={scanned} urls_seen={urls_total} "
                        f"seeded={seeded} skipped_existing={skipped_existing} (max_new reached)"
                    )
                    return 0

        conn.commit()
        print(
            f"[tg-ext-seed] scanned={scanned} urls_seen={urls_total} "
            f"seeded={seeded} skipped_existing={skipped_existing}"
        )
        print(f"[tg-ext-seed] db={db_path}")
        return 0
    finally:
        conn.close()


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Seed external-apply leads from Telegram posts with external links.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--scan-limit", type=int, default=1200, help="How many latest telegram leads to inspect")
    ap.add_argument("--days", type=int, default=60, help="Inspect only last N days (0 = all)")
    ap.add_argument("--max-new", type=int, default=0, help="Stop after seeding this many new leads (0 = unlimited)")
    ap.add_argument("--allow-nonqa", action="store_true", help="Allow seeding non-QA links (default: off)")
    return ap


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
