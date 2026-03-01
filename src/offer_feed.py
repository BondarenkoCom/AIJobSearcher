from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

from src.activity_db import connect as db_connect, init_db
from src.offer_profiles import OfferProfile, load_offer_profiles


def safe_text(value: Any) -> str:
    return str(value or "").strip()


def parse_json(raw: str) -> Dict[str, Any]:
    try:
        out = json.loads(raw or "{}")
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def compose_offer_text(row: Dict[str, Any], raw: Dict[str, Any]) -> str:
    parts = [
        safe_text(row.get("job_title")),
        safe_text(row.get("company")),
        safe_text(row.get("location")),
        safe_text(raw.get("text")),
        safe_text(raw.get("description")),
        safe_text(raw.get("snippet")),
        safe_text(raw.get("skills")),
    ]
    return "\n".join([p for p in parts if p]).lower()


def offer_contact_method(row: Dict[str, Any], raw: Dict[str, Any], offer: OfferProfile) -> str:
    raw_emails = raw.get("emails")
    if isinstance(raw_emails, list) and any("@" in safe_text(v) for v in raw_emails):
        return "email"
    contact = safe_text(row.get("contact"))
    url = safe_text(row.get("url"))
    if contact.startswith("@") or "t.me/" in contact.lower() or "t.me/" in url.lower():
        return "telegram_dm"
    return "platform_apply"


def matches_offer(row: Dict[str, Any], raw: Dict[str, Any], offer: OfferProfile) -> bool:
    exp = offer.export
    platforms = {str(v).strip().lower() for v in exp.get("allowed_platforms") or [] if str(v).strip()}
    lead_types = {str(v).strip().lower() for v in exp.get("allowed_lead_types") or [] if str(v).strip()}
    title_keywords = [str(v).strip().lower() for v in exp.get("title_keyword_any") or [] if str(v).strip()]
    title_excludes = [str(v).strip().lower() for v in exp.get("title_exclude_keywords") or [] if str(v).strip()]
    keywords = [str(v).strip().lower() for v in exp.get("keyword_any") or [] if str(v).strip()]
    excludes = [str(v).strip().lower() for v in exp.get("exclude_keywords") or [] if str(v).strip()]

    platform = safe_text(row.get("platform")).lower()
    lead_type = safe_text(row.get("lead_type")).lower()
    title = safe_text(row.get("job_title")).lower()
    text = compose_offer_text(row, raw)

    if platforms and platform not in platforms:
        return False
    if lead_types and lead_type not in lead_types:
        return False
    if title_keywords and not any(k in title for k in title_keywords):
        return False
    if title_excludes and any(k in title for k in title_excludes):
        return False
    if keywords and not any(k in text for k in keywords):
        return False
    if excludes and any(k in text for k in excludes):
        return False
    return True


def offer_score(row: Dict[str, Any], raw: Dict[str, Any], offer: OfferProfile) -> int:
    title = safe_text(row.get("job_title")).lower()
    text = compose_offer_text(row, raw)
    score = 0
    for word in offer.export.get("title_keyword_any") or []:
        token = str(word).strip().lower()
        if token and token in title:
            score += 3
    for word in offer.export.get("keyword_any") or []:
        token = str(word).strip().lower()
        if token and token in text:
            score += 2
    if "remote" in text or "worldwide" in text or "anywhere" in text:
        score += 2
    if any(x in text for x in ("urgent", "paid", "fixed price", "hourly", "contract", "freelance")):
        score += 2
    if offer_contact_method(row, raw, offer) in set(offer.export.get("prefer_contact") or []):
        score += 2
    return score


def latest_rows(conn, limit: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT lead_id, platform, lead_type, contact, url, company, job_title, location, source, created_at, raw_json
    FROM leads
    ORDER BY datetime(created_at) DESC, rowid DESC
    LIMIT ?
    """
    return [dict(r) for r in conn.execute(sql, (max(limit, 1),)).fetchall()]


def build_offer_rows(conn, *, offer: OfferProfile, scan_limit: int, limit: int) -> List[Dict[str, Any]]:
    rows = latest_rows(conn, limit=max(int(scan_limit), int(limit) * 4))
    selected: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        raw = parse_json(safe_text(row.get("raw_json")))
        if not matches_offer(row, raw, offer):
            continue
        dedupe_key = (safe_text(row.get("url")).lower(), safe_text(row.get("job_title")).lower())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        text = compose_offer_text(row, raw)
        snippet = re.sub(r"\s+", " ", text).strip()[:220]
        selected.append(
            {
                "lead_id": safe_text(row.get("lead_id")),
                "title": safe_text(row.get("job_title")),
                "company": safe_text(row.get("company")),
                "platform": safe_text(row.get("platform")),
                "lead_type": safe_text(row.get("lead_type")),
                "location": safe_text(row.get("location")),
                "url": safe_text(row.get("url")),
                "contact_method": offer_contact_method(row, raw, offer),
                "created_at": safe_text(row.get("created_at")),
                "score": offer_score(row, raw, offer),
                "snippet": snippet,
            }
        )
        if len(selected) >= int(limit):
            break

    selected.sort(key=lambda item: (int(item.get("score") or 0), safe_text(item.get("created_at"))), reverse=True)
    return selected


def load_offer(offers_path: Path, offer_slug: str) -> OfferProfile:
    offers = load_offer_profiles(offers_path)
    offer = offers.get(str(offer_slug).strip())
    if offer is None:
        raise KeyError(f"Unknown offer: {offer_slug}")
    return offer


def build_offer_rows_from_db(
    *,
    db_path: Path,
    offers_path: Path,
    offer_slug: str,
    scan_limit: int,
    limit: int,
) -> List[Dict[str, Any]]:
    offer = load_offer(offers_path, offer_slug)
    conn = db_connect(db_path)
    try:
        init_db(conn)
        return build_offer_rows(conn, offer=offer, scan_limit=scan_limit, limit=limit)
    finally:
        conn.close()
