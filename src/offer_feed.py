from __future__ import annotations

import html
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

from src.activity_db import connect as db_connect, init_db
from src.apply_assistant import ApplyAssistant, ApplyAssistantError
from src.offer_profiles import OfferProfile, load_offer_profiles
from src.telegram_paid_store import log_llm_usage


_LEAD_READER: ApplyAssistant | None = None
_LEAD_READ_CACHE: Dict[str, Dict[str, Any]] = {}


def safe_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for _ in range(2):
        text = html.unescape(text)
    return text.strip()


def parse_json(raw: str) -> Dict[str, Any]:
    try:
        out = json.loads(raw or "{}")
        return out if isinstance(out, dict) else {}
    except Exception:
        return {}


def _lead_reader_enabled() -> bool:
    raw = safe_text(os.getenv("ENABLE_AI_LEAD_READER") or "1").lower()
    if raw in ("0", "false", "no", "off"):
        return False
    return bool(safe_text(os.getenv("XAI_API_KEY")) or safe_text(os.getenv("OPENAI_API_KEY")))


def _get_lead_reader() -> ApplyAssistant:
    global _LEAD_READER
    if _LEAD_READER is None:
        _LEAD_READER = ApplyAssistant()
    return _LEAD_READER


def _lead_read_cache_key(row: Dict[str, Any], raw: Dict[str, Any]) -> str:
    payload = "|".join(
        [
            safe_text(row.get("lead_id")),
            safe_text(row.get("job_title")),
            safe_text(row.get("company")),
            safe_text(row.get("location")),
            safe_text(row.get("url")),
            json.dumps(raw or {}, ensure_ascii=False, sort_keys=True),
        ]
    )
    return hashlib.sha1(payload.encode("utf-8", errors="ignore")).hexdigest()


def _needs_ai_read(row: Dict[str, Any], raw: Dict[str, Any]) -> bool:
    title = safe_text(row.get("job_title"))
    company = safe_text(row.get("company"))
    location = safe_text(row.get("location"))
    text = compose_offer_text(row, raw)
    if not raw:
        return False
    if not title:
        return True
    if len(text) < 50:
        return True
    if not company and any(safe_text(raw.get(k)) for k in ("company", "employer", "organization", "client")):
        return True
    if not location and any(safe_text(raw.get(k)) for k in ("location", "country", "city", "remote")):
        return True
    return False


def _prefer_value(existing: str, proposed: str) -> str:
    existing = safe_text(existing)
    proposed = safe_text(proposed)
    if not proposed:
        return existing
    if not existing:
        return proposed
    if len(existing) < 4 and len(proposed) > len(existing):
        return proposed
    return existing


def _augment_row_with_ai(conn, *, row: Dict[str, Any], raw: Dict[str, Any], offer: OfferProfile) -> tuple[Dict[str, Any], Dict[str, Any]]:
    if not _lead_reader_enabled():
        return dict(row), dict(raw)
    if not _needs_ai_read(row, raw):
        return dict(row), dict(raw)
    cache_key = _lead_read_cache_key(row, raw)
    cached = _LEAD_READ_CACHE.get(cache_key)
    if cached is not None:
        row_copy = dict(row)
        raw_copy = dict(raw)
        row_copy["job_title"] = _prefer_value(row_copy.get("job_title"), cached.get("title"))
        row_copy["company"] = _prefer_value(row_copy.get("company"), cached.get("company"))
        row_copy["location"] = _prefer_value(row_copy.get("location"), cached.get("location"))
        if not safe_text(raw_copy.get("snippet")) and safe_text(cached.get("snippet")):
            raw_copy["snippet"] = safe_text(cached.get("snippet"))
        return row_copy, raw_copy
    try:
        result = _get_lead_reader().read_lead_fields(existing_row=row, raw=raw)
    except ApplyAssistantError:
        return dict(row), dict(raw)
    _LEAD_READ_CACHE[cache_key] = {
        "title": result.title,
        "company": result.company,
        "location": result.location,
        "snippet": result.snippet,
    }
    log_llm_usage(
        conn,
        user_id=0,
        offer_slug=offer.slug,
        lead_id=safe_text(row.get("lead_id")),
        provider=result.usage.provider,
        model=result.usage.model,
        task_type="lead_read",
        prompt_tokens=result.usage.prompt_tokens,
        completion_tokens=result.usage.completion_tokens,
        total_tokens=result.usage.total_tokens,
        estimated_cost_usd=result.usage.estimated_cost_usd,
        details={"source": safe_text(row.get("source")), "platform": safe_text(row.get("platform"))},
    )
    row_copy = dict(row)
    raw_copy = dict(raw)
    row_copy["job_title"] = _prefer_value(row_copy.get("job_title"), result.title)
    row_copy["company"] = _prefer_value(row_copy.get("company"), result.company)
    row_copy["location"] = _prefer_value(row_copy.get("location"), result.location)
    if not safe_text(raw_copy.get("snippet")) and safe_text(result.snippet):
        raw_copy["snippet"] = safe_text(result.snippet)
    return row_copy, raw_copy


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


def offer_stack_hits(row: Dict[str, Any], raw: Dict[str, Any], offer: OfferProfile) -> List[str]:
    text = compose_offer_text(row, raw)
    out: List[str] = []
    for word in offer.export.get("stack_keywords") or []:
        token = str(word).strip().lower()
        if token and token in text and token not in out:
            out.append(token)
    return out


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
    stack_hits = offer_stack_hits(row, raw, offer)
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
    score += min(3, len(stack_hits))
    return score


def latest_rows(conn, limit: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT lead_id, platform, lead_type, contact, url, company, job_title, location, source, created_at, raw_json
    FROM leads
    ORDER BY datetime(created_at) DESC, rowid DESC
    LIMIT ?
    """
    return [dict(r) for r in conn.execute(sql, (max(limit, 1),)).fetchall()]


def get_offer_row_by_lead_id(conn, *, offer: OfferProfile, lead_id: str) -> Dict[str, Any]:
    row = conn.execute(
        """
        SELECT lead_id, platform, lead_type, contact, url, company, job_title, location, source, created_at, raw_json
        FROM leads
        WHERE lead_id = ?
        LIMIT 1
        """,
        (safe_text(lead_id),),
    ).fetchone()
    if row is None:
        return {}
    raw = parse_json(safe_text(row["raw_json"]))
    row_dict = dict(row)
    row_dict, raw = _augment_row_with_ai(conn, row=row_dict, raw=raw, offer=offer)
    if not matches_offer(row_dict, raw, offer):
        return {}
    text = compose_offer_text(row_dict, raw)
    return {
        "lead_id": safe_text(row_dict.get("lead_id")),
        "title": safe_text(row_dict.get("job_title")),
        "company": safe_text(row_dict.get("company")),
        "platform": safe_text(row_dict.get("platform")),
        "source": safe_text(row_dict.get("source")),
        "lead_type": safe_text(row_dict.get("lead_type")),
        "location": safe_text(row_dict.get("location")),
        "url": safe_text(row_dict.get("url")),
        "contact_method": offer_contact_method(row_dict, raw, offer),
        "created_at": safe_text(row_dict.get("created_at")),
        "score": offer_score(row_dict, raw, offer),
        "snippet": re.sub(r"\s+", " ", text).strip()[:220],
        "stack_hits": offer_stack_hits(row_dict, raw, offer),
    }


def build_offer_rows(conn, *, offer: OfferProfile, scan_limit: int, limit: int) -> List[Dict[str, Any]]:
    rows = latest_rows(conn, limit=max(int(scan_limit), int(limit) * 4))
    selected: List[Dict[str, Any]] = []
    seen = set()
    for row in rows:
        raw = parse_json(safe_text(row.get("raw_json")))
        row = dict(row)
        row, raw = _augment_row_with_ai(conn, row=row, raw=raw, offer=offer)
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
                "source": safe_text(row.get("source")),
                "lead_type": safe_text(row.get("lead_type")),
                "location": safe_text(row.get("location")),
                "url": safe_text(row.get("url")),
                "contact_method": offer_contact_method(row, raw, offer),
                "created_at": safe_text(row.get("created_at")),
                "score": offer_score(row, raw, offer),
                "snippet": snippet,
                "stack_hits": offer_stack_hits(row, raw, offer),
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
