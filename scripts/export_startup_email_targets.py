import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Set
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.IGNORECASE)
AI_HINT_RE = re.compile(
    r"\b("
    r"ai|a\.i\.|artificial intelligence|machine learning|ml\b|deep learning|llm|large language model|"
    r"generative|genai|computer vision|nlp|embedding|recommendation|speech|voice"
    r")\b",
    re.IGNORECASE,
)
US_LOCATION_RE = re.compile(
    r"\b(usa|u\.s\.a\.|united states|us|u\.s\.|california|new york|texas|florida|washington|seattle|san francisco|los angeles|boston|chicago|austin)\b",
    re.IGNORECASE,
)
BAD_EMAIL_DOM_SUFFIXES = (
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".svg",
    ".webp",
    ".avif",
    ".heic",
    ".ico",
    ".css",
    ".js",
    ".woff",
    ".woff2",
)
BAD_EMAIL_LOCAL_HINT_RE = re.compile(
    r"(no-?reply|mailer-daemon|postmaster|abuse|privacy|legal|security|admin|support|sales|press|media|help|billing|feedback|accommodat|investor|partnership|webmaster|marketing|receipt|customer|success|service|advertis)",
    re.IGNORECASE,
)
PLACEHOLDER_LOCAL_RE = re.compile(
    r"(john\.doe|demo(student)?|sample|fake|testuser|yourname|firstname|lastname)",
    re.IGNORECASE,
)
BAD_EMAIL_DOMAIN_HINT_RE = re.compile(
    r"(sentry|wixpress|example\.com|yourcompany\.com|domain\.com|company\.com|localhost)",
    re.IGNORECASE,
)
GOOD_EMAIL_LOCAL_HINT_RE = re.compile(
    r"(careers?|jobs?|hiring|talent|recruit|hr|people|team|contact|info|hello)",
    re.IGNORECASE,
)
PERSONAL_LOCAL_RE = re.compile(r"^[a-z]{2,20}[._-][a-z]{2,20}$", re.IGNORECASE)
HEX_LOCAL_RE = re.compile(r"^[a-f0-9]{20,}$", re.IGNORECASE)


def _is_valid_email(value: str) -> bool:
    e = (value or "").strip().lower()
    if not e or not EMAIL_RE.match(e):
        return False
    local, domain = e.split("@", 1)
    if not local or "." not in domain:
        return False
    # Scraped pages sometimes include URL-encoded garbage like "%20info@domain.com".
    # Percent is technically allowed in local-part, but it's extremely rare in real hiring contacts.
    if "%" in local:
        return False
    if any(domain.endswith(sfx) for sfx in BAD_EMAIL_DOM_SUFFIXES):
        return False
    if ".." in e or "/@" in e:
        return False
    if BAD_EMAIL_DOMAIN_HINT_RE.search(domain):
        return False
    if BAD_EMAIL_LOCAL_HINT_RE.search(local):
        return False
    if PLACEHOLDER_LOCAL_RE.search(local):
        return False
    local_flat = re.sub(r"[^a-z0-9]+", "", local.lower())
    if local_flat in {
        "johndoe",
        "demo",
        "demostudent",
        "sample",
        "testuser",
        "yourname",
        "firstname",
        "lastname",
    }:
        return False
    if (local, domain) in {
        ("your", "email.com"),
        ("example", "example.com"),
        ("name", "domain.com"),
    }:
        return False
    return True


def _root_domain(url: str) -> str:
    host = (urlparse((url or "").strip()).netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _score_email(email: str, preferred_domain: str) -> int:
    e = (email or "").strip().lower()
    if not _is_valid_email(e):
        return -999
    local, domain = e.split("@", 1)
    score = 0
    if preferred_domain and (domain == preferred_domain or domain.endswith("." + preferred_domain)):
        score += 3
    if GOOD_EMAIL_LOCAL_HINT_RE.search(local):
        score += 3
    if PERSONAL_LOCAL_RE.match(local):
        score += 2
    if HEX_LOCAL_RE.match(local):
        score -= 5
    if len(local) > 32 and re.search(r"\d{3,}", local):
        score -= 3
    return score


def _email_domain(email: str) -> str:
    e = (email or "").strip().lower()
    if "@" not in e:
        return ""
    return e.split("@", 1)[1]


def _is_domain_match(email: str, preferred_domain: str) -> bool:
    dom = _email_domain(email)
    pd = (preferred_domain or "").strip().lower()
    if not dom or not pd:
        return False
    return dom == pd or dom.endswith("." + pd)


def _split_semicolon(value: str) -> List[str]:
    if not value:
        return []
    out = []
    for chunk in value.split(";"):
        x = (chunk or "").strip().lower()
        if x:
            out.append(x)
    return out


def _load_sent_contacts(conn) -> Set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT LOWER(l.contact) AS contact
        FROM events e
        JOIN leads l ON l.lead_id = e.lead_id
        WHERE e.event_type = 'email_sent' AND l.contact LIKE '%@%'
        """
    ).fetchall()
    return {str(r["contact"] or "").strip().lower() for r in rows if str(r["contact"] or "").strip()}


def _load_blocked_contacts(conn) -> Set[str]:
    rows = conn.execute("SELECT LOWER(contact) AS contact FROM blocklist").fetchall()
    return {str(r["contact"] or "").strip().lower() for r in rows if str(r["contact"] or "").strip()}


def _extract_email_candidates(contact: str, raw_json: str) -> List[str]:
    out: List[str] = []
    c = (contact or "").strip().lower()
    if _is_valid_email(c):
        out.append(c)

    try:
        raw = json.loads(raw_json or "{}")
    except Exception:
        raw = {}
    if isinstance(raw, dict):
        for e in _split_semicolon(str(raw.get("emails") or "")):
            if _is_valid_email(e):
                out.append(e)

    uniq: List[str] = []
    seen: Set[str] = set()
    for e in out:
        if e in seen:
            continue
        seen.add(e)
        uniq.append(e)
    return uniq


def _is_ai_friendly(raw_json: str) -> bool:
    try:
        raw = json.loads(raw_json or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return False
    text = " ".join(
        [
            str(raw.get("industry") or ""),
            str(raw.get("subindustry") or ""),
            str(raw.get("one_liner") or ""),
            str(raw.get("company") or ""),
            str(raw.get("job_title") or ""),
        ]
    )
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return False
    return bool(AI_HINT_RE.search(text))


def _is_hiring(raw_json: str) -> bool:
    try:
        raw = json.loads(raw_json or "{}")
    except Exception:
        raw = {}
    if not isinstance(raw, dict):
        return False
    return bool(raw.get("is_hiring"))


def _is_us_location(location: str) -> bool:
    t = (location or "").strip().lower()
    if not t:
        return False
    return bool(US_LOCATION_RE.search(t))


def main() -> int:
    ap = argparse.ArgumentParser(description="Export send-ready startup contacts from activity DB.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--limit", type=int, default=30)
    ap.add_argument("--title", default="QA Automation Engineer (Open Application)")
    ap.add_argument("--out", default="data/out/startup_email_wave.csv")
    ap.add_argument("--min-score", type=int, default=2)
    ap.add_argument("--require-domain-match", action="store_true")
    ap.add_argument("--ai-only", action="store_true", help="Keep only AI/ML-oriented startups (best-effort keyword match)")
    ap.add_argument("--require-hiring", action="store_true", help="Keep only startups marked as hiring in YC dataset")
    ap.add_argument("--non-us-only", action="store_true", help="Exclude startups with US location markers")
    ap.add_argument("--allow-unknown-location", action="store_true", help="With --non-us-only keep rows with empty location")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    out_path = resolve_path(ROOT, args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    conn = db_connect(db_path)
    init_db(conn)
    try:
        sent_contacts = _load_sent_contacts(conn)
        blocked_contacts = _load_blocked_contacts(conn)

        rows = conn.execute(
            """
            SELECT lead_id, contact, url, company, location, source, raw_json, created_at
            FROM leads
            WHERE platform = 'startup_scan' AND lead_type = 'startup_company'
            ORDER BY created_at DESC
            """
        ).fetchall()

        selected: List[Dict[str, str]] = []
        seen_email: Set[str] = set()
        stats = {
            "rows_total": len(rows),
            "selected": 0,
            "skip_no_email": 0,
            "skip_low_score": 0,
            "skip_blocked": 0,
            "skip_sent": 0,
            "skip_dup": 0,
            "skip_domain_mismatch": 0,
            "skip_not_ai": 0,
            "skip_not_hiring": 0,
            "skip_us_location": 0,
            "skip_unknown_location": 0,
        }

        for r in rows:
            if len(selected) >= max(1, int(args.limit)):
                break
            location = str(r["location"] or "").strip()
            if args.non_us_only:
                if _is_us_location(location):
                    stats["skip_us_location"] += 1
                    continue
                if not location and not args.allow_unknown_location:
                    stats["skip_unknown_location"] += 1
                    continue

            raw_json = str(r["raw_json"] or "")
            if args.ai_only and not _is_ai_friendly(raw_json):
                stats["skip_not_ai"] += 1
                continue
            if args.require_hiring and not _is_hiring(raw_json):
                stats["skip_not_hiring"] += 1
                continue

            candidates = _extract_email_candidates(str(r["contact"] or ""), raw_json)
            if not candidates:
                stats["skip_no_email"] += 1
                continue

            preferred_domain = _root_domain(str(r["url"] or ""))
            chosen = ""
            chosen_score = -999
            for c in candidates:
                score = _score_email(c, preferred_domain)
                if score <= chosen_score:
                    continue
                if c in blocked_contacts:
                    stats["skip_blocked"] += 1
                    continue
                if c in sent_contacts:
                    stats["skip_sent"] += 1
                    continue
                if c in seen_email:
                    stats["skip_dup"] += 1
                    continue
                chosen = c
                chosen_score = score
            if chosen_score < int(args.min_score):
                stats["skip_low_score"] += 1
                continue
            if args.require_domain_match and preferred_domain and not _is_domain_match(chosen, preferred_domain):
                stats["skip_domain_mismatch"] += 1
                continue
            if not chosen:
                continue

            seen_email.add(chosen)
            selected.append(
                {
                    "title": str(args.title).strip(),
                    "company": str(r["company"] or "Startup Company").strip(),
                    "location": location or "Remote",
                    "url": str(r["url"] or "").strip(),
                    "description": "",
                    "contact_email": chosen,
                    "contact_name": "Hiring Team",
                    "source": f"startup_scan:{str(r['source'] or '').strip()}",
                }
            )

        stats["selected"] = len(selected)

        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "title",
                    "company",
                    "location",
                    "url",
                    "description",
                    "contact_email",
                    "contact_name",
                    "source",
                ],
            )
            w.writeheader()
            w.writerows(selected)

        print(
            "[startup-export]"
            f" rows_total={stats['rows_total']}"
            f" selected={stats['selected']}"
            f" skip_no_email={stats['skip_no_email']}"
            f" skip_low_score={stats['skip_low_score']}"
            f" skip_blocked={stats['skip_blocked']}"
            f" skip_sent={stats['skip_sent']}"
            f" skip_dup={stats['skip_dup']}"
            f" skip_domain_mismatch={stats['skip_domain_mismatch']}"
            f" skip_us_location={stats['skip_us_location']}"
            f" skip_unknown_location={stats['skip_unknown_location']}"
            f" out={out_path}"
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
