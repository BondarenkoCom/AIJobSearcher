import argparse
import json
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402


YC_ALL_URL = "https://yc-oss.github.io/api/companies/all.json"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
)
EMAIL_RE = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
CONTACT_HINT_RE = re.compile(r"(contact|about|team|careers|jobs|join|company)", re.IGNORECASE)
SOCIAL_KEYS = ("linkedin", "twitter", "x", "github", "facebook", "instagram", "youtube", "tiktok", "telegram")
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


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_url(value: str) -> str:
    url = _safe_text(value)
    if not url:
        return ""
    parsed = urlparse(url)
    if not parsed.scheme:
        url = "https://" + url
    return url.strip()


def _domain(url: str) -> str:
    host = (urlparse(_norm_url(url)).netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _extract_emails(text: str) -> List[str]:
    if not text:
        return []
    out = sorted({m.group(0).lower().strip(" .;,)]}") for m in EMAIL_RE.finditer(text)})
    ignore = {"example.com", "yourcompany.com", "domain.com"}
    bad_suffixes = (".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".ico", ".css", ".js", ".woff", ".woff2")
    cleaned: List[str] = []
    for e in out:
        if "@" not in e:
            continue
        dom = e.split("@", 1)[1].strip().lower()
        if dom in ignore:
            continue
        if any(dom.endswith(sfx) for sfx in bad_suffixes):
            continue
        if ".." in e or "/@" in e:
            continue
        cleaned.append(e)
    return cleaned[:12]


def _social_bucket(href: str) -> Optional[str]:
    h = (href or "").lower()
    if "linkedin.com" in h:
        return "linkedin"
    if "twitter.com" in h:
        return "twitter"
    if "x.com" in h:
        return "x"
    if "github.com" in h:
        return "github"
    if "facebook.com" in h:
        return "facebook"
    if "instagram.com" in h:
        return "instagram"
    if "youtube.com" in h or "youtu.be" in h:
        return "youtube"
    if "tiktok.com" in h:
        return "tiktok"
    if "t.me/" in h:
        return "telegram"
    return None


def _parse_links(base_url: str, html: str) -> Tuple[List[str], Dict[str, List[str]], List[str]]:
    emails: Set[str] = set()
    socials: Dict[str, Set[str]] = {k: set() for k in SOCIAL_KEYS}
    internal_candidates: Set[str] = set()

    soup = BeautifulSoup(html, "html.parser")
    for a in soup.find_all("a", href=True):
        href_raw = _safe_text(a.get("href"))
        if not href_raw:
            continue

        if href_raw.lower().startswith("mailto:"):
            email = href_raw.split(":", 1)[1].split("?", 1)[0].strip().lower()
            if "@" in email:
                emails.add(email)
            continue

        abs_href = _norm_url(urljoin(base_url, href_raw))
        if not abs_href:
            continue

        social_key = _social_bucket(abs_href)
        if social_key:
            socials[social_key].add(abs_href)
            continue

        try:
            same_domain = _domain(abs_href) == _domain(base_url)
        except Exception:
            same_domain = False
        anchor_text = _safe_text(a.get_text(" ", strip=True))
        if same_domain and CONTACT_HINT_RE.search(abs_href + " " + anchor_text):
            internal_candidates.add(abs_href)

    for e in _extract_emails(html):
        emails.add(e)

    socials_clean = {k: sorted(v)[:5] for k, v in socials.items() if v}
    return sorted(emails)[:12], socials_clean, sorted(internal_candidates)[:3]


def _fetch_html(session: requests.Session, url: str, timeout_sec: float) -> Tuple[str, str]:
    u = _norm_url(url)
    if not u:
        return "", ""
    try:
        r = session.get(
            u,
            timeout=(max(2.0, timeout_sec / 2.0), timeout_sec),
            allow_redirects=True,
            headers={"User-Agent": DEFAULT_UA, "Accept-Language": "en-US,en;q=0.9"},
        )
    except Exception:
        return "", ""
    if r.status_code >= 400:
        return "", ""
    ctype = (r.headers.get("content-type") or "").lower()
    if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
        return "", ""
    text = r.text or ""
    if len(text) > 600_000:
        text = text[:600_000]
    return _norm_url(r.url), text


def _enrich_card(base: Dict[str, Any], timeout_sec: float) -> Dict[str, Any]:
    website = _norm_url(base.get("website", ""))
    if not website:
        base["enrich_status"] = "no_website"
        return base

    original_website = website
    original_domain = _domain(original_website)
    session = requests.Session()
    page_url, html = _fetch_html(session, website, timeout_sec=timeout_sec)
    if not html:
        base["enrich_status"] = "fetch_failed"
        return base

    emails, socials, candidates = _parse_links(page_url or website, html)
    if not emails and candidates:
        for cu in candidates[:2]:
            cu_url, cu_html = _fetch_html(session, cu, timeout_sec=timeout_sec)
            if not cu_html:
                continue
            e2, s2, _ = _parse_links(cu_url or cu, cu_html)
            emails.extend(e2)
            for sk, sv in s2.items():
                if sk not in socials:
                    socials[sk] = []
                socials[sk] = sorted(set(socials[sk]) | set(sv))[:5]
            if emails:
                break

    final_url = page_url or original_website
    final_domain = _domain(final_url)
    base["website"] = original_website
    base["final_url"] = final_url
    base["domain"] = original_domain
    base["redirected"] = bool(final_domain and original_domain and final_domain != original_domain)
    base["emails"] = ";".join(sorted(set(emails))[:12])
    for sk in SOCIAL_KEYS:
        base[f"social_{sk}"] = ";".join((socials.get(sk) or [])[:5])
    base["enrich_status"] = "ok"
    return base


def _fetch_yc_companies(active_only: bool = True) -> List[Dict[str, Any]]:
    r = requests.get(YC_ALL_URL, timeout=35, headers={"User-Agent": DEFAULT_UA, "Accept-Language": "en-US,en;q=0.9"})
    r.raise_for_status()
    data = r.json()
    out: List[Dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        status = _safe_text(row.get("status"))
        if active_only and status.lower() != "active":
            continue
        website = _norm_url(_safe_text(row.get("website")))
        if not website:
            continue
        item = {
            "company": _safe_text(row.get("name")),
            "website": website,
            "domain": _domain(website),
            "location": _safe_text(row.get("all_locations")),
            "batch": _safe_text(row.get("batch")),
            "stage": _safe_text(row.get("stage")),
            "status": status,
            "is_hiring": bool(row.get("isHiring")),
            "industry": _safe_text(row.get("industry")),
            "subindustry": _safe_text(row.get("subindustry")),
            "regions": ";".join([_safe_text(x) for x in (row.get("regions") or []) if _safe_text(x)]),
            "one_liner": _safe_text(row.get("one_liner")),
            "source": "yc_oss",
            "source_url": _safe_text(row.get("url")),
            "api_url": _safe_text(row.get("api")),
            "raw": row,
        }
        if item["company"] and item["website"]:
            out.append(item)
    return out


def _dedupe_cards(cards: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str]] = set()
    out: List[Dict[str, Any]] = []
    for card in cards:
        key = (card.get("company", "").strip().lower(), card.get("domain", "").strip().lower())
        if not key[0] or not key[1]:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(card)
    return out


def _is_ai_card(card: Dict[str, Any]) -> bool:
    if not isinstance(card, dict):
        return False
    text = " ".join(
        [
            _safe_text(card.get("industry")),
            _safe_text(card.get("subindustry")),
            _safe_text(card.get("one_liner")),
            _safe_text(card.get("company")),
        ]
    )
    text = re.sub(r"\s+", " ", text).strip()
    if not text:
        return False
    return bool(AI_HINT_RE.search(text))


def _is_us_location(location: str) -> bool:
    t = _safe_text(location).lower()
    if not t:
        return False
    return bool(US_LOCATION_RE.search(t))


def _write_csv(path: Path, cards: List[Dict[str, Any]]) -> None:
    import csv

    fieldnames = [
        "company",
        "website",
        "final_url",
        "domain",
        "redirected",
        "location",
        "batch",
        "stage",
        "status",
        "is_hiring",
        "industry",
        "subindustry",
        "regions",
        "one_liner",
        "emails",
        "social_linkedin",
        "social_twitter",
        "social_x",
        "social_github",
        "social_facebook",
        "social_instagram",
        "social_youtube",
        "social_tiktok",
        "social_telegram",
        "source",
        "source_url",
        "api_url",
        "enrich_status",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for c in cards:
            w.writerow(c)


def _write_json(path: Path, cards: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    clean: List[Dict[str, Any]] = []
    for c in cards:
        x = dict(c)
        x.pop("raw", None)
        clean.append(x)
    path.write_text(json.dumps(clean, ensure_ascii=False, indent=2), encoding="utf-8")


def _persist_db(cards: List[Dict[str, Any]], db_path: Path) -> Dict[str, int]:
    conn = db_connect(db_path)
    init_db(conn)
    inserted = 0
    updated = 0
    try:
        for idx, c in enumerate(cards, start=1):
            emails = [e for e in _safe_text(c.get("emails")).split(";") if e]
            contact = emails[0] if emails else _safe_text(c.get("website"))
            lead = LeadUpsert(
                platform="startup_scan",
                lead_type="startup_company",
                contact=contact,
                url=_safe_text(c.get("website")),
                company=_safe_text(c.get("company")),
                job_title=_safe_text(c.get("one_liner")) or "Startup company lead",
                location=_safe_text(c.get("location")),
                source=_safe_text(c.get("source")) or "yc_oss",
                created_at=_now_iso(),
                raw=c,
            )
            lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
            add_event(
                conn,
                lead_id=lead_id,
                event_type="startup_scanned",
                status="ok",
                occurred_at=_now_iso(),
                details={
                    "company": c.get("company"),
                    "website": c.get("website"),
                    "emails": c.get("emails"),
                    "source": c.get("source"),
                },
            )
            if was_inserted:
                inserted += 1
            else:
                updated += 1
            if idx % 50 == 0:
                conn.commit()
        conn.commit()
    finally:
        conn.close()
    return {"inserted": inserted, "updated": updated}


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Global startup scanner (YC + website enrichment) -> DB + CSV cards")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--limit", type=int, default=300)
    ap.add_argument("--workers", type=int, default=10)
    ap.add_argument("--timeout-sec", type=float, default=10.0)
    ap.add_argument("--active-only", action="store_true", default=True)
    ap.add_argument("--include-inactive", action="store_true", help="Include non-active startups too")
    ap.add_argument("--ai-only", action="store_true", help="Select only AI/ML-oriented startups (best-effort keyword match)")
    ap.add_argument("--hiring-only", action="store_true", help="Select only startups marked as hiring in YC dataset")
    ap.add_argument("--non-us-only", action="store_true", help="Select only startups without US location markers")
    ap.add_argument("--shuffle", action="store_true", help="Shuffle cards before applying --limit")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for --shuffle")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--out-json", default="")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_csv = resolve_path(ROOT, args.out_csv) if args.out_csv.strip() else (ROOT / "data" / "out" / f"startup_cards_global_{stamp}.csv")
    out_json = resolve_path(ROOT, args.out_json) if args.out_json.strip() else (ROOT / "data" / "out" / f"startup_cards_global_{stamp}.json")

    active_only = not args.include_inactive
    print(f"[startup-scan] source=yc_oss active_only={active_only}")
    raw_cards = _fetch_yc_companies(active_only=active_only)
    print(f"[startup-scan] fetched: {len(raw_cards)}")
    if not raw_cards:
        print("[startup-scan] no cards fetched.")
        return 2

    cards = _dedupe_cards(raw_cards)
    if args.ai_only:
        before = len(cards)
        cards = [c for c in cards if _is_ai_card(c)]
        print(f"[startup-scan] ai-only filter: {before} -> {len(cards)}")
    if args.hiring_only:
        before = len(cards)
        cards = [c for c in cards if bool(c.get('is_hiring'))]
        print(f"[startup-scan] hiring-only filter: {before} -> {len(cards)}")
    if args.non_us_only:
        before = len(cards)
        cards = [c for c in cards if not _is_us_location(c.get("location", ""))]
        print(f"[startup-scan] non-us filter: {before} -> {len(cards)}")
    if args.shuffle:
        random.seed(args.seed)
        random.shuffle(cards)
    cards = cards[: max(1, args.limit)]
    print(f"[startup-scan] selected for enrichment: {len(cards)}")

    enriched: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(_enrich_card, dict(card), args.timeout_sec) for card in cards]
        done_count = 0
        for fut in as_completed(futures):
            done_count += 1
            try:
                enriched.append(fut.result())
            except Exception:
                continue
            if done_count % 25 == 0 or done_count == len(cards):
                print(f"[startup-scan] enriched {done_count}/{len(cards)}")

    enriched = _dedupe_cards(enriched)
    enriched = sorted(enriched, key=lambda x: (x.get("company", "").lower(), x.get("domain", "")))
    _write_csv(out_csv, enriched)
    _write_json(out_json, enriched)

    db_stats = _persist_db(enriched, db_path=db_path)
    with_email = sum(1 for c in enriched if _safe_text(c.get("emails")))
    with_social = sum(
        1
        for c in enriched
        if any(_safe_text(c.get(f"social_{k}")) for k in SOCIAL_KEYS)
    )
    print(f"[startup-scan] cards={len(enriched)} with_email={with_email} with_social={with_social}")
    print(f"[startup-scan] db inserted={db_stats['inserted']} updated={db_stats['updated']}")
    print(f"[startup-scan] csv={out_csv}")
    print(f"[startup-scan] json={out_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
