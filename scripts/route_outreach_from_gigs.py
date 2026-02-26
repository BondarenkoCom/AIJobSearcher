import argparse
import csv
import json
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse, urlunparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402

EMAIL_RE = re.compile(r"(?i)^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$")


def _canonical_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    try:
        p = urlparse(u)
        path = (p.path or "").rstrip("/")
        netloc = (p.netloc or "").lower()
        if "linkedin.com" in netloc and (
            path.startswith("/jobs/view/")
            or path.startswith("/feed/update/")
            or path.startswith("/posts/")
            or path.startswith("/in/")
            or path.startswith("/company/")
        ):
            return urlunparse((p.scheme or "https", p.netloc, path, "", "", ""))
        return urlunparse((p.scheme or "https", p.netloc, path, "", p.query, ""))
    except Exception:
        return u


def _is_valid_email(v: str) -> bool:
    s = (v or "").strip().lower()
    return bool(s and EMAIL_RE.match(s))


def _is_linkedin_url(u: str) -> bool:
    try:
        p = urlparse((u or "").strip())
        return "linkedin.com" in (p.netloc or "").lower()
    except Exception:
        return False


def _is_linkedin_job_url(u: str) -> bool:
    try:
        p = urlparse((u or "").strip())
        return "linkedin.com" in (p.netloc or "").lower() and "/jobs/view/" in (p.path or "").lower()
    except Exception:
        return False


def _is_linkedin_profile_url(u: str) -> bool:
    try:
        p = urlparse((u or "").strip())
        return "linkedin.com" in (p.netloc or "").lower() and (p.path or "").startswith("/in/")
    except Exception:
        return False


def _extract_tg_handle(v: str) -> str:
    s = (v or "").strip()
    if not s:
        return ""
    low = s.lower()
    if low.startswith("tg_username:"):
        core = s.split(":", 1)[1].strip().lstrip("@")
        return f"@{core}" if core else ""
    if s.startswith("@"):
        core = s.strip().lstrip("@")
        return f"@{core}" if core else ""
    if low.startswith("https://t.me/") or low.startswith("http://t.me/"):
        core = re.sub(r"^https?://t\.me/", "", s, flags=re.IGNORECASE).strip()
        core = core.split("?", 1)[0].split("#", 1)[0].strip("/")
        core = core.split("/", 1)[0]
        return f"@{core}" if core else ""
    return ""


def _parse_raw(raw_json: str) -> Dict[str, object]:
    try:
        v = json.loads(raw_json or "{}")
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def _load_linkedin_post_index(conn) -> Dict[str, Dict[str, str]]:
    rows = conn.execute(
        """
        SELECT lead_id, url, contact, company, job_title, source, raw_json
        FROM leads
        WHERE platform='linkedin' AND lead_type='post'
        """
    ).fetchall()

    idx: Dict[str, Dict[str, str]] = {}
    for r in rows:
        raw = _parse_raw(str(r["raw_json"] or ""))
        author_url = str(raw.get("author_url") or "").strip()
        author_name = str(raw.get("author_name") or "").strip()
        rec = {
            "lead_id": str(r["lead_id"] or "").strip(),
            "url": str(r["url"] or "").strip(),
            "contact": str(r["contact"] or "").strip(),
            "company": str(r["company"] or "").strip(),
            "job_title": str(r["job_title"] or "").strip(),
            "source": str(r["source"] or "").strip(),
            "author_url": author_url,
            "author_name": author_name,
        }
        for key in {rec["url"], rec["contact"], author_url, str(raw.get("post_url") or "").strip()}:
            cu = _canonical_url(key)
            if cu and cu not in idx:
                idx[cu] = rec
    return idx


def _pick_input_url(row: Dict[str, str]) -> str:
    for k in ("url", "post_or_job_url", "post_url", "job_url", "contact_target", "contact"):
        v = (row.get(k) or "").strip()
        if v.startswith("http://") or v.startswith("https://"):
            return v
    return ""


def _pick_email(row: Dict[str, str]) -> str:
    method = (row.get("contact_method") or "").strip().lower()
    target = (row.get("contact_target") or "").strip()
    if method == "email" and _is_valid_email(target):
        return target.lower()
    for k in ("contact_email", "email", "contact"):
        v = (row.get(k) or "").strip()
        if _is_valid_email(v):
            return v.lower()
    return ""


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _write_url_list(path: Path, urls: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        for u in urls:
            if str(u or "").strip():
                f.write(str(u).strip() + "\n")


def _target_from_row(
    row: Dict[str, str],
    li_idx: Dict[str, Dict[str, str]],
    *,
    allow_linkedin_outreach: bool,
) -> Tuple[
    Optional[Dict[str, str]],
    Optional[Dict[str, str]],
    Optional[Dict[str, str]],
    Optional[Dict[str, str]],
    Optional[Dict[str, str]],
]:
    title = (row.get("title") or row.get("job_title") or "").strip()
    company = (row.get("company") or "").strip()
    location = (row.get("location") or "Remote").strip() or "Remote"
    source = (row.get("source") or "").strip()
    lead_id = (row.get("lead_id") or "").strip()
    contact_method = (row.get("contact_method") or "").strip().lower()
    contact_target = (row.get("contact_target") or "").strip()

    email = _pick_email(row)
    url = _pick_input_url(row)

    if email:
        return (
            {
                "title": title or "QA Automation Engineer",
                "company": company or "Hiring Company",
                "location": location,
                "url": url,
                "description": "",
                "contact_email": email,
                "contact_name": "Hiring Team",
                "source": source or "gig_router",
            },
            None,
            None,
            None,
            None,
        )

    tg_handle = _extract_tg_handle(contact_target) or _extract_tg_handle((row.get("contact") or "").strip())
    if contact_method == "telegram_dm" and tg_handle:
        return (
            None,
            {
                "lead_id": lead_id,
                "handle": tg_handle,
                "title": title or "QA task",
                "company": company,
                "url": url,
                "source": source or "gig_router",
            },
            None,
            None,
            None,
        )

    li_apply_url = ""
    if _is_linkedin_job_url(url):
        li_apply_url = _canonical_url(url)
    elif _is_linkedin_job_url(contact_target):
        li_apply_url = _canonical_url(contact_target)
    if contact_method == "platform_apply" and li_apply_url:
        return (
            None,
            None,
            {
                "lead_id": lead_id,
                "job_url": li_apply_url,
                "title": title,
                "company": company,
                "source": source or "gig_router",
            },
            None,
            None,
        )

    if not _is_linkedin_url(url):
        return (
            None,
            None,
            None,
            None,
            {
                "reason": "no_email_non_linkedin",
                "title": title,
                "company": company,
                "location": location,
                "url": url,
                "source": source,
            },
        )

    # Default safety policy: LinkedIn only via Apply/Easy Apply URL, no DM/connect.
    if not allow_linkedin_outreach:
        return (
            None,
            None,
            None,
            None,
            {
                "reason": "linkedin_apply_only",
                "title": title,
                "company": company,
                "location": location,
                "url": url,
                "source": source,
            },
        )

    rec = li_idx.get(_canonical_url(url), {})
    profile_url = ""
    if rec:
        au = (rec.get("author_url") or "").strip()
        ct = (rec.get("contact") or "").strip()
        if _is_linkedin_profile_url(au):
            profile_url = au
        elif _is_linkedin_profile_url(ct):
            profile_url = ct

    if not profile_url:
        return (
            None,
            None,
            None,
            None,
            {
                "reason": "linkedin_no_profile_url",
                "title": title,
                "company": company,
                "location": location,
                "url": url,
                "source": source,
            },
        )

    routed_lead_id = lead_id or (rec.get("lead_id") or "").strip() or f"post_{abs(hash(profile_url))}"
    author_name = (rec.get("author_name") or "").strip() or "Hiring Team"
    return (
        None,
        None,
        None,
        {
            "created_at": "",
            "status": "review",
            "action": "connect",
            "score": "5",
            "job_title": title or (rec.get("job_title") or "QA role"),
            "author_name": author_name,
            "author_url": profile_url,
            "post_or_job_url": url,
            "source_query": source or (rec.get("source") or "gig_router"),
            "snippet": "",
            "lead_id": routed_lead_id,
        },
        None,
    )


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Route gig shortlist: email first, then Telegram DM; LinkedIn job links to apply list. "
            "Legacy LinkedIn DM/connect output is optional."
        )
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")
    ap.add_argument("--in-csv", required=True, help="Input shortlist CSV.")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = all)")
    ap.add_argument("--email-out", default="data/out/gig_route_email_targets.csv")
    ap.add_argument("--tg-out", default="data/out/gig_route_telegram_targets.csv")
    ap.add_argument("--li-apply-out", default="data/out/gig_route_linkedin_apply_urls.txt")
    ap.add_argument("--li-out", default="data/out/gig_route_linkedin_targets.csv")
    ap.add_argument("--manual-out", default="data/out/gig_route_manual_targets.csv")
    ap.add_argument("--allow-linkedin-outreach", action="store_true", help="Enable legacy LinkedIn DM/connect CSV.")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if args.db.strip():
        db_path = resolve_path(ROOT, args.db.strip())

    in_csv = resolve_path(ROOT, args.in_csv)
    if not in_csv.exists():
        print(f"[route-gig] missing input: {in_csv}")
        return 2

    conn = db_connect(db_path)
    init_db(conn)
    try:
        li_idx = _load_linkedin_post_index(conn)
    finally:
        conn.close()

    rows = list(csv.DictReader(in_csv.open("r", encoding="utf-8", newline="")))
    if int(args.limit) > 0:
        rows = rows[: int(args.limit)]

    email_rows: List[Dict[str, str]] = []
    tg_rows: List[Dict[str, str]] = []
    li_apply_rows: List[Dict[str, str]] = []
    li_rows: List[Dict[str, str]] = []
    manual_rows: List[Dict[str, str]] = []

    seen_email = set()
    seen_tg = set()
    seen_li_apply = set()
    seen_profile = set()
    seen_manual = set()

    for row in rows:
        e_row, tg_row, li_apply_row, li_row, m_row = _target_from_row(
            row,
            li_idx,
            allow_linkedin_outreach=bool(args.allow_linkedin_outreach),
        )
        if e_row is not None:
            em = e_row.get("contact_email", "").strip().lower()
            if em and em not in seen_email:
                seen_email.add(em)
                email_rows.append(e_row)
            continue

        if tg_row is not None:
            h = (tg_row.get("handle") or "").strip().lower()
            if h and h not in seen_tg:
                seen_tg.add(h)
                tg_rows.append(tg_row)
            continue

        if li_apply_row is not None:
            ju = _canonical_url(li_apply_row.get("job_url", ""))
            if ju and ju not in seen_li_apply:
                seen_li_apply.add(ju)
                li_apply_rows.append(li_apply_row)
            continue

        if li_row is not None:
            pu = _canonical_url(li_row.get("author_url", ""))
            if pu and pu not in seen_profile:
                seen_profile.add(pu)
                li_rows.append(li_row)
            continue

        if m_row is not None:
            key = (_canonical_url(m_row.get("url", "")) or (m_row.get("title", "") + "|" + m_row.get("company", ""))).lower()
            if key not in seen_manual:
                seen_manual.add(key)
                manual_rows.append(m_row)

    email_out = resolve_path(ROOT, args.email_out)
    tg_out = resolve_path(ROOT, args.tg_out)
    li_apply_out = resolve_path(ROOT, args.li_apply_out)
    li_out = resolve_path(ROOT, args.li_out)
    manual_out = resolve_path(ROOT, args.manual_out)

    _write_csv(
        email_out,
        ["title", "company", "location", "url", "description", "contact_email", "contact_name", "source"],
        email_rows,
    )
    _write_csv(
        tg_out,
        ["lead_id", "handle", "title", "company", "url", "source"],
        tg_rows,
    )
    _write_url_list(li_apply_out, [r.get("job_url", "") for r in li_apply_rows])
    _write_csv(
        li_out,
        [
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
            "lead_id",
        ],
        li_rows,
    )
    _write_csv(
        manual_out,
        ["reason", "title", "company", "location", "url", "source"],
        manual_rows,
    )

    print(f"[route-gig] input={in_csv} rows={len(rows)}")
    print(f"[route-gig] email_targets={len(email_rows)} -> {email_out}")
    print(f"[route-gig] telegram_targets={len(tg_rows)} -> {tg_out}")
    print(f"[route-gig] linkedin_apply_urls={len(li_apply_rows)} -> {li_apply_out}")
    print(
        f"[route-gig] linkedin_outreach_targets={len(li_rows)} -> {li_out} "
        f"(enabled={int(bool(args.allow_linkedin_outreach))})"
    )
    print(f"[route-gig] manual_targets={len(manual_rows)} -> {manual_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
