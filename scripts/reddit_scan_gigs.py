import argparse
import csv
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.activity_db import LeadUpsert, add_event, connect as db_connect, init_db, upsert_lead_with_flag  # noqa: E402
from src.config import cfg_get, load_config  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402


QA_TERMS = {
    "qa",
    "quality assurance",
    "sdet",
    "test automation",
    "tester",
    "automation testing",
    "api testing",
    "playwright",
    "selenium",
    "c#",
    ".net",
    "postman",
    "graphql",
}
GIG_TERMS = {
    "gig",
    "freelance",
    "contract",
    "one-off",
    "one off",
    "quick task",
    "small task",
    "urgent",
    "bug fix",
    "short-term",
    "short term",
}
PAY_TERMS = {
    "$",
    "usd",
    "eur",
    "paid",
    "paying",
    "payment",
    "budget",
    "hourly",
    "fixed price",
}
REMOTE_TERMS = {"remote", "worldwide", "global", "anywhere", "wfh"}
EXCLUDE_TERMS = {"intern", "internship", "director", "vp", "head of qa", "manager"}
SCAM_TERMS = {"pay to apply", "upfront fee", "activation fee", "crypto investment", "mlm"}

EMAIL_RE = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
URL_RE = re.compile(r"https?://[^\s)>\]}]+", re.IGNORECASE)
TG_HANDLE_RE = re.compile(r"(?<![\w])@[A-Za-z0-9_]{4,}")


def split_items(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[\r\n,;]+", str(raw or "")) if x.strip()]


def hits(text_low: str, terms: Sequence[str]) -> List[str]:
    return sorted({t for t in terms if t and t in text_low})


def evaluate_fit(text: str, min_score: int, require_pay: bool) -> Dict[str, Any]:
    low = str(text or "").lower()
    qa = hits(low, QA_TERMS)
    gig = hits(low, GIG_TERMS)
    pay = hits(low, PAY_TERMS)
    rem = hits(low, REMOTE_TERMS)
    bad = hits(low, EXCLUDE_TERMS)
    scam = hits(low, SCAM_TERMS)
    score = (2 * len(qa)) + len(gig) + len(pay) + len(rem) - (2 * len(bad))
    is_gig = any(x in low for x in ("one-off", "one off", "quick task", "small task", "bug fix", "urgent"))
    ok = bool(qa) and bool(gig or pay) and score >= int(min_score)
    if require_pay and not pay:
        ok = False
    if bad or scam:
        ok = False
    return {
        "ok": ok,
        "score": score,
        "qa": qa,
        "gig": gig,
        "pay": pay,
        "rem": rem,
        "bad": bad,
        "scam": scam,
        "lead_type": "gig" if is_gig else "project",
    }


def extract_contacts(text: str) -> Tuple[List[str], List[str], List[str]]:
    emails = sorted({m.strip().lower() for m in EMAIL_RE.findall(text or "")})
    urls = sorted({m.strip().rstrip(".,;:!?)]}") for m in URL_RE.findall(text or "")})
    handles = sorted({m.strip() for m in TG_HANDLE_RE.findall(text or "")})
    return emails, urls, handles


def bool_env(name: str, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def iso_utc(ts_utc: float) -> str:
    try:
        dt = datetime.fromtimestamp(float(ts_utc), tz=timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec="seconds")


def canonical_reddit_url(permalink: str) -> str:
    p = str(permalink or "").strip()
    if not p:
        return ""
    if p.startswith("http://") or p.startswith("https://"):
        return p
    if not p.startswith("/"):
        p = "/" + p
    return f"https://www.reddit.com{p}"


def read_json(url: str, params: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    headers = {"User-Agent": "AIJobSearcher/1.0 (+https://github.com/)"}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else {}


def parse_listing(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    root = data.get("data")
    if not isinstance(root, dict):
        return out
    children = root.get("children")
    if not isinstance(children, list):
        return out
    for ch in children:
        if not isinstance(ch, dict):
            continue
        item = ch.get("data")
        if isinstance(item, dict):
            out.append(item)
    return out


def csv_path(value: str) -> Path:
    if value.strip():
        p = Path(value)
        return p if p.is_absolute() else ROOT / p
    return ROOT / "data" / "out" / f"reddit_gigs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def run(args: argparse.Namespace) -> int:
    cfg = load_config(str(ROOT / "config" / "config.yaml"))
    include_terms = list(dict.fromkeys(list(QA_TERMS) + [str(x).lower() for x in cfg_get(cfg, "profile.keywords.include", []) or []]))
    # Keep include terms list for additional QA hits if config expands.
    for t in include_terms:
        if t not in QA_TERMS:
            QA_TERMS.add(t)

    subreddits = split_items(args.subreddits)
    queries = split_items(args.queries)
    if not subreddits or not queries:
        print("[reddit-scan] no subreddits/queries provided.")
        return 2

    cutoff = datetime.now(timezone.utc) - timedelta(days=max(0, int(args.days)))
    conn = None
    if args.write_db:
        db_path = Path(args.db) if Path(args.db).is_absolute() else ROOT / args.db
        conn = db_connect(db_path)
        init_db(conn)
    rows: List[Dict[str, Any]] = []
    seen_keys = set()
    scanned = 0
    matched = 0
    inserted = 0

    for sub in subreddits:
        for q in queries:
            url = f"https://www.reddit.com/r/{sub}/search.json"
            params = {
                "q": q,
                "restrict_sr": "1",
                "sort": "new",
                "t": args.time_filter,
                "limit": int(args.limit_per_query),
            }
            try:
                payload = read_json(url, params=params, timeout=float(args.timeout_sec))
            except Exception as e:
                print(f"[reddit-scan] failed r/{sub} q='{q}': {e}")
                continue

            for it in parse_listing(payload):
                if matched >= int(args.max_results):
                    break
                scanned += 1
                post_id = str(it.get("id") or "").strip()
                permalink = canonical_reddit_url(str(it.get("permalink") or ""))
                if not post_id or not permalink:
                    continue
                if post_id in seen_keys:
                    continue
                seen_keys.add(post_id)

                title = str(it.get("title") or "").strip()
                selftext = str(it.get("selftext") or "").strip()
                author = str(it.get("author") or "").strip()
                created_utc = float(it.get("created_utc") or 0.0)
                posted_at = iso_utc(created_utc)
                try:
                    posted_dt = datetime.fromtimestamp(created_utc, tz=timezone.utc)
                except Exception:
                    posted_dt = datetime.now(timezone.utc)
                if posted_dt < cutoff:
                    continue

                text = (title + "\n" + selftext).strip()
                fit = evaluate_fit(text, int(args.min_score), bool(args.require_pay_signal))
                if not fit["ok"]:
                    continue

                emails, urls, handles = extract_contacts(text)
                if args.require_contact_signal and not (emails or handles):
                    continue

                row = {
                    "subreddit": sub,
                    "query": q,
                    "post_id": post_id,
                    "title": title,
                    "posted_at": posted_at,
                    "author": author,
                    "url": permalink,
                    "contact_email": emails[0] if emails else "",
                    "contact_handle": handles[0] if handles else "",
                    "score": fit["score"],
                    "lead_type": fit["lead_type"],
                    "pay_signal": "yes" if fit["pay"] else "no",
                    "remote_signal": "yes" if fit["rem"] else "no",
                    "qa_hits": "|".join(fit["qa"]),
                    "gig_hits": "|".join(fit["gig"]),
                    "pay_hits": "|".join(fit["pay"]),
                    "snippet": re.sub(r"\s+", " ", text).strip()[:450],
                }
                rows.append(row)
                matched += 1

                if conn is not None:
                    contact = emails[0] if emails else (f"reddit_user:{author}" if author else f"reddit_post:{post_id}")
                    lead = LeadUpsert(
                        platform="reddit",
                        lead_type=str(fit["lead_type"]),
                        contact=contact,
                        url=permalink,
                        company=f"r/{sub}",
                        job_title=title[:160] if title else "Reddit gig",
                        location="Remote",
                        source=f"reddit:r/{sub}",
                        created_at=posted_at,
                        raw={
                            "source": "reddit_scan_gigs",
                            "query": q,
                            "post_id": post_id,
                            "author": author,
                            "text": text,
                            "score": fit["score"],
                            "qa_hits": fit["qa"],
                            "gig_hits": fit["gig"],
                            "pay_hits": fit["pay"],
                            "remote_hits": fit["rem"],
                            "emails": emails,
                            "handles": handles,
                            "urls": urls,
                        },
                    )
                    lead_id, was_inserted = upsert_lead_with_flag(conn, lead)
                    if was_inserted:
                        inserted += 1
                        add_event(
                            conn,
                            lead_id=lead_id,
                            event_type="reddit_gig_collected",
                            status="ok",
                            occurred_at=posted_at,
                            details={"subreddit": sub, "query": q, "post_id": post_id, "score": fit["score"]},
                        )

            if matched >= int(args.max_results):
                break
        if matched >= int(args.max_results):
            break

    if conn is not None:
        conn.commit()
        conn.close()

    out_csv = csv_path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "subreddit",
        "query",
        "post_id",
        "title",
        "posted_at",
        "author",
        "url",
        "contact_email",
        "contact_handle",
        "score",
        "lead_type",
        "pay_signal",
        "remote_signal",
        "qa_hits",
        "gig_hits",
        "pay_hits",
        "snippet",
    ]
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fields})

    print(
        f"[reddit-scan] subreddits={len(subreddits)} queries={len(queries)} "
        f"scanned={scanned} matched={matched} inserted={inserted}"
    )
    print(f"[reddit-scan] csv={out_csv}")
    if args.write_db:
        db_path = Path(args.db) if Path(args.db).is_absolute() else ROOT / args.db
        print(f"[reddit-scan] db={db_path}")
    if args.telegram and bool_env("TELEGRAM_REPORT", True):
        send_telegram_message(
            "\n".join(
                [
                    "AIJobSearcher: Reddit gig scan",
                    f"Subreddits: {len(subreddits)}",
                    f"Queries: {len(queries)}",
                    f"Scanned: {scanned}",
                    f"Matched: {matched}",
                    f"Inserted: {inserted}" if args.write_db else "Inserted: disabled",
                    f"CSV: {out_csv}",
                ]
            )
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="Scan Reddit subreddits for paid remote QA gigs.")
    ap.add_argument(
        "--subreddits",
        default="forhire,freelance_forhire,forhireuk,slavelabour,testautomation,QualityAssurance",
    )
    ap.add_argument(
        "--queries",
        default="qa automation freelance,test automation gig,playwright bug fix,api testing task,selenium fix paid",
    )
    ap.add_argument("--limit-per-query", type=int, default=40)
    ap.add_argument("--max-results", type=int, default=120)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--time-filter", default="month", choices=["day", "week", "month", "year", "all"])
    ap.add_argument("--min-score", type=int, default=3)
    ap.add_argument("--require-pay-signal", action="store_true")
    ap.add_argument("--require-contact-signal", action="store_true")
    ap.add_argument("--write-db", action="store_true")
    ap.add_argument("--db", default="data/out/activity.sqlite")
    ap.add_argument("--out-csv", default="")
    ap.add_argument("--timeout-sec", type=float, default=18.0)
    ap.add_argument("--telegram", action="store_true")
    return ap


def main() -> int:
    return run(build_parser().parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
