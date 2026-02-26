import argparse
import csv
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

import requests

ROOT = Path(__file__).resolve().parents[1]

TME_RE = re.compile(r"https?://t\.me/([A-Za-z0-9_]{4,})", re.IGNORECASE)
WORK_TERMS = {
    "job",
    "jobs",
    "hiring",
    "hire",
    "vacancy",
    "vacancies",
    "freelance",
    "gig",
    "remote",
    "work",
    "contract",
    "qa",
    "tester",
    "test automation",
    "sdet",
    "automation",
}
BAD_HANDLES = {"joinchat", "addlist", "iv"}


def split_items(raw: str) -> List[str]:
    return [x.strip() for x in re.split(r"[\r\n,;]+", str(raw or "")) if x.strip()]


def read_list_file(path: Path) -> List[str]:
    if not path.exists():
        return []
    out: List[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip().lstrip("\ufeff").strip()
        if s and not s.startswith("#"):
            out.append(s)
    return out


def append_unique_lines(path: Path, values: Sequence[str]) -> List[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = {line.strip().lower() for line in read_list_file(path)}
    added: List[str] = []
    for v in values:
        s = str(v or "").strip()
        if not s:
            continue
        if not s.startswith("@"):
            s = "@" + s.lstrip("@")
        if s.lower() in existing:
            continue
        existing.add(s.lower())
        added.append(s)
    if added:
        prefix = "\n" if path.exists() and path.read_text(encoding="utf-8").strip() else ""
        with path.open("a", encoding="utf-8") as f:
            f.write(prefix)
            for x in added:
                f.write(x + "\n")
    return added


def _norm_handle(h: str) -> str:
    core = str(h or "").strip().lstrip("@")
    if not core:
        return ""
    if core.lower() in BAD_HANDLES:
        return ""
    return "@" + core


def _extract_handles(text: str) -> List[str]:
    out: List[str] = []
    for m in TME_RE.findall(text or ""):
        h = _norm_handle(m)
        if h:
            out.append(h)
    uniq: List[str] = []
    seen: Set[str] = set()
    for h in out:
        if h.lower() in seen:
            continue
        seen.add(h.lower())
        uniq.append(h)
    return uniq


def _has_work_context(text: str) -> bool:
    low = str(text or "").lower()
    return any(t in low for t in WORK_TERMS)


def _reddit_search(subreddit: str, query: str, limit: int, time_filter: str, timeout_sec: float, *, global_search: bool) -> Dict[str, Any]:
    if global_search:
        url = "https://www.reddit.com/search.json"
    else:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
    headers = {"User-Agent": "AIJobSearcher/1.0 (+https://github.com/)"}
    params = {
        "q": query,
        "restrict_sr": "0" if global_search else "1",
        "sort": "new",
        "t": time_filter,
        "limit": int(limit),
    }
    r = requests.get(url, params=params, headers=headers, timeout=timeout_sec)
    r.raise_for_status()
    payload = r.json()
    return payload if isinstance(payload, dict) else {}


def _iter_posts(payload: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    root = payload.get("data")
    if not isinstance(root, dict):
        return []
    children = root.get("children")
    if not isinstance(children, list):
        return []
    out: List[Dict[str, Any]] = []
    for ch in children:
        if isinstance(ch, dict) and isinstance(ch.get("data"), dict):
            out.append(ch["data"])
    return out


def _out_csv(path: str) -> Path:
    if str(path or "").strip():
        p = Path(path)
        return p if p.is_absolute() else ROOT / p
    return ROOT / "data" / "out" / f"reddit_tg_sources_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"


def main() -> int:
    ap = argparse.ArgumentParser(description="Discover Telegram job/gig sources from Reddit and append to telegram_chats.")
    ap.add_argument(
        "--subreddits",
        default="forhire,freelance_forhire,RemoteJobs,remotework,jobs,jobbit,forhireuk,digitalnomad",
    )
    ap.add_argument(
        "--queries",
        default=(
            "telegram remote jobs channel,"
            "telegram freelance jobs,"
            "t.me qa jobs,"
            "telegram tester jobs,"
            "telegram hiring channel"
        ),
    )
    ap.add_argument("--limit-per-query", type=int, default=40)
    ap.add_argument("--time-filter", default="year", choices=["day", "week", "month", "year", "all"])
    ap.add_argument("--timeout-sec", type=float, default=18.0)
    ap.add_argument("--sources-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--append", action="store_true")
    ap.add_argument("--global-search", action="store_true", help="Use reddit.com/search.json over all subreddits")
    ap.add_argument("--out-csv", default="")
    args = ap.parse_args()

    subreddits = split_items(args.subreddits)
    queries = split_items(args.queries)
    if not subreddits or not queries:
        print("[reddit-tg] no subreddits/queries.")
        return 2

    rows: List[Dict[str, str]] = []
    all_handles: Set[str] = set()
    scanned_posts = 0

    subs_loop = subreddits if not args.global_search else ["_global_"]
    for sub in subs_loop:
        for q in queries:
            try:
                payload = _reddit_search(
                    subreddit=sub,
                    query=q,
                    limit=max(1, int(args.limit_per_query)),
                    time_filter=args.time_filter,
                    timeout_sec=float(args.timeout_sec),
                    global_search=bool(args.global_search),
                )
            except Exception as e:
                print(f"[reddit-tg] failed r/{sub} q='{q}': {e}")
                continue

            for post in _iter_posts(payload):
                scanned_posts += 1
                title = str(post.get("title") or "").strip()
                selftext = str(post.get("selftext") or "").strip()
                link_url = str(post.get("url") or "").strip()
                permalink = str(post.get("permalink") or "").strip()
                source_url = f"https://www.reddit.com{permalink}" if permalink.startswith("/") else permalink
                blob = "\n".join([title, selftext, link_url, source_url]).strip()
                if not _has_work_context(blob):
                    continue
                handles = _extract_handles(blob)
                if not handles:
                    continue
                for h in handles:
                    all_handles.add(h.lower())
                    rows.append(
                        {
                            "subreddit": sub,
                            "query": q,
                            "handle": h,
                            "reddit_url": source_url,
                            "title": title[:180],
                        }
                    )

    uniq_rows: List[Dict[str, str]] = []
    seen_key: Set[str] = set()
    for r in rows:
        k = f"{r['handle'].lower()}|{r['reddit_url']}"
        if k in seen_key:
            continue
        seen_key.add(k)
        uniq_rows.append(r)

    out_csv = _out_csv(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["subreddit", "query", "handle", "reddit_url", "title"])
        w.writeheader()
        w.writerows(uniq_rows)

    added: List[str] = []
    if args.append:
        src = Path(args.sources_file)
        if not src.is_absolute():
            src = ROOT / src
        handles = sorted({r["handle"] for r in uniq_rows if r.get("handle")})
        added = append_unique_lines(src, handles)

    print(f"[reddit-tg] subreddits={len(subreddits)} queries={len(queries)} scanned_posts={scanned_posts}")
    print(f"[reddit-tg] discovered_handles={len({r['handle'].lower() for r in uniq_rows})} rows={len(uniq_rows)}")
    if args.append:
        print(f"[reddit-tg] appended_sources={len(added)}")
    print(f"[reddit-tg] csv={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
