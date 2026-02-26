import argparse
import csv
import shlex
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]


def _python_exe() -> str:
    candidates = [
        ROOT / ".venv" / "Scripts" / "python.exe",
        ROOT / ".venv" / "bin" / "python",
        Path(sys.executable),
    ]
    for p in candidates:
        if p.exists():
            return str(p)
    return sys.executable


def _fmt_cmd(cmd: Sequence[str]) -> str:
    return " ".join(shlex.quote(str(x)) for x in cmd)


def _run_step(label: str, cmd: Sequence[str]) -> int:
    print(f"[upwork-share] step={label}")
    print(f"[upwork-share] cmd={_fmt_cmd(cmd)}")
    t0 = time.monotonic()
    rc = subprocess.run([str(x) for x in cmd], cwd=str(ROOT)).returncode
    dt = time.monotonic() - t0
    print(f"[upwork-share] step={label} rc={rc} elapsed={dt:.1f}s")
    return int(rc)


def _safe(v: object) -> str:
    return str(v or "").strip()


def _chat_ref(row: Dict[str, str]) -> str:
    raw = _safe(row.get("chat_ref")) or _safe(row.get("chat_username")) or _safe(row.get("chat"))
    if raw.startswith("@"):
        return raw
    if raw.lower().startswith("https://t.me/") or raw.lower().startswith("http://t.me/"):
        core = raw.split("t.me/", 1)[1].split("?", 1)[0].split("#", 1)[0].strip("/")
        core = core.split("/", 1)[0].strip()
        return f"@{core}" if core else ""
    if raw and all(ch.isalnum() or ch == "_" for ch in raw):
        return f"@{raw}"
    return ""


def _to_float(v: object, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _to_bool_yes(v: object) -> bool:
    return _safe(v).lower() in {"yes", "true", "1", "y"}


def _write_csv(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})


def _build_tg_share_targets(
    *,
    scan_csv: Path,
    out_csv: Path,
    min_score: float,
    max_targets: int,
    require_remote: bool,
) -> int:
    if not scan_csv.exists():
        print(f"[upwork-share] missing telegram scan csv: {scan_csv}")
        return 0

    rows = list(csv.DictReader(scan_csv.open("r", encoding="utf-8-sig", newline="")))
    agg: Dict[str, Dict[str, object]] = defaultdict(
        lambda: {
            "chat_ref": "",
            "chat": "",
            "chat_username": "",
            "hits": 0,
            "pay_hits": 0,
            "remote_hits": 0,
            "scam_hits": 0,
            "max_score": 0.0,
            "score_sum": 0.0,
            "last_posted": "",
            "source": "telegram_scan_gigs",
        }
    )

    for r in rows:
        ref = _chat_ref(r)
        if not ref:
            continue
        score = _to_float(r.get("score"), 0.0)
        if score < float(min_score):
            continue
        pay = _to_bool_yes(r.get("pay_signal"))
        remote = _to_bool_yes(r.get("remote_signal"))
        scam = _to_bool_yes(r.get("scam_signal"))
        if require_remote and not remote:
            continue

        st = agg[ref.lower()]
        st["chat_ref"] = ref
        st["chat"] = _safe(r.get("chat")) or _safe(st["chat"])
        st["chat_username"] = _safe(r.get("chat_username")) or _safe(st["chat_username"])
        st["hits"] = int(st["hits"]) + 1
        st["pay_hits"] = int(st["pay_hits"]) + (1 if pay else 0)
        st["remote_hits"] = int(st["remote_hits"]) + (1 if remote else 0)
        st["scam_hits"] = int(st["scam_hits"]) + (1 if scam else 0)
        st["max_score"] = max(float(st["max_score"]), score)
        st["score_sum"] = float(st["score_sum"]) + score
        posted = _safe(r.get("posted_at"))
        if posted and posted > _safe(st["last_posted"]):
            st["last_posted"] = posted

    ranked: List[Dict[str, str]] = []
    for st in agg.values():
        hits = int(st["hits"])
        if hits <= 0:
            continue
        scam_hits = int(st["scam_hits"])
        if scam_hits > 0:
            continue
        max_score = float(st["max_score"])
        avg_score = float(st["score_sum"]) / max(1, hits)
        pay_hits = int(st["pay_hits"])
        remote_hits = int(st["remote_hits"])
        rank_score = round((2.2 * pay_hits) + (1.2 * remote_hits) + max_score + (0.35 * avg_score), 3)
        ranked.append(
            {
                "chat_ref": _safe(st["chat_ref"]),
                "chat": _safe(st["chat"]),
                "chat_username": _safe(st["chat_username"]),
                "hits": str(hits),
                "pay_hits": str(pay_hits),
                "remote_hits": str(remote_hits),
                "max_score": f"{max_score:.2f}",
                "avg_score": f"{avg_score:.2f}",
                "rank_score": f"{rank_score:.3f}",
                "last_posted": _safe(st["last_posted"]),
                "source": _safe(st["source"]),
            }
        )

    ranked.sort(
        key=lambda x: (
            _to_float(x.get("rank_score"), 0.0),
            _to_float(x.get("max_score"), 0.0),
            _to_float(x.get("hits"), 0.0),
        ),
        reverse=True,
    )
    if int(max_targets) > 0:
        ranked = ranked[: int(max_targets)]

    _write_csv(
        out_csv,
        [
            "chat_ref",
            "chat",
            "chat_username",
            "hits",
            "pay_hits",
            "remote_hits",
            "max_score",
            "avg_score",
            "rank_score",
            "last_posted",
            "source",
        ],
        ranked,
    )
    print(f"[upwork-share] telegram targets: {len(ranked)} -> {out_csv}")
    return len(ranked)


def _build_reddit_queue(*, reddit_csv: Path, out_csv: Path, min_score: float, max_rows: int) -> int:
    if not reddit_csv.exists():
        return 0
    rows = list(csv.DictReader(reddit_csv.open("r", encoding="utf-8-sig", newline="")))
    picked: List[Dict[str, str]] = []
    seen_url = set()
    for r in rows:
        score = _to_float(r.get("score"), 0.0)
        if score < float(min_score):
            continue
        if not _to_bool_yes(r.get("remote_signal")):
            continue
        url = _safe(r.get("url"))
        if not url or url in seen_url:
            continue
        seen_url.add(url)
        picked.append(
            {
                "subreddit": _safe(r.get("subreddit")),
                "title": _safe(r.get("title")),
                "url": url,
                "score": f"{score:.2f}",
                "posted_at": _safe(r.get("posted_at")),
                "action_hint": "Post a short public reply with Upwork link only if subreddit rules allow promo.",
            }
        )
        if int(max_rows) > 0 and len(picked) >= int(max_rows):
            break
    if not picked:
        return 0
    _write_csv(out_csv, ["subreddit", "title", "url", "score", "posted_at", "action_hint"], picked)
    print(f"[upwork-share] reddit queue: {len(picked)} -> {out_csv}")
    return len(picked)


def _cmd_tg_scan(py: str, args: argparse.Namespace, out_csv: Path) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "telegram_scan_gigs.py"),
        "--chats-file",
        str(args.chats_file),
        "--limit-per-chat",
        str(max(10, int(args.tg_limit_per_chat))),
        "--days",
        str(max(1, int(args.tg_days))),
        "--max-results",
        str(max(20, int(args.tg_max_results))),
        "--min-score",
        str(max(1, int(args.tg_min_score))),
        "--write-db",
        "--db",
        str(args.db),
        "--out-csv",
        str(out_csv),
        "--require-pay-signal",
        "--require-contact-signal",
    ]
    if args.tg_discover:
        cmd.extend(
            [
                "--discover",
                "--discover-queries-file",
                str(args.discover_queries_file),
                "--discover-limit-per-query",
                str(max(10, int(args.tg_discover_limit_per_query))),
                "--discover-source-limit",
                str(max(10, int(args.tg_discover_source_limit))),
                "--append-discovered-sources",
                "--append-sources-file",
                str(args.chats_file),
            ]
        )
        if args.tg_join_discovered:
            cmd.extend(
                [
                    "--join-discovered",
                    "--join-max",
                    str(max(1, int(args.tg_join_max))),
                    "--join-min-delay-sec",
                    str(max(8.0, float(args.tg_join_min_delay_sec))),
                    "--join-max-delay-sec",
                    str(max(12.0, float(args.tg_join_max_delay_sec))),
                ]
            )
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_public_share(py: str, args: argparse.Namespace, targets_csv: Path) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "telegram_public_share_batch.py"),
        "--config",
        str(args.config),
        "--db",
        str(args.db),
        "--csv",
        str(targets_csv),
        "--project-url",
        str(args.project_url),
        "--offer-title",
        str(args.offer_title),
        "--template",
        str(args.template),
        "--limit",
        str(max(1, int(args.share_limit))),
        "--daily-cap",
        str(max(1, int(args.share_daily_cap))),
        "--cooldown-days",
        str(max(1, int(args.share_cooldown_days))),
        "--min-delay-sec",
        str(max(10.0, float(args.share_min_delay_sec))),
        "--max-delay-sec",
        str(max(12.0, float(args.share_max_delay_sec))),
        "--long-break-every",
        str(max(1, int(args.share_long_break_every))),
        "--long-break-min-sec",
        str(max(30.0, float(args.share_long_break_min_sec))),
        "--long-break-max-sec",
        str(max(40.0, float(args.share_long_break_max_sec))),
    ]
    if args.dry_run:
        cmd.append("--dry-run")
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_reddit_scan(py: str, args: argparse.Namespace, out_csv: Path) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "reddit_scan_gigs.py"),
        "--subreddits",
        str(args.reddit_subreddits),
        "--queries",
        str(args.reddit_queries),
        "--limit-per-query",
        str(max(5, int(args.reddit_limit_per_query))),
        "--max-results",
        str(max(10, int(args.reddit_max_results))),
        "--days",
        str(max(1, int(args.reddit_days))),
        "--time-filter",
        str(args.reddit_time_filter),
        "--min-score",
        str(max(1, int(args.reddit_min_score))),
        "--require-pay-signal",
        "--write-db",
        "--db",
        str(args.db),
        "--out-csv",
        str(out_csv),
    ]
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _write_summary(
    *,
    path: Path,
    project_url: str,
    dry_run: bool,
    tg_scan_csv: Path,
    tg_targets_csv: Path,
    reddit_scan_csv: Path,
    reddit_queue_csv: Path,
    step_rc: Dict[str, int],
    tg_targets: int,
    reddit_rows: int,
) -> None:
    lines = [
        "Upwork Project Share Orchestrator",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"Project URL: {project_url}",
        f"Dry run: {int(bool(dry_run))}",
        "",
        "Steps:",
    ]
    for name in ("telegram_scan", "build_telegram_targets", "telegram_public_share", "reddit_scan", "build_reddit_queue"):
        if name in step_rc:
            lines.append(f"- {name}: rc={step_rc[name]}")
    lines.extend(
        [
            "",
            f"telegram_scan_csv: {tg_scan_csv}",
            f"telegram_targets_csv: {tg_targets_csv}",
            f"telegram_targets_count: {tg_targets}",
            f"reddit_scan_csv: {reddit_scan_csv}",
            f"reddit_queue_csv: {reddit_queue_csv}",
            f"reddit_queue_count: {reddit_rows}",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def _latest_file(dir_path: Path, patterns: Sequence[str]) -> Optional[Path]:
    candidates: List[Path] = []
    for pat in patterns:
        candidates.extend(list(dir_path.glob(pat)))
    if not candidates:
        return None
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def main() -> int:
    ap = argparse.ArgumentParser(
        description="End-to-end public sharing of an Upwork project link: Telegram scan -> target ranking -> public posting."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="data/out/activity.sqlite")
    ap.add_argument("--project-url", required=True)
    ap.add_argument("--offer-title", default="API QA smoke and regression tests with actionable findings")
    ap.add_argument("--template", default="templates/tg_public_share_upwork_en.txt")
    ap.add_argument("--prefix", default=f"upwork_share_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--telegram", action="store_true")

    ap.add_argument("--scan-telegram", dest="scan_telegram", action="store_true")
    ap.add_argument("--no-scan-telegram", dest="scan_telegram", action="store_false")
    ap.set_defaults(scan_telegram=True)
    ap.add_argument("--run-telegram-share", dest="run_telegram_share", action="store_true")
    ap.add_argument("--no-telegram-share", dest="run_telegram_share", action="store_false")
    ap.set_defaults(run_telegram_share=True)
    ap.add_argument("--telegram-scan-csv", default="", help="Use existing telegram scan CSV when --no-scan-telegram.")
    ap.add_argument("--chats-file", default="data/inbox/telegram_chats.txt")
    ap.add_argument("--tg-days", type=int, default=14)
    ap.add_argument("--tg-limit-per-chat", type=int, default=90)
    ap.add_argument("--tg-max-results", type=int, default=260)
    ap.add_argument("--tg-min-score", type=int, default=3)
    ap.add_argument("--tg-discover", action="store_true")
    ap.add_argument("--discover-queries-file", default="data/inbox/telegram_discovery_queries.txt")
    ap.add_argument("--tg-discover-limit-per-query", type=int, default=60)
    ap.add_argument("--tg-discover-source-limit", type=int, default=40)
    ap.add_argument("--tg-join-discovered", action="store_true")
    ap.add_argument("--tg-join-max", type=int, default=6)
    ap.add_argument("--tg-join-min-delay-sec", type=float, default=12.0)
    ap.add_argument("--tg-join-max-delay-sec", type=float, default=28.0)

    ap.add_argument("--target-min-score", type=float, default=4.0)
    ap.add_argument("--target-limit", type=int, default=25)
    ap.add_argument("--target-require-remote", action="store_true")

    ap.add_argument("--share-limit", type=int, default=8)
    ap.add_argument("--share-daily-cap", type=int, default=8)
    ap.add_argument("--share-cooldown-days", type=int, default=14)
    ap.add_argument("--share-min-delay-sec", type=float, default=28.0)
    ap.add_argument("--share-max-delay-sec", type=float, default=65.0)
    ap.add_argument("--share-long-break-every", type=int, default=3)
    ap.add_argument("--share-long-break-min-sec", type=float, default=120.0)
    ap.add_argument("--share-long-break-max-sec", type=float, default=260.0)

    ap.add_argument("--scan-reddit", action="store_true")
    ap.add_argument("--reddit-subreddits", default="forhire,freelance_forhire,testautomation,QualityAssurance")
    ap.add_argument(
        "--reddit-queries",
        default="qa automation freelance,api testing task,playwright bug fix,manual qa paid",
    )
    ap.add_argument("--reddit-limit-per-query", type=int, default=20)
    ap.add_argument("--reddit-max-results", type=int, default=60)
    ap.add_argument("--reddit-days", type=int, default=21)
    ap.add_argument("--reddit-time-filter", default="month", choices=["day", "week", "month", "year", "all"])
    ap.add_argument("--reddit-min-score", type=int, default=3)
    ap.add_argument("--reddit-queue-min-score", type=float, default=4.0)
    ap.add_argument("--reddit-queue-max-rows", type=int, default=40)
    args = ap.parse_args()

    py = _python_exe()
    out_dir = ROOT / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)

    tg_scan_csv = out_dir / f"{args.prefix}_telegram_scan.csv"
    tg_targets_csv = out_dir / f"{args.prefix}_telegram_targets.csv"
    reddit_scan_csv = out_dir / f"{args.prefix}_reddit_scan.csv"
    reddit_queue_csv = out_dir / f"{args.prefix}_reddit_public_queue.csv"
    summary_txt = out_dir / f"{args.prefix}_summary.txt"

    step_rc: Dict[str, int] = {}

    if args.run_telegram_share and args.scan_telegram:
        rc = _run_step("telegram_scan", _cmd_tg_scan(py, args, tg_scan_csv))
        step_rc["telegram_scan"] = rc
        if rc != 0:
            _write_summary(
                path=summary_txt,
                project_url=args.project_url,
                dry_run=bool(args.dry_run),
                tg_scan_csv=tg_scan_csv,
                tg_targets_csv=tg_targets_csv,
                reddit_scan_csv=reddit_scan_csv,
                reddit_queue_csv=reddit_queue_csv,
                step_rc=step_rc,
                tg_targets=0,
                reddit_rows=0,
            )
            print(f"[upwork-share] summary: {summary_txt}")
            return rc
    elif args.run_telegram_share:
        explicit_raw = _safe(args.telegram_scan_csv)
        if explicit_raw:
            explicit_scan_csv = Path(explicit_raw)
            if not explicit_scan_csv.is_absolute():
                explicit_scan_csv = ROOT / explicit_scan_csv
            if explicit_scan_csv.exists():
                tg_scan_csv = explicit_scan_csv
        if not tg_scan_csv.exists():
            fallback = _latest_file(out_dir, ["telegram_gigs_*.csv", "*_telegram_scan.csv"])
            if fallback is not None and fallback.exists():
                tg_scan_csv = fallback
                print(f"[upwork-share] using fallback telegram scan csv: {tg_scan_csv}")

    tg_targets = 0
    if args.run_telegram_share:
        tg_targets = _build_tg_share_targets(
            scan_csv=tg_scan_csv,
            out_csv=tg_targets_csv,
            min_score=float(args.target_min_score),
            max_targets=int(args.target_limit),
            require_remote=bool(args.target_require_remote),
        )
        step_rc["build_telegram_targets"] = 0

        if tg_targets > 0:
            rc = _run_step("telegram_public_share", _cmd_public_share(py, args, tg_targets_csv))
            step_rc["telegram_public_share"] = rc
        else:
            step_rc["telegram_public_share"] = 0
            print("[upwork-share] skip telegram_public_share: no ranked chat targets.")
    else:
        step_rc["build_telegram_targets"] = 0
        step_rc["telegram_public_share"] = 0
        print("[upwork-share] telegram sharing disabled by flag.")

    reddit_rows = 0
    if args.scan_reddit:
        rc = _run_step("reddit_scan", _cmd_reddit_scan(py, args, reddit_scan_csv))
        step_rc["reddit_scan"] = rc
        if rc == 0:
            reddit_rows = _build_reddit_queue(
                reddit_csv=reddit_scan_csv,
                out_csv=reddit_queue_csv,
                min_score=float(args.reddit_queue_min_score),
                max_rows=int(args.reddit_queue_max_rows),
            )
            step_rc["build_reddit_queue"] = 0
        else:
            step_rc["build_reddit_queue"] = rc

    _write_summary(
        path=summary_txt,
        project_url=args.project_url,
        dry_run=bool(args.dry_run),
        tg_scan_csv=tg_scan_csv,
        tg_targets_csv=tg_targets_csv,
        reddit_scan_csv=reddit_scan_csv,
        reddit_queue_csv=reddit_queue_csv,
        step_rc=step_rc,
        tg_targets=tg_targets,
        reddit_rows=reddit_rows,
    )
    print(f"[upwork-share] summary: {summary_txt}")

    for key in ("telegram_public_share", "reddit_scan", "build_reddit_queue"):
        if key in step_rc and int(step_rc[key]) != 0:
            return int(step_rc[key])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
