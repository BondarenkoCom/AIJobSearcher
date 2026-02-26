import argparse
import shlex
import subprocess
import sys
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

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
    print(f"[gig-multibrain] step={label}")
    print(f"[gig-multibrain] cmd={_fmt_cmd(cmd)}")
    t0 = time.monotonic()
    rc = subprocess.run([str(x) for x in cmd], cwd=str(ROOT)).returncode
    dt = time.monotonic() - t0
    print(f"[gig-multibrain] step={label} rc={rc} elapsed={dt:.1f}s")
    return int(rc)


def _dedupe_queries(raw: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for part in str(raw or "").split(","):
        q = part.strip()
        if not q:
            continue
        key = q.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(q)
    return out


def _month_shift(base: date, delta_months: int) -> str:
    y = int(base.year)
    m = int(base.month) - int(delta_months)
    while m <= 0:
        y -= 1
        m += 12
    return f"{y:04d}-{m:02d}"


def _recent_months(count: int) -> List[str]:
    n = max(1, int(count))
    today = date.today()
    return [_month_shift(today, i) for i in range(n)]


def _append_common_args(cmd: List[str], *, config: str, db: str) -> None:
    if str(config).strip():
        cmd.extend(["--config", str(config).strip()])
    if str(db).strip():
        cmd.extend(["--db", str(db).strip()])


def _cmd_scan_web(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "web_scan_contract_jobs.py"),
        "--contracts-only",
        "--limit",
        str(max(1, int(args.web_limit))),
        "--min-score",
        str(int(args.web_min_score)),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    return cmd


def _cmd_scan_hn(py: str, args: argparse.Namespace, month: str) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "hn_scan_whoishiring.py"),
        "--month",
        str(month),
        "--contracts-only",
        "--limit",
        str(max(1, int(args.hn_limit))),
        "--min-score",
        str(int(args.hn_min_score)),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    return cmd


def _cmd_scan_linkedin_posts(py: str, args: argparse.Namespace, query: str) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "linkedin_scan_posts.py"),
        "--write-db",
        "--query",
        str(query),
        "--sort",
        str(args.linkedin_sort),
        "--limit",
        str(max(1, int(args.linkedin_limit))),
        "--scrolls",
        str(max(1, int(args.linkedin_scrolls))),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    return cmd


def _cmd_scan_fm(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "freelancermap_scan_projects.py"),
        "--write-db",
        "--limit",
        str(max(1, int(args.fm_limit))),
        "--min-score",
        str(int(args.fm_min_score)),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.require_remote:
        cmd.append("--require-remote")
        if args.include_hybrid:
            cmd.append("--include-hybrid")
    return cmd


def _cmd_scan_reddit(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "reddit_scan_gigs.py"),
        "--subreddits",
        str(args.reddit_subreddits),
        "--queries",
        str(args.reddit_queries),
        "--limit-per-query",
        str(max(1, int(args.reddit_limit_per_query))),
        "--max-results",
        str(max(1, int(args.reddit_max_results))),
        "--days",
        str(max(1, int(args.reddit_days))),
        "--time-filter",
        str(args.reddit_time_filter),
        "--min-score",
        str(int(args.reddit_min_score)),
        "--write-db",
    ]
    if args.reddit_require_pay_signal:
        cmd.append("--require-pay-signal")
    if args.reddit_require_contact_signal:
        cmd.append("--require-contact-signal")
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_select(py: str, args: argparse.Namespace, *, out_csv: Path, out_json: Path) -> List[str]:
    platforms_raw = str(args.platforms).strip()
    if args.include_workana:
        parts = [p.strip() for p in platforms_raw.split(",") if p.strip()]
        if "workana.com" not in {p.lower() for p in parts}:
            parts.append("workana.com")
        platforms_raw = ",".join(parts)

    lead_types_raw = str(args.lead_types).strip()
    if args.include_job_leads:
        lt = [x.strip() for x in lead_types_raw.split(",") if x.strip()]
        if "job" not in {x.lower() for x in lt}:
            lt.append("job")
        lead_types_raw = ",".join(lt)

    cmd = [
        py,
        str(ROOT / "scripts" / "gig_hunt_select.py"),
        "--platforms",
        platforms_raw,
        "--lead-types",
        lead_types_raw,
        "--candidate-limit",
        str(max(20, int(args.candidate_limit))),
        "--limit",
        str(max(1, int(args.limit))),
        "--min-heuristic",
        str(max(0.0, float(args.min_heuristic))),
        "--council-timeout-sec",
        str(max(10.0, float(args.council_timeout_sec))),
        "--out-csv",
        str(out_csv),
        "--out-json",
        str(out_json),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.no_council:
        cmd.append("--no-council")
    if args.strict_oneoff:
        cmd.append("--strict-oneoff")
    if args.prefer_posts:
        cmd.append("--prefer-posts")
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_route(py: str, args: argparse.Namespace, *, in_csv: Path, prefix: str) -> Tuple[List[str], Path, Path, Path, Path, Path]:
    email_out = ROOT / "data" / "out" / f"{prefix}_email_targets.csv"
    tg_out = ROOT / "data" / "out" / f"{prefix}_telegram_targets.csv"
    li_apply_out = ROOT / "data" / "out" / f"{prefix}_linkedin_apply_urls.txt"
    li_out = ROOT / "data" / "out" / f"{prefix}_linkedin_targets.csv"
    manual_out = ROOT / "data" / "out" / f"{prefix}_manual_targets.csv"
    cmd = [
        py,
        str(ROOT / "scripts" / "route_outreach_from_gigs.py"),
        "--in-csv",
        str(in_csv),
        "--email-out",
        str(email_out),
        "--tg-out",
        str(tg_out),
        "--li-apply-out",
        str(li_apply_out),
        "--li-out",
        str(li_out),
        "--manual-out",
        str(manual_out),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    return cmd, email_out, tg_out, li_apply_out, li_out, manual_out


def _write_summary(
    *,
    out_path: Path,
    prefix: str,
    failures: List[Tuple[str, int]],
    selection_csv: Path,
    selection_json: Path,
    routed_email: Path,
    routed_tg: Path,
    routed_li_apply: Path,
    routed_li: Path,
    routed_manual: Path,
    scans_enabled: Dict[str, bool],
    strict_oneoff: bool,
    prefer_posts: bool,
    min_heuristic: float,
) -> None:
    lines: List[str] = []
    lines.append("Gig multibrain orchestrator report")
    lines.append(f"prefix: {prefix}")
    lines.append("")
    lines.append("brains:")
    lines.append("- Arbiter: deterministic QA/gig/remote filters in gig_hunt_select.py")
    lines.append("- GPT: OpenAI council score")
    lines.append("- Grok: xAI council score")
    lines.append(
        f"- shortlist policy: strict_oneoff={int(bool(strict_oneoff))}, "
        f"prefer_posts={int(bool(prefer_posts))}, min_heuristic={float(min_heuristic):.2f}"
    )
    lines.append("")
    lines.append("scans:")
    for k in ("linkedin_posts", "hn", "web_jobs", "freelancermap", "reddit"):
        lines.append(f"- {k}: {'on' if scans_enabled.get(k) else 'off'}")
    lines.append("")
    lines.append("outputs:")
    lines.append(f"- shortlist_csv: {selection_csv}")
    lines.append(f"- shortlist_json: {selection_json}")
    lines.append(f"- route_email_csv: {routed_email}")
    lines.append(f"- route_telegram_csv: {routed_tg}")
    lines.append(f"- route_linkedin_apply_urls: {routed_li_apply}")
    lines.append(f"- route_linkedin_csv: {routed_li}")
    lines.append(f"- route_manual_csv: {routed_manual}")
    lines.append("")
    if failures:
        lines.append("failures:")
        for label, rc in failures:
            lines.append(f"- {label}: rc={rc}")
    else:
        lines.append("failures: none")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Three-brain gig hunter: optional scans (posts/APIs/public pages) + "
            "multibrain ranking (heuristic + GPT + Grok) + outreach routing."
        )
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="")

    ap.add_argument("--scan-only", action="store_true", help="Run scanning steps only")
    ap.add_argument("--select-only", action="store_true", help="Skip scans and run shortlist + routing only")
    ap.add_argument("--stop-on-error", action="store_true", help="Stop pipeline on first non-zero step")
    ap.add_argument("--telegram", action="store_true", help="Forward shortlist summary via Telegram")

    ap.add_argument(
        "--prefix",
        default=f"gig_multibrain_{time.strftime('%Y%m%d_%H%M%S')}",
        help="Output file prefix in data/out",
    )

    ap.add_argument("--scan-linkedin-posts", action="store_true", help="Run LinkedIn post scans before selection")
    ap.add_argument(
        "--linkedin-queries",
        default=(
            "urgent qa freelancer remote paid,"
            "need qa automation help paid,"
            "looking for tester freelance remote,"
            "playwright bug fix paid"
        ),
    )
    ap.add_argument("--linkedin-sort", default="latest", choices=["top", "latest"])
    ap.add_argument("--linkedin-limit", type=int, default=40)
    ap.add_argument("--linkedin-scrolls", type=int, default=12)

    ap.add_argument("--scan-hn", action="store_true", help="Run HN Who-Is-Hiring scans before selection")
    ap.add_argument("--hn-months", type=int, default=2, help="How many recent HN months to scan")
    ap.add_argument("--hn-limit", type=int, default=200)
    ap.add_argument("--hn-min-score", type=int, default=2)

    ap.add_argument("--scan-web", action="store_true", help="Run web contract boards scan before selection")
    ap.add_argument("--web-limit", type=int, default=250)
    ap.add_argument("--web-min-score", type=int, default=2)

    ap.add_argument("--scan-freelancermap", action="store_true", help="Run Freelancermap public scan before selection")
    ap.add_argument("--fm-limit", type=int, default=120)
    ap.add_argument("--fm-min-score", type=int, default=8)
    ap.add_argument("--require-remote", action="store_true")
    ap.add_argument("--include-hybrid", action="store_true")

    ap.add_argument("--scan-reddit", action="store_true", help="Run Reddit gigs scan before selection")
    ap.add_argument("--reddit-subreddits", default="forhire,freelance_forhire,forhireuk,slavelabour,testautomation,QualityAssurance")
    ap.add_argument(
        "--reddit-queries",
        default="qa automation freelance,test automation gig,playwright bug fix,api testing task,selenium fix paid",
    )
    ap.add_argument("--reddit-limit-per-query", type=int, default=25)
    ap.add_argument("--reddit-max-results", type=int, default=50)
    ap.add_argument("--reddit-days", type=int, default=30)
    ap.add_argument("--reddit-time-filter", default="month", choices=["day", "week", "month", "year", "all"])
    ap.add_argument("--reddit-min-score", type=int, default=3)
    ap.add_argument("--reddit-require-pay-signal", action="store_true")
    ap.add_argument("--reddit-require-contact-signal", action="store_true")

    ap.add_argument(
        "--platforms",
        default="telegram,reddit,hn,job_board,freelancermap.com,linkedin",
        help="Platforms from leads.platform to rank (comma-separated)",
    )
    ap.add_argument(
        "--lead-types",
        default="post,project",
        help="Lead types to rank (default post+project for gig focus).",
    )
    ap.add_argument("--include-workana", action="store_true", help="Include workana.com in ranking set")
    ap.add_argument("--include-job-leads", action="store_true", help="Also include lead_type=job in ranking.")
    ap.add_argument("--candidate-limit", type=int, default=180)
    ap.add_argument("--limit", type=int, default=10)
    ap.add_argument("--min-heuristic", type=float, default=4.8)
    ap.add_argument("--strict-oneoff", dest="strict_oneoff", action="store_true")
    ap.add_argument("--no-strict-oneoff", dest="strict_oneoff", action="store_false")
    ap.add_argument("--prefer-posts", dest="prefer_posts", action="store_true")
    ap.add_argument("--no-prefer-posts", dest="prefer_posts", action="store_false")
    ap.add_argument("--council-timeout-sec", type=float, default=70.0)
    ap.add_argument("--no-council", action="store_true", help="Disable GPT/Grok scoring, use arbiter only")

    ap.add_argument("--skip-route", action="store_true", help="Do not build email/linkedin/manual route CSVs")
    ap.set_defaults(strict_oneoff=True, prefer_posts=True)
    args = ap.parse_args()

    if args.scan_only and args.select_only:
        print("[gig-multibrain] error: cannot use --scan-only and --select-only together.")
        return 2

    py = _python_exe()
    out_dir = ROOT / "data" / "out"
    out_dir.mkdir(parents=True, exist_ok=True)
    prefix = str(args.prefix).strip() or f"gig_multibrain_{time.strftime('%Y%m%d_%H%M%S')}"

    selection_csv = out_dir / f"{prefix}_top.csv"
    selection_json = out_dir / f"{prefix}_top.json"
    routed_email = out_dir / f"{prefix}_email_targets.csv"
    routed_tg = out_dir / f"{prefix}_telegram_targets.csv"
    routed_li_apply = out_dir / f"{prefix}_linkedin_apply_urls.txt"
    routed_li = out_dir / f"{prefix}_linkedin_targets.csv"
    routed_manual = out_dir / f"{prefix}_manual_targets.csv"
    report_txt = out_dir / f"{prefix}_report.txt"

    failures: List[Tuple[str, int]] = []

    def run_or_continue(label: str, cmd: Sequence[str]) -> bool:
        rc = _run_step(label, cmd)
        if rc == 0:
            return True
        failures.append((label, rc))
        return not bool(args.stop_on_error)

    do_scan = not bool(args.select_only)
    do_select = not bool(args.scan_only)

    scans_enabled: Dict[str, bool] = {
        "linkedin_posts": bool(args.scan_linkedin_posts),
        "hn": bool(args.scan_hn),
        "web_jobs": bool(args.scan_web),
        "freelancermap": bool(args.scan_freelancermap),
        "reddit": bool(args.scan_reddit),
    }

    print("[gig-multibrain] strategy:")
    print("- stage 1: optional scans from public sources/posts")
    print("- stage 2: shortlist by 3 brains (arbiter + GPT + Grok)")
    print("- stage 3: route outreach (email -> Telegram DM -> LinkedIn Apply URLs -> manual)")
    print(f"- output prefix: {prefix}")

    if do_scan:
        if args.scan_web:
            if not run_or_continue("scan_web_contracts", _cmd_scan_web(py, args)):
                return 1

        if args.scan_hn:
            for m in _recent_months(int(args.hn_months)):
                if not run_or_continue(f"scan_hn[{m}]", _cmd_scan_hn(py, args, month=m)):
                    return 1

        if args.scan_freelancermap:
            if not run_or_continue("scan_freelancermap", _cmd_scan_fm(py, args)):
                return 1

        if args.scan_reddit:
            if not run_or_continue("scan_reddit_gigs", _cmd_scan_reddit(py, args)):
                return 1

        if args.scan_linkedin_posts:
            queries = _dedupe_queries(args.linkedin_queries)
            for q in queries:
                if not run_or_continue(f"scan_linkedin_posts[{q}]", _cmd_scan_linkedin_posts(py, args, query=q)):
                    return 1

    if do_select:
        if not run_or_continue("select_multibrain_gigs", _cmd_select(py, args, out_csv=selection_csv, out_json=selection_json)):
            return 1

        if not args.skip_route:
            route_cmd, routed_email, routed_tg, routed_li_apply, routed_li, routed_manual = _cmd_route(
                py,
                args,
                in_csv=selection_csv,
                prefix=prefix,
            )
            if not run_or_continue("route_outreach", route_cmd):
                return 1

    _write_summary(
        out_path=report_txt,
        prefix=prefix,
        failures=failures,
        selection_csv=selection_csv,
        selection_json=selection_json,
        routed_email=routed_email,
        routed_tg=routed_tg,
        routed_li_apply=routed_li_apply,
        routed_li=routed_li,
        routed_manual=routed_manual,
        scans_enabled=scans_enabled,
        strict_oneoff=bool(args.strict_oneoff),
        prefer_posts=bool(args.prefer_posts),
        min_heuristic=float(args.min_heuristic),
    )

    print(f"[gig-multibrain] report: {report_txt}")
    if failures:
        print("[gig-multibrain] completed with failures:")
        for label, rc in failures:
            print(f"- {label}: rc={rc}")
        return 1
    print("[gig-multibrain] completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
