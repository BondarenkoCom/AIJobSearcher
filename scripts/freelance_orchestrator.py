import argparse
import shlex
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Sequence, Tuple


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
    print(f"[freelance-orch] step={label}")
    print(f"[freelance-orch] cmd={_fmt_cmd(cmd)}")
    t0 = time.monotonic()
    rc = subprocess.run([str(x) for x in cmd], cwd=str(ROOT)).returncode
    dt = time.monotonic() - t0
    print(f"[freelance-orch] step={label} rc={rc} elapsed={dt:.1f}s")
    return int(rc)


def _dedupe_queries(raw: str) -> List[str]:
    seen = set()
    out: List[str] = []
    for part in str(raw or "").split(","):
        q = part.strip()
        if not q:
            continue
        k = q.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(q)
    return out


def _append_common_args(cmd: List[str], *, config: str, db: str) -> None:
    if str(config).strip():
        cmd.extend(["--config", str(config).strip()])
    if str(db).strip():
        cmd.extend(["--db", str(db).strip()])


def _cmd_fm_scan(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "freelancermap_scan_projects.py"),
        "--limit",
        str(max(1, int(args.fm_scan_limit))),
        "--min-score",
        str(int(args.fm_scan_min_score)),
        "--write-db",
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.require_remote:
        cmd.append("--require-remote")
        if args.include_hybrid:
            cmd.append("--include-hybrid")
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_workana_scan(py: str, args: argparse.Namespace, query: str) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "workana_scan_projects.py"),
        "--query",
        str(query),
        "--pages",
        str(max(1, int(args.workana_pages))),
        "--limit",
        str(max(1, int(args.workana_scan_limit))),
        "--min-score",
        str(int(args.workana_scan_min_score)),
        "--sleep-sec",
        str(max(0.0, float(args.workana_sleep_sec))),
        "--write-db",
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.require_remote:
        cmd.append("--require-remote")
        if args.include_hybrid:
            cmd.append("--include-hybrid")
    if args.strict_qa_title_scan:
        cmd.append("--strict-qa-title")
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def _cmd_fm_apply(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "freelancermap_apply_batch.py"),
        "--limit",
        str(max(1, int(args.fm_apply_limit))),
        "--min-score",
        str(int(args.fm_apply_min_score)),
        "--remote-modes",
        str(args.remote_modes),
        "--engagements",
        str(args.engagements),
        "--step-timeout-ms",
        str(max(5_000, int(args.step_timeout_ms))),
        "--min-delay-ms",
        str(max(0, int(args.min_delay_ms))),
        "--max-delay-ms",
        str(max(0, int(args.max_delay_ms))),
        "--long-break-every",
        str(max(0, int(args.long_break_every))),
        "--long-break-min-ms",
        str(max(0, int(args.long_break_min_ms))),
        "--long-break-max-ms",
        str(max(0, int(args.long_break_max_ms))),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.strict_language:
        cmd.append("--strict-language")
    if not args.submit:
        cmd.append("--no-submit")
    return cmd


def _cmd_workana_apply(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "workana_apply_batch.py"),
        "--limit",
        str(max(1, int(args.workana_apply_limit))),
        "--min-score",
        str(int(args.workana_apply_min_score)),
        "--remote-modes",
        str(args.remote_modes),
        "--engagements",
        str(args.engagements),
        "--step-timeout-ms",
        str(max(5_000, int(args.step_timeout_ms))),
        "--min-delay-ms",
        str(max(0, int(args.min_delay_ms))),
        "--max-delay-ms",
        str(max(0, int(args.max_delay_ms))),
        "--long-break-every",
        str(max(0, int(args.long_break_every))),
        "--long-break-min-ms",
        str(max(0, int(args.long_break_min_ms))),
        "--long-break-max-ms",
        str(max(0, int(args.long_break_max_ms))),
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if not args.submit:
        cmd.append("--no-submit")
    return cmd


def _cmd_watchdog(py: str, args: argparse.Namespace) -> List[str]:
    cmd = [
        py,
        str(ROOT / "scripts" / "replies_watchdog.py"),
        "--sync-freelancermap",
    ]
    _append_common_args(cmd, config=args.config, db=args.db)
    if args.scan_email:
        cmd.append("--scan-email")
    if args.telegram:
        cmd.append("--telegram")
    return cmd


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Unified freelance pipeline (Freelancermap + Workana): "
            "scan via HTTP/public data, then apply via Playwright with human pacing."
        )
    )
    ap.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--scan-only", action="store_true", help="Run only collection steps")
    ap.add_argument("--apply-only", action="store_true", help="Run only apply steps")
    ap.add_argument("--skip-watchdog", action="store_true", help="Skip replies_watchdog at the end")
    ap.add_argument("--continue-on-error", action="store_true", help="Continue next steps even if one step fails")
    ap.add_argument("--telegram", action="store_true", help="Forward scanner/watchdog summaries to Telegram")
    ap.add_argument("--scan-email", action="store_true", help="Include email inbox analytics in watchdog")
    ap.add_argument("--submit", action="store_true", help="Actually submit applications (default is no-submit)")

    ap.add_argument("--require-remote", action="store_true", help="Keep remote-only in scans")
    ap.add_argument("--include-hybrid", action="store_true", help="When require-remote, include hybrid too")
    ap.add_argument("--strict-qa-title-scan", action="store_true", help="Workana scan requires QA keyword in title")
    ap.add_argument("--strict-language", action="store_true", help="Freelancermap apply hard language filter")
    ap.add_argument("--remote-modes", default="remote,hybrid", help="Apply filter, e.g. remote,hybrid")
    ap.add_argument("--engagements", default="long,gig", help="Apply filter, e.g. long,gig")

    ap.add_argument("--workana-queries", default="qa,qa automation,sdet,test automation,api testing")
    ap.add_argument("--workana-pages", type=int, default=4)
    ap.add_argument("--workana-sleep-sec", type=float, default=1.2)
    ap.add_argument("--workana-scan-limit", type=int, default=120)
    ap.add_argument("--workana-scan-min-score", type=int, default=7)
    ap.add_argument("--workana-apply-limit", type=int, default=8)
    ap.add_argument("--workana-apply-min-score", type=int, default=9)

    ap.add_argument("--fm-scan-limit", type=int, default=140)
    ap.add_argument("--fm-scan-min-score", type=int, default=8)
    ap.add_argument("--fm-apply-limit", type=int, default=8)
    ap.add_argument("--fm-apply-min-score", type=int, default=10)

    ap.add_argument("--step-timeout-ms", type=int, default=35_000)
    ap.add_argument("--min-delay-ms", type=int, default=22_000)
    ap.add_argument("--max-delay-ms", type=int, default=70_000)
    ap.add_argument("--long-break-every", type=int, default=4)
    ap.add_argument("--long-break-min-ms", type=int, default=90_000)
    ap.add_argument("--long-break-max-ms", type=int, default=180_000)
    args = ap.parse_args()

    if args.scan_only and args.apply_only:
        print("[freelance-orch] error: cannot use --scan-only and --apply-only together.")
        return 2

    py = _python_exe()
    print("[freelance-orch] strategy:")
    print("- scan: HTTP/public sources first (no browser unless needed)")
    print("- apply: Playwright only for authenticated/bid actions")
    print("- dedupe + pacing enforced by platform apply scripts")
    print(f"- submit mode: {'ON' if args.submit else 'OFF (no-submit)'}")

    failures: List[Tuple[str, int]] = []

    def run_or_stop(label: str, cmd: Sequence[str]) -> bool:
        rc = _run_step(label, cmd)
        if rc == 0:
            return True
        failures.append((label, rc))
        if args.continue_on_error:
            return True
        return False

    do_scan = not args.apply_only
    do_apply = not args.scan_only

    if do_scan:
        if not run_or_stop("freelancermap_scan", _cmd_fm_scan(py, args)):
            return 1
        queries = _dedupe_queries(args.workana_queries)
        for q in queries:
            if not run_or_stop(f"workana_scan[{q}]", _cmd_workana_scan(py, args, q)):
                return 1

    if do_apply:
        if not run_or_stop("freelancermap_apply", _cmd_fm_apply(py, args)):
            return 1
        if not run_or_stop("workana_apply", _cmd_workana_apply(py, args)):
            return 1

    if not args.skip_watchdog:
        if not run_or_stop("replies_watchdog", _cmd_watchdog(py, args)):
            return 1

    if failures:
        print("[freelance-orch] completed with failures:")
        for label, rc in failures:
            print(f"- {label}: rc={rc}")
        return 1

    print("[freelance-orch] completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
