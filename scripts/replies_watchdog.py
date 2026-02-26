import argparse
import csv
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except Exception:
    pass

from src.activity_db import connect as db_connect, init_db  # noqa: E402
from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import bool_env  # noqa: E402
from src.telegram_notify import send_telegram_message  # noqa: E402

APPLY_REPLY_MAP = {
    "fm_apply_submitted": "fm_reply_received",
    "wa_apply_submitted": "wa_reply_received",
    "li_apply_submitted": "li_reply_received",
    "external_apply_submitted": "external_reply_received",
}


def _now_stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _text(v: Any) -> str:
    return str(v or "").strip()


def _run_subprocess(cmd: List[str], *, cwd: Path) -> int:
    try:
        p = subprocess.run(cmd, cwd=str(cwd), check=False)
        return int(p.returncode or 0)
    except Exception:
        return 1


def _mailbox_slug(mailbox: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", _text(mailbox).lower()).strip("_")
    return slug or "inbox"


def _latest_email_inbox_csv(out_dir: Path, *, mailbox: str, days: int) -> Optional[Path]:
    pat = f"inbox_messages_{_mailbox_slug(mailbox)}_{days}d_*.csv"
    files = sorted(out_dir.glob(pat), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _email_stats_from_csv(path: Path) -> Dict[str, int]:
    counts = Counter()
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            counts[_text(row.get("msg_type")).lower()] += 1
    return dict(counts)


def _collect_apply_statuses(conn) -> List[Dict[str, str]]:
    apply_events = tuple(APPLY_REPLY_MAP.keys())
    placeholders = ",".join(["?"] * len(apply_events))
    rows = conn.execute(
        f"""
        SELECT
            e.lead_id,
            e.event_type AS apply_event,
            MIN(e.occurred_at) AS applied_at,
            l.platform,
            l.company,
            l.job_title,
            l.url
        FROM events e
        JOIN leads l ON l.lead_id = e.lead_id
        WHERE e.event_type IN ({placeholders})
        GROUP BY e.lead_id, e.event_type, l.platform, l.company, l.job_title, l.url
        ORDER BY applied_at DESC
        """,
        apply_events,
    ).fetchall()

    out: List[Dict[str, str]] = []
    for r in rows:
        lead_id = _text(r["lead_id"])
        apply_event = _text(r["apply_event"])
        reply_event = APPLY_REPLY_MAP.get(apply_event, "")
        replied_at = ""
        if reply_event:
            rr = conn.execute(
                """
                SELECT MAX(occurred_at) AS replied_at
                FROM events
                WHERE lead_id = ? AND event_type = ?
                """,
                (lead_id, reply_event),
            ).fetchone()
            replied_at = _text(rr["replied_at"]) if rr else ""

        out.append(
            {
                "lead_id": lead_id,
                "platform": _text(r["platform"]),
                "company": _text(r["company"]),
                "job_title": _text(r["job_title"]),
                "url": _text(r["url"]),
                "apply_event": apply_event,
                "reply_event": reply_event,
                "applied_at": _text(r["applied_at"]),
                "replied_at": replied_at,
                "status": "replied" if replied_at else "pending",
            }
        )
    return out


def _write_status_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "lead_id",
        "platform",
        "company",
        "job_title",
        "url",
        "apply_event",
        "reply_event",
        "applied_at",
        "replied_at",
        "status",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_summary_txt(path: Path, *, rows: List[Dict[str, str]], email_stats: Optional[Dict[str, int]]) -> str:
    total = len(rows)
    replied = sum(1 for r in rows if r["status"] == "replied")
    pending = total - replied

    by_platform = defaultdict(lambda: {"total": 0, "replied": 0, "pending": 0})
    for r in rows:
        p = r["platform"] or "unknown"
        by_platform[p]["total"] += 1
        by_platform[p][r["status"]] += 1

    lines: List[str] = []
    lines.append("Replies watchdog")
    lines.append(f"Generated at: {datetime.now().isoformat(timespec='seconds')}")
    lines.append("")
    lines.append(f"Applied leads tracked: {total}")
    lines.append(f"Replied: {replied}")
    lines.append(f"Pending: {pending}")
    lines.append("")
    lines.append("By platform:")
    for p in sorted(by_platform.keys()):
        st = by_platform[p]
        lines.append(f"- {p}: total={st['total']} replied={st['replied']} pending={st['pending']}")

    if email_stats is not None:
        lines.append("")
        lines.append("Email inbox stats (latest run):")
        lines.append(f"- reply: {int(email_stats.get('reply', 0))}")
        lines.append(f"- auto_reply: {int(email_stats.get('auto_reply', 0))}")
        lines.append(f"- bounce: {int(email_stats.get('bounce', 0))}")

    top_pending = [r for r in rows if r["status"] == "pending"][:15]
    if top_pending:
        lines.append("")
        lines.append("Pending (top 15):")
        for r in top_pending:
            lines.append(f"- {r['platform']} | {r['company']} | {r['job_title']} | applied_at={r['applied_at']}")

    text = "\n".join(lines)
    path.write_text(text, encoding="utf-8")
    return text


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Aggregate reply status across apply channels and optionally sync source inboxes."
    )
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--db", default="", help="Override DB path")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--sync-freelancermap", action="store_true", help="Run freelancermap_inbox_sync before report")
    ap.add_argument("--freelancermap-limit", type=int, default=120)
    ap.add_argument("--scan-email", action="store_true", help="Run inbox_analytics before report")
    ap.add_argument("--email-mailbox", default="INBOX")
    ap.add_argument("--email-days", type=int, default=14)
    ap.add_argument("--email-max", type=int, default=500)
    ap.add_argument("--telegram", action="store_true", help="Send summary to Telegram")
    args = ap.parse_args()

    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _text(args.db):
        db_path = resolve_path(ROOT, _text(args.db))
    out_dir = resolve_path(ROOT, args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.sync_freelancermap:
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "freelancermap_inbox_sync.py"),
            "--db",
            str(db_path),
            "--out-dir",
            str(out_dir),
            "--limit",
            str(int(args.freelancermap_limit)),
        ]
        rc = _run_subprocess(cmd, cwd=ROOT)
        print(f"[watchdog] freelancermap inbox sync rc={rc}")

    email_stats: Optional[Dict[str, int]] = None
    if args.scan_email:
        cmd = [
            sys.executable,
            str(ROOT / "scripts" / "inbox_analytics.py"),
            "--mailbox",
            _text(args.email_mailbox) or "INBOX",
            "--days",
            str(int(args.email_days)),
            "--max",
            str(int(args.email_max)),
        ]
        rc = _run_subprocess(cmd, cwd=ROOT)
        print(f"[watchdog] inbox analytics rc={rc}")
        latest = _latest_email_inbox_csv(out_dir, mailbox=args.email_mailbox, days=int(args.email_days))
        if latest and latest.exists():
            try:
                email_stats = _email_stats_from_csv(latest)
                print(f"[watchdog] email csv: {latest}")
            except Exception:
                email_stats = None

    conn = None
    try:
        conn = db_connect(db_path)
        init_db(conn)
        rows = _collect_apply_statuses(conn)

        stamp = _now_stamp()
        out_csv = out_dir / f"replies_watchdog_{stamp}.csv"
        out_txt = out_dir / f"replies_watchdog_{stamp}.txt"
        _write_status_csv(out_csv, rows)
        text = _write_summary_txt(out_txt, rows=rows, email_stats=email_stats)

        total = len(rows)
        replied = sum(1 for r in rows if r["status"] == "replied")
        pending = total - replied
        print(f"[watchdog] tracked={total} replied={replied} pending={pending}")
        print(f"[watchdog] csv: {out_csv}")
        print(f"[watchdog] txt: {out_txt}")

        if args.telegram and bool_env("TELEGRAM_REPORT", True):
            msg = "\n".join(
                [
                    "AIJobSearcher: replies watchdog",
                    f"Tracked applies: {total}",
                    f"Replied: {replied}",
                    f"Pending: {pending}",
                    f"TXT: {out_txt}",
                ]
            )
            send_telegram_message(msg)

        return 0
    except KeyboardInterrupt:
        print("[watchdog] interrupted")
        return 130
    except Exception as e:
        print(f"[watchdog] error: {e}")
        return 1
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
