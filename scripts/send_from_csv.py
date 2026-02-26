import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import load_config, resolve_path  # noqa: E402
from src.email_jobs import send_applications  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Send application emails from a CSV source using config/email template.")
    ap.add_argument("--config", default="config/config.yaml", help="Path to config YAML")
    ap.add_argument("--csv", required=True, help="CSV path with columns: title,company,location,url,description,contact_email,contact_name,source")
    ap.add_argument("--dry-run", action="store_true", help="Do not send, only print what would be sent")
    ap.add_argument("--run-limit", type=int, default=0, help="Override email.rate_limit.run_limit for this run (0 = keep config)")
    ap.add_argument("--template", default="", help="Override email.template for this run (path to .txt)")
    ap.add_argument("--subject", default="", help="Override email.subject for this run (format string)")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    csv_path = resolve_path(ROOT, args.csv)
    if not csv_path.exists():
        print(f"[send-csv] missing: {csv_path}")
        return 2

    cfg.setdefault("email", {})
    cfg["email"]["csv_source"] = str(csv_path)
    if args.dry_run:
        cfg["email"]["dry_run"] = True
    if args.template.strip():
        cfg["email"]["template"] = str(resolve_path(ROOT, args.template.strip()))
    if args.subject.strip():
        cfg["email"]["subject"] = str(args.subject.strip())
    if int(args.run_limit) > 0:
        cfg.setdefault("email", {}).setdefault("rate_limit", {})
        cfg["email"]["rate_limit"]["run_limit"] = int(args.run_limit)

    sent = send_applications(ROOT, cfg)
    print(f"[send-csv] sent={sent} source={csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
