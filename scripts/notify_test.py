import argparse
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import load_config, resolve_path  # noqa: E402
from src.notify import notify  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser(description="Play notify sounds (done/attention/timeout/error) using config.yaml")
    ap.add_argument("--config", default="config/config.yaml", help="Config YAML")
    ap.add_argument("--kinds", default="done,attention,timeout,error", help="Comma-separated kinds to play")
    ap.add_argument("--pause-sec", type=float, default=0.35, help="Pause between sounds")
    args = ap.parse_args()

    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}

    kinds = [k.strip() for k in (args.kinds or "").split(",") if k.strip()]
    if not kinds:
        kinds = ["done"]

    for k in kinds:
        print(f"[notify-test] {k}")
        notify(ROOT, cfg, kind=k)
        time.sleep(max(0.0, float(args.pause_sec)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
