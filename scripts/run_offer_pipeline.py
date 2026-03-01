from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.config import cfg_get, load_config, resolve_path  # noqa: E402
from src.email_sender import load_env_file  # noqa: E402
from src.offer_profiles import load_offer_profiles  # noqa: E402
from src.offer_runtime_config import write_runtime_config  # noqa: E402


def _safe(value: object) -> str:
    return str(value or "").strip()


def _build_context(args) -> Dict[str, str]:
    cfg_path = resolve_path(ROOT, args.config)
    cfg = load_config(str(cfg_path)) if cfg_path.exists() else {}
    db_path = resolve_path(ROOT, str(cfg_get(cfg, "activity.db_path", "data/out/activity.sqlite")))
    if _safe(args.db):
        db_path = resolve_path(ROOT, _safe(args.db))
    out_dir = resolve_path(ROOT, _safe(args.out_dir) or "data/out")
    out_dir.mkdir(parents=True, exist_ok=True)
    return {
        "python": sys.executable,
        "db": str(db_path),
        "out_dir": str(out_dir),
        "offer": _safe(args.offer),
        "short_limit": str(max(1, int(args.short_limit))),
        "runtime_config": str(cfg_path),
    }


def _run_one(command_template: str, ctx: Dict[str, str]) -> int:
    command = command_template.format(**ctx)
    print(f"[offer-run] exec: {command}")
    proc = subprocess.run(command, cwd=str(ROOT), shell=True)
    return int(proc.returncode)


def main() -> int:
    ap = argparse.ArgumentParser(description="Run one monetizable offer pipeline end-to-end.")
    ap.add_argument("--config", default="config/config.yaml")
    ap.add_argument("--offers", default="config/offers.yaml")
    ap.add_argument("--offer", default="qa_gig_hunter")
    ap.add_argument("--db", default="")
    ap.add_argument("--out-dir", default="data/out")
    ap.add_argument("--short-limit", type=int, default=12)
    ap.add_argument("--with-optional", action="store_true", help="Run scanners that need login/session/deps.")
    ap.add_argument("--export-only", action="store_true", help="Skip scanners and only build the sellable feed.")
    args = ap.parse_args()

    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    offer_path = resolve_path(ROOT, args.offers)
    offers = load_offer_profiles(offer_path)
    offer = offers.get(_safe(args.offer))
    if offer is None:
        print(f"[offer-run] unknown offer: {args.offer}")
        print(f"[offer-run] available: {', '.join(sorted(offers))}")
        return 2

    ctx = _build_context(args)
    print(f"[offer-run] offer={offer.slug}")
    print(f"[offer-run] title={offer.title}")
    print(f"[offer-run] product_hint={offer.product_hint}")

    runtime_cfg_path = resolve_path(ROOT, args.config)
    if offer.config_overrides:
        runtime_cfg_path = resolve_path(ROOT, f"data/out/runtime_configs/{offer.slug}.yaml")
        write_runtime_config(
            base_cfg_path=resolve_path(ROOT, args.config),
            offer=offer,
            out_path=runtime_cfg_path,
        )
        print(f"[offer-run] runtime_config={runtime_cfg_path}")
    ctx["runtime_config"] = str(runtime_cfg_path)

    failures: List[str] = []
    if not args.export_only:
        for scanner in offer.scanners:
            name = _safe(scanner.get("name")) or "unnamed"
            optional = bool(scanner.get("optional", False))
            if optional and (not args.with_optional):
                print(f"[offer-run] skip optional scanner={name}")
                continue
            command = _safe(scanner.get("command"))
            if not command:
                continue
            rc = _run_one(command, ctx)
            if rc != 0:
                failures.append(f"{name}:{rc}")
                if not optional:
                    print(f"[offer-run] required scanner failed: {name} rc={rc}")
                    return rc
                print(f"[offer-run] optional scanner failed: {name} rc={rc}")

    export_cmd = (
        "{python} scripts/export_offer_feed.py --config {config} --offers {offers} "
        "--offer {offer} --db {db} --out-dir {out_dir} --limit {short_limit}"
    )
    export_ctx = dict(ctx)
    export_ctx["config"] = str(resolve_path(ROOT, args.config))
    export_ctx["offers"] = str(resolve_path(ROOT, args.offers))
    rc = _run_one(export_cmd, export_ctx)
    if rc != 0:
        return rc

    if failures:
        print(f"[offer-run] completed with optional failures: {', '.join(failures)}")
    else:
        print("[offer-run] completed cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
