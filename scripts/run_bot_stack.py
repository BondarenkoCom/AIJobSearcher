from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.email_sender import load_env_file  # noqa: E402


def _safe(value: object) -> str:
    return str(value or "").strip()


def _split_csv(raw: str) -> List[str]:
    out: List[str] = []
    for item in str(raw or "").replace("\n", ",").split(","):
        token = item.strip()
        if token and token not in out:
            out.append(token)
    return out


def _bool_env(name: str, default: bool) -> bool:
    raw = _safe(os.getenv(name))
    if not raw:
        return bool(default)
    return raw.lower() in ("1", "true", "yes", "on")


def _playwright_browser_root() -> Path:
    raw = _safe(os.getenv("PLAYWRIGHT_BROWSERS_PATH"))
    if raw and raw != "0":
        return Path(raw)
    return ROOT / "data" / "ms-playwright"


def _has_playwright_browser(browser_root: Path, browser_name: str) -> bool:
    prefixes = {
        "chromium": ("chromium-",),
        "firefox": ("firefox-",),
        "webkit": ("webkit-",),
    }.get(browser_name, (f"{browser_name}-", browser_name))
    try:
        return any(
            any(child.name.startswith(prefix) for prefix in prefixes)
            for child in browser_root.iterdir()
            if child.is_dir()
        )
    except Exception:
        return False


def _prepare_optional_runtime() -> None:
    if not _safe(os.getenv("PLAYWRIGHT_HEADLESS")):
        os.environ["PLAYWRIGHT_HEADLESS"] = "1"

    browser_root = _playwright_browser_root()
    os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", str(browser_root))
    browser_root.mkdir(parents=True, exist_ok=True)

    if not _bool_env("PLAYWRIGHT_INSTALL_ON_BOOT", True):
        print("[bot-stack] playwright bootstrap skipped by PLAYWRIGHT_INSTALL_ON_BOOT=0")
        return

    browser_name = _safe(os.getenv("PLAYWRIGHT_INSTALL_BROWSER")) or "chromium"
    if _has_playwright_browser(browser_root, browser_name):
        print(f"[bot-stack] playwright browser already present: {browser_name} @ {browser_root}")
        return

    cmd = [sys.executable, "-m", "playwright", "install", browser_name]
    print(f"[bot-stack] installing playwright browser: {' '.join(cmd)}")
    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), timeout=1200)
        print(f"[bot-stack] playwright install rc={proc.returncode}")
    except Exception as e:
        print(f"[bot-stack] playwright install failed: {e}")


def _run_pipeline(offer_slug: str, short_limit: int, *, with_optional: bool) -> int:
    cmd = [
        sys.executable,
        str(ROOT / "scripts" / "run_offer_pipeline.py"),
        "--offer",
        offer_slug,
        "--short-limit",
        str(max(1, int(short_limit))),
    ]
    if with_optional:
        cmd.append("--with-optional")
    print(f"[bot-stack] pipeline start offer={offer_slug} with_optional={with_optional}")
    proc = subprocess.run(cmd, cwd=str(ROOT))
    print(f"[bot-stack] pipeline done offer={offer_slug} rc={proc.returncode}")
    return int(proc.returncode)


def _refresh_loop(
    *,
    offers: List[str],
    short_limit: int,
    with_optional: bool,
    interval_hours: float,
    stop_event: threading.Event,
) -> None:
    interval_sec = max(900.0, float(interval_hours) * 3600.0)
    while not stop_event.is_set():
        for offer in offers:
            if stop_event.is_set():
                return
            try:
                _run_pipeline(offer, short_limit=short_limit, with_optional=with_optional)
            except Exception as e:
                print(f"[bot-stack] pipeline error offer={offer}: {e}")
        if stop_event.wait(interval_sec):
            return


def main() -> int:
    ap = argparse.ArgumentParser(description="Bootstrap Remote Work Hunter feeds and run the Telegram bot.")
    ap.add_argument("--default-offer", default="")
    ap.add_argument("--offers", default="")
    ap.add_argument("--short-limit", type=int, default=12)
    ap.add_argument("--refresh-hours", type=float, default=6.0)
    ap.add_argument("--skip-bootstrap", action="store_true")
    ap.add_argument("--skip-refresh-loop", action="store_true")
    ap.add_argument("--with-optional", action="store_true", help="Run optional scanners that need extra sessions/deps.")
    ap.add_argument("--without-optional", action="store_true", help="Force-disable optional scanners.")
    args = ap.parse_args()

    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    default_offer = _safe(args.default_offer) or _safe(os.getenv("TELEGRAM_BOT_OFFER")) or "qa_gig_hunter"
    offers = _split_csv(_safe(args.offers) or _safe(os.getenv("REMOTE_WORK_HUNTER_OFFERS")))
    if not offers:
        offers = [
            "qa_gig_hunter",
            "software_engineering_hunter",
            "data_ai_hunter",
            "cybersecurity_hunter",
            "devops_cloud_hunter",
        ]
    if default_offer not in offers:
        offers.insert(0, default_offer)
    with_optional = bool(args.with_optional or _bool_env("REMOTE_WORK_HUNTER_WITH_OPTIONAL", True))
    if args.without_optional:
        with_optional = False
    if with_optional:
        _prepare_optional_runtime()

    stop_event = threading.Event()
    refresh_thread = None
    if not args.skip_refresh_loop:
        refresh_thread = threading.Thread(
            target=_refresh_loop,
            kwargs={
                "offers": offers,
                "short_limit": args.short_limit,
                "with_optional": with_optional,
                "interval_hours": args.refresh_hours,
                "stop_event": stop_event,
            },
            daemon=True,
        )
        refresh_thread.start()
    elif not args.skip_bootstrap:
        for offer in offers:
            _run_pipeline(offer, short_limit=args.short_limit, with_optional=with_optional)

    bot_cmd = [
        sys.executable,
        str(ROOT / "scripts" / "telegram_paid_bot.py"),
        "--offer",
        default_offer,
    ]
    print(f"[bot-stack] bot start default_offer={default_offer}")
    proc = subprocess.Popen(bot_cmd, cwd=str(ROOT))
    try:
        return int(proc.wait())
    finally:
        stop_event.set()
        if proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass
        if refresh_thread is not None and refresh_thread.is_alive():
            refresh_thread.join(timeout=5.0)


if __name__ == "__main__":
    raise SystemExit(main())
