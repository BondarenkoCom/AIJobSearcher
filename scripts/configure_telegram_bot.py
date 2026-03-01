from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.email_sender import load_env_file  # noqa: E402
from src.offer_feed import load_offer  # noqa: E402
from src.telegram_bot_api import TelegramBotApi  # noqa: E402


def _safe(value: object) -> str:
    return str(value or "").strip()


def main() -> int:
    ap = argparse.ArgumentParser(description="Configure Telegram bot profile and commands from offers.yaml")
    ap.add_argument("--offers", default="config/offers.yaml")
    ap.add_argument("--offer", default="qa_gig_hunter")
    ap.add_argument("--drop-pending-updates", action="store_true")
    args = ap.parse_args()

    load_env_file(ROOT / ".env.accounts")
    load_env_file(ROOT / ".env")

    token = _safe(__import__("os").getenv("TELEGRAM_BOT_TOKEN"))
    if not token:
        print("[tg-config] missing TELEGRAM_BOT_TOKEN")
        return 2

    offer = load_offer(ROOT / args.offers, args.offer)
    bot_cfg = dict(offer.bot or {})
    api = TelegramBotApi(token=token)
    me = api.get_me()
    commands = [
        {
            "command": _safe(item.get("cmd")).lower(),
            "description": _safe(item.get("description"))[:256],
        }
        for item in list(bot_cfg.get("commands") or [])
        if _safe(item.get("cmd")) and _safe(item.get("description"))
    ]

    if args.drop_pending_updates:
        api.delete_webhook(drop_pending_updates=True)
    else:
        api.delete_webhook(drop_pending_updates=False)
    if commands:
        api.set_my_commands(commands)
    api.set_my_description(_safe(bot_cfg.get("about")) or offer.summary)
    api.set_my_short_description(_safe(bot_cfg.get("short_description")) or offer.title)

    print(f"[tg-config] bot=@{_safe(me.get('username'))}")
    print(f"[tg-config] offer={offer.slug}")
    print(f"[tg-config] commands={len(commands)}")
    print("[tg-config] description applied")
    print("[tg-config] short description applied")
    print("[tg-config] webhook disabled for polling")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
