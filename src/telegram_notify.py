import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import requests


@dataclass(frozen=True)
class TelegramBotConfig:
    token: str
    chat_id: str


def _candidate_config_paths() -> list[Path]:
    env_path = (os.getenv("TT_BOT_CONFIG") or "").strip()
    if env_path:
        return [Path(env_path)]

    return [
        Path("config/tt-bot.local.json"),
        Path("config/tt-bot.json"),
    ]


def _load_config(path: Path) -> TelegramBotConfig:
    data = json.loads(path.read_text(encoding="utf-8"))
    token = str(data.get("token") or "").strip()
    chat_id = str(data.get("chat_id") or "").strip()
    if not token or not chat_id:
        raise ValueError(f"Bad Telegram config (missing token/chat_id): {path}")
    return TelegramBotConfig(token=token, chat_id=chat_id)


def load_telegram_config(config_path: Optional[Path] = None) -> Tuple[TelegramBotConfig, Path]:
    if config_path is not None:
        cfg = _load_config(config_path)
        return cfg, config_path

    for p in _candidate_config_paths():
        try:
            if p.exists():
                return _load_config(p), p
        except Exception:
            continue

    raise FileNotFoundError(
        "Telegram config not found. Set TT_BOT_CONFIG or create one of the default files."
    )


def send_telegram_message(
    text: str,
    *,
    config_path: Optional[Path] = None,
    timeout_sec: int = 12,
    disable_web_page_preview: bool = True,
) -> bool:
    """
    Sends a plain-text Telegram message via the local bot config.

    Never prints secrets (token/chat_id). Returns True/False.
    """
    try:
        cfg, _path = load_telegram_config(config_path=config_path)
    except Exception as e:
        print(f"[telegram] disabled: {e}")
        return False

    msg = (text or "").strip()
    if not msg:
        return False
    if len(msg) > 3800:
        msg = msg[:3800] + "\n...(truncated)"

    url = f"https://api.telegram.org/bot{cfg.token}/sendMessage"
    payload = {
        "chat_id": cfg.chat_id,
        "text": msg,
        "disable_web_page_preview": bool(disable_web_page_preview),
    }
    try:
        r = requests.post(url, json=payload, timeout=timeout_sec)
        if r.status_code >= 400:
            print(f"[telegram] send failed: HTTP {r.status_code}")
            return False
        return True
    except Exception as e:
        print(f"[telegram] send failed: {e}")
        return False
