import os
from dataclasses import dataclass
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

from .email_sender import load_env_file


@dataclass(frozen=True)
class TelethonAuth:
    api_id: int
    api_hash: str
    session_string: str
    session_file: Path


def load_telethon_auth(root: Path) -> TelethonAuth:
    # Keep accounts first so local secrets in .env.accounts win.
    load_env_file(root / ".env.accounts")
    load_env_file(root / ".env")

    api_id_raw = str(os.getenv("TELETHON_API_ID") or os.getenv("API_ID") or "").strip()
    api_hash = str(os.getenv("TELETHON_API_HASH") or os.getenv("API_HASH") or "").strip()
    session_string = str(os.getenv("TELETHON_SESSION_STRING") or os.getenv("SESSION_STRING") or "").strip()
    session_file_raw = str(os.getenv("TELETHON_SESSION_FILE") or "").strip()

    api_id = 0
    if api_id_raw:
        try:
            api_id = int(api_id_raw)
        except Exception:
            api_id = 0

    if not api_id or not api_hash:
        raise RuntimeError("Missing TELETHON_API_ID/TELETHON_API_HASH for Telegram user scan.")

    if session_file_raw:
        session_file = Path(session_file_raw)
    else:
        session_file = root / "data" / "telegram" / "telethon_user.session"

    return TelethonAuth(
        api_id=api_id,
        api_hash=api_hash,
        session_string=session_string,
        session_file=session_file,
    )


def make_telethon_client(auth: TelethonAuth, *, fallback_name: str = "aijobsearcher_tg") -> TelegramClient:
    if auth.session_string:
        return TelegramClient(StringSession(auth.session_string), auth.api_id, auth.api_hash)

    session_path = auth.session_file
    if session_path.suffix.lower() != ".session":
        session_path = session_path / f"{fallback_name}.session"
    session_path.parent.mkdir(parents=True, exist_ok=True)
    return TelegramClient(str(session_path), auth.api_id, auth.api_hash)
