import sys
import subprocess
from pathlib import Path
from typing import Dict

from .config import cfg_get, resolve_path


def _try_winsound_beep() -> None:
    if sys.platform != "win32":
        return
    try:
        import winsound

        winsound.MessageBeep()
    except Exception:
        return


def _mci_error_message(code: int) -> str:
    try:
        import ctypes

        buf = ctypes.create_unicode_buffer(256)
        if ctypes.windll.winmm.mciGetErrorStringW(code, buf, len(buf)):
            return buf.value
    except Exception:
        pass
    return f"mci error {code}"


def _play_wav(path: Path, *, wait: bool = True) -> bool:
    if sys.platform != "win32":
        return False
    if not path.exists():
        return False
    try:
        import winsound

        flags = winsound.SND_FILENAME
        if not wait:
            flags |= winsound.SND_ASYNC
        winsound.PlaySound(str(path), flags)
        return True
    except Exception:
        return False


def _convert_mp3_to_wav(mp3_path: Path, *, out_dir: Path) -> Path | None:
    try:
        import imageio_ffmpeg

        ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
    except Exception as e:
        print(f"[notify] ffmpeg not available: {e}")
        return None

    out_dir.mkdir(parents=True, exist_ok=True)
    wav_path = out_dir / f"{mp3_path.stem}.wav"
    try:
        if wav_path.exists() and wav_path.stat().st_mtime >= mp3_path.stat().st_mtime:
            return wav_path
    except Exception:
        pass

    try:
        subprocess.run(
            [
                ffmpeg,
                "-y",
                "-hide_banner",
                "-loglevel",
                "error",
                "-i",
                str(mp3_path),
                "-vn",
                str(wav_path),
            ],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        print(f"[notify] mp3->wav failed: {e}")
        return None

    return wav_path if wav_path.exists() else None


def _play_mp3_mci(path: Path, *, wait: bool = True) -> bool:
    """Play mp3 on Windows using MCI (may be unavailable on some Windows builds)."""
    if sys.platform != "win32":
        return False
    if not path.exists():
        return False

    try:
        import ctypes

        winmm = ctypes.windll.winmm
        alias = "aijobsearcher_notify"

        winmm.mciSendStringW(f"close {alias}", None, 0, None)

        code = winmm.mciSendStringW(
            f'open "{str(path)}" type mpegvideo alias {alias}', None, 0, None
        )
        if code != 0:
            return False

        play_cmd = f"play {alias} wait" if wait else f"play {alias}"
        code = winmm.mciSendStringW(play_cmd, None, 0, None)
        if code != 0:
            winmm.mciSendStringW(f"close {alias}", None, 0, None)
            return False

        winmm.mciSendStringW(f"close {alias}", None, 0, None)
        return True
    except Exception as e:
        print(f"[notify] audio failed: {e}")
        return False


def _notify_path(root: Path, cfg: Dict[str, object], *, kind: str) -> str:
    sp = str(cfg_get(cfg, f"notify.sounds.{kind}", "")).strip()
    if sp:
        return sp

    legacy = str(cfg_get(cfg, "notify.sound_path", "")).strip()
    if kind == "done":
        return legacy
    return legacy


def notify(root: Path, cfg: Dict[str, object], *, kind: str = "done") -> None:
    """
    Optional notification (sound) on Windows.
    kind:
      - done: successful completion
      - attention: needs human input
      - error: failure
      - timeout: long timeout (falls back to error if not configured)
    """
    if not cfg_get(cfg, "notify.enabled", False):
        return

    wait = bool(cfg_get(cfg, "notify.wait", True))
    sound_path = _notify_path(root, cfg, kind=kind)
    if not sound_path and kind == "timeout":
        sound_path = _notify_path(root, cfg, kind="error")

    if sound_path:
        path = resolve_path(root, sound_path)
        suffix = path.suffix.lower()
        if suffix == ".wav":
            if _play_wav(path, wait=wait):
                return
        elif suffix == ".mp3":
            if _play_mp3_mci(path, wait=wait):
                return
            wav = _convert_mp3_to_wav(path, out_dir=(root / "data" / "out" / "notify_sounds"))
            if wav and _play_wav(wav, wait=wait):
                return
        else:
            if _play_wav(path, wait=wait):
                return

    _try_winsound_beep()


def notify_done(root: Path, cfg: Dict[str, object]) -> None:
    """Back-compat shim: end-of-run sound notification."""
    notify(root, cfg, kind="done")
