import asyncio
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from playwright.async_api import BrowserContext, Page


CHECKPOINT_RE = re.compile(r"/checkpoint/|/login|/uas/login|captcha|security verification", re.IGNORECASE)
DEBUG_DIR_MAX_BYTES = 100 * 1024 * 1024
DEBUG_DIR_TARGET_BYTES = 80 * 1024 * 1024


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def bool_env(name: str, default: bool) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return default
    return v in {"1", "true", "yes", "y", "on"}


def int_env(name: str, default: int) -> int:
    v = (os.getenv(name) or "").strip()
    try:
        return int(v)
    except Exception:
        return default


def is_checkpoint_url(url: str) -> bool:
    return bool(CHECKPOINT_RE.search(url or ""))


def _safe_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(data, encoding="utf-8", errors="replace")


def _debug_dir_size_bytes(debug_dir: Path) -> int:
    total = 0
    try:
        for p in debug_dir.iterdir():
            if p.is_file():
                total += p.stat().st_size
    except Exception:
        return total
    return total


def _prune_debug_dir(debug_dir: Path) -> None:
    """
    Keep data/debug under a hard limit by removing oldest files first.
    This avoids long-term disk bloat from repeated screenshot/html dumps.
    """
    try:
        limit_bytes = int(os.getenv("DEBUG_MAX_MB", "100")) * 1024 * 1024
    except Exception:
        limit_bytes = DEBUG_DIR_MAX_BYTES
    target_bytes = min(DEBUG_DIR_TARGET_BYTES, max(1, int(limit_bytes * 0.8)))

    current = _debug_dir_size_bytes(debug_dir)
    if current <= limit_bytes:
        return

    try:
        files = [p for p in debug_dir.iterdir() if p.is_file()]
    except Exception:
        return
    files.sort(key=lambda p: p.stat().st_mtime)

    for p in files:
        if current <= target_bytes:
            break
        try:
            sz = p.stat().st_size
            p.unlink(missing_ok=True)
            current = max(0, current - sz)
        except Exception:
            continue


async def dump_debug(root: Path, page: Page, tag: str) -> Tuple[Optional[Path], Optional[Path]]:
    """
    Save HTML + PNG to data/debug/.
    Returns (html_path, png_path) best-effort.
    """
    html_path = None
    png_path = None
    try:
        debug_dir = root / "data" / "debug"
        debug_dir.mkdir(parents=True, exist_ok=True)
        _prune_debug_dir(debug_dir)
        stamp = _ts()
        html_path = debug_dir / f"linkedin_{tag}_{stamp}.html"
        png_path = debug_dir / f"linkedin_{tag}_{stamp}.png"
        _safe_write(html_path, await page.content())
        await page.screenshot(path=str(png_path), full_page=True)
    except Exception:
        return (html_path, png_path)
    return (html_path, png_path)


async def has_li_at_cookie(ctx: BrowserContext) -> bool:
    try:
        cookies = await ctx.cookies(["https://www.linkedin.com/"])
        return any((c.get("name") == "li_at" and (c.get("value") or "").strip()) for c in cookies)
    except Exception:
        return False


async def goto_guarded(
    *,
    root: Path,
    page: Page,
    url: str,
    timeout_ms: int = 30_000,
    tag_on_fail: str = "nav_failed",
) -> bool:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
    except Exception:
        await dump_debug(root, page, tag_on_fail)
        return False

    if is_checkpoint_url(page.url):
        await dump_debug(root, page, "checkpoint")
        return False

    return True


async def ensure_linkedin_session(
    *,
    root: Path,
    ctx: BrowserContext,
    page: Page,
    timeout_ms: int = 30_000,
) -> bool:
    """
    Consider session OK if:
    - li_at cookie exists, AND
    - /feed can be opened without redirecting to login/checkpoint.
    """
    if not await has_li_at_cookie(ctx):
        return False

    ok = await goto_guarded(root=root, page=page, url="https://www.linkedin.com/feed/", timeout_ms=timeout_ms)
    if not ok:
        return False

    # One more cookie check after navigation.
    return await has_li_at_cookie(ctx)


@dataclass
class SafeCloser:
    ctx: Optional[BrowserContext] = None
    pw: Optional[object] = None

    async def close(self, timeout_sec: float = 12.0) -> None:
        async def _close_ctx() -> None:
            if self.ctx is not None:
                await self.ctx.close()

        async def _stop_pw() -> None:
            if self.pw is not None:
                await self.pw.stop()

        # Best-effort: timebox each close step so we don't hang.
        try:
            await asyncio.wait_for(_close_ctx(), timeout=timeout_sec)
        except Exception:
            pass
        try:
            await asyncio.wait_for(_stop_pw(), timeout=timeout_sec)
        except Exception:
            pass
