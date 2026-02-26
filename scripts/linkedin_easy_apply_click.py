import argparse
import asyncio
import os
import re
import sys
from pathlib import Path

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import (  # noqa: E402
    SafeCloser,
    bool_env,
    dump_debug,
    ensure_linkedin_session,
    goto_guarded,
    int_env,
)


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))

    closer = SafeCloser()
    try:
        closer.pw = await async_playwright().start()
        closer.ctx = await closer.pw.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=headless,
            slow_mo=slow_mo,
            viewport=None,
            locale="en-US",
            extra_http_headers={"Accept-Language": "en-US,en;q=0.9"},
            args=["--start-maximized", "--lang=en-US"],
        )

        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        ok = await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page, timeout_ms=args.step_timeout_ms)
        if not ok:
            print("[li-apply] not logged in / checkpoint. Run scripts/linkedin_login.py first.")
            await dump_debug(ROOT, page, "apply_not_logged_in")
            return 2

        url = args.job_url.strip()
        if not url:
            print("[li-apply] missing --job-url")
            return 1

        print(f"[li-apply] open job: {url}")
        ok = await goto_guarded(root=ROOT, page=page, url=url, timeout_ms=args.step_timeout_ms, tag_on_fail="apply_job_open_failed")
        if not ok:
            print("[li-apply] blocked by checkpoint/captcha while opening job; stopping.")
            return 3

        # Let the job header/actions render.
        await page.wait_for_timeout(2000)

        # Easy Apply is often rendered as a link.
        easy_a = page.locator("a[href*='openSDUIApplyFlow=true'], a[href*='/apply/?openSDUIApplyFlow=true']").first
        easy_btn = page.get_by_role("button", name=re.compile(r"easy apply", re.IGNORECASE)).first

        clicked = False
        try:
            if await easy_a.is_visible(timeout=3_000):
                await easy_a.click()
                clicked = True
        except Exception:
            pass

        if not clicked:
            try:
                if await easy_btn.is_visible(timeout=2_000):
                    await easy_btn.click()
                    clicked = True
            except Exception:
                pass

        if not clicked:
            await dump_debug(ROOT, page, "apply_no_easy_apply")
            print("[li-apply] Easy Apply not found on this job (see debug dump).")
            return 4

        print("[li-apply] clicked Easy Apply. Waiting for apply UI...")
        try:
            await page.wait_for_selector("div[role='dialog']", timeout=10_000)
        except PlaywrightTimeoutError:
            # Some flows navigate instead of opening a modal.
            pass

        await dump_debug(ROOT, page, "apply_after_click")
        print("[li-apply] ready. Complete the form manually in the opened browser.")

        if args.keep_open:
            print("[li-apply] keep-open enabled; press Ctrl+C in this terminal to stop.")
            while True:
                await asyncio.sleep(5)

        return 0
    except asyncio.TimeoutError:
        print("[li-apply] hard timeout hit; closing browser...")
        return 6
    except KeyboardInterrupt:
        print("[li-apply] interrupted; closing browser...")
        return 130
    except Exception as e:
        print(f"[li-apply] error: {e}")
        try:
            if closer.ctx and closer.ctx.pages:
                await dump_debug(ROOT, closer.ctx.pages[0], "apply_error")
        except Exception:
            pass
        return 1
    finally:
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="Open a LinkedIn job and click Easy Apply (stops for manual completion)")
    ap.add_argument("--job-url", required=True, help="LinkedIn job URL, e.g. https://www.linkedin.com/jobs/view/<id>/")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step Playwright timeout")
    ap.add_argument("--timeout-seconds", type=int, default=300, help="Overall timeout")
    ap.add_argument("--keep-open", action="store_true", help="Keep browser open after clicking Easy Apply")
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[li-apply] hard timeout hit; exiting.")
        return 6


if __name__ == "__main__":
    raise SystemExit(main())

