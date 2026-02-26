import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.email_sender import load_env_file  # noqa: E402
from src.linkedin_playwright import SafeCloser, bool_env, dump_debug, ensure_linkedin_session, int_env  # noqa: E402


async def _handle_rehab_notice(page) -> bool:
    try:
        title = page.locator("h1:has-text('Important notice from LinkedIn')").first
        if await title.is_visible(timeout=1200):
            btn = page.get_by_role("button", name="Agree to comply").first
            if await btn.is_visible(timeout=1500):
                await btn.click(timeout=5_000)
                await page.wait_for_timeout(1200)
                return True
    except Exception:
        return False
    return False


async def run(args: argparse.Namespace) -> int:
    load_env_file(ROOT / ".env")
    load_env_file(ROOT / ".env.accounts")

    headless = bool_env("PLAYWRIGHT_HEADLESS", False)
    slow_mo = int_env("PLAYWRIGHT_SLOW_MO_MS", 0)
    user_data_dir = Path(os.getenv("PLAYWRIGHT_USER_DATA_DIR") or (ROOT / "data" / "profiles" / "default"))
    storage_state = Path(os.getenv("LINKEDIN_STORAGE_STATE") or (ROOT / "data" / "profiles" / "linkedin_state.json"))

    email_ = (os.getenv("LINKEDIN_EMAIL") or "").strip()
    password_ = os.getenv("LINKEDIN_PASSWORD") or ""

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
            args=[
                "--start-maximized",
                "--lang=en-US",
            ],
        )

        page = closer.ctx.pages[0] if closer.ctx.pages else await closer.ctx.new_page()
        page.set_default_timeout(args.step_timeout_ms)
        page.set_default_navigation_timeout(args.step_timeout_ms)

        print("[linkedin] opening feed...")
        await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")

        url = page.url.lower()
        login_like = ("/login" in url) or ("/checkpoint/" in url) or ("/uas/" in url)
        if login_like:
            print(f"[linkedin] login required (url: {page.url})")

            try:
                if email_ and password_ and not args.manual_only:
                    print("[linkedin] filling credentials...")
                    remembered_btn = page.locator("button.member-profile__details:visible").first
                    try:
                        if await remembered_btn.is_visible(timeout=3_000):
                            await remembered_btn.click(timeout=5_000)
                            await page.wait_for_timeout(1200)
                    except Exception:
                        pass

                    user_sel = "input#username, input[name='session_key'], input[autocomplete='username']"
                    pass_sel = "input#password, input[name='session_password'], input[autocomplete='current-password']"
                    pass_input = page.locator(pass_sel).first
                    user_input = page.locator(user_sel).first

                    if await pass_input.is_visible(timeout=min(args.step_timeout_ms, 20_000)):
                        try:
                            if await user_input.is_visible(timeout=700):
                                await user_input.fill(email_)
                        except Exception:
                            pass
                        await pass_input.fill(password_)
                        submit_btn = page.locator("button[type=submit]:visible, button[data-litms-control-urn*='login-submit']:visible").first
                        await submit_btn.click(timeout=5_000)
                else:
                    print("[linkedin] manual login needed (no creds set or manual-only).")
            except PlaywrightTimeoutError:
                print("[linkedin] login form not detected quickly; likely checkpoint/2FA/CAPTCHA.")

            await _handle_rehab_notice(page)

            if args.wait_for_manual_seconds > 0:
                print(f"[linkedin] waiting up to {args.wait_for_manual_seconds}s for manual login...")
                deadline = asyncio.get_running_loop().time() + args.wait_for_manual_seconds
                while asyncio.get_running_loop().time() < deadline:
                    await asyncio.sleep(2)
                    await _handle_rehab_notice(page)
                    if "linkedin.com/feed" in page.url.lower() and await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page):
                        break
                    if "checkpoint" not in page.url.lower() and "login" not in page.url.lower():
                        try:
                            await page.goto("https://www.linkedin.com/feed/", wait_until="domcontentloaded")
                        except Exception:
                            pass

        print("[linkedin] validating session...")
        try:
            ok = await ensure_linkedin_session(root=ROOT, ctx=closer.ctx, page=page)
            if not ok:
                raise RuntimeError("session not ready (login/checkpoint/cookie)")
            print("[linkedin] session looks OK (feed reachable + li_at cookie set).")
        except Exception:
            await dump_debug(ROOT, page, "not_logged_in")
            print("[linkedin] session validation failed (still not on feed).")
            return 2

        if args.save_state:
            storage_state.parent.mkdir(parents=True, exist_ok=True)
            await closer.ctx.storage_state(path=str(storage_state))
            print(f"[linkedin] wrote storage state: {storage_state}")

        if args.keep_open:
            print("[linkedin] keep-open enabled; press Ctrl+C in this terminal to stop.")
            while True:
                await asyncio.sleep(5)

        return 0
    except asyncio.TimeoutError:
        print("[linkedin] timed out; closing browser...")
        return 3
    except KeyboardInterrupt:
        print("[linkedin] interrupted; closing browser...")
        return 130
    except Exception as e:
        print(f"[linkedin] error: {e}")
        try:
            if closer.ctx and closer.ctx.pages:
                await dump_debug(ROOT, closer.ctx.pages[0], "error")
        except Exception:
            pass
        return 1
    finally:
        await closer.close()


def main() -> int:
    ap = argparse.ArgumentParser(description="LinkedIn session bootstrap (Playwright)")
    ap.add_argument("--timeout-seconds", type=int, default=180, help="Overall timeout; script closes browser on timeout")
    ap.add_argument("--step-timeout-ms", type=int, default=30_000, help="Per-step Playwright timeout")
    ap.add_argument(
        "--wait-for-manual-seconds",
        type=int,
        default=120,
        help="How long to wait for user to complete manual login / 2FA (0 = don't wait)",
    )
    ap.add_argument("--manual-only", action="store_true", help="Never type credentials; user logs in manually")
    ap.add_argument("--save-state", action="store_true", help="Save storage_state JSON after successful login")
    ap.add_argument("--keep-open", action="store_true", help="Keep the browser open (for interactive use)")
    args = ap.parse_args()

    try:
        return asyncio.run(asyncio.wait_for(run(args), timeout=args.timeout_seconds))
    except asyncio.TimeoutError:
        print("[linkedin] hard timeout hit; exiting.")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
