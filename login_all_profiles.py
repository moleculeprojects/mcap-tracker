#!/usr/bin/env python3
"""

Login All Profiles

Purpose:
- Open a persistent Chrome profile per channel so you can log into target services once
- Sessions are saved under playwright/playwright-chrome-profile-<channel>
- Intended to be re-run anytime cookies expire

Usage examples:
    python login_all_profiles.py --hold 150
    python login_all_profiles.py --channel moleculeTokenFinder --hold 300
    python login_all_profiles.py --services grok --hold 300

Notes:
- This script does not automate credentials; it just opens the pages and preserves the session
- Auto-cleanup and uploads are handled elsewhere (main.py and uploaders)
"""

import asyncio
import json
import os
import sys
import time
import argparse
from typing import Iterable, List

from playwright.async_api import async_playwright

sys.path.append('.')

TIMEOUT_SECONDS = 12000000
# Chrome executable used elsewhere in the project
CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"


def get_user_data_root(channel_name: str) -> str:
    return os.path.abspath(f"playwright/playwright-chrome-profile-{channel_name}")


def read_channels_config() -> dict:
    try:
        with open('json_files/channels_config.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            # Handle both list format and dict format
            if isinstance(data, list):
                return {"channels": data, "system_config": {}}
            elif isinstance(data, dict):
                return data
            else:
                return {"channels": [], "system_config": {}}
    except Exception:
        return {"channels": [], "system_config": {}}


def list_channels(filter_name: str | None) -> List[dict]:
    cfg = read_channels_config()
    channels = cfg.get('channels', []) or []

    # Expand any channel entries where "name" is a list, so that each
    # Playwright profile (e.g. "moleculeTokenFinder", "stevemajor", "sullivano")
    # is treated as its own logical channel for login/profile purposes.
    expanded: List[dict] = []
    for ch in channels:
        raw_name = ch.get("name")

        # Base config shared by all names in the list
        base_cfg = {k: v for k, v in ch.items() if k != "name"}

        if isinstance(raw_name, list):
            for nm in raw_name:
                if not isinstance(nm, str) or not nm.strip():
                    continue
                if filter_name and nm != filter_name:
                    continue
                entry = dict(base_cfg)
                entry["name"] = nm
                expanded.append(entry)
        else:
            if filter_name and raw_name != filter_name:
                continue
            expanded.append(ch)

    return expanded


DEFAULT_SERVICE_URLS = {
    # Add any persistent-login pages you want to keep sessions for.
    # The script will loop through these and open one tab per entry.
     # "grok": "https://x.com/i/grok",
    # "dexscreener": "https://dexscreener.com",
    "cpi": "https://www.investing.com/economic-calendar/unemployment-rate-300",
    "gmgn": "https://gmgn.ai/sol/token/8pSKfAQmtjMNBVQyKHo79uSRMHmfL9mtz9RBkZ23pump"
}

# Sites that should have "Don't allow site to save data" set in Chrome preferences.
# Blocking dexscreener.com and challenges.cloudflare.com prevents Cloudflare's
# challenge scripts from writing to storage, so the challenge page is bypassed
# and the site loads normally.
BLOCK_SITE_DATA_FOR = [
    "dexscreener.com",
    "challenges.cloudflare.com",
]


def patch_profile_preferences(user_data_dir: str, block_sites: List[str] = None) -> None:
    """
    Write Chrome's Default/Preferences JSON to set 'Don't allow site to save data'
    (cookies content-setting = 2 / BLOCK) for the specified sites.

    This is the programmatic equivalent of:
      Chrome padlock → Cookies and site data → Don't allow site to save data

    Effect on Cloudflare: challenge scripts cannot write to storage, so the
    interactive checkbox is silently skipped and the page loads normally.
    """
    if block_sites is None:
        block_sites = BLOCK_SITE_DATA_FOR

    prefs_path = os.path.join(user_data_dir, "Default", "Preferences")
    os.makedirs(os.path.dirname(prefs_path), exist_ok=True)

    # Load existing prefs (Chrome may not have written them yet for brand-new profiles)
    prefs = {}
    if os.path.exists(prefs_path):
        try:
            with open(prefs_path, "r", encoding="utf-8") as f:
                prefs = json.load(f)
        except Exception as e:
            print(f"  ⚠️  Could not read Preferences (will create fresh): {e}")

    # Navigate to the cookies and javascript exceptions dicts
    ts_us = str(int(time.time() * 1_000_000))  # Chrome timestamps in microseconds
    exceptions = (
        prefs
        .setdefault("profile", {})
        .setdefault("content_settings", {})
        .setdefault("exceptions", {})
    )
    exceptions.setdefault("cookies", {})
    exceptions.setdefault("javascript", {})

    cookies_exc = exceptions["cookies"]
    js_exc = exceptions["javascript"]

    for site in block_sites:
        pattern = f"{site},*"   # Chrome's canonical pattern: "site,origin"
        cookies_exc[pattern] = {
            "expiration": "0",
            "last_modified": ts_us,
            "model": 0,
            "setting": 2,   # 2 = BLOCK
        }
        js_exc[pattern] = {
            "expiration": "0",
            "last_modified": ts_us,
            "model": 0,
            "setting": 1,   # 1 = ALLOW
        }
        print(f"  🔒 Block cookies / ✅ Allow JS: {pattern}")

    try:
        with open(prefs_path, "w", encoding="utf-8") as f:
            json.dump(prefs, f, indent=2)
        print(f"  ✅ Preferences patched: {prefs_path}")
    except Exception as e:
        print(f"  ❌ Could not write Preferences: {e}")


def services_for_channel(ch: dict, requested: Iterable[str]) -> List[str]:
    req = [s.strip().lower() for s in (requested or [])]
    if not req:
        return list(DEFAULT_SERVICE_URLS.keys())

    # keep only known
    allowed = set(DEFAULT_SERVICE_URLS.keys())
    return [s for s in req if s in allowed]


async def open_login_tabs_for_channel(pw, channel: dict, services: List[str], hold_seconds: int) -> None:
    # At this point list_channels() guarantees "name" is a single string
    channel_name = channel.get('name') or 'default'
    user_data_dir = get_user_data_root(channel_name)

    print(f"\n🔐 Preparing login for channel: {channel_name}")
    print(f"📂 Profile: {user_data_dir}")

    # Patch Chrome preferences BEFORE launch so settings take effect on startup
    print(f"🛡️  Patching profile preferences (block site data for DexScreener/Cloudflare)…")
    patch_profile_preferences(user_data_dir)

    browser = await pw.chromium.launch_persistent_context(
        user_data_dir=user_data_dir,
        executable_path=CHROME_EXE,
        headless=False,
        ignore_default_args=["--enable-automation", "--no-sandbox"],
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--disable-dev-shm-usage",
            "--no-first-run",
            "--no-default-browser-check",
            "--disable-component-update",
            "--disable-features=IsolateOrigins,site-per-process",
        ],
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    )

    try:
        # Loop through requested services and open each default URL in a new tab
        for svc in services:
            url = DEFAULT_SERVICE_URLS.get(svc)
            if not url:
                continue
            page = await browser.new_page()
            print(f"🔗 {svc} → {url}")
            try:
                await page.goto(url, timeout=TIMEOUT_SECONDS)
            except Exception:
                pass

        # Hold window open for manual login; allow Enter to proceed early
        if hold_seconds > 0:
            print(f"⏳ Hold open for {hold_seconds}s — complete login, then cookies will persist…")
            print("👉 Press Enter here any time to proceed to the next channel immediately.")
            try:
                sleep_task = asyncio.create_task(asyncio.sleep(hold_seconds))
                enter_task = asyncio.create_task(asyncio.to_thread(input, "\nPress Enter to continue now… "))
                done, pending = await asyncio.wait({sleep_task, enter_task}, return_when=asyncio.FIRST_COMPLETED)
                # Cancel the other task if still pending
                for t in pending:
                    try:
                        t.cancel()
                    except Exception:
                        pass
            except Exception:
                # Fallback to fixed sleep on any error
                try:
                    await asyncio.sleep(hold_seconds)
                except Exception:
                    pass
        else:
            print("⏳ Hold disabled (0s); press Enter to continue to next channel…")
            try:
                await asyncio.to_thread(input, "\nPress Enter to continue now… ")
            except Exception:
                pass
    finally:
        try:
            await browser.close()
        except Exception:
            pass
        print(f"✅ Saved session for: {channel_name}")


async def main_async():
    parser = argparse.ArgumentParser(description="Open persistent Chrome profiles for all channels to log in")
    parser.add_argument("--channel", help="Single channel name to process")
    parser.add_argument(
        "--services",
        nargs="*",
        help=f"Subset of services to open (default: all). Known: {', '.join(DEFAULT_SERVICE_URLS.keys())}",
    )
    parser.add_argument("--hold", type=int, default=TIMEOUT_SECONDS, help="Seconds to hold browser open per channel for manual login")
    args = parser.parse_args()

    channels = list_channels(args.channel)
    if not channels:
        print("❌ No channels found in json_files/channels_config.json")
        return

    async with async_playwright() as pw:
        for ch in channels:
            targets = services_for_channel(ch, args.services)
            await open_login_tabs_for_channel(pw, ch, targets, max(0, int(args.hold)))


def main():
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main_async())


if __name__ == "__main__":
    main()