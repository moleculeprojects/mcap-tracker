#!/usr/bin/env python3
"""
Dip Scanner
===========
Triggered by fresh_scanner.py during its 5-minute idle window.
Scans DexScreener for tokens that have already dumped to sub-$10K MCAP
but are trending again (second wave / dip recovery plays).

Key differences from fresh_scanner.py:
  - Different BASE_URL: maxMarketCap=10000, max24HChg=100, minLiq=1000
  - Own JSON files: dead_pending_tokens.json, dead_failed_tokens.json, dead_valid_tokens.json
  - Max runtime: 5 minutes, then returns
  - Grok context framed around "second wave / revival" potential
  - Telegram label: "🔄 Dip token found"
  - Server posts with source="dip"
"""

import os
import time
import asyncio
import json
import html
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from typing import Optional
import re
import requests

from utils.grok_brain import check_token_trend_with_grok, DIP_GROK_RULES
from utils.server_sync import post_token_to_server

# --- Telegram configuration (shared with main bot) ---
BOT_TOKEN = None
CHAT_IDS = []


def load_telegram_config():
    global BOT_TOKEN, CHAT_IDS
    if BOT_TOKEN and CHAT_IDS:
        return
    bot_token = os.getenv("BOT_TOKEN")
    chat_ids_raw = os.getenv("CHAT_IDS")
    if (not bot_token or not chat_ids_raw) and os.path.exists(".env"):
        try:
            env_values = {}
            with open(".env", "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, value = line.split("=", 1)
                        env_values[key.strip()] = value.strip()
            bot_token = bot_token or env_values.get("BOT_TOKEN")
            chat_ids_raw = chat_ids_raw or env_values.get("CHAT_IDS")
        except Exception as e:
            print(f"[WARNING] Could not read .env file for Telegram config: {e}")
    if not bot_token or not chat_ids_raw:
        print("[WARNING] Telegram config missing. Notifications disabled.")
        BOT_TOKEN = None
        CHAT_IDS = []
        return
    BOT_TOKEN = bot_token
    CHAT_IDS = [cid.strip() for cid in chat_ids_raw.split(",") if cid.strip()]


def send_to_telegram(message: str):
    load_telegram_config()
    if not BOT_TOKEN or not CHAT_IDS:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in CHAT_IDS:
        payload = {"chat_id": chat_id, "text": message, "parse_mode": "HTML"}
        try:
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                print(f"[INFO] Telegram: Message sent to chat {chat_id}.")
            else:
                print(f"[ERROR] Telegram: Failed to send to {chat_id}. Status: {response.status_code}")
        except Exception as e:
            print(f"[EXCEPTION] Telegram: Exception while sending to {chat_id}: {e}")


# --- Dead Scanner Constants ---
PAGE_LIMIT = 5  # Fewer pages since we only have 5 minutes

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

# Dead scanner URL — low MCAP trending tokens (dip recovery plays)
BASE_URL = (
    "https://dexscreener.com/?rankBy=trendingScoreM5&order=desc"
    "&chainIds=solana&dexIds=pumpswap,pumpfun,raydium"
    "&minLiq=1000&maxMarketCap=10000&maxAge=7&max24HChg=100&profile=1&launchpads=1"
)
QUERY_STRING = (
    "rankBy=trendingScoreM5&order=desc"
    "&chainIds=solana&dexIds=pumpswap,pumpfun,raydium"
    "&minLiq=1000&maxMarketCap=10000&maxAge=7&max24HChg=100&profile=1&launchpads=1"
)

TOKEN_ROW_SELECTOR = "a.ds-dex-table-row"
TOKEN_NAME_SELECTOR = "span.ds-dex-table-row-base-token-symbol"

NORMAL_BORDER_COLOR = "rgb(77, 87, 115)"

# Pre-filter settings (stricter than main bot for dip plays)
MAX_TOKEN_AGE_HOURS = 7        # Must be <7h old (matches DexScreener maxAge=7)
MAX_PUMP_FROM_LAUNCH_PERCENT = 100  # Reject if already pumped >100% — early momentum only
MIN_BUY_SELL_RATIO = 0.3


# JSON file paths (separate from main bot)
DEAD_VALID_JSON = os.path.join("json_files", "dead_valid_tokens.json")
DEAD_FAILED_JSON = os.path.join("json_files", "dead_failed_tokens.json")
DEAD_PENDING_JSON = os.path.join("json_files", "dead_pending_tokens.json")


def get_user_data_root(channel_name):
    return os.path.abspath(f"playwright/playwright-chrome-profile-{channel_name}")


async def launch_profile_context(pw, profile_name: str):
    """Launch a persistent Chrome context for the given profile. Returns (context, page)."""
    user_data_dir = get_user_data_root(profile_name)
    context = await pw.chromium.launch_persistent_context(
        user_data_dir,
        headless=False,
        executable_path=CHROME_EXE,
        ignore_default_args=["--enable-automation"],
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page


# --- JSON helpers ---

def load_dead_valid_tokens():
    valid_links = set()
    if os.path.exists(DEAD_VALID_JSON):
        try:
            with open(DEAD_VALID_JSON, "r", encoding="utf-8") as f:
                data = json.load(f) or []
            for item in data:
                if isinstance(item, dict) and item.get("link"):
                    valid_links.add(item["link"])
                elif isinstance(item, str):
                    valid_links.add(item)
        except Exception as e:
            print(f"[DipScanner] WARNING: Could not read {DEAD_VALID_JSON}: {e}")
    return valid_links


def save_dead_valid_token(token_name, token_link, address=None, pair_address=None, liquidity=None, market_cap=None, narrative=None):
    existing = []
    seen = set()
    try:
        if os.path.exists(DEAD_VALID_JSON):
            with open(DEAD_VALID_JSON, "r", encoding="utf-8") as f:
                data = json.load(f) or []
                if isinstance(data, list):
                    existing = data
    except Exception:
        existing = []
    for item in existing:
        if isinstance(item, dict) and item.get("link"):
            seen.add(item["link"])
        elif isinstance(item, str):
            seen.add(item)
    if token_link in seen:
        return
    entry = {
        "name": token_name,
        "link": token_link,
        "address": address,
        "pairAddress": pair_address,
        "timestamp": int(time.time())
    }
    if liquidity is not None:
        entry["liquidity"] = liquidity
    if market_cap is not None:
        entry["market_cap"] = market_cap
    if narrative is not None:
        entry["narrative"] = narrative
    existing.append(entry)
    try:
        os.makedirs("json_files", exist_ok=True)
        with open(DEAD_VALID_JSON, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        print(f"[DipScanner] WARNING: Could not write {DEAD_VALID_JSON}: {e}")


def load_dead_failed_tokens():
    failed_links = set()
    if os.path.exists(DEAD_FAILED_JSON):
        try:
            with open(DEAD_FAILED_JSON, "r", encoding="utf-8") as f:
                data = json.load(f) or []
            for item in data:
                if isinstance(item, dict) and item.get("link"):
                    failed_links.add(item["link"])
                elif isinstance(item, str):
                    failed_links.add(item)
        except Exception as e:
            print(f"[DipScanner] WARNING: Could not read {DEAD_FAILED_JSON}: {e}")
    return failed_links


def save_dead_failed_token(token_name, token_link, reason: Optional[str] = None):
    existing = []
    seen = set()
    try:
        if os.path.exists(DEAD_FAILED_JSON):
            with open(DEAD_FAILED_JSON, "r", encoding="utf-8") as f:
                data = json.load(f) or []
                if isinstance(data, list):
                    existing = data
    except Exception:
        existing = []
    for item in existing:
        if isinstance(item, dict) and item.get("link"):
            seen.add(item["link"])
        elif isinstance(item, str):
            seen.add(item)
    if token_link in seen:
        return
    entry = {"name": token_name, "link": token_link, "timestamp": int(time.time())}
    if reason:
        entry["reason"] = reason
    existing.append(entry)
    try:
        os.makedirs("json_files", exist_ok=True)
        with open(DEAD_FAILED_JSON, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        print(f"[DipScanner] WARNING: Could not write {DEAD_FAILED_JSON}: {e}")


def load_dead_pending_tokens():
    if os.path.exists(DEAD_PENDING_JSON):
        try:
            with open(DEAD_PENDING_JSON, "r", encoding="utf-8") as f:
                return json.load(f) or []
        except Exception:
            return []
    return []


def save_dead_pending_tokens(tokens):
    os.makedirs("json_files", exist_ok=True)
    temp = DEAD_PENDING_JSON + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    os.replace(temp, DEAD_PENDING_JSON)




# --- Token scraping ---

async def load_all_tokens(page, base_url, query_string):
    """
    Extract token data from window.__SERVER_DATA — the React hydration state
    embedded by DexScreener's server-side rendering. This is instant and does
    NOT require the UI to render, scroll, or wait for TradingView/Cloudflare.
    """
    all_tokens = []
    current_page = 1
    while True:
        if PAGE_LIMIT is not None and current_page > PAGE_LIMIT:
            print(f"[DipScanner] Reached max page {PAGE_LIMIT}, stopping.")
            break
        if current_page == 1:
            current_url = base_url
        else:
            current_url = f"https://dexscreener.com/page-{current_page}?{query_string}"

        print(f"[DipScanner] Processing page {current_page} with URL: {current_url}")

        # Navigate without waiting for the full DOM to load to avoid timeouts
        await page.goto(current_url, wait_until="commit")

        # Wait specifically for the server hydration data to be available
        try:
            await page.wait_for_function('() => window.__SERVER_DATA !== undefined', timeout=30000)
            await asyncio.sleep(1)
        except Exception:
            print(f"[DipScanner] Page {current_page} - Timeout waiting for __SERVER_DATA. Proceeding anyway.")

        # Extract pairs directly from the embedded React server data
        pairs = await page.evaluate('''
            () => {
                try {
                    const sd = window.__SERVER_DATA;
                    if (!sd || !sd.route || !sd.route.data || !sd.route.data.dexScreenerData)
                        return [];
                    return sd.route.data.dexScreenerData.pairs || [];
                } catch (e) {
                    return [];
                }
            }
        ''')

        if not pairs or len(pairs) == 0:
            print(f"[DipScanner] Page {current_page} - No tokens found in __SERVER_DATA. Pagination complete.")
            break

        for pair in pairs:
            try:
                base_token = pair.get("baseToken", {})
                token_name = base_token.get("symbol") or base_token.get("name") or ""
                token_address = base_token.get("address", "")
                pair_address = pair.get("pairAddress", "")
                chain = pair.get("chainId", "solana")
                full_url = f"https://dexscreener.com/{chain}/{pair_address}"

                # Metadata extraction
                cms_profile = pair.get("cmsProfile") or {}
                launchpad = pair.get("launchpad") or {}
                twitter_url = ""
                links = cms_profile.get("links", [])
                for link_obj in links:
                    if link_obj.get("type") == "twitter":
                        twitter_url = link_obj.get("url", "")
                        break

                token = {
                    "name": token_name,
                    "link": full_url,
                    "address": token_address,
                    "pairAddress": pair_address,
                    "description": cms_profile.get("description", ""),
                    "twitter_url": twitter_url,
                    "creator": launchpad.get("creator", ""),
                    "migrationDex": launchpad.get("migrationDex", ""),
                    "pair_data": pair # Pass the raw data through for pre-filtering (no API call needed)
                }
                all_tokens.append(token)
            except Exception as e:
                print(f"[DipScanner] Error extracting token from __SERVER_DATA pair: {e}")

        print(f"[DipScanner] Page {current_page} - Extracted {len(pairs)} tokens from __SERVER_DATA.")
        current_page += 1
    return all_tokens


# --- Pre-filter ---

def extract_pair_address(token_link):
    try:
        if '/solana/' in token_link:
            parts = token_link.split('/solana/')
            if len(parts) > 1:
                return parts[1].split('?')[0].split('#')[0].strip()
    except Exception:
        pass
    return None


def pre_filter_token_via_api(token_data):
    """
    Use the pair_data already embedded in __SERVER_DATA (from load_all_tokens)
    to quickly check whether a token is worth the expensive Playwright analysis.
    """
    token_link = token_data.get("link", "")
    token_name = token_data.get("name", "?")

    try:
        # Prefer the __SERVER_DATA pair blob; fall back to API if not present
        pair = token_data.get("pair_data")
        if not pair:
            address = extract_pair_address(token_link)
            if not address:
                print(f"[DipScanner][PreFilter] ⚠️ Could not extract address for {token_name}, skipping pre-filter")
                return True, {}
            try:
                api_url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{address}"
                resp = requests.get(api_url, timeout=10)
                data = resp.json()
                pair = data.get("pair") or (data.get("pairs") and data["pairs"][0])
            except Exception as e:
                print(f"[DipScanner][PreFilter] ⚠️ API error for {token_name}: {e}")
                return True, {}

        if not pair:
            print(f"[DipScanner][PreFilter] ⚠️ No pair data for {token_name}, allowing through")
            return True, {}

        info = {}

        # --- Check 1: Token Age ---
        created_at_ms = pair.get("pairCreatedAt")
        if created_at_ms:
            age_hours = (time.time() * 1000 - created_at_ms) / (1000 * 3600)
            info["age_hours"] = round(age_hours, 1)
            if age_hours > MAX_TOKEN_AGE_HOURS:
                return False, f"Too old: {age_hours:.1f}h (max {MAX_TOKEN_AGE_HOURS}h)"
            print(f"[DipScanner][PreFilter] ✅ Age: {age_hours:.1f}h old (< {MAX_TOKEN_AGE_HOURS}h)")

        # --- Check 2: Price Pump from Launch (stricter for dip plays) ---
        price_change = pair.get("priceChange", {})
        h1_change = price_change.get("h1")
        h6_change = price_change.get("h6")
        h24_change = price_change.get("h24")

        from_launch_change = None
        from_launch_source = None
        if h24_change is not None:
            from_launch_change = h24_change
            from_launch_source = "24h"
        elif h6_change is not None:
            from_launch_change = h6_change
            from_launch_source = "6h"
        elif h1_change is not None:
            from_launch_change = h1_change
            from_launch_source = "1h"

        if from_launch_change is not None:
            info["from_launch_change"] = from_launch_change
            info["from_launch_source"] = from_launch_source
            if from_launch_change > MAX_PUMP_FROM_LAUNCH_PERCENT:
                return False, f"Already pumped +{from_launch_change:.0f}% from launch ({from_launch_source}) — max +{MAX_PUMP_FROM_LAUNCH_PERCENT}%"
            print(f"[DipScanner][PreFilter] ✅ From-launch pump: {from_launch_change:+.0f}% ({from_launch_source})")

        # --- Check 3: Buy/Sell Ratio (dump detection) ---
        txns = pair.get("txns", {})
        h1_txns = txns.get("h1", {})
        buys = h1_txns.get("buys", 0)
        sells = h1_txns.get("sells", 0)
        total_txns = buys + sells
        if total_txns > 0:
            buy_ratio = buys / total_txns
            info["buy_ratio"] = round(buy_ratio, 2)
            info["h1_buys"] = buys
            info["h1_sells"] = sells
            is_dumped = buy_ratio < MIN_BUY_SELL_RATIO
            info["is_dumped"] = is_dumped
            dump_label = "⚠️ DUMPED" if is_dumped else "OK"
            print(f"[DipScanner][PreFilter] ✅ Txns: {buys}B/{sells}S (ratio={buy_ratio:.2f}) [{dump_label}]")
        else:
            info["is_dumped"] = False

        # --- Extra info for logging ---
        fdv = pair.get("fdv")
        if fdv:
            info["fdv"] = fdv
        volume = pair.get("volume", {})
        if volume.get("h1"):
            info["h1_volume"] = volume["h1"]

        return True, info
    except Exception as e:
        print(f"[DipScanner][PreFilter] ⚠️ API error for {token_name}: {e}")
        return True, {}


# --- Token analysis (liquidity lock + bubble map + Grok) ---

def check_liquidity_lock_via_api(token_data):
    """
    Use the Rugcheck API to verify that liquidity is >= 100% locked.
    Returns:
        (True,  lp_pct, risks)  -> passes
        (False, lp_pct, risks)  -> rejected
    """
    # Prefer the flattened address if available, then fallback to nested pair_data
    base_token_address = token_data.get("address")
    if not base_token_address:
        pair = token_data.get("pair_data", {})
        base_token_address = pair.get("baseToken", {}).get("address") if pair else None

    if not base_token_address:
        # Try to get it from the link (pair address) — less accurate
        token_link = token_data.get("link", "")
        base_token_address = extract_pair_address(token_link)

    if not base_token_address:
        print(f"[RugCheck] ⚠️ No token address for {token_data.get('name', '?')}")
        return False, 0, []

    token_name = token_data.get("name", "?")
    try:
        url = f"https://api.rugcheck.xyz/v1/tokens/{base_token_address}/report/summary"
        resp = requests.get(url, timeout=15)
        data = resp.json()

        lp_locked_pct = data.get("lpLockedPct", 0) or 0
        risks = data.get("risks", [])
        score = data.get("score", 0)

        is_burnt = lp_locked_pct >= 99.9
        lock_status = "burnt" if is_burnt else f"{lp_locked_pct:.1f}% locked"
        print(f"[RugCheck] {token_name}: {lock_status}, score={score}, risks={len(risks)}")

        if lp_locked_pct < 99:
            return False, lp_locked_pct, risks
        return True, lp_locked_pct, risks

    except Exception as e:
        error_msg = str(e).lower()
        if "timeout" in error_msg or "failed to resolve" in error_msg or "connection" in error_msg or "max retries exceeded" in error_msg:
            print(f"[RugCheck] ⚠️ Network/DNS error for {token_name}: {e}. Tagging for retry.")
            return "network_retry", 0, []
        print(f"[RugCheck] ⚠️ API error for {token_name}: {e}")
        return False, 0, []


def extract_latest_liq_mcap_via_api(token_data):
    """
    Fetch the latest liquidity and market cap from the DexScreener API
    instead of navigating back to the token page in the browser.

    Returns:
        (liquidity_text, market_cap_text)
    """
    token_link = token_data.get("link", "")
    token_name = token_data.get("name", "?")
    address = extract_pair_address(token_link)

    if not address:
        print(f"[API LiqMcap] ⚠️ Could not extract address for {token_name}")
        return None, None

    try:
        api_url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{address}"
        resp = requests.get(api_url, timeout=10)
        data = resp.json()
        pair = data.get("pair") or (data.get("pairs") and data["pairs"][0])

        if not pair:
            print(f"[API LiqMcap] ⚠️ No pair data for {token_name}")
            return None, None

        liq_usd = pair.get("liquidity", {}).get("usd")
        mcap = pair.get("marketCap") or pair.get("fdv")

        # Format values like the UI does (e.g. $36.5K, $110K)
        def fmt(val):
            if val is None:
                return None
            if val >= 1_000_000:
                return f"${val / 1_000_000:.1f}M"
            elif val >= 1_000:
                return f"${val / 1_000:.1f}K"
            else:
                return f"${val:.0f}"

        liq_text = fmt(liq_usd)
        mcap_text = fmt(mcap)

        print(f"[API LiqMcap] {token_name}: Liq={liq_text}, Mcap={mcap_text}")
        return liq_text, mcap_text

    except Exception as e:
        print(f"[API LiqMcap] ⚠️ Error fetching data for {token_name}: {e}")
        return None, None


async def analyze_bubble_map_direct(page, token_data):
    """
    Analyze the bubble map on the DIRECT iframe.bubblemaps.io page.
    """
    token_name = token_data.get("name", "?")
    print(f"\n[Bubble Map Direct] Starting bubble map analysis for {token_name}")

    try:
        from fresh_scanner import load_channel_config
        config = load_channel_config()
        min_bundling_percentage = config.get("min_total_bundling_percentage", 5)
        print(f"[Bubble Map Direct] Min bundling percentage threshold: {min_bundling_percentage}%")

        # Step 1: Wait for the bubble map to fully load or show "No holders"
        print("[Bubble Map Direct] Waiting for right-panel or 'No holders' message...")
        try:
            await page.wait_for_function('''
                () => {
                    return !!document.querySelector('div[data-testid="right-panel"]#right-panel') ||
                           !!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('No holders found')) ||
                           !!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('Page not found'));
                }
            ''', timeout=30000)

            # Check for non-loaded states
            status = await page.evaluate('''
                () => {
                    if (!!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('No holders found'))) return "no_holders";
                    if (!!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('Page not found'))) return "not_found";
                    return "loaded";
                }
            ''')

            if status in ("no_holders", "not_found"):
                reason = "No holders found" if status == "no_holders" else "Page not found"
                print(f"[Bubble Map Direct] ⚠️ {reason} for {token_name}. Proceeding anyway.")
                return True, 0.0, "none"

            await asyncio.sleep(1)
            print("[Bubble Map Direct] ✅ Found right-panel — bubble map fully loaded")
        except Exception as e:
            if "Timeout" in str(e):
                print(f"[Bubble Map Direct] ⚠️ Timeout waiting for Bubble Map load: {e}. Retrying.")
                return "network_retry", 0.0, "single"
            print(f"[Bubble Map Direct] ❌ Error waiting for Bubble Map load: {e}")
            return False, 0.0, "single"

        # Step 2: Click the expand icon
        print("[Bubble Map Direct] Clicking expand icon...")
        try:
            await page.wait_for_selector('svg[viewBox="0 0 24 24"]', timeout=10000)
            await page.evaluate('''
                () => {
                    const svgs = document.querySelectorAll('svg[viewBox="0 0 24 24"]');
                    for (let svg of svgs) {
                        const paths = svg.querySelectorAll('path');
                        for (let path of paths) {
                            const d = path.getAttribute('d');
                            if (d && d.includes('M9 22H15') && d.includes('M13.5 10.4995')) {
                                const button = svg.closest('button');
                                if (button) {
                                    button.click();
                                    return true;
                                }
                            }
                        }
                    }
                    return false;
                }
            ''')
            await asyncio.sleep(2)
            print("[Bubble Map Direct] ✅ Clicked expand icon")
        except Exception as e:
            if "Timeout" in str(e) or "Target closed" in str(e):
                print(f"[Bubble Map Direct] ⚠️ Network issue/Timeout clicking expand icon: {e}")
                return "network_retry", 0.0, "single"
            print(f"[Bubble Map Direct] ❌ Error clicking expand icon: {e}")
            return False, 0.0, "single"

        # Step 3: Wait for map nodes to fully render
        print("[Bubble Map Direct] Waiting for map nodes to render...")
        try:
            await page.wait_for_selector('button[aria-label^="Remove All"]', timeout=30000)
            print("[Bubble Map Direct] ✅ Map nodes rendered")
        except Exception as e:
            print(f"[Bubble Map Direct] ⚠️ Map nodes did not render in time: {e}. Retrying.")
            return "network_retry", 0.0, "single"

        # Step 4: Uncheck visibility checkboxes
        print("[Bubble Map Direct] Unchecking visibility checkboxes...")
        checkbox_ids = ["contractVisibilityCheckbox", "cexVisibilityCheckbox", "dexVisibilityCheckbox"]

        async def uncheck_checkbox(checkbox_id):
            try:
                checkbox = await page.wait_for_selector(f'input#{checkbox_id}', timeout=10000)
                is_checked = await checkbox.is_checked()
                is_indeterminate = await page.evaluate('(el) => el.indeterminate', checkbox)
                if not is_checked and not is_indeterminate:
                    return
                label = await page.query_selector(f'label:has(input#{checkbox_id})')
                if label:
                    await label.click()
                else:
                    await checkbox.click()
            except Exception:
                pass

        await asyncio.gather(*[uncheck_checkbox(cb_id) for cb_id in checkbox_ids])

        # Step 5: Analyze rows
        print("[Bubble Map Direct] Scrolling and analyzing rows...")
        passed, max_bundle_pct, max_bundle_source = await analyze_bubble_map_rows(
            page, min_bundling_percentage, token_name
        )
        return passed, max_bundle_pct, max_bundle_source

    except Exception as e:
        print(f"[Bubble Map Direct] ❌ Error in bubble map analysis: {e}")
        return False, 0.0, "single"


async def analyze_bubble_map_rows(frame, min_bundling_percentage, token_name):
    """
    Scroll through the bubble map list and check for bundling violations.
    """
    scroller_selector = 'div[data-testid="virtuoso-scroller"]'
    try:
        await frame.wait_for_selector(scroller_selector, timeout=10000, state='visible')
        scroller = await frame.query_selector(scroller_selector)
        if not scroller: return False, 0.0, "single"

        seen_row_numbers = set()
        color_groups = {}
        color_to_group = {}
        next_group_number = 1
        last_found_row_number = 0
        consecutive_no_new_rows = 0
        last_numbered_row_found = False
        max_single_pct = 0.0
        max_group_pct = 0.0

        max_scrolls = 200
        scroll_step = 1000
        scroll_count = 0

        while scroll_count < max_scrolls:
            list_items = await frame.query_selector_all('li.MuiListItem-root')
            new_rows = False
            for item in list_items:
                try:
                    num_elem = await item.query_selector('span.MuiTypography-body2.css-jlmh9i')
                    if not num_elem: continue
                    match = re.search(r'#(\d+)', await num_elem.inner_text())
                    if not match: continue
                    row_num = int(match.group(1))
                    if row_num in seen_row_numbers: continue
                    seen_row_numbers.add(row_num)
                    last_found_row_number = max(last_found_row_number, row_num)
                    new_rows = True

                    # Ignore list
                    label_elem = await item.query_selector('p[aria-label]')
                    if label_elem:
                        label = (await label_elem.get_attribute('aria-label') or "").lower()
                        if any(t in label for t in ['pumpswap', 'pump.fun', 'raydium']): continue

                    # Pct
                    pct_elem = await item.query_selector('span.MuiTypography-body2.css-6un1d7')
                    if not pct_elem: continue
                    pct_match = re.search(r'([\d.]+)%', await pct_elem.inner_text())
                    if not pct_match: continue
                    pct = float(pct_match.group(1))

                    if pct > max_single_pct:
                        max_single_pct = pct
                    if pct >= min_bundling_percentage:
                        return False, pct, "single"

                    # Color
                    icon_boxes = await item.query_selector_all('div.MuiBox-root')
                    border_color = None
                    for box in icon_boxes:
                        color = await frame.evaluate('''(el) => {
                            const s = window.getComputedStyle(el);
                            const b = s.border || s.borderTop || '';
                            const m = b.match(/rgb[a]?\\s*\\([^)]+\\)/);
                            if (m) {
                                const c = m[0].replace(/\\s/g, '').toLowerCase().match(/rgb\\([^)]+\\)/)[0];
                                if (c !== 'rgb(77,87,115)') return c;
                            }
                            return null;
                        }''', box)
                        if color:
                            border_color = color
                            break

                    if border_color:
                        if border_color not in color_groups:
                            color_groups[border_color] = []
                            color_to_group[border_color] = next_group_number
                            next_group_number += 1
                        color_groups[border_color].append({'percentage': pct})
                except: continue

            for color, rows in color_groups.items():
                total_pct = sum(r['percentage'] for r in rows)
                if total_pct > max_group_pct:
                    max_group_pct = total_pct
                if total_pct >= min_bundling_percentage:
                    return False, total_pct, "group"

            if new_rows: consecutive_no_new_rows = 0
            else: consecutive_no_new_rows += 1
            if last_found_row_number > 0 and consecutive_no_new_rows >= 3: break

            await frame.evaluate(f"() => {{ const s = document.querySelector('{scroller_selector}'); if (s) s.scrollTop += {scroll_step}; }}")
            await asyncio.sleep(0.05)
            scroll_count += 1

        overall_max = max(max_single_pct, max_group_pct)
        source = "single" if overall_max == max_single_pct else "group"
        return True, overall_max, source
    except Exception as e:
        print(f"[Bubble Map] ❌ Error: {e}")
        return False, 0.0, "single"


async def perform_token_analysis(page, token_data):
    """
    Main analysis flow:
    1. Liquidity Check (RugCheck API)
    2. Bubble Map Check (Direct Bubblemaps URL)
    3. Grok Narrative Check
    4. Data Extraction (DexScreener API)
    """
    token_name = token_data.get("name", "?")
    token_link = token_data.get("link", "")
    pair_address = extract_pair_address(token_link)

    # 1. Liquidity check via API (RugCheck)
    print(f"\n[Analyze] Token {token_name} - {token_link}")
    liq_passed, lp_pct, risks = check_liquidity_lock_via_api(token_data)
    if liq_passed == "network_retry":
        return "network_retry", None, None

    if not liq_passed:
        print(f"❌ Liquidity lock is {lp_pct}% for {token_name} (need >= 99%) - {token_link}")
        return False

    # 2. Bubble Map analysis (Navigate directly to Bubblemaps iframe URL)
    bubble_url = f"https://iframe.bubblemaps.io/solana/token/{pair_address}"
    print(f"[Bubble Map] Navigating directly to: {bubble_url}")
    try:
        await page.goto(bubble_url, timeout=60000)
    except Exception as e:
        print(f"[Bubble Map] ⚠️ Navigation error: {e}")
        return "network_retry", None, None

    passed, max_bundle_pct, max_bundle_source = await analyze_bubble_map_direct(page, token_data)
    if passed == "network_retry":
        return "network_retry", None, None
    if not passed:
        print(f"❌ {token_name} FAILED bundling check: {max_bundle_pct}% ({max_bundle_source})")
        return False

    # 3. Grok Narrative Check
    grok_decision = None
    grok_explanation = None
    print(f"[Grok] Starting evaluation for {token_name}...")
    try:
        grok_decision, grok_explanation = await check_token_trend_with_grok(
            page,
            token_name=token_name,
            token_link=token_link,
            system_prompt=DIP_GROK_RULES,
        )
    except Exception as e:
        print(f"[Grok] ❌ Exception: {e}")
        grok_decision = None

    if grok_decision in ("rate_limit", "grok_error"):
        print(f"[Grok] ⚠️ {grok_decision} for {token_name}")
        # Need to return enough data to allow retry on another profile
        return (grok_decision, None, None, max_bundle_pct, max_bundle_source)

    if grok_decision is not True:
        print(f"❌ Token {token_name} FAILED Grok narrative check")
        return False

    # 4. Success — Extract latest data via API
    print(f"✅ {token_name} PASSED all checks! Finalizing...")
    liquidity_text, market_cap_text = extract_latest_liq_mcap_via_api(token_data)

    mc_text = market_cap_text or "N/A"
    liq_text = liquidity_text or "N/A"
    highest_bundle_text = f"{max_bundle_pct:.2f}%({max_bundle_source})" if max_bundle_pct else "N/A"

    narrative_line = ""
    stored_narrative = None
    if grok_explanation:
        expl = grok_explanation.strip()
        full_expl = expl[:697] + "..." if len(expl) > 700 else expl
        narrative_line = f"Grok: <i>{html.escape(full_expl)}</i>\n"
        stored_narrative = expl

    # Telegram notification
    message = (
        "🔄 Dip token found:\n"
        f"<b>{token_name}</b>\n"
        f"Liquidity: <b>{liq_text}</b>\n"
        f"Mcap: <b>{mc_text}</b>\n"
        f"Highest bundle: <b>{highest_bundle_text}</b>\n"
        f"{narrative_line}\n\n"
        f"{token_link}"
    )
    try:
        send_to_telegram(message)
    except Exception as e:
        print(f"⚠️ Telegram failure: {e}")

    # Server sync
    try:
        post_token_to_server(
            token_name=token_name,
            token_link=token_link,
            liquidity=liq_text,
            market_cap=mc_text,
            narrative=stored_narrative,
            timestamp=token_data.get("timestamp"),
            is_dumped=token_data.get("is_dumped", False),
            source="dip",
            pair_address=pair_address,
        )
    except Exception as e:
        print(f"⚠️ Server sync failed: {e}")

    # Local save
    save_dead_valid_token(
        token_name,
        token_link,
        address=token_data.get("address"),
        pair_address=pair_address,
        liquidity=liq_text,
        market_cap=mc_text,
        narrative=stored_narrative,
    )

    return True


async def analyze_token(page, token_data, index, total):
    """
    Called by run_dead_scanner for each token.
    Now just a wrapper for perform_token_analysis.
    """
    token_name = token_data.get("name", "?")
    print(f"\n[Analyze] Token {token_name} [{index}/{total}]")
    return await perform_token_analysis(page, token_data)


# --- Complete token processing after successful Grok retry ---

async def complete_token_processing_dead(page, token_data, liquidity_text, market_cap_text,
                                          max_bundle_pct, max_bundle_source, grok_explanation):
    """
    Complete token processing after a successful Grok retry.
    Used when Grok succeeds on a retry profile — navigates back to the token page,
    sends Telegram alert, posts to server, and saves locally.
    """
    print(f"[DipScanner] ✅ {token_data['name']} PASSED all checks (retry path)")

    # Navigate back to token page (we're on x.com/i/grok after Grok retry)
    await navigate_and_wait_for_token_page(page, token_data.get("link", ""))

    # Re-extract if we don't have values from before
    if not liquidity_text or not market_cap_text:
        liquidity_text, market_cap_text = await extract_liquidity_and_market_cap(page)

    mc_text = market_cap_text or "N/A"
    liq_text = liquidity_text or "N/A"
    if isinstance(max_bundle_pct, (int, float)):
        highest_bundle_text = f"{max_bundle_pct:.2f}%({max_bundle_source or 'single'})"
    else:
        highest_bundle_text = "N/A"

    narrative_line = ""
    stored_narrative = None
    if grok_explanation:
        expl = grok_explanation.strip()
        full_expl = expl[:697] + "..." if len(expl) > 700 else expl
        narrative_line = f"Grok: <i>{html.escape(full_expl)}</i>\n"
        stored_narrative = expl

    message = (
        "🔄 Dip token found:\n"
        f"<b>{token_data['name']}</b>\n"
        f"Liquidity: <b>{liq_text}</b>\n"
        f"Mcap: <b>{mc_text}</b>\n"
        f"Highest bundle: <b>{highest_bundle_text}</b>\n"
        f"{narrative_line}\n\n"
        f"{token_data.get('link', '')}"
    )
    try:
        send_to_telegram(message)
    except Exception as e:
        print(f"[DipScanner] WARNING: Telegram failed: {e}")
async def complete_token_processing_dead(page, token_data, liquidity_text, market_cap_text,
                                          max_bundle_pct, max_bundle_source, grok_explanation):
    """
    Complete token processing after a successful Grok retry.
    """
    print(f"[DipScanner] ✅ {token_data['name']} PASSED all checks (retry path)")

    # Re-extract via API if we don't have values from before
    if not liquidity_text or not market_cap_text:
        liquidity_text, market_cap_text = extract_latest_liq_mcap_via_api(token_data)

    mc_text = market_cap_text or "N/A"
    liq_text = liquidity_text or "N/A"
    highest_bundle_text = f"{max_bundle_pct:.2f}%({max_bundle_source})" if max_bundle_pct else "N/A"

    narrative_line = ""
    stored_narrative = None
    if grok_explanation:
        expl = grok_explanation.strip()
        full_expl = expl[:697] + "..." if len(expl) > 700 else expl
        narrative_line = f"Grok: <i>{html.escape(full_expl)}</i>\n"
        stored_narrative = expl

    message = (
        "🔄 Dip token found:\n"
        f"<b>{token_data['name']}</b>\n"
        f"Liquidity: <b>{liq_text}</b>\n"
        f"Mcap: <b>{mc_text}</b>\n"
        f"Highest bundle: <b>{highest_bundle_text}</b>\n"
        f"{narrative_line}\n\n"
        f"{token_data.get('link', '')}"
    )
    try:
        send_to_telegram(message)
    except Exception as e:
        print(f"[DipScanner] WARNING: Telegram failed: {e}")

    try:
        post_token_to_server(
            token_name=token_data["name"],
            token_link=token_data.get("link", ""),
            liquidity=liq_text,
            market_cap=mc_text,
            narrative=stored_narrative,
            timestamp=token_data.get("timestamp"),
            is_dumped=token_data.get("is_dumped", False),
            source="dip",
            pair_address=extract_pair_address(token_data.get("link", "")),
        )
    except Exception as e:
        print(f"[DipScanner] WARNING: Server sync failed: {e}")

    try:
        save_dead_valid_token(
            token_data["name"],
            token_data.get("link", ""),
            address=token_data.get("address"),
            pair_address=extract_pair_address(token_data.get("link", "")),
            liquidity=liq_text,
            market_cap=mc_text,
            narrative=stored_narrative,
        )
    except Exception as e:
        print(f"[DipScanner] WARNING: Save failed: {e}")


# --- Grok-only retry helper ---

async def retry_grok_only_dead(grok_page, token_data, liquidity_text=None, market_cap_text=None):
    """
    Retry only the Grok check on a different profile page.
    Skips DexScreener analysis since we already have liquidity/mcap.
    """
    print(f"[DipScanner][Grok][Retry] Running Grok-only retry for {token_data['name']}...")
    try:
        grok_decision, grok_explanation = await check_token_trend_with_grok(
            grok_page,
            token_name=token_data["name"],
            token_link=token_data.get("link", ""),
            system_prompt=DIP_GROK_RULES,
        )
        if grok_decision in ("rate_limit", "grok_error"):
            return grok_decision, None
        return grok_decision, grok_explanation
    except Exception as e:
        print(f"[DipScanner][Grok][Retry] ❌ Exception: {e}")
        return None, None


# --- Main entry point (called from fresh_scanner.py) ---

async def run_dead_scanner(page, context, pw, profile_names, profile_idx):
    """
    Entry point called by fresh_scanner.py during its 5-minute idle window.
    Runs the dead token scan for at most 5 minutes then returns.
    Always returns (page, context, profile_idx) so the main bot gets correct browser state.
    """
    print("\n" + "=" * 60)
    print("[DipScanner] 🔄 Starting dead token scan (5-minute window)...")
    print("=" * 60)

    start_time = time.time()
    MAX_RUNTIME = 295  # 295 seconds (~5 min), leaving 5s buffer

    processed_tokens = load_dead_valid_tokens()
    failed_tokens = load_dead_failed_tokens()
    pending_tokens = load_dead_pending_tokens()
    consecutive_crash_count = 0

    def time_remaining():
        return MAX_RUNTIME - (time.time() - start_time)

    if time_remaining() <= 0:
        print("[DipScanner] No time remaining. Exiting.")
        return page, context, profile_idx

    # Scrape DexScreener for dip tokens
    if not pending_tokens:
        print("[DipScanner] Scraping DexScreener for dip tokens...")
        try:
            await page.goto(BASE_URL)
            await page.wait_for_selector("div.ds-dex-table", timeout=15000)
            await asyncio.sleep(2)
            token_list = await load_all_tokens(page, BASE_URL, QUERY_STRING)
            print(f"[DipScanner] Extracted {len(token_list)} tokens from DexScreener.")
            pending_tokens = [
                t for t in token_list
                if t["link"] not in processed_tokens
                and t["link"] not in failed_tokens
            ]
            removed = len(token_list) - len(pending_tokens)
            print(f"[DipScanner] After dedup: {len(pending_tokens)} new tokens ({removed} already seen).")
            save_dead_pending_tokens(pending_tokens)
        except Exception as e:
            print(f"[DipScanner] ❌ Failed to scrape DexScreener: {e}")
            return page, context, profile_idx

    if not pending_tokens:
        print("[DipScanner] No new dip tokens found. Exiting scan.")
        return page, context, profile_idx

    print(f"\n[DipScanner] ====STARTING DIP TOKEN ANALYSIS====")
    print(f"[DipScanner] Processing {len(pending_tokens)} pending tokens (time left: {time_remaining():.0f}s)...")

    for i, token_data in enumerate(pending_tokens[:], start=1):
        if time_remaining() <= 10:
            print(f"[DipScanner] ⏱️ Time limit reached after {i-1} tokens.")
            break

        # Pre-filter
        passes, pre_info = pre_filter_token_via_api(token_data)
        if not passes:
            print(f"[DipScanner][PreFilter] ❌ {token_data['name']} REJECTED: {pre_info}")
            pending_tokens.remove(token_data)
            save_dead_pending_tokens(pending_tokens)
            save_dead_failed_token(token_data["name"], token_data["link"], reason=f"pre_filter: {pre_info}")
            failed_tokens.add(token_data["link"])
            continue
        token_data["is_dumped"] = pre_info.get("is_dumped", False) if isinstance(pre_info, dict) else False

        result = await analyze_token(page, token_data, i, len(pending_tokens))

        # --- Page crash: relaunch browser, rotate profile after 3 crashes ---
        if result == "page_crash":
            consecutive_crash_count += 1
            print(f"[DipScanner] ⚠️ Page crash ({consecutive_crash_count} consecutive) on {token_data['name']}")
            if consecutive_crash_count >= 3 and len(profile_names) > 1:
                profile_idx = (profile_idx + 1) % len(profile_names)
                new_profile = profile_names[profile_idx]
                print(f"[DipScanner] 🔄 3 crashes — switching to profile '{new_profile}'")
                consecutive_crash_count = 0
            try:
                await context.close()
            except Exception:
                pass
            current_profile = profile_names[profile_idx]
            try:
                context, page = await launch_profile_context(pw, current_profile)
                print(f"[DipScanner] ✅ Relaunched browser with profile '{current_profile}'")
            except Exception as relaunch_err:
                print(f"[DipScanner] ❌ Relaunch failed: {relaunch_err}. Waiting 30s...")
                await asyncio.sleep(30)
            break  # Token stays in pending for next cycle

        # --- Network timeout: leave in pending, retry next loop ---
        if result == "network_retry":
            print(f"[DipScanner] Network retry for {token_data['name']} — leaving in pending.")
            continue

        # --- Grok retry: rate_limit or grok_error → switch profile and retry ---
        liquidity_text = None
        market_cap_text = None
        max_bundle_pct = None
        max_bundle_source = None
        retry_needed = False

        if isinstance(result, tuple) and len(result) >= 3:
            retry_code = result[0]
            liquidity_text = result[1]
            market_cap_text = result[2]
            if len(result) >= 5:
                max_bundle_pct = result[3]
                max_bundle_source = result[4]
            retry_needed = retry_code in ("rate_limit", "grok_error") or retry_code is None
        elif result is None:
            retry_needed = True

        if retry_needed and len(profile_names) > 1:
            for attempt in range(1, len(profile_names)):
                if time_remaining() <= 10:
                    break
                profile_idx = (profile_idx + 1) % len(profile_names)
                prof = profile_names[profile_idx]
                retry_reason = (
                    "rate limit" if (isinstance(result, tuple) and result[0] == "rate_limit")
                    else "grok error" if (isinstance(result, tuple) and result[0] == "grok_error")
                    else "no decision"
                )
                print(f"[DipScanner][Grok][Retry] {retry_reason.capitalize()}. Retrying with '{prof}' ({attempt}/{len(profile_names)-1})...")
                try:
                    await context.close()
                except Exception:
                    pass
                context, new_page = await launch_profile_context(pw, prof)
                page = new_page

                grok_decision, grok_explanation = await retry_grok_only_dead(
                    page, token_data, liquidity_text, market_cap_text
                )

                if grok_decision in ("rate_limit", "grok_error"):
                    result = (grok_decision, liquidity_text, market_cap_text, max_bundle_pct, max_bundle_source)
                    continue

                if grok_decision is not None:
                    if grok_decision is True:
                        # Complete token processing
                        await complete_token_processing_dead(
                            page, token_data, liquidity_text, market_cap_text,
                            max_bundle_pct, max_bundle_source, grok_explanation
                        )
                        result = True
                    else:
                        result = False
                    print("=" * 50)
                    print("=" * 50)
                    break
                else:
                    result = (None, liquidity_text, market_cap_text, max_bundle_pct, max_bundle_source)

            # If all profiles exhausted and still failing
            if isinstance(result, tuple) and result[0] in ("rate_limit", "grok_error", None):
                result = False

        # --- Terminal result ---
        pending_tokens.remove(token_data)
        save_dead_pending_tokens(pending_tokens)

        if result is True:
            processed_tokens.add(token_data["link"])
            consecutive_crash_count = 0
            if not retry_needed:
                print("=" * 50)
                print("=" * 50)
        else:
            reason = "grok_no_decision" if result is None else None
            save_dead_failed_token(token_data["name"], token_data["link"], reason=reason)
            failed_tokens.add(token_data["link"])
            consecutive_crash_count = 0
            if not retry_needed:
                print("=" * 50)
                print("=" * 50)

    print(f"\n[DipScanner] ✅ Dead scanner cycle complete. Returning to main bot.")
    print("=" * 60 + "\n")
    return page, context, profile_idx


if __name__ == "__main__":
    # Standalone mode for testing
    async def _standalone():
        from playwright.async_api import async_playwright
        import json as _json
        # Read first profile from channels_config.json — no hardcoded name
        try:
            with open("json_files/channels_config.json", "r") as _f:
                _cfg = _json.load(_f)
            _names = _cfg[0].get("name", []) if _cfg else []
            profile = _names[0] if _names else None
        except Exception:
            profile = None
        if not profile:
            print("[DipScanner] ❌ No profiles found in channels_config.json — cannot run standalone.")
            return
        profile_names = _cfg[0].get("name", [profile])
        profile_idx = 0
        print(f"[DipScanner] 🚀 Standalone: using profile '{profile}' (pool: {profile_names})")
        async with async_playwright() as pw:
            user_data_dir = os.path.abspath(f"playwright/playwright-chrome-profile-{profile}")
            context = await pw.chromium.launch_persistent_context(
                user_data_dir,
                headless=False,
                executable_path=CHROME_EXE,
                ignore_default_args=["--enable-automation"],
            )
            page = context.pages[0] if context.pages else await context.new_page()
            page, context, profile_idx = await run_dead_scanner(
                page, context, pw, profile_names, profile_idx
            )
            await context.close()

    asyncio.run(_standalone())
