#!/usr/bin/env python3
import os
import time
import asyncio
import json
import html
from filelock import FileLock
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from typing import Optional
import re
import requests

from utils.grok_brain import check_token_trend_with_grok
from utils.server_sync import post_token_to_server
from dip_scanner import run_dead_scanner
from onchain_price_monitor import OnChainPriceMonitor
from utils.candle_tracker import CandleTracker

# --- Telegram configuration ---
BOT_TOKEN = None
CHAT_IDS = []


def load_telegram_config():
    """
    Load Telegram BOT_TOKEN and CHAT_IDS from environment or .env file.
    CHAT_IDS should be a comma-separated list.
    """
    global BOT_TOKEN, CHAT_IDS

    # Already loaded
    if BOT_TOKEN and CHAT_IDS:
        return

    bot_token = os.getenv("BOT_TOKEN")
    chat_ids_raw = os.getenv("CHAT_IDS")

    # Fallback: try to read from .env file in current directory
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
        print("[WARNING] Telegram config missing (BOT_TOKEN / CHAT_IDS). Notifications will be disabled.")
        BOT_TOKEN = None
        CHAT_IDS = []
        return

    BOT_TOKEN = bot_token
    CHAT_IDS = [cid.strip() for cid in chat_ids_raw.split(",") if cid.strip()]


def send_to_telegram(message: str):
    """
    Send a message to all configured Telegram chat IDs.
    Uses BOT_TOKEN and CHAT_IDS from environment or .env.
    """
    load_telegram_config()
    if not BOT_TOKEN or not CHAT_IDS:
        return

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    for chat_id in CHAT_IDS:
        payload = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        try:
            response = requests.post(url, data=payload, timeout=10)
            if response.status_code == 200:
                print(f"[INFO] Telegram: Message sent to chat {chat_id}.")
            else:
                error_details = ""
                try:
                    error_body = response.text
                    if error_body:
                        error_details = f" - Response: {error_body[:200]}"
                except:
                    pass
                print(f"[ERROR] Telegram: Failed to send message to chat {chat_id}. Status: {response.status_code}{error_details}")
        except Exception as e:
            print(f"[EXCEPTION] Telegram: Exception while sending to chat {chat_id}: {e}")


# ← scrape at most this many dexscreener pages; set to None for no limit
PAGE_LIMIT = 10   # = None if you want to scrap all available pages

# --- Playwright session setup ---
def get_user_data_root(channel_name):
    return os.path.abspath(f"playwright/playwright-chrome-profile-{channel_name}")

CHROME_EXE = r"C:\Program Files\Google\Chrome\Application\chrome.exe"


async def launch_profile_context(pw, profile_name: str):
    """
    Launch a persistent Chrome context for a specific Playwright profile name.
    Returns (context, page).
    """
    user_data_dir = get_user_data_root(profile_name)
    context = await pw.chromium.launch_persistent_context(
        user_data_dir,
        headless=False,
        executable_path=CHROME_EXE,
        ignore_default_args=["--enable-automation"],
    )
    page = context.pages[0] if context.pages else await context.new_page()
    return context, page





async def load_all_tokens(page, base_url, query_string):
    """
    Extract token data from window.__SERVER_DATA — the React hydration state
    embedded by DexScreener's server-side rendering.  This is instant and does
    NOT require the UI to render, scroll, or wait for TradingView/Cloudflare.
    """
    all_tokens = []
    current_page = 1
    while True:
        if PAGE_LIMIT is not None and current_page > PAGE_LIMIT:
            print(f"Reached max page {PAGE_LIMIT}, stopping.")
            break
        if current_page == 1:
            current_url = base_url
        else:
            current_url = f"https://dexscreener.com/page-{current_page}?{query_string}"
        print(f"Processing page {current_page} with URL: {current_url}")
        # Navigate without waiting for the full DOM to load to avoid timeouts
        await page.goto(current_url, wait_until="commit")

        # Wait specifically for the server hydration data to be available
        try:
            await page.wait_for_function('() => window.__SERVER_DATA !== undefined', timeout=30000)
            await asyncio.sleep(1)
        except Exception:
            print(f"Page {current_page} - Timeout waiting for __SERVER_DATA. Proceeding anyway.")

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
            print(f"Page {current_page} - No tokens found in __SERVER_DATA. Pagination complete.")
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
                }
                all_tokens.append(token)
            except Exception as e:
                print(f"Error extracting token from __SERVER_DATA pair: {e}")

        print(f"Page {current_page} - Extracted {len(pairs)} tokens from __SERVER_DATA.")
        current_page += 1
    return all_tokens


def load_valid_tokens():
    """
    Load previously validated tokens from disk (JSON list).

    Returns:
      set[str]: set of token links
    """
    valid_links = set()

    # Preferred format
    valid_json_path = os.path.join("json_files", "valid_tokens.json")
    if os.path.exists(valid_json_path):
        try:
            with open(valid_json_path, "r", encoding="utf-8") as f:
                data = json.load(f) or []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        link = item.get("link")
                        if link:
                            valid_links.add(link)
                    elif isinstance(item, str):
                        # allow list of links
                        valid_links.add(item)
            return valid_links
        except Exception as e:
            print(f"[WARNING] Could not read {valid_json_path}: {e}")

    # Backwards compatibility: migrate from valid_tokens.txt if present
    valid_txt_path = os.path.join("json_files", "valid_tokens.txt")
    if os.path.exists(valid_txt_path):
        try:
            migrated = []
            with open(valid_txt_path, "r", encoding="utf-8") as f:
                for line in f:
                    parts = line.strip().split(": ", 1)
                    if len(parts) == 2:
                        migrated.append({"name": parts[0], "link": parts[1]})
                        valid_links.add(parts[1])
            # Write migrated JSON once
            if migrated:
                with open(valid_json_path, "w", encoding="utf-8") as f:
                    json.dump(migrated, f, indent=2)
        except Exception as e:
            print(f"[WARNING] Could not migrate valid_tokens.txt -> valid_tokens.json: {e}")

    return valid_links


def save_valid_token(token_name, token_link, address=None, pair_address=None, liquidity=None, market_cap=None, narrative=None):
    """
    Append a newly validated token to valid_tokens.json (deduped by link).
    """
    existing = []
    seen = set()
    valid_json_path = os.path.join("json_files", "valid_tokens.json")
    try:
        if os.path.exists(valid_json_path):
            with open(valid_json_path, "r", encoding="utf-8") as f:
                data = json.load(f) or []
                if isinstance(data, list):
                    existing = data
    except Exception:
        existing = []

    # Build set of existing links
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
        "timestamp": int(time.time()),
    }
    # Optionally enrich with on-chain / narrative context
    if liquidity is not None:
        entry["liquidity"] = liquidity
    if market_cap is not None:
        entry["market_cap"] = market_cap
    if narrative is not None:
        entry["narrative"] = narrative

    existing.append(entry)
    try:
        os.makedirs("json_files", exist_ok=True)
        with open(valid_json_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not write {valid_json_path}: {e}")


def load_failed_tokens():
    """
    Load previously failed tokens from disk (JSON list).

    Returns:
      set[str]: set of token links that have already failed checks
    """
    failed_links = set()

    failed_json_path = os.path.join("json_files", "failed_tokens.json")
    if os.path.exists(failed_json_path):
        try:
            with open(failed_json_path, "r", encoding="utf-8") as f:
                data = json.load(f) or []
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        link = item.get("link")
                        if link:
                            failed_links.add(link)
                    elif isinstance(item, str):
                        # allow list of links
                        failed_links.add(item)
        except Exception as e:
            print(f"[WARNING] Could not read {failed_json_path}: {e}")

    return failed_links


def save_failed_token(token_name, token_link, reason: Optional[str] = None):
    """
    Append a newly failed token to failed_tokens.json (deduped by link).
    """
    existing = []
    seen = set()
    failed_json_path = os.path.join("json_files", "failed_tokens.json")
    try:
        if os.path.exists(failed_json_path):
            with open(failed_json_path, "r", encoding="utf-8") as f:
                data = json.load(f) or []
                if isinstance(data, list):
                    existing = data
    except Exception:
        existing = []

    # Build set of existing links
    for item in existing:
        if isinstance(item, dict) and item.get("link"):
            seen.add(item["link"])
        elif isinstance(item, str):
            seen.add(item)

    if token_link in seen:
        return

    failed_entry = {
        "name": token_name,
        "link": token_link,
        "timestamp": int(time.time()),
    }
    if reason:
        failed_entry["reason"] = reason

    existing.append(failed_entry)
    try:
        os.makedirs("json_files", exist_ok=True)
        with open(failed_json_path, "w", encoding="utf-8") as f:
            json.dump(existing, f, indent=2)
    except Exception as e:
        print(f"[WARNING] Could not write {failed_json_path}: {e}")

def load_pending_tokens():
    pending_path = os.path.join("json_files", "pending_tokens.json")
    if os.path.exists(pending_path):
        try:
            with open(pending_path, "r", encoding="utf-8") as f:
                tokens = json.load(f)
                return tokens
        except Exception:
            return []
    return []

def load_channel_config(channel_name=None):
    """Load channel configuration from JSON file.

    Searches configs for an entry whose 'name' list contains channel_name.
    If not found, falls back to the FIRST entry in the file (not a synthetic
    default) so we never invent a ghost profile name like 'moleculeTokenFinder'.
    """
    config_path = "json_files/channels_config.json"

    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                configs = json.load(f)

            if not isinstance(configs, list) or not configs:
                raise ValueError("channels_config.json is empty or not a list")

            # If no name given, just return the first entry directly (no search needed)
            if channel_name is None:
                return configs[0]

            # --- Try exact match ---
            for config in configs:
                name_field = config.get("name")
                if isinstance(name_field, list):
                    if channel_name in name_field:
                        return config
                elif name_field == channel_name:
                    return config

            # --- No match found: fall back to first entry with a warning ---
            first = configs[0]
            name_field = first.get("name", [])
            print(
                f"[Config] ⚠️  Channel '{channel_name}' not found in channels_config.json — "
                f"falling back to first entry (profiles: {name_field})"
            )
            return first

        except Exception as e:
            print(f"[Config] Error loading channel config: {e}")

    # Absolute last resort — only reached if the file doesn't exist at all
    print(f"[Config] ❌ channels_config.json not found — using bare defaults")
    return {
        "name": [],
        "min_total_bundling_percentage": 5,
        "last_scraping_timestamp": 0,
        "scrape_gap_hr": 1,
        "active_playwright_profile": None,
    }


def save_channel_config(updated_config, channel_name=None):
    """
    Persist updated channel configuration back to json_files/channels_config.json.
    If channel_name is None, replaces the first entry (the default behaviour).
    """
    config_path = "json_files/channels_config.json"
    configs = []

    if os.path.exists(config_path):
        try:
            with open(config_path, "r") as f:
                existing = json.load(f)
                if isinstance(existing, list):
                    configs = existing
                elif isinstance(existing, dict):
                    configs = [existing]
        except Exception as e:
            print(f"Error reading channel config for save: {e}")

    # If no name given, replace the first entry (there's only one config block)
    if channel_name is None:
        if configs:
            configs[0] = updated_config
        else:
            configs = [updated_config]
    else:
        # Update by matching name
        updated = False
        for idx, cfg in enumerate(configs):
            name_field = cfg.get("name")
            if (isinstance(name_field, list) and channel_name in name_field) or (name_field == channel_name):
                configs[idx] = updated_config
                updated = True
                break
        if not updated:
            configs.append(updated_config)

    try:
        with open(config_path, "w") as f:
            json.dump(configs, f, indent=2)
    except Exception as e:
        print(f"Error saving channel config: {e}")

def save_pending_tokens(tokens):
    os.makedirs("json_files", exist_ok=True)
    temp_filename = os.path.join("json_files", "pending_tokens_temp.json")
    final_filename = os.path.join("json_files", "pending_tokens.json")
    with open(temp_filename, "w", encoding="utf-8") as f:
        json.dump(tokens, f, indent=2)
    os.replace(temp_filename, final_filename)


# Define parse function to extract values from currency text
def parse_amount(amount_text):
    if not amount_text or amount_text.strip() == "-":
        return None

    # Handle **$XXX** format by removing asterisks
    cleaned_text = amount_text.replace('*', '')

    # Remove $ and any commas from the text
    cleaned_text = cleaned_text.replace('$', '').replace(',', '')

    # Match number and any K/M/B suffix
    match = re.search(r'(\d+\.?\d*)([KMB])?', cleaned_text, re.IGNORECASE)
    if not match:
        return None

    value = float(match.group(1))
    if match.group(2):
        multipliers = {'K': 1000, 'M': 1000000, 'B': 1000000000}
        value *= multipliers.get(match.group(2).upper(), 1)

    return value


# --- Base URLs ---
# Strategy: Find tokens BEFORE they trend
# - trendingScoreH1 catches momentum in its first hour (not 6h lagging)
# - maxAge=1 limits to tokens < 1 day old (API pre-filter narrows to <4h)
# - Lower min 24h volume: new tokens haven't accumulated much yet
# - Lower max market cap: earlier entry = lower cap
BASE_URL = "https://dexscreener.com/?rankBy=trendingScoreM5&order=desc&chainIds=solana&dexIds=pumpswap,pumpfun,raydium&minLiq=2000&minMarketCap=1000&maxMarketCap=300000&maxAge=7&max24HChg=300&profile=1&launchpads=1"
QUERY_STRING = "rankBy=trendingScoreM5&order=desc&chainIds=solana&dexIds=pumpswap,pumpfun,raydium&minLiq=2000&minMarketCap=1000&maxMarketCap=300000&maxAge=7&max24HChg=300&profile=1&launchpads=1"
TOKEN_ROW_SELECTOR = "a.ds-dex-table-row"
TOKEN_NAME_SELECTOR = "span.ds-dex-table-row-base-token-symbol"

# --- Bubble Map Configuration ---
NORMAL_BORDER_COLOR = "rgb(77, 87, 115)"  # Normal border color (not considered strange/bundled)

# --- Pre-filter Configuration (DexScreener API) ---
MAX_TOKEN_AGE_HOURS = 7              # Reject tokens older than this (matches DexScreener maxAge=7)
MAX_PUMP_FROM_LAUNCH_PERCENT = 300   # Reject if already pumped >3x from launch (within 2-3x = safe)
MIN_BUY_SELL_RATIO = 0.3             # Reject if buy ratio below this (dump in progress)


def extract_pair_address(token_link):
    """Extract the pair address from a DexScreener URL like https://dexscreener.com/solana/xxxxx"""
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

    If pair_data is missing (e.g. from a saved pending list), fall back to the
    DexScreener API.

    Checks:
      1. Token age  — must be < MAX_TOKEN_AGE_HOURS
      2. Price pump — reject if already pumped too much (buying the top)
      3. Buy/Sell   — reject if heavy selling (dump in progress)

    Returns:
      (True,  info_dict)   -> passes pre-filter
      (False, reason_str)  -> rejected
    """
    token_link = token_data.get("link", "")
    token_name = token_data.get("name", "?")

    # Prefer the __SERVER_DATA pair blob; fall back to API if not present
    pair = token_data.get("pair_data")
    if not pair:
        address = extract_pair_address(token_link)
        if not address:
            print(f"[PreFilter] \u26a0\ufe0f Could not extract address for {token_name}, skipping pre-filter")
            return True, {}
        try:
            api_url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{address}"
            resp = requests.get(api_url, timeout=10)
            data = resp.json()
            pair = data.get("pair") or (data.get("pairs") and data["pairs"][0])
        except Exception as e:
            print(f"[PreFilter] \u26a0\ufe0f API error for {token_name}: {e}")
            return True, {}

    if not pair:
        print(f"[PreFilter] \u26a0\ufe0f No pair data for {token_name}, allowing through")
        return True, {}

    info = {}

    # --- Check 1: Token Age ---
    created_at_ms = pair.get("pairCreatedAt")
    if created_at_ms:
        age_hours = (time.time() * 1000 - created_at_ms) / (1000 * 3600)
        info["age_hours"] = round(age_hours, 1)
        if age_hours > MAX_TOKEN_AGE_HOURS:
            return False, f"Too old: {age_hours:.1f}h (max {MAX_TOKEN_AGE_HOURS}h)"
        print(f"[PreFilter] \u2705 Age: {age_hours:.1f}h old (< {MAX_TOKEN_AGE_HOURS}h)")

    # --- Check 2: Price Pump from Launch (are we buying the top?) ---
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
            return False, f"Already pumped +{from_launch_change:.0f}% from launch ({from_launch_source}) \u2014 max +{MAX_PUMP_FROM_LAUNCH_PERCENT}%"
        print(f"[PreFilter] \u2705 From-launch pump: {from_launch_change:+.0f}% ({from_launch_source})")

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
        dump_label = "\u26a0\ufe0f DUMPED" if is_dumped else "OK"
        print(f"[PreFilter] \u2705 Txns: {buys}B/{sells}S (ratio={buy_ratio:.2f}) [{dump_label}]")
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
        print(f"[RugCheck] \u26a0\ufe0f No token address for {token_data.get('name', '?')}")
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


async def extract_latest_liq_mcap_onchain(token_data):
    """
    Fetch the latest liquidity and market cap directly from the Solana blockchain
    using the OnChainPriceMonitor, ensuring consistency with the server dashboard.
    """
    token_link = token_data.get("link", "")
    token_name = token_data.get("name", "?")
    address = extract_pair_address(token_link)

    if not address:
        print(f"[OnChain] \u26a0\ufe0f Could not extract address for {token_name}")
        return None, None

    try:
        # One-shot fetch from on-chain
        monitor = OnChainPriceMonitor(pair_addresses=[address])
        data = await monitor.fetch_one(address)

        if not data or "error" in data:
            err = data.get("error", "Unknown error")
            print(f"[OnChain] \u26a0\ufe0f Error for {token_name}: {err}")
            return None, None

        mcap = data.get("mcap_usd")
        liq_usd = data.get("liquidity_usd")

        # Format values like the UI does (e.g. $36.5K, $110K)
        def fmt(val):
            if val is None or val == 0:
                return None
            if val >= 1_000_000:
                return f"${val / 1_000_000:.1f}M"
            elif val >= 1_000:
                return f"${val / 1_000:.1f}K"
            else:
                return f"${val:.0f}"

        liq_text = fmt(liq_usd)
        mcap_text = fmt(mcap)

        print(f"[OnChain] {token_name}: Liq={liq_text}, Mcap={mcap_text}")
        return liq_text, mcap_text

    except Exception as e:
        print(f"[OnChain] \u26a0\ufe0f Error for {token_name}: {e}")
        return None, None


async def main():
    processed_tokens = load_valid_tokens()
    failed_tokens = load_failed_tokens()
    pending_tokens = load_pending_tokens()
    # Profile pool — read directly from the first entry in channels_config.json
    channel_config = load_channel_config()
    profile_names = channel_config.get("name") or []
    if isinstance(profile_names, str):
        profile_names = [profile_names]
    profile_names = [p for p in profile_names if isinstance(p, str) and p.strip()]
    if not profile_names:
        raise RuntimeError("No profiles found in channels_config.json — add at least one name.")

    # Prefer starting from the last successful Grok profile if available
    active_profile = channel_config.get("active_playwright_profile")
    if isinstance(active_profile, str) and active_profile in profile_names:
        profile_idx = profile_names.index(active_profile)
    else:
        profile_idx = 0

    async with async_playwright() as pw:
        current_profile = profile_names[profile_idx]
        print(f"\n{'='*60}")
        print(f"[Browser] 🚀 Launching Chrome profile: '{current_profile}'")
        print(f"[Browser] 📋 Profile pool: {profile_names}")
        print(f"[Browser] 📂 Profile dir: playwright/playwright-chrome-profile-{current_profile}")
        print(f"{'='*60}\n")
        context, page = await launch_profile_context(pw, current_profile)
        # Retry initial navigation in case DexScreener is slow to respond
        for _nav_attempt in range(10):
            try:
                # Navigate without waiting for the full DOM to load
                await page.goto(BASE_URL, wait_until="commit")

                # Wait specifically for the server hydration data to be available
                await page.wait_for_function('() => window.__SERVER_DATA !== undefined', timeout=30000)
                await asyncio.sleep(1)
                break  # Success — carry on
            except PlaywrightTimeoutError:
                print(f"[Startup] ⚠️ Timeout loading DexScreener on attempt {_nav_attempt + 1}. Trying to reload page first...")
                try:
                    await page.reload(wait_until="commit")
                    await page.wait_for_function('() => window.__SERVER_DATA !== undefined', timeout=30000)
                    await asyncio.sleep(1)
                    break
                except Exception as reload_err:
                    print(f"[Startup] ⚠️ Reload failed/timed out too. Relaunching browser in 15s...")
                    await asyncio.sleep(15)
                    try:
                        await context.close()
                    except Exception:
                        pass
                    context, page = await launch_profile_context(pw, current_profile)
            except Exception as _nav_err:
                print(f"[Startup] ❌ Unexpected error loading DexScreener: {_nav_err}. Retrying in 15s...")
                await asyncio.sleep(15)
                try:
                    await context.close()
                except Exception:
                    pass
                context, page = await launch_profile_context(pw, current_profile)
        else:
            print("[Startup] ❌ Could not load DexScreener after 10 attempts. Exiting.")
            raise SystemExit(1)
        consecutive_crash_count = 0  # Track back-to-back page crashes
        while True:
            try:
                # Reload config each loop so edits to channels_config.json take effect without restart
                channel_config = load_channel_config()

                # --- Scrape gap logic based on channel config ---
                try:
                    now_ts = int(time.time())
                    last_ts = int(channel_config.get("last_scraping_timestamp", 0) or 0)
                    gap_hr = channel_config.get("scrape_gap_hr", 1) or 1
                    gap_seconds = int(gap_hr * 3600)
                    if now_ts - last_ts >= gap_seconds:
                        print(f"[Scheduler] Scrape gap exceeded ({gap_hr}h). Clearing pending tokens and preparing to rescrape.")
                        pending_tokens = []
                        save_pending_tokens(pending_tokens)
                    else:
                        remaining = gap_seconds - (now_ts - last_ts)
                        print(f"[Scheduler] Next rescrape allowed in ~{remaining // 60} minutes.")
                except Exception as e:
                    print(f"[Scheduler] Warning: could not apply scrape gap logic: {e}")

                if not pending_tokens:
                    # load_all_tokens now navigates internally and reads __SERVER_DATA
                    token_list = await load_all_tokens(page, BASE_URL, QUERY_STRING)
                    total_tokens = len(token_list)
                    print(f"Extracted {total_tokens} tokens from all pages.")
                    pending_tokens = [
                        token
                        for token in token_list
                        if token["link"] not in processed_tokens
                        and token["link"] not in failed_tokens
                    ]
                    remaining_count = len(pending_tokens)
                    print(f"After deduplication: {remaining_count} tokens remaining (removed {total_tokens - remaining_count} already processed/failed).")
                    save_pending_tokens(pending_tokens)

                    # Check if no new tokens after deduplication
                    if not pending_tokens:
                        print(
                            f"[Scheduler] No new tokens found after deduplication "
                            f"(all {total_tokens} tokens already processed/failed)."
                        )

                        # --- Check if Dip Scanner is enabled in config ---
                        if channel_config.get("trigger_dip_scanner", True) is False:
                            print("[Scheduler] ⏭️ DIP SCANNER IS DISABLED (trigger_dip_scanner=false). Waiting 60s...")
                            await asyncio.sleep(60)
                            continue

                        print("\n" + "~" * 60)
                        print("[Scheduler] 🔄 SWITCHING TO: DIP SCANNER")
                        print("~" * 60)
                        # Update last scraping timestamp
                        try:
                            channel_config["last_scraping_timestamp"] = int(time.time())
                            save_channel_config(channel_config, channel_name=channel_config.get("active_playwright_profile"))
                        except Exception as e:
                            print(f"[Scheduler] Warning: could not update last_scraping_timestamp: {e}")
                        page, context, profile_idx = await run_dead_scanner(
                            page, context, pw, profile_names, profile_idx
                        )
                        print("\n" + "~" * 60)
                        print("[Scheduler] ✅ BACK TO: FRESH SCANNER")
                        print("~" * 60 + "\n")
                        continue  # Skip to next iteration

                    # Update last scraping timestamp on every scrape run
                    try:
                        channel_config["last_scraping_timestamp"] = int(time.time())
                        save_channel_config(channel_config)
                    except Exception as e:
                        print(f"[Scheduler] Warning: could not update last_scraping_timestamp: {e}")
                    # Immediately loop back to process the newly scraped pending tokens
                    continue
                else:
                    # Process pending tokens - no need to navigate to BASE_URL first
                    print("\n====STARTING TOKEN ANALYSIS=====")
                    print(f"Processing {len(pending_tokens)} pending tokens...")
                    for i, token_data in enumerate(pending_tokens[:], start=1):
                        # --- Quick API-based pre-filter (no Playwright needed) ---
                        passes_prefilter, pre_info = pre_filter_token_via_api(token_data)
                        if not passes_prefilter:
                            print(f"[PreFilter] \u274c {token_data['name']} REJECTED: {pre_info}")
                            pending_tokens.remove(token_data)
                            save_pending_tokens(pending_tokens)
                            save_failed_token(token_data["name"], token_data["link"], reason=f"pre_filter: {pre_info}")
                            failed_tokens.add(token_data["link"])
                            continue
                        elif pre_info:
                            age_str = f"{pre_info.get('age_hours', '?')}h old"
                            print(f"[PreFilter] \u2705 {token_data['name']} passed ({age_str})")
                        # Store dump status from pre-filter on token_data for downstream use
                        token_data["is_dumped"] = pre_info.get("is_dumped", False) if isinstance(pre_info, dict) else False

                        result = await analyze_token(page, token_data, i, len(pending_tokens))

                        # If navigation failed due to a transient network timeout, keep
                        # the token in the pending list so it can be retried later.
                        if result == "network_retry":
                            print(
                                f"[Network] Token {token_data['name']} hit a navigation timeout; "
                                "leaving in pending_tokens for automatic retry."
                            )
                            continue

                        # If Cloudflare blocked us, stop processing ALL tokens and wait.
                        # The token stays in pending_tokens for retry.
                        # If the browser crashed, relaunch immediately and stop processing
                        # remaining tokens — they'll be retried on the next loop iteration.
                        if result == "page_crash":
                            consecutive_crash_count += 1
                            print(
                                f"[Recovery] ⚠️ Page crash on token {token_data['name']} "
                                f"(consecutive crashes: {consecutive_crash_count}) — "
                                "relaunching browser before retrying."
                            )
                            # After 3 consecutive crashes, assume current profile is problematic
                            # and switch to the next one in the pool before relaunching.
                            if consecutive_crash_count >= 3 and len(profile_names) > 1:
                                profile_idx = (profile_idx + 1) % len(profile_names)
                                new_profile = profile_names[profile_idx]
                                print(
                                    f"[Recovery] 🔄 3 consecutive crashes — switching profile to '{new_profile}' "
                                    f"and resetting crash counter."
                                )
                                consecutive_crash_count = 0
                                try:
                                    cfg = load_channel_config()
                                    cfg["active_playwright_profile"] = new_profile
                                    save_channel_config(cfg)
                                    print(f"[Recovery] ✅ active_playwright_profile updated to '{new_profile}'.")
                                except Exception as cfg_err:
                                    print(f"[Recovery] ⚠️ Could not save new profile: {cfg_err}")
                            try:
                                await context.close()
                            except Exception:
                                pass
                            current_profile = profile_names[profile_idx]
                            try:
                                context, page = await launch_profile_context(pw, current_profile)
                                print(f"[Recovery] ✅ Fresh browser context launched for profile '{current_profile}'.")
                            except Exception as relaunch_err:
                                print(f"[Recovery] ❌ Failed to relaunch browser: {relaunch_err}. Waiting 30s...")
                                await asyncio.sleep(30)
                            # Break out of the token loop; pending_tokens still contains this
                            # token so the next loop iteration will retry it first.
                            break

                        # Handle retry cases (rate_limit or None with metadata)
                        retry_needed = False
                        liquidity_text = None
                        market_cap_text = None
                        max_bundle_pct = None
                        max_bundle_source = None

                        if isinstance(result, tuple) and len(result) >= 3:
                            retry_code = result[0]
                            liquidity_text = result[1]
                            market_cap_text = result[2]
                            if len(result) >= 5:
                                max_bundle_pct = result[3]
                                max_bundle_source = result[4]
                            retry_needed = (retry_code in ("rate_limit", "grok_error") or retry_code is None)
                        elif result is None:
                            retry_needed = True

                        # If Grok failed (rate_limit or no decision), retry with other profiles
                        if retry_needed and len(profile_names) > 1:
                            for attempt in range(1, len(profile_names)):
                                profile_idx = (profile_idx + 1) % len(profile_names)
                                prof = profile_names[profile_idx]
                                retry_reason = (
                                    "rate limit" if (isinstance(result, tuple) and len(result) > 0 and result[0] == "rate_limit")
                                    else "grok error (unable to reply)" if (isinstance(result, tuple) and len(result) > 0 and result[0] == "grok_error")
                                    else "no decision"
                                )
                                print(f"[Grok][Retry] {retry_reason.capitalize()} detected. Retrying with profile '{prof}' ({attempt}/{len(profile_names)-1})...")
                                try:
                                    await context.close()
                                except Exception:
                                    pass
                                # Launch new profile context and update both context and page
                                context, new_page = await launch_profile_context(pw, prof)
                                page = new_page  # Update main page variable so subsequent tokens use new profile
                                # Use Grok-only retry to skip Dexscreener analysis
                                grok_decision, grok_explanation = await retry_grok_only(
                                    page, token_data, liquidity_text, market_cap_text
                                )

                                # Handle rate limit or grok error in retry — try next profile
                                if grok_decision in ("rate_limit", "grok_error"):
                                    result = (grok_decision, liquidity_text, market_cap_text, max_bundle_pct, max_bundle_source)
                                    continue  # Try next profile

                                # If we got a decision, complete the token processing
                                if grok_decision is not None:
                                    # Update active_playwright_profile FIRST (before == demarcator)
                                    try:
                                        cfg = load_channel_config()
                                        cfg["active_playwright_profile"] = prof
                                        save_channel_config(cfg)
                                        print(f"[Grok] ✅ Updated active_playwright_profile to '{prof}'")
                                    except Exception as cfg_err:
                                        print(f"[Grok] ⚠️ Could not update active_playwright_profile: {cfg_err}")

                                    if grok_decision is True:
                                        # Success - complete token processing with stored values
                                        await complete_token_processing(
                                            page, token_data, liquidity_text, market_cap_text,
                                            max_bundle_pct, max_bundle_source, grok_explanation
                                        )
                                        result = True
                                        # profile update already done above; == demarcator goes here
                                        print("=" * 50)
                                        print("=" * 50)
                                        break
                                    else:
                                        print("=" * 50)
                                        print("=" * 50)
                                        result = False
                                        break
                                else:
                                    result = (None, liquidity_text, market_cap_text, max_bundle_pct, max_bundle_source)
                                    # Continue to next profile

                            # After retry loop, if still retry_needed, finalize result
                            if isinstance(result, tuple) and len(result) > 0 and result[0] in ("rate_limit", "grok_error", None):
                                # All profiles exhausted, treat as network timeout to try again later
                                result = "network_retry"
                            elif result is None:
                                result = "network_retry"

                        # If result is network_retry, we don't remove from pending — we keep it for the next run.
                        if result == "network_retry":
                            print(
                                f"[Network] Token {token_data['name']} exhausted Grok profiles / hit timeouts; "
                                "leaving in pending_tokens for automatic retry."
                            )
                            continue

                        # Terminal result (True or False): remove from pending and persist
                        pending_tokens.remove(token_data)
                        save_pending_tokens(pending_tokens)

                        # Update active_playwright_profile whenever we get a successful Grok result (YES or NO)
                        # Only update if we got a definitive result
                        if result is True or result is False:
                            # Avoid double-updating when retry loop already handled this
                            if not retry_needed:
                                try:
                                    success_profile = profile_names[profile_idx]
                                    cfg = load_channel_config()
                                    cfg["active_playwright_profile"] = success_profile
                                    save_channel_config(cfg)
                                    print(f"[Grok] ✅ Updated active_playwright_profile to '{success_profile}'")
                                except Exception as cfg_err:
                                    print(f"[Grok] ⚠️ Could not update active_playwright_profile: {cfg_err}")
                                # == demarcator: profile update is now logged, then separator
                                print("=" * 50)
                                print("=" * 50)

                        if result is True:
                            # Token details are now persisted from perform_token_analysis
                            processed_tokens.add(token_data["link"])
                            consecutive_crash_count = 0  # Successful result resets crash streak
                        else:
                            # Record failures so they are not reprocessed on future scrapes
                            reason = "failed_checks"
                            save_failed_token(token_data["name"], token_data["link"], reason=reason)
                            failed_tokens.add(token_data["link"])
                            consecutive_crash_count = 0  # Normal failure also resets crash streak
                print("Cycle complete. Waiting for 60 seconds before next check...")
                await asyncio.sleep(60)
            except Exception as e:
                error_str = str(e)
                is_page_crash = "Page crashed" in error_str or "Target closed" in error_str or "Session closed" in error_str
                if is_page_crash:
                    print(f"Error detected: {error_str}. Retrying in 15 seconds...")
                    print("[Recovery] ⚠️ Page crash detected — the current browser context is dead.")
                    print("[Recovery] Closing broken context and relaunching a fresh browser...")
                    await asyncio.sleep(15)
                    try:
                        await context.close()
                    except Exception:
                        pass  # Context may already be gone
                    # Relaunch with the current active profile
                    current_profile = profile_names[profile_idx]
                    try:
                        context, page = await launch_profile_context(pw, current_profile)
                        print(f"[Recovery] ✅ Fresh browser context launched for profile '{current_profile}'.")
                    except Exception as relaunch_err:
                        print(f"[Recovery] ❌ Failed to relaunch browser: {relaunch_err}. Waiting 60s before retry...")
                        await asyncio.sleep(60)
                else:
                    print(f"Error detected: {error_str}. Retrying in 15 seconds...")
                    await asyncio.sleep(15)
        await context.close()

async def analyze_token(page, token_data, index, total, grok_page=None):
    """
    Analyze a single token.  No DexScreener token page navigation is needed;
    liquidity lock is checked via Rugcheck API, Bubblemaps is opened directly
    via iframe.bubblemaps.io, and Grok uses x.com/i/grok as before.
    """
    token_name = token_data["name"]
    print(f"\n[Analyze] Token {token_name} - {token_data.get('link')} [{index}/{total}]")
    try:
        result = await perform_token_analysis(page, token_data, grok_page)
        # Handle tuple returns (retry cases with metadata)
        if isinstance(result, tuple) and len(result) >= 3:
            return result
        return result
    except Exception as e:
        error_str = str(e)
        if "Page crashed" in error_str or "Target closed" in error_str or "Session closed" in error_str:
            print(f"[Analyze] Page crash during analysis of {token_name}: {e}")
            return "page_crash"
        print(f"[Analyze] Error during analysis of {token_name}: {e}")
        return False

async def navigate_and_wait_for_token_page(page, token_link):
    """
    Navigate to a DexScreener token page and wait for it to be fully loaded,
    using the same indicators as the initial page load:
      1. goto + sleep(3)
      2. Wait for 'Loading chart settings...' to disappear
      3. Wait for TradingView chart iframe to appear
    """
    try:
        await page.goto(token_link)
        await asyncio.sleep(3)
    except Exception as nav_err:
        print(f"[WARNING] Error navigating back to token page: {nav_err}")
        return
    # Wait for 'Loading chart settings...' to disappear
    try:
        await page.wait_for_function('''
            () => {
                const loadingText = document.querySelector('span.chakra-text.custom-1mgdzye');
                if (loadingText) {
                    const text = loadingText.textContent || '';
                    return !text.includes('Loading chart settings...');
                }
                return true;
            }
        ''', timeout=30000)
    except Exception:
        pass  # Carry on even if the element is not found
    # Wait for TradingView chart iframe
    try:
        await page.wait_for_function('''
            () => {
                const container = document.querySelector('#tv-chart-container');
                if (!container) return false;
                const iframe = container.querySelector('iframe[id^="tradingview_"]');
                return iframe !== null;
            }
        ''', timeout=10000)
        print("[NavBack] \u2705 Token page fully loaded (chart iframe detected)")
    except Exception:
        print("[NavBack] \u26a0\ufe0f Chart iframe not detected after nav-back, proceeding anyway")


async def complete_token_processing(page, token_data, liquidity_text, market_cap_text,
                                     max_bundle_pct, max_bundle_source, grok_explanation):
    """
    Complete token processing after successful Grok check (Telegram notification + save).
    Used when Grok succeeds in a retry after initial failure.
    """
    print(f"✅ Token {token_data['name']} PASSED all checks (including Grok trend)")

    # Fetch latest liquidity and market cap via on-chain monitor
    liquidity_text, market_cap_text = await extract_latest_liq_mcap_onchain(token_data)

    # Check candle trader criteria
    play = "No"
    try:
        tracker = CandleTracker()
        pair_addr = extract_pair_address(token_data.get("link", ""))
        if pair_addr:
            analysis = tracker.analyze(pair_addr, name=token_data["name"])
            if analysis:
                if analysis.get("play_1_match"):
                    play = "Play 1"
                elif analysis.get("play_2_match"):
                    play = "Play 2"
                print(f"[CandleTracker] Result: {play}")
    except Exception as e:
        print(f"[CandleTracker] ⚠️ Error during analysis: {e}")

    print(f"   Liquidity: {liquidity_text if liquidity_text is not None else 'N/A'}")
    print(f"   Mkt Cap : {market_cap_text if market_cap_text is not None else 'N/A'}")

    # Notify via Telegram
    try:
        mc_text = market_cap_text or "N/A"
        liq_text = liquidity_text or "N/A"
        if isinstance(max_bundle_pct, (int, float)):
            source_label = max_bundle_source or "single"
            highest_bundle_text = f"{max_bundle_pct:.2f}%({source_label})"
        else:
            highest_bundle_text = "N/A"

        narrative_line = ""
        stored_narrative = None
        if grok_explanation:
            expl = grok_explanation.strip()
            full_expl = expl
            if len(full_expl) > 700:
                full_expl = full_expl[:697] + "..."
            narrative_line = f"Grok: <i>{html.escape(full_expl)}</i>\n"
            stored_narrative = expl

        message = (
            "✅ Valid token found:\n"
            f"<b>{token_data['name']}</b>\n"
            f"Liquidity: <b>{liq_text}</b>\n"
            f"Mcap: <b>{mc_text}</b>\n"
            f"Highest bundle: <b>{highest_bundle_text}</b>\n"
            f"Play: <b>{play}</b>\n"
            f"{narrative_line}\n\n"
            f"{token_data.get('link', '')}"
        )
        channel_config = load_channel_config()
        if channel_config.get("send_to_telegram", True):
            send_to_telegram(message)
        else:
            print("[Telegram] Telegram notifications are disabled in config. Skipping.")
    except Exception as notif_err:
        print(f"[WARNING] Failed to send Telegram notification: {notif_err}")

    # Post token to server
    try:
        channel_config = load_channel_config()
        if channel_config.get("send_to_server", True):
            post_token_to_server(
                token_name=token_data["name"],
                token_link=token_data.get("link", ""),
                liquidity=liq_text if liquidity_text else None,
                market_cap=mc_text if market_cap_text else None,
                narrative=stored_narrative,
                timestamp=token_data.get("timestamp"),
                is_dumped=token_data.get("is_dumped", False),
                pair_address=token_data.get("pairAddress"),
                play=play,
            )
        else:
            print("[Server] Server POST is disabled in config. Skipping.")
    except Exception as server_err:
        print(f"[WARNING] Failed to post token to server: {server_err}")

    # Save token
    try:
        save_valid_token(
            token_data["name"],
            token_data.get("link", ""),
            address=token_data.get("address"),
            pair_address=token_data.get("pairAddress"),
            liquidity=liq_text,
            market_cap=mc_text,
            narrative=stored_narrative,
        )
    except Exception as save_err:
        print(f"[WARNING] Failed to persist valid token details: {save_err}")


async def retry_grok_only(grok_page, token_data, liquidity_text=None, market_cap_text=None):
    """
    Retry only the Grok check without re-running Dexscreener analysis.
    Used when switching profiles after a Grok failure.
    """
    print(f"[Grok][Retry] Running Grok check only for {token_data['name']}...")
    try:
        extra_ctx_parts = []
        if liquidity_text:
            extra_ctx_parts.append(f"Liquidity: {liquidity_text}")
        if market_cap_text:
            extra_ctx_parts.append(f"Market Cap: {market_cap_text}")
        extra_ctx = " | ".join(extra_ctx_parts) if extra_ctx_parts else ""

        grok_decision, grok_explanation = await check_token_trend_with_grok(
            grok_page,
            token_name=token_data["name"],
            token_link=token_data.get("link", ""),
            extra_context=extra_ctx,
        )

        # Handle rate limit or grok error - both need another profile
        if grok_decision in ("rate_limit", "grok_error"):
            return grok_decision, None

        return grok_decision, grok_explanation
    except Exception as e:
        print(f"[Grok][Retry] ❌ Exception during Grok retry check: {e}")
        return None, None


async def perform_token_analysis(page, token_data, grok_page=None):
    """
    Perform full analysis on a token WITHOUT opening any DexScreener token page.
    """
    token_name = token_data["name"]
    try:
        # ----- STEP 1: Liquidity lock via Rugcheck API -----
        liq_locked, lp_pct, risks = check_liquidity_lock_via_api(token_data)
        if liq_locked == "network_retry":
            print(f"⚠️ Token {token_name} hit network retry during RugCheck")
            return "network_retry"
        if not liq_locked:
            token_link = token_data.get("link", "")
            print(f"❌ Liquidity lock is {lp_pct:.1f}% for {token_name} (need >= 99%) - {token_link}")
            return False

        # ----- STEP 2: Bubblemaps bundling check (direct URL) -----
        # Extract base token address for Bubblemaps URL
        base_token_address = token_data.get("address")
        if not base_token_address:
            pair = token_data.get("pair_data", {})
            base_token_address = pair.get("baseToken", {}).get("address") if pair else None

        if not base_token_address:
            base_token_address = extract_pair_address(token_data.get("link", ""))

        if not base_token_address:
            print(f"❌ Cannot determine token address for Bubblemaps: {token_name}")
            return False

        bubblemaps_url = f"https://iframe.bubblemaps.io/map?partnerId=demo&address={base_token_address}&chain=solana&limit=80"
        print(f"[Bubble Map] Navigating directly to: {bubblemaps_url}")

        try:
            await page.goto(bubblemaps_url)
            await asyncio.sleep(3)
        except PlaywrightTimeoutError:
            print(f"[Bubble Map] ⚠️ Timeout navigating to Bubblemaps for {token_name}")
            return "network_retry"
        except Exception as nav_err:
            nav_err_str = str(nav_err)
            if "Page crashed" in nav_err_str or "Target closed" in nav_err_str or "Session closed" in nav_err_str:
                raise  # propagate to analyze_token for page_crash handling
            print(f"[Bubble Map] ❌ Error navigating to Bubblemaps: {nav_err}")
            return False

        # Now perform the Bubblemaps analysis on the direct page (not in an iframe)
        bundling_result, max_bundle_pct, max_bundle_source = await analyze_bubble_map_direct(
            page, token_data
        )
        if bundling_result == "network_retry":
            print(f"⚠️ Token {token_name} hit network retry during bundling check")
            return "network_retry"
        elif not bundling_result:
            print(f"❌ Token {token_name} FAILED bundling check")
            return False

        # ----- STEP 3: Grok trend / narrative validation -----
        grok_decision = None
        grok_explanation = None

        try:
            target_page = grok_page if grok_page is not None else page
            grok_decision, grok_explanation = await check_token_trend_with_grok(
                target_page,
                token_name=token_data["name"],
                token_link=token_data.get("link", ""),
                extra_context="",
            )
        except Exception as e:
            print(f"[Grok] ❌ Exception during Grok trend check: {e}")
            grok_decision = None

        # Handle rate limit or grok error - signal immediate profile switch
        if grok_decision == "rate_limit":
            print(f"[Grok] ⚠️ Rate limit detected for {token_name} (will retry profile)")
            print("+" * 50)
            return ("rate_limit", None, None, max_bundle_pct, max_bundle_source)

        if grok_decision == "grok_error":
            print(f"[Grok] ⚠️ Grok error ('unable to reply') for {token_name} (will retry profile)")
            print("+" * 50)
            return ("grok_error", None, None, max_bundle_pct, max_bundle_source)

        if grok_decision is None:
            print(f"[Grok] ⚠️ No YES/NO decision detected for {token_name} (will retry profile)")
            print("+" * 50)
            return (None, None, None, max_bundle_pct, max_bundle_source)

        # Only treat token as valid if Grok explicitly answers YES
        if grok_decision is not True:
            print(
                f"❌ Token {token_name} FAILED Grok narrative/trend check "
                f"(decision={grok_decision})"
            )
            return False

        print(f"✅ Token {token_name} PASSED all checks (including Grok trend)")

        # ----- STEP 4: Extract latest Liquidity and Market Cap via On-Chain Monitor -----
        liquidity_text, market_cap_text = await extract_latest_liq_mcap_onchain(token_data)

        # Check candle trader criteria
        play = "No"
        try:
            tracker = CandleTracker()
            pair_addr = extract_pair_address(token_data.get("link", ""))
            if pair_addr:
                analysis = tracker.analyze(pair_addr, name=token_data["name"])
                if analysis:
                    if analysis.get("play_1_match"):
                        play = "Play 1"
                    elif analysis.get("play_2_match"):
                        play = "Play 2"
                    print(f"[CandleTracker] Result: {play}")
        except Exception as e:
            print(f"[CandleTracker] ⚠️ Error during analysis: {e}")

        print(f"   Liquidity: {liquidity_text if liquidity_text is not None else 'N/A'}")
        print(f"   Mkt Cap : {market_cap_text if market_cap_text is not None else 'N/A'}")

        # ----- STEP 5: Notify via Telegram -----
        try:
            mc_text = market_cap_text or "N/A"
            liq_text = liquidity_text or "N/A"
            if isinstance(max_bundle_pct, (int, float)):
                source_label = max_bundle_source or "single"
                highest_bundle_text = f"{max_bundle_pct:.2f}%({source_label})"
            else:
                highest_bundle_text = "N/A"

            narrative_line = ""
            stored_narrative = None
            if grok_explanation:
                expl = grok_explanation.strip()
                full_expl = expl
                if len(full_expl) > 700:
                    full_expl = full_expl[:697] + "..."
                narrative_line = f"Grok: <i>{html.escape(full_expl)}</i>\n"
                stored_narrative = expl

            message = (
                "✅ Valid token found:\n"
                f"<b>{token_data['name']}</b>\n"
                f"Liquidity: <b>{liq_text}</b>\n"
                f"Mcap: <b>{mc_text}</b>\n"
                f"Highest bundle: <b>{highest_bundle_text}</b>\n"
                f"Play: <b>{play}</b>\n"
                f"{narrative_line}\n\n"
                f"{token_data.get('link', '')}"
            )
            channel_config = load_channel_config()
            if channel_config.get("send_to_telegram", True):
                send_to_telegram(message)
            else:
                print("[Telegram] Telegram notifications are disabled in config. Skipping.")
        except Exception as notif_err:
            print(f"[WARNING] Failed to send Telegram notification: {notif_err}")

        # Post token to server
        try:
            channel_config = load_channel_config()
            if channel_config.get("send_to_server", True):
                post_token_to_server(
                    token_name=token_data["name"],
                    token_link=token_data.get("link", ""),
                    liquidity=liq_text if liquidity_text else None,
                    market_cap=mc_text if market_cap_text else None,
                    narrative=stored_narrative,
                    timestamp=token_data.get("timestamp"),
                    is_dumped=token_data.get("is_dumped", False),
                    pair_address=token_data.get("pairAddress"),
                    play=play,
                )
            else:
                print("[Server] Server POST is disabled in config. Skipping.")
        except Exception as server_err:
            print(f"[WARNING] Failed to post token to server: {server_err}")

        # Persist enriched token details
        try:
            save_valid_token(
                token_data.get("link", ""),
                address=token_data.get("address"),
                pair_address=token_data.get("pairAddress"),
                liquidity=liq_text,
                market_cap=mc_text,
                narrative=stored_narrative,
            )
        except Exception as save_err:
            print(f"[WARNING] Failed to persist valid token details: {save_err}")
        return True
    except Exception as e:
        print(f"Error during token analysis: {e}")
        return False


async def analyze_bubble_map_direct(page, token_data):
    """
    Analyze the bubble map on the DIRECT iframe.bubblemaps.io page (not inside
    a DexScreener iframe/modal).  The page content is rendered directly in
    the main frame, so we use `page` instead of an iframe `frame`.

    Returns:
        (passed, max_bundle_pct, max_bundle_source)
    """
    token_name = token_data.get("name", "?")
    print(f"\n[Bubble Map Direct] Starting bubble map analysis for {token_name}")

    try:
        config = load_channel_config()
        min_bundling_percentage = config.get("min_total_bundling_percentage", 5)
        print(f"[Bubble Map Direct] Min bundling percentage threshold: {min_bundling_percentage}%")

        # Step 1: Wait for the bubble map to fully load or show "No holders"
        print("[Bubble Map Direct] Waiting for right-panel or 'No holders' message...")
        try:
            # Wait for either the data panel (success) or the error message
            await page.wait_for_function('''
                () => {
                    return !!document.querySelector('div[data-testid="right-panel"]#right-panel') ||
                           !!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('No holders found')) ||
                           !!Array.from(document.querySelectorAll('h5')).find(el => el.textContent.includes('Page not found'));
                }
            ''', timeout=30000)

            # Check which one we found
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
                return True, 0.0, "none" # Treat as passed but with 0% bundling

            await asyncio.sleep(1)
            print("[Bubble Map Direct] ✅ Found right-panel — bubble map fully loaded")
        except Exception as e:
            # If it's a timeout error, treat as network_retry
            if "Timeout" in str(e):
                print(f"[Bubble Map Direct] ⚠️ Timeout waiting for Bubble Map load: {e}. Retrying.")
                return "network_retry", 0.0, "single"
            print(f"[Bubble Map Direct] ❌ Error waiting for Bubble Map load: {e}")
            return False, 0.0, "single"

        # Step 2: Click the expand icon (SVG with specific paths)
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

        # Step 2.5: Wait for map nodes to fully render on the main map
        # This confirms bubble map data is successfully loaded before we start checking rows
        print("[Bubble Map Direct] Waiting for map nodes to render...")
        try:
            await page.wait_for_selector('button[aria-label^="Remove All"]', timeout=30000)
            print("[Bubble Map Direct] ✅ Map nodes rendered")
        except Exception as e:
            print(f"[Bubble Map Direct] ⚠️ Map nodes did not render in time (Timeout/Network issue): {e}. Retrying.")
            return "network_retry", 0.0, "single"

        # Step 3: Click "Address List" button (DISABLED - was causing tab to close)
        # print("[Bubble Map Direct] Clicking Address List button...")
        # try:
        #     address_list_button = await page.wait_for_selector(
        #         'button:has-text("Address List")', timeout=8000
        #     )
        #     await address_list_button.click()
        #     await asyncio.sleep(1)
        #     print("[Bubble Map Direct] ✅ Clicked Address List button")
        # except Exception as e:
        #     print(f"[Bubble Map Direct] ⚠️ Address List button not found: {e}. Trying XPath...")
        #     try:
        #         xpath_sel = 'xpath=/html/body/div/div[1]/div/div[2]/div/button'
        #         btn = await page.wait_for_selector(xpath_sel, timeout=8000)
        #         await btn.click()
        #         await asyncio.sleep(1)
        #         print("[Bubble Map Direct] ✅ Clicked Address List button (XPath)")
        #     except Exception as e2:
        #         print(f"[Bubble Map Direct] ⚠️ Could not click Address List: {e2}")

        # Step 4: Uncheck visibility checkboxes
        print("[Bubble Map Direct] Unchecking visibility checkboxes...")
        checkbox_ids = ["contractVisibilityCheckbox", "cexVisibilityCheckbox", "dexVisibilityCheckbox"]

        async def uncheck_checkbox(checkbox_id):
            try:
                checkbox = await page.wait_for_selector(f'input#{checkbox_id}', timeout=10000)
                is_checked = await checkbox.is_checked()
                is_indeterminate = await page.evaluate('(el) => el.indeterminate', checkbox)
                if not is_checked and not is_indeterminate:
                    print(f"[Bubble Map Direct] ✅ {checkbox_id} already unchecked")
                    return
                label = await page.query_selector(f'label:has(input#{checkbox_id})')
                if label:
                    await label.click()
                else:
                    await checkbox.click()
                final_checked = await checkbox.is_checked()
                final_indeterminate = await page.evaluate('(el) => el.indeterminate', checkbox)
                if not final_checked and not final_indeterminate:
                    print(f"[Bubble Map Direct] ✅ Unchecked {checkbox_id}")
                else:
                    if label:
                        await label.click()
                    else:
                        await checkbox.click()
                    print(f"[Bubble Map Direct] ✅ Unchecked {checkbox_id}")
            except Exception as e:
                print(f"[Bubble Map Direct] ⚠️ Could not find/uncheck {checkbox_id}: {e}")

        await asyncio.gather(*[uncheck_checkbox(cb_id) for cb_id in checkbox_ids])

        # Step 5: Scroll through the list and analyze rows
        # analyze_bubble_map_rows works on any frame/page with the virtuoso scroller
        print("[Bubble Map Direct] Scrolling and analyzing rows...")
        passed, max_bundle_pct, max_bundle_source = await analyze_bubble_map_rows(
            page, min_bundling_percentage, token_name
        )
        return passed, max_bundle_pct, max_bundle_source

    except PlaywrightTimeoutError as e:
        print(f"[Bubble Map Direct] ⚠️ Timeout in bubble map analysis: {e}")
        return "network_retry", 0.0, "single"
    except Exception as e:
        print(f"[Bubble Map Direct] ❌ Error in bubble map analysis: {e}")
        return False, 0.0, "single"


async def check_liquidity_lock(page):
    """
    Check if the liquidity lock is 100% by finding the progress bar element.
    Returns True if liquidity lock is 100%, False otherwise.
    """
    try:
        # Wait for the progress bar element to appear
        await page.wait_for_selector('div.chakra-progress[role="progressbar"]', timeout=10000)

        # Find the progress bar element
        progress_element = await page.query_selector('div.chakra-progress[role="progressbar"]')

        if progress_element:
            # Get the aria-valuenow attribute which contains the percentage
            liquidity_value = await progress_element.get_attribute('aria-valuenow')

            if liquidity_value:
                liquidity_percentage = int(liquidity_value)
                print(f"Liquidity lock value: {liquidity_percentage}%")
                return liquidity_percentage == 100
            else:
                print("Could not find liquidity lock value")
                return False
        else:
            print("Could not find liquidity lock progress bar")
            return False

    except PlaywrightTimeoutError:
        print("Timeout waiting for liquidity lock progress bar")
        return False
    except Exception as e:
        print(f"Error checking liquidity lock: {e}")
        return False

async def analyze_bubble_map(page, token_data):
    """
    Analyze the bubble map to check for bundling violations.
    Returns True if token passes (no bundling detected), False if bundling detected.
    """
    print(f"\n[BUbble Map] Starting bubble map analysis for {token_data['name']}")

    try:
        # Load channel config
        config = load_channel_config()
        min_bundling_percentage = config.get("min_total_bundling_percentage", 5)
        print(f"[Bubble Map] Min bundling percentage threshold: {min_bundling_percentage}%")

        # Step 1: Click Bubblemaps button
        print("[Bubble Map] Step 1: Clicking Bubblemaps button...")
        try:
            bubblemaps_button = await page.wait_for_selector('button:has-text("Bubblemaps")', timeout=20000)
            await bubblemaps_button.click()
            await asyncio.sleep(3)  # Give time for iframe to load

            # Check for network error after clicking (check both main page and iframes)
            print("[Bubble Map] Checking for network errors...")
            await asyncio.sleep(2)  # Give more time for error to appear

            # Check main page
            network_error = await page.evaluate('''
                () => {
                    const errorDiv = document.querySelector('#sub-frame-error');
                    if (!errorDiv) return null;
                    const details = errorDiv.querySelector('#sub-frame-error-details');
                    if (details && details.textContent.includes('iframe.bubblemaps.io') &&
                        details.textContent.includes('took too long to respond')) {
                        return details.textContent;
                    }
                    return null;
                }
            ''')

            # Also check in iframes
            if not network_error:
                frames = page.frames
                for frame in frames:
                    try:
                        error_in_frame = await frame.evaluate('''
                            () => {
                                const errorDiv = document.querySelector('#sub-frame-error');
                                if (!errorDiv) return null;
                                const details = errorDiv.querySelector('#sub-frame-error-details');
                                if (details && (details.textContent.includes('bubblemaps.io') ||
                                    details.textContent.includes('took too long to respond'))) {
                                    return details.textContent;
                                }
                                return null;
                            }
                        ''')
                        if error_in_frame:
                            network_error = error_in_frame
                            break
                    except:
                        continue

            if network_error:
                print(f"[Bubble Map] ❌ NETWORK ERROR DETECTED: {network_error}")
                print("[Bubble Map] ⚠️ Bubble map iframe failed to load - network timeout")
                return False

            print("[Bubble Map] ✅ Clicked Bubblemaps button - no network errors detected")
        except Exception as e:
            print(f"[Bubble Map] ❌ Error clicking Bubblemaps button: {e}")
            return False

        # Step 2: Wait for modal to appear, then wait for bubble map to fully load
        print("[Bubble Map] Step 2: Waiting for bubble map modal to appear...")
        frame = None
        try:
            # Wait for the modal to appear
            await page.wait_for_selector('section[role="dialog"][id^="chakra-modal-"]', timeout=10000)
            print("[Bubble Map] ✅ Modal appeared")

            # Wait for the iframe inside the modal
            await page.wait_for_selector('section[role="dialog"] iframe.custom-uwwqev', timeout=10000)
            print("[Bubble Map] ✅ Iframe found in modal")

            # Get the iframe and wait for content to load
            iframe_element = await page.query_selector('section[role="dialog"] iframe.custom-uwwqev')
            if not iframe_element:
                print("[Bubble Map] ❌ Could not find iframe in modal")
                return False

            # Get the frame object
            frame = await iframe_element.content_frame()
            if not frame:
                # Wait a bit more for frame to be ready
                await asyncio.sleep(2)
                frame = await iframe_element.content_frame()

            if frame:
                print("[Bubble Map] Waiting for right-panel to appear inside iframe...")
                # Wait for the right-panel div to appear (indicates bubble map is loaded)
                await frame.wait_for_selector('div[data-testid="right-panel"]#right-panel', timeout=30000)
                await asyncio.sleep(1)
                print("[Bubble Map] ✅ Found right-panel - bubble map fully loaded")
            else:
                print("[Bubble Map] ⚠️ Could not access iframe content. Proceeding anyway...")

        except PlaywrightTimeoutError:
            print("[Bubble Map] ⚠️ Timeout waiting for bubble map to load. Proceeding anyway...")
        except Exception as e:
            print(f"[Bubble Map] ❌ Error waiting for bubble map to load: {e}")
            return False

        if not frame:
            print("[Bubble Map] ❌ Cannot proceed without iframe access")
            return False

        # Step 3: Click the expand icon (SVG with specific paths) inside iframe
        print("[Bubble Map] Step 3: Clicking expand icon...")
        try:
            # Find and click the expand icon inside the iframe
            await frame.wait_for_selector('svg[viewBox="0 0 24 24"]', timeout=10000)
            await frame.evaluate('''
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
            print("[Bubble Map] ✅ Clicked expand icon")
        except Exception as e:
            print(f"[Bubble Map] ❌ Error clicking expand icon: {e}")
            return False

        # Step 4: Click "Address List" button to expand the panel
        print("[Bubble Map] Step 4: Clicking Address List button to expand panel...")
        try:
            # Primary: find by visible text
            address_list_button = await frame.wait_for_selector('button:has-text("Address List")', timeout=8000)
            await address_list_button.click()
            await asyncio.sleep(1)  # Wait for panel to expand
            print("[Bubble Map] ✅ Clicked Address List button (text selector)")
        except Exception as e:
            print(f"[Bubble Map] ⚠️ Text-based Address List button not found: {e}. Trying XPath fallback...")
            try:
                # Fallback: use provided full XPath inside the iframe
                xpath_selector = 'xpath=/html/body/div/div[1]/div/div[2]/div/button'
                address_list_button_xpath = await frame.wait_for_selector(xpath_selector, timeout=8000)
                await address_list_button_xpath.click()
                await asyncio.sleep(1)
                print("[Bubble Map] ✅ Clicked Address List button (XPath fallback)")
            except Exception as e2:
                print(f"[Bubble Map] ⚠️ Could not find/click Address List button by XPath: {e2}")
                # Continue anyway, maybe it's already expanded

        # Step 5: Wait for checkboxes and uncheck them inside iframe (DISABLED)
        # print("[Bubble Map] Step 5: Unchecking visibility checkboxes...")
        # checkbox_ids = ["contractVisibilityCheckbox", "cexVisibilityCheckbox", "dexVisibilityCheckbox"]
        #
        # # Process all checkboxes in parallel for speed
        # async def uncheck_checkbox(checkbox_id):
        #     try:
        #         checkbox = await frame.wait_for_selector(f'input#{checkbox_id}', timeout=10000)
        #
        #         # Quick check if already unchecked
        #         is_checked = await checkbox.is_checked()
        #         is_indeterminate = await frame.evaluate('(el) => el.indeterminate', checkbox)
        #
        #         if not is_checked and not is_indeterminate:
        #             print(f"[Bubble Map] ✅ {checkbox_id} already unchecked (no action needed)")
        #             return
        #
        #         # Uncheck if needed - try label first, fallback to checkbox
        #         label = await frame.query_selector(f'label:has(input#{checkbox_id})')
        #         if label:
        #             await label.click()
        #         else:
        #             await checkbox.click()
        #
        #         # Quick verification
        #         final_checked = await checkbox.is_checked()
        #         final_indeterminate = await frame.evaluate('(el) => el.indeterminate', checkbox)
        #
        #         if not final_checked and not final_indeterminate:
        #             print(f"[Bubble Map] ✅ Unchecked {checkbox_id}")
        #         else:
        #             # If still checked/indeterminate, try one more click
        #             if label:
        #                 await label.click()
        #             else:
        #                 await checkbox.click()
        #             print(f"[Bubble Map] ✅ Unchecked {checkbox_id}")
        #     except Exception as e:
        #         print(f"[Bubble Map] ⚠️ Could not find/uncheck {checkbox_id}: {e}")
        #
        # # Process all checkboxes concurrently
        # await asyncio.gather(*[uncheck_checkbox(cb_id) for cb_id in checkbox_ids])

        # Step 5.5: Wait for map nodes to fully render on the main map
        # This confirms bubble map data is successfully loaded before we start checking rows
        print("[Bubble Map] Waiting for map nodes to render...")
        await frame.wait_for_selector('button[aria-label^="Remove All"]', timeout=30000)
        print("[Bubble Map] ✅ Map nodes rendered")

        # Step 6: Scroll through the list and analyze rows
        print("[Bubble Map] Step 6: Scrolling and analyzing rows...")
        passed, max_bundle_pct, max_bundle_source = await analyze_bubble_map_rows(
            frame, min_bundling_percentage, token_data['name']
        )
        return passed, max_bundle_pct, max_bundle_source

    except PlaywrightTimeoutError as e:
        print(f"[Bubble Map] ⚠️ Timeout in bubble map analysis: {e}")
        return "network_retry", 0.0, "single"
    except Exception as e:
        print(f"[Bubble Map] ❌ Error in bubble map analysis: {e}")
        return False, 0.0, "single"


async def close_bubble_map_modal(page):
    """
    Close the Bubble Map modal after a successful check.

    The close button HTML (for reference) looks like:
      <button type="button" aria-label="Close" class="custom-3r7yd1">...</button>
    """
    try:
        # Prefer a robust selector that stays within the open modal
        close_button = await page.wait_for_selector(
            'section[role="dialog"] button[aria-label="Close"]',
            timeout=5000,
        )
        await close_button.click()
        await asyncio.sleep(1)  # Give the UI a moment to settle
        print("[Bubble Map] ✅ Closed bubble map modal")
    except PlaywrightTimeoutError:
        print("[Bubble Map] ⚠️ Timeout waiting for bubble map close button (modal may already be closed)")
    except Exception as e:
        print(f"[Bubble Map] ⚠️ Could not close bubble map modal: {e}")


async def extract_liquidity_and_market_cap(page):
    """
    Extract the displayed Liquidity and Market Cap values from the token page
    after the Bubble Map modal has been closed.

    Expected HTML structure (simplified):
      <span class="chakra-text ...">Liquidity</span>
      <span class="chakra-text custom-0"> $4.4K ... </span>
      ...
      <span class="chakra-text ...">Mkt Cap</span>
      <span class="chakra-text custom-0">$3.1K</span>

    Returns:
        (liquidity_text, market_cap_text)
    """
    try:
        result = await page.evaluate(
            """
            () => {
                function getValueByLabel(labelText) {
                    const allSpans = Array.from(document.querySelectorAll('span.chakra-text'));
                    const labelEl = allSpans.find(el => (el.textContent || '').trim() === labelText);
                    if (!labelEl) return null;

                    // Move up to the stack that holds label + value
                    let stack = labelEl.parentElement;
                    while (stack && !String(stack.className).includes('chakra-stack')) {
                        stack = stack.parentElement;
                    }
                    if (!stack) return null;

                    // Find the first value-looking span in this stack that is not the label itself
                    const valueSpan = Array.from(stack.querySelectorAll('span.chakra-text'))
                        .find(el => el !== labelEl);
                    if (!valueSpan) return null;

                    return (valueSpan.textContent || '').trim();
                }

                return {
                    liquidity: getValueByLabel('Liquidity'),
                    marketCap: getValueByLabel('Mkt Cap'),
                };
            }
            """
        )

        liquidity_text = result.get("liquidity") if result else None
        market_cap_text = result.get("marketCap") if result else None

        return liquidity_text, market_cap_text
    except Exception as e:
        print(f"[Token Page] ⚠️ Failed to extract Liquidity / Mkt Cap: {e}")
        return None, None

async def analyze_bubble_map_rows(frame, min_bundling_percentage, token_name):
    """
    Scroll through the bubble map list smoothly and check for bundling violations as rows appear.
    Works within the iframe context. Stops at the last numbered row.
    """
    scroller_selector = 'div[data-testid="virtuoso-scroller"]'

    try:
        # Wait for the scroller to appear and be visible inside the iframe
        await frame.wait_for_selector(scroller_selector, timeout=10000, state='visible')
        scroller = await frame.query_selector(scroller_selector)

        if not scroller:
            print("[Bubble Map] ❌ Could not find scroller element")
            return False, 0.0, "single"

        seen_row_numbers = set()
        color_groups = {}  # Track percentages by border color
        color_to_group = {}  # Map color to group number for logging
        next_group_number = 1  # Track next group number to assign
        last_found_row_number = 0
        consecutive_no_new_rows = 0
        last_numbered_row_found = False  # Track if we've found the last numbered row
        max_single_pct = 0.0
        max_group_pct = 0.0
        max_source = "single"

        print(f"[Bubble Map] Starting row analysis (threshold: {min_bundling_percentage}%)...")

        # Step 1: Fast scroll to bottom to load all rows
        print("[Bubble Map] Scrolling to bottom to load all rows...")
        scroller_selector = 'div[data-testid="virtuoso-scroller"]'
        try:
            await frame.wait_for_selector(scroller_selector, timeout=10000, state='visible')
            scroller = await frame.query_selector(scroller_selector)
            if scroller:
                # Fast scroll to bottom
                await frame.evaluate(f'''
                    () => {{
                        const scroller = document.querySelector('{scroller_selector}');
                        if (scroller) {{
                            scroller.scrollTop = scroller.scrollHeight;
                        }}
                    }}
                ''')
                await asyncio.sleep(0.5)  # Brief wait for rows to load
                print("[Bubble Map] ✅ Scrolled to bottom, all rows loaded")
        except Exception as e:
            print(f"[Bubble Map] ⚠️ Could not scroll to bottom: {e}")

        # Step 2: Toggle all visibility buttons at once
        print("[Bubble Map] Toggling all visibility buttons at once...")
        try:
            # Use JavaScript to find and click all eye-closed buttons in one go
            clicked_count = await frame.evaluate('''
                () => {
                    const eyeClosedPath = 'M12 6c3.79 0 7.17 2.13 8.82 5.5-.59 1.22-1.42 2.27-2.41 3.12l1.41 1.41c1.39-1.23 2.49-2.77 3.18-4.53C21.27 7.11 17 4 12 4c-1.27 0-2.49.2-3.64.57l1.65 1.65C10.66 6.09 11.32 6 12 6m-1.07 1.14L13 9.21c.57.25 1.03.71 1.28 1.28l2.07 2.07c.08-.34.14-.7.14-1.07C16.5 9.01 14.48 7 12 7c-.37 0-.72.05-1.07.14M2.01 3.87l2.68 2.68C3.06 7.83 1.77 9.53 1 11.5 2.73 15.89 7 19 12 19c1.52 0 2.98-.29 4.32-.82l3.42 3.42 1.41-1.41L3.42 2.45zm7.5 7.5 2.61 2.61c-.04.01-.08.02-.12.02-1.38 0-2.5-1.12-2.5-2.5 0-.05.01-.08.01-.13m-3.4-3.4 1.75 1.75c-.23.55-.36 1.15-.36 1.78 0 2.48 2.02 4.5 4.5 4.5.63 0 1.23-.13 1.77-.36l.98.98c-.88.24-1.8.38-2.75.38-3.79 0-7.17-2.13-8.82-5.5.7-1.43 1.72-2.61 2.93-3.53';
                    const ignoreTerms = ['pumpswap', 'pump.fun', 'raydium'];
                    let clicked = 0;
                    const buttons = document.querySelectorAll('button.MuiIconButton-root');
                    for (const button of buttons) {
                        // Check if row should be ignored
                        const listItem = button.closest('li.MuiListItem-root');
                        if (listItem) {
                            const labelElem = listItem.querySelector('p[aria-label]');
                            if (labelElem) {
                                const labelText = (labelElem.getAttribute('aria-label') || '').toLowerCase();
                                if (ignoreTerms.some(term => labelText.includes(term))) {
                                    continue; // Skip ignored rows
                                }
                            }
                        }
                        // Check if button has eye-closed icon
                        const svg = button.querySelector('svg');
                        if (svg) {
                            const svgHTML = svg.innerHTML;
                            if (svgHTML.includes(eyeClosedPath)) {
                                button.click();
                                clicked++;
                            }
                        }
                    }
                    return clicked;
                }
            ''')
            print(f"[Bubble Map] ✅ Toggled {clicked_count} visibility buttons")
            await asyncio.sleep(0.3)  # Brief wait for visibility changes
        except Exception as e:
            print(f"[Bubble Map] ⚠️ Could not toggle visibility buttons: {e}")

        # Step 3: Scroll back to top and analyze
        print("[Bubble Map] Scrolling back to top for analysis...")
        try:
            await frame.evaluate(f'''
                () => {{
                    const scroller = document.querySelector('{scroller_selector}');
                    if (scroller) {{
                        scroller.scrollTop = 0;
                    }}
                }}
            ''')
            await asyncio.sleep(0.2)
        except:
            pass

        # Step 4: Ensure visibility checkboxes are unchecked before color analysis
        print("[Bubble Map] Verifying visibility checkboxes are unchecked before analysis...")
        try:
            await frame.evaluate('''
                () => {
                    const checkboxIds = ['contractVisibilityCheckbox', 'cexVisibilityCheckbox', 'dexVisibilityCheckbox'];
                    let unchecked = 0;
                    for (const id of checkboxIds) {
                        const checkbox = document.getElementById(id);
                        if (checkbox) {
                            const isChecked = checkbox.checked;
                            const isIndeterminate = checkbox.indeterminate || checkbox.getAttribute('data-indeterminate') === 'true';
                            if (isChecked || isIndeterminate) {
                                // Find the label and click it to uncheck
                                const label = checkbox.closest('label');
                                if (label) {
                                    label.click();
                                } else {
                                    checkbox.click();
                                }
                                unchecked++;
                            }
                        }
                    }
                    return unchecked;
                }
            ''')
            print("[Bubble Map] ✅ Verified visibility checkboxes are unchecked")
        except Exception as e:
            print(f"[Bubble Map] ⚠️ Could not verify checkboxes: {e}")

        # Fast scroll and analyze as we go
        max_scrolls = 200
        scroll_count = 0
        scroll_step = 1000  # Large scroll step for faster scrolling

        while scroll_count < max_scrolls and not last_numbered_row_found:

            # Analyze currently visible rows (but only numbered rows, and stop at last numbered row)
            list_items = await frame.query_selector_all('li.MuiListItem-root')
            new_rows_found = False

            for item in list_items:
                try:
                    # Get row number
                    row_number_elem = await item.query_selector('span.MuiTypography-body2.css-jlmh9i')
                    if not row_number_elem:
                        continue

                    row_number_text = await row_number_elem.inner_text()
                    # Extract number from "#1 ", "#2 ", etc.
                    match = re.search(r'#(\d+)', row_number_text)
                    if not match:
                        continue

                    row_number = int(match.group(1))

                    # If we've already found the last numbered row, skip any rows beyond it
                    if last_numbered_row_found and row_number > last_found_row_number:
                        continue

                    if row_number in seen_row_numbers:
                        continue

                    seen_row_numbers.add(row_number)
                    last_found_row_number = max(last_found_row_number, row_number)
                    new_rows_found = True

                    # Check if row should be ignored (pumpswap, pump.fun, raydium)
                    label_elem = await item.query_selector('p[aria-label]')
                    if label_elem:
                        label_text = await label_elem.get_attribute('aria-label') or ""
                        label_lower = label_text.lower()
                        if any(term in label_lower for term in ['pumpswap', 'pump.fun', 'raydium']):
                            print(f"[Bubble Map] Row #{row_number}: Ignoring (DEX/Protocol): {label_text}")
                            continue

                    # Get percentage
                    percentage_elem = await item.query_selector('span.MuiTypography-body2.css-6un1d7')
                    if not percentage_elem:
                        continue

                    percentage_text = await percentage_elem.inner_text()
                    percentage_match = re.search(r'([\d.]+)%', percentage_text)
                    if not percentage_match:
                        continue

                    percentage = float(percentage_match.group(1))

                    # Track the maximum single-holder percentage
                    if percentage > max_single_pct:
                        max_single_pct = percentage
                        max_source = "single"

                    # Check rule 1: Single row holding >= min_bundling_percentage
                    if percentage >= min_bundling_percentage:
                        print(f"[Bubble Map] ❌ Row #{row_number}: {percentage}% >= {min_bundling_percentage}% threshold")
                        print(f"[Bubble Map] Token {token_name} FAILED: Single holder exceeds threshold")
                        overall_max = max(max_single_pct, max_group_pct)
                        # Decide which source produced the overall max
                        source = "single" if overall_max == max_single_pct else "group"
                        return False, overall_max, source

                    # Check rule 2: Border color analysis
                    icon_boxes = await item.query_selector_all('div.MuiBox-root')
                    border_color = None
                    for box in icon_boxes:
                        try:
                            color = await frame.evaluate('''
                                (element) => {
                                    const style = window.getComputedStyle(element);
                                    const normalColor = 'rgb(77,87,115)';

                                    // Check if border actually exists (has width)
                                    const borderWidth = parseFloat(style.borderWidth || style.borderTopWidth || '0');
                                    if (borderWidth <= 0) {
                                        return null; // No border, skip
                                    }

                                    // Try to get color from border property first
                                    const border = style.border || style.borderTop || '';
                                    if (border && border.includes('rgb')) {
                                        const match = border.match(/rgb[a]?\\s*\\([^)]+\\)/);
                                        if (match) {
                                            const colorNormalized = match[0].replace(/\\s/g, '').toLowerCase();
                                            // Extract just rgb(...) part
                                            const rgbMatch = colorNormalized.match(/rgb\\([^)]+\\)/);
                                            if (rgbMatch) {
                                                const normalized = rgbMatch[0];
                                                if (normalized !== normalColor) {
                                                    return normalized;
                                                }
                                            }
                                        }
                                    }

                                    // Try borderColor property (can have multiple values, take first)
                                    const borderColor = style.borderColor || style.borderTopColor || '';
                                    if (borderColor && borderColor.includes('rgb')) {
                                        // borderColor can be "rgb(x,y,z) rgb(x,y,z) rgb(x,y,z) rgb(x,y,z)" or just one value
                                        const firstColorMatch = borderColor.match(/rgb[a]?\\s*\\([^)]+\\)/);
                                        if (firstColorMatch) {
                                            const colorNormalized = firstColorMatch[0].replace(/\\s/g, '').toLowerCase();
                                            // Extract just rgb(...) part
                                            const rgbMatch = colorNormalized.match(/rgb\\([^)]+\\)/);
                                            if (rgbMatch) {
                                                const normalized = rgbMatch[0];
                                                if (normalized !== normalColor) {
                                                    return normalized;
                                                }
                                            }
                                        }
                                    }
                                    return null;
                                }
                            ''', box)
                            if color:
                                border_color = color
                                break
                        except:
                            continue

                    if border_color:
                        if border_color not in color_groups:
                            color_groups[border_color] = []
                            # Assign group number if first time seeing this color
                            if border_color not in color_to_group:
                                color_to_group[border_color] = next_group_number
                                next_group_number += 1
                        label_text = await label_elem.get_attribute('aria-label') if label_elem else 'Unknown'
                        color_groups[border_color].append({
                            'row': row_number,
                            'percentage': percentage,
                            'label': label_text
                        })
                        print(f"[Bubble Map] Row #{row_number}: Strange border color {border_color}, percentage: {percentage}%")

                except Exception as e:
                    continue

            # Check if any color group totals exceed threshold
            for color, rows in color_groups.items():
                total_percentage = sum(r['percentage'] for r in rows)
                # Track maximum grouped (bundled) percentage
                if total_percentage > max_group_pct:
                    max_group_pct = total_percentage
                    max_source = "group"
                if total_percentage >= min_bundling_percentage:
                    group_num = color_to_group.get(color, 0)
                    row_numbers = [r['row'] for r in rows]
                    print(f"[Bubble Map] ❌ Group {group_num} ({color}) with rows {row_numbers} = {total_percentage:.2f}% → FAILS (≥ {min_bundling_percentage}%)")
                    print(f"[Bubble Map] Token {token_name} FAILED: Bundled holders exceed threshold")
                    overall_max = max(max_single_pct, max_group_pct)
                    # Decide which source produced the overall max
                    source = "single" if overall_max == max_single_pct else "group"
                    return False, overall_max, source

            # Smooth scroll down
            if new_rows_found:
                consecutive_no_new_rows = 0
            else:
                consecutive_no_new_rows += 1

            # Check if we've found the last numbered row (no new numbered rows for several iterations)
            if not last_numbered_row_found and last_found_row_number > 0 and consecutive_no_new_rows >= 3:
                # We've likely found the last numbered row
                last_numbered_row_found = True
                print(f"[Bubble Map] Reached last numbered row: #{last_found_row_number}")
                break

            # Get current scroll position
            scroll_info = await frame.evaluate(f'''
                () => {{
                    const scroller = document.querySelector('{scroller_selector}');
                    if (!scroller) return {{scrollTop: 0, scrollHeight: 0}};
                    return {{scrollTop: scroller.scrollTop, scrollHeight: scroller.scrollHeight}};
                }}
            ''')

            current_scroll = scroll_info.get('scrollTop', 0)
            max_scroll = scroll_info.get('scrollHeight', 0)

            # Stop if we've reached the bottom (but only if we've found at least one numbered row)
            if not last_numbered_row_found and last_found_row_number > 0 and current_scroll + scroll_step >= max_scroll:
                # We've reached the bottom and found numbered rows - this is likely the last one
                last_numbered_row_found = True
                print(f"[Bubble Map] Reached last numbered row: #{last_found_row_number}")
                break

            # Don't scroll if we've found the last numbered row
            if last_numbered_row_found:
                break

            # Fast scroll down - scroll directly to position for speed
            await frame.evaluate(f'''
                () => {{
                    const scroller = document.querySelector('{scroller_selector}');
                    if (scroller) {{
                        scroller.scrollTop += {scroll_step};
                    }}
                }}
            ''')

            await asyncio.sleep(0.05)  # Minimal delay for faster scrolling
            scroll_count += 1

        print(f"[Bubble Map] ✅ Analyzed {len(seen_row_numbers)} rows (up to row #{last_found_row_number})")
        print(f"[Bubble Map] ✅ No bundling violations detected")

        # Log all strange color groups found (even if they didn't exceed threshold)
        if color_groups and len(color_groups) > 0:
            print(f"[Bubble Map] Strange color groups found ({len(color_groups)} group(s)):")
            # Sort colors by their group number to maintain consistency
            sorted_colors = sorted(color_groups.items(), key=lambda x: color_to_group.get(x[0], 999))
            for color, rows in sorted_colors:
                group_num = color_to_group.get(color, 0)
                total_percentage = sum(r['percentage'] for r in rows)
                print(f"[Bubble Map]   group{group_num}({color}): {{")
                for r in rows:
                    print(f"[Bubble Map]   #{r['row']}:{r['percentage']},")
                print(f"[Bubble Map]   }}")
                print(f"[Bubble Map]   TOTAL: {total_percentage:.1f}")
        else:
            print(f"[Bubble Map] No strange color groups found (all rows have normal border color: {NORMAL_BORDER_COLOR})")

        print(f"[Bubble Map] Token {token_name} PASSED bubble map check")
        overall_max = max(max_single_pct, max_group_pct)
        # Decide which source produced the overall max
        source = "single" if overall_max == max_single_pct else "group"
        return True, overall_max, source

    except PlaywrightTimeoutError as e:
        print(f"[Bubble Map] ⚠️ Timeout analyzing rows: {e}")
        return "network_retry", 0.0, "single"
    except Exception as e:
        print(f"[Bubble Map] ❌ Error analyzing rows: {e}")
        return False, 0.0, "single"

if __name__ == "__main__":
    asyncio.run(main())
