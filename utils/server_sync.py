"""
Server Sync Utility
Posts validated tokens to the MCAP tracking server
"""

import os
import re
import requests
from typing import Optional, Dict


def load_server_config():
    """Load server URL from environment or .env file."""
    server_url = os.getenv("MCAP_SERVER_URL")

    # Fallback: try to read from .env file in project root
    if not server_url:
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith("#") and "=" in line:
                            key, value = line.split("=", 1)
                            if key.strip() == "MCAP_SERVER_URL":
                                server_url = value.strip()
                                break
            except Exception as e:
                print(f"[Server Sync] ⚠️ Could not read .env file: {e}")

    return server_url or None


def extract_token_address(link: str) -> Optional[str]:
    """
    Extract Solana token address from DexScreener URL.
    Example: https://dexscreener.com/solana/7ybnyfkx6t4qn4a8qbkfgphdfvhot1h2ugdblafgjva2
    Returns: 7ybnyfkx6t4qn4a8qbkfgphdfvhot1h2ugdblafgjva2
    """
    try:
        if "/solana/" in link:
            parts = link.split("/solana/")
            if len(parts) > 1:
                address = parts[1].split("?")[0].split("#")[0].strip()
                if address:
                    return address
    except Exception:
        pass
    return None


def post_token_to_server(
    token_name: str,
    token_link: str,
    liquidity: Optional[str] = None,
    market_cap: Optional[str] = None,
    narrative: Optional[str] = None,
    timestamp: Optional[int] = None,
    is_dumped: Optional[bool] = None,
    source: Optional[str] = "fresh",
    pair_address: Optional[str] = None,
    play: Optional[str] = "No",
) -> bool:
    """
    POST a validated token to the server endpoint /add-token.

    Args:
        token_name: Token name
        token_link: DexScreener URL
        liquidity: Liquidity value (optional)
        market_cap: Market cap value (optional)
        narrative: Grok narrative explanation (optional)
        timestamp: Unix timestamp (optional)
        play: Candle trader play (optional)

    Returns:
        True if successful, False otherwise
    """
    server_url = load_server_config()

    if not server_url:
        print("[Server Sync] ⚠️ MCAP_SERVER_URL not configured. Skipping server sync.")
        return False

    # Skip if using default placeholder URL
    if "your-mcap-server.com" in server_url:
        print("[Server Sync] ⚠️ Using default placeholder URL. Set MCAP_SERVER_URL to enable server sync.")
        return False

    import time
    address = extract_token_address(token_link)

    payload = {
        "name": token_name,
        "link": token_link,
        "address": address,
        "timestamp": timestamp or int(time.time()),
    }

    # Add optional fields if present, but ignore "N/A"
    if liquidity and liquidity != "N/A":
        payload["liquidity"] = liquidity
    if market_cap and market_cap != "N/A":
        payload["market_cap"] = market_cap
    if narrative:
        payload["narrative"] = narrative
    if is_dumped is not None:
        payload["is_dumped"] = is_dumped
    if source:
        payload["source"] = source
    if pair_address:
        payload["pairAddress"] = pair_address
    if play:
        payload["play"] = play

    url = f"{server_url.rstrip('/')}/add-token"

    try:
        response = requests.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )

        if response.status_code in (200, 201):
            print(f"[Server Sync] ✅ Successfully posted token '{token_name}' to server (Status: {response.status_code})")
            return True
        else:
            error_msg = ""
            try:
                error_body = response.text[:200]
                if error_body:
                    error_msg = f" - Response: {error_body}"
            except:
                pass
            print(f"[Server Sync] ❌ Failed to post token '{token_name}' to server. Status: {response.status_code}{error_msg}")
            return False

    except requests.exceptions.RequestException as e:
        print(f"[Server Sync] ❌ Failed to post token '{token_name}' to server: {e}")
        return False
    except Exception as e:
        print(f"[Server Sync] ❌ Unexpected error posting token '{token_name}': {e}")
        return False

