#!/usr/bin/env python3
"""
Token Sync Service
Monitors valid_tokens.json and POSTs new tokens to the server endpoint /add-token
"""

import os
import json
import time
import requests
from pathlib import Path
from typing import Set, Dict, List, Optional
from datetime import datetime

# Load .env file if it exists
ENV_FILE = Path(__file__).parent / ".env"
if ENV_FILE.exists():
    try:
        with open(ENV_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()
    except Exception as e:
        print(f"[WARNING] Could not load .env file: {e}")

# Configuration
VALID_TOKENS_PATH = Path(__file__).parent.parent / "json_files" / "valid_tokens.json"
SENT_TOKENS_PATH = Path(__file__).parent / "sent_tokens.json"
SERVER_URL = os.getenv("MCAP_SERVER_URL", "https://your-mcap-server.com")
ENDPOINT = "/add-token"
CHECK_INTERVAL = int(os.getenv("SYNC_CHECK_INTERVAL", "5"))  # Check every N seconds
MAX_RETRIES = int(os.getenv("SYNC_MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("SYNC_RETRY_DELAY", "5"))  # seconds


def load_json_file(file_path: Path, default: List = None) -> List:
    """Load JSON file, return default if file doesn't exist."""
    if default is None:
        default = []
    try:
        if file_path.exists():
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, list) else default
    except Exception as e:
        print(f"[ERROR] Failed to load {file_path}: {e}")
    return default


def save_json_file(file_path: Path, data: List) -> bool:
    """Save data to JSON file."""
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to save {file_path}: {e}")
        return False


def get_sent_token_links() -> Set[str]:
    """Get set of token links that have already been sent."""
    sent_tokens = load_json_file(SENT_TOKENS_PATH, [])
    return {token.get("link", "") for token in sent_tokens if token.get("link")}


def mark_token_as_sent(token: Dict) -> None:
    """Mark a token as sent by adding it to sent_tokens.json."""
    sent_tokens = load_json_file(SENT_TOKENS_PATH, [])

    # Check if already exists
    token_link = token.get("link", "")
    if not token_link:
        return

    # Remove if exists (to update timestamp)
    sent_tokens = [t for t in sent_tokens if t.get("link") != token_link]

    # Add with sent timestamp
    sent_entry = {
        "link": token_link,
        "name": token.get("name", ""),
        "sent_at": int(time.time()),
        "sent_at_iso": datetime.now().isoformat(),
    }
    sent_tokens.append(sent_entry)

    save_json_file(SENT_TOKENS_PATH, sent_tokens)


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


def prepare_token_payload(token: Dict) -> Dict:
    """Prepare token data for POST request."""
    link = token.get("link", "")
    address = extract_token_address(link)

    payload = {
        "name": token.get("name", ""),
        "link": link,
        "address": address,
        "timestamp": token.get("timestamp", int(time.time())),
    }

    # Add optional fields if present
    if "liquidity" in token:
        payload["liquidity"] = token["liquidity"]
    if "market_cap" in token:
        payload["market_cap"] = token["market_cap"]
    if "narrative" in token:
        payload["narrative"] = token["narrative"]

    return payload


def send_token_to_server(token: Dict) -> bool:
    """POST token to server endpoint. Returns True if successful."""
    url = f"{SERVER_URL.rstrip('/')}{ENDPOINT}"
    payload = prepare_token_payload(token)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                url,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=10,
            )

            if response.status_code == 200:
                print(f"[SUCCESS] Sent token '{token.get('name', 'Unknown')}' to server")
                return True
            elif response.status_code == 201:
                print(f"[SUCCESS] Created token '{token.get('name', 'Unknown')}' on server")
                return True
            else:
                error_msg = ""
                try:
                    error_body = response.text[:200]
                    if error_body:
                        error_msg = f" - {error_body}"
                except:
                    pass
                print(
                    f"[ERROR] Server returned status {response.status_code} for '{token.get('name', 'Unknown')}'{error_msg}"
                )

                # Don't retry on client errors (4xx) except 429 (rate limit)
                if 400 <= response.status_code < 500 and response.status_code != 429:
                    return False

        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Request failed for '{token.get('name', 'Unknown')}' (attempt {attempt + 1}/{MAX_RETRIES}): {e}")

        if attempt < MAX_RETRIES - 1:
            time.sleep(RETRY_DELAY)

    return False


def sync_new_tokens() -> int:
    """Check for new tokens and sync them to server. Returns count of tokens synced."""
    # Load current valid tokens
    valid_tokens = load_json_file(VALID_TOKENS_PATH, [])
    if not valid_tokens:
        return 0

    # Get already sent tokens
    sent_links = get_sent_token_links()

    # Find new tokens
    new_tokens = [
        token for token in valid_tokens
        if isinstance(token, dict) and token.get("link") and token.get("link") not in sent_links
    ]

    if not new_tokens:
        return 0

    print(f"[SYNC] Found {len(new_tokens)} new token(s) to sync")

    synced_count = 0
    for token in new_tokens:
        token_name = token.get("name", "Unknown")
        token_link = token.get("link", "")

        print(f"[SYNC] Processing token: {token_name}")

        if send_token_to_server(token):
            mark_token_as_sent(token)
            synced_count += 1
        else:
            print(f"[SYNC] Failed to sync token '{token_name}', will retry on next check")

    return synced_count


def main():
    """Main loop: monitor valid_tokens.json and sync new tokens."""
    print("=" * 60)
    print("Token Sync Service Started")
    print(f"Monitoring: {VALID_TOKENS_PATH}")
    print(f"Server URL: {SERVER_URL}")
    print(f"Endpoint: {ENDPOINT}")
    print(f"Check interval: {CHECK_INTERVAL} seconds")
    print("=" * 60)

    # Validate server URL
    if "your-mcap-server.com" in SERVER_URL:
        print("[WARNING] Using default server URL. Set MCAP_SERVER_URL environment variable to configure.")

    last_file_mtime = 0

    try:
        while True:
            try:
                # Check if file was modified
                current_mtime = VALID_TOKENS_PATH.stat().st_mtime if VALID_TOKENS_PATH.exists() else 0

                if current_mtime != last_file_mtime:
                    last_file_mtime = current_mtime
                    synced = sync_new_tokens()
                    if synced > 0:
                        print(f"[SYNC] Successfully synced {synced} token(s)")
                else:
                    # File hasn't changed, do a quick check anyway (in case of race conditions)
                    synced = sync_new_tokens()
                    if synced > 0:
                        print(f"[SYNC] Successfully synced {synced} token(s)")

            except KeyboardInterrupt:
                print("\n[INFO] Shutting down gracefully...")
                break
            except Exception as e:
                print(f"[ERROR] Unexpected error in sync loop: {e}")
                import traceback
                traceback.print_exc()

            time.sleep(CHECK_INTERVAL)

    except KeyboardInterrupt:
        print("\n[INFO] Shutting down gracefully...")
    except Exception as e:
        print(f"[ERROR] Fatal error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

