# MCAP Tracker - Token Sync Service

This service monitors `valid_tokens.json` and automatically syncs new tokens to your hosted server.

## Architecture

```
LOCAL MACHINE
--------------
molecule_token_finder/
    ├── json_files/
    │   └── valid_tokens.json  ← Monitored by token_sync.py
    └── mcap-tracker/
        └── token_sync.py      ← Watches for new tokens, POSTs to server

HOSTED SERVER
--------------
POST /add-token               ← Receives new tokens
GET /tokens                   ← Frontend fetches tokens
Cron (every 5sec)            ← Polls DexScreener API
Updates highestMcap          ← Tracks highest market cap
```

## Setup

1. **Configure Server URL**

   Set the `MCAP_SERVER_URL` environment variable:

   ```bash
   # Windows PowerShell
   $env:MCAP_SERVER_URL = "https://your-mcap-server.com"

   # Windows CMD
   set MCAP_SERVER_URL=https://your-mcap-server.com

   # Linux/Mac
   export MCAP_SERVER_URL=https://your-mcap-server.com
   ```

   Or create a `.env` file in the `mcap-tracker` folder:
   ```
   MCAP_SERVER_URL=https://your-mcap-server.com
   ```

2. **Install Dependencies**

   ```bash
   pip install requests
   ```

3. **Run the Sync Service**

   ```bash
   python mcap-tracker/token_sync.py
   ```

## How It Works

1. **Monitoring**: The script checks `valid_tokens.json` every 5 seconds for new tokens
2. **Deduplication**: Uses `sent_tokens.json` to track which tokens have already been sent
3. **POST Request**: When a new token is found, it POSTs to `{SERVER_URL}/add-token` with:
   ```json
   {
     "name": "TokenName",
     "link": "https://dexscreener.com/solana/...",
     "address": "7ybnyfkx6t4qn4a8qbkfgphdfvhot1h2ugdblafgjva2",
     "timestamp": 1771446418,
     "liquidity": "$3.8K",
     "market_cap": "$2.4K",
     "narrative": "YES - ..."
   }
   ```
4. **Retry Logic**: Failed requests are retried up to 3 times (except for 4xx client errors)
5. **Tracking**: Successfully sent tokens are recorded in `sent_tokens.json`

## Server Endpoint Requirements

Your server should implement:

**POST /add-token**
- Accepts JSON payload with token data
- Returns 200/201 on success
- Returns 4xx/5xx on error

**GET /tokens**
- Returns list of tokens with updated `highestMcap` values
- Should be polled by your frontend

## Files

- `token_sync.py` - Main sync service script
- `sent_tokens.json` - Tracks which tokens have been sent (auto-generated)
- `README.md` - This file

## Configuration

Default values (can be changed in code):
- `CHECK_INTERVAL = 5` seconds
- `MAX_RETRIES = 3`
- `RETRY_DELAY = 5` seconds

## Troubleshooting

- **No tokens syncing**: Check that `MCAP_SERVER_URL` is set correctly
- **Connection errors**: Verify server is accessible and endpoint exists
- **Duplicate sends**: Check `sent_tokens.json` - tokens are deduplicated by `link` field

