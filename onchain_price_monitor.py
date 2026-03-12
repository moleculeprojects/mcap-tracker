#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════╗
║          OnChain Price Monitor  —  onchain_price_monitor.py         ║
║  Fetches live price + market-cap DIRECTLY from Solana on-chain.     ║
║  NO DexScreener dependency.  Polls every 100 ms.                    ║
╠══════════════════════════════════════════════════════════════════════╣
║  Supported pool types (auto-detected by program owner):             ║
║    • PumpFun bonding curve  —  virtualSolReserves / virtualTokenRes ║
║    • PumpSwap AMM pool      —  SPL token vault balances             ║
║    • Raydium CPMM pool      —  SPL token vault balances             ║
║    • Raydium CLMM pool      —  sqrtPriceX64 from pool state         ║
║    • Raydium AMM V4 pool    —  coinVault / pcVault balances         ║
╠══════════════════════════════════════════════════════════════════════╣
║  CLI usage:                                                          ║
║    python onchain_price_monitor.py <pair_addr1> [pair_addr2] ...    ║
║    python onchain_price_monitor.py <addr1> <addr2> --duration 60    ║
║                                                                      ║
║  Library usage:                                                      ║
║    from onchain_price_monitor import OnChainPriceMonitor             ║
║                                                                      ║
║    async def my_callback(pair_addr, data):                           ║
║        print(pair_addr, data['price_usd'], data['mcap_usd'])        ║
║                                                                      ║
║    monitor = OnChainPriceMonitor(                                    ║
║        pair_addresses=["<addr1>", "<addr2>"],                        ║
║        poll_interval_ms=100,                                         ║
║        on_update=my_callback,  # optional                            ║
║    )                                                                 ║
║    await monitor.start()                                             ║
║    ...                                                               ║
║    await monitor.stop()                                              ║
║                                                                      ║
║    # One-shot fetch (no loop):                                       ║
║    data = await monitor.fetch_one("<pair_addr>")                     ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import struct
import time
import sys
import base64
import logging
from typing import Callable, Dict, List, Optional, Any

import aiohttp

# ─────────────────────────────────────────────────────────────────────────────
#  RPC endpoints — add your own premium endpoint at the TOP for best latency
#  e.g. Helius:   https://mainnet.helius-rpc.com/?api-key=YOUR_KEY
#       QuickNode: https://your-endpoint.solana-mainnet.quiknode.pro/YOUR_KEY/
# ─────────────────────────────────────────────────────────────────────────────
RPC_ENDPOINTS: List[str] = [
    "https://mainnet.helius-rpc.com/?api-key=1b7550b1-47c2-4a79-83c1-4a88a8dcc2e7",  # Helius premium
    "https://api.mainnet-beta.solana.com",          # Solana Foundation public (fallback)
    "https://solana-api.projectserum.com",           # Project Serum public mirror (fallback)
]

# SOL/USD price source (CoinGecko simple price — cached every 10 s)
SOL_PRICE_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=solana&vs_currencies=usd"
)

# ─────────────────────────────────────────────────────────────────────────────
#  Known Solana Program IDs
# ─────────────────────────────────────────────────────────────────────────────
PUMPFUN_PROGRAM_ID    = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMPSWAP_PROGRAM_ID   = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"   # PumpSwap AMM
RAYDIUM_CPMM_PROGRAM  = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
RAYDIUM_CLMM_PROGRAM  = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"
RAYDIUM_AMM_V4        = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
SOL_MINT              = "So11111111111111111111111111111111111111112"

# PumpFun bonding-curve account layout (little-endian packed):
#   offset  0 :  8 bytes  discriminator
#   offset  8 :  8 bytes  virtualTokenReserves  (u64)
#   offset 16 :  8 bytes  virtualSolReserves    (u64)
#   offset 24 :  8 bytes  realTokenReserves     (u64)
#   offset 32 :  8 bytes  realSolReserves       (u64)
#   offset 40 :  8 bytes  tokenTotalSupply      (u64)
#   offset 48 :  1 byte   complete              (bool)
PUMPFUN_VTOKEN_OFF    = 8
PUMPFUN_VSOL_OFF      = 16
PUMPFUN_SUPPLY_OFF    = 40
PUMPFUN_COMPLETE_OFF  = 48
PUMPFUN_TOTAL_SUPPLY  = 1_000_000_000   # 1 billion tokens (fixed)
PUMPFUN_TOKEN_DEC     = 6

# Raydium CPMM pool-state layout (relevant offsets only):
CPMM_MINT0_OFF   = 168
CPMM_MINT1_OFF   = 200
CPMM_VAULT0_OFF  = 72
CPMM_VAULT1_OFF  = 104

# PumpSwap AMM pool-state layout (reverse-engineered from on-chain data):
# The pool account (typically ~283 bytes) stores:
#   offset 43: pool_bump (1 byte)
#   offset 44: index (u16)
#   offset 46: creator (32 bytes) — wallet that created pool
#   offset 78: base_mint (32 bytes)
#   offset 110: quote_mint (32 bytes)
#   offset 142: lp_mint (32 bytes)
#   offset 174: pool_base_token_account (32 bytes)  — "vault 0"
#   offset 206: pool_quote_token_account (32 bytes) — "vault 1"
#   offset 238: lp_supply, fees, etc.
PUMPSWAP_BASE_MINT_OFF   = 78
PUMPSWAP_QUOTE_MINT_OFF  = 110
PUMPSWAP_VAULT0_OFF      = 174   # pool_base_token_account
PUMPSWAP_VAULT1_OFF      = 206   # pool_quote_token_account

# Raydium CLMM pool-state layout:
CLMM_MINT_A_OFF        = 73
CLMM_MINT_B_OFF        = 105
CLMM_SQRT_PRICE_OFF    = 253   # u128 little-endian

# Raydium AMM V4 pool-state layout (752 bytes):
#   Offsets 0-335: numeric fields (status, nonce, fees, swap amounts, etc.)
#   [336-367] poolCoinTokenAccount (Pubkey) — coin vault
#   [368-399] poolPcTokenAccount (Pubkey)   — PC vault
#   [400-431] coinMintAddress (Pubkey)
#   [432-463] pcMintAddress (Pubkey)
#   [464-495] lpMintAddress (Pubkey)
#   [496+]    openOrders, serumMarket, etc.
V4_COIN_VAULT_OFF = 336
V4_PC_VAULT_OFF   = 368
V4_COIN_MINT_OFF  = 400
V4_PC_MINT_OFF    = 432

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="[%(asctime)s][OnChain] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("OnChainMonitor")


# ─────────────────────────────────────────────────────────────────────────────
#  Tiny base58 encoder (no external deps)
# ─────────────────────────────────────────────────────────────────────────────
_B58 = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

def _b58enc(v: bytes) -> str:
    n = int.from_bytes(v, "big")
    out = []
    while n >= 58:
        n, r = divmod(n, 58)
        out.append(_B58[r])
    out.append(_B58[n])
    for b in v:
        if b == 0:
            out.append(_B58[0])
        else:
            break
    return bytes(reversed(out)).decode()

def _pubkey(data: bytes, off: int) -> str:
    return _b58enc(data[off: off + 32])

def _u64(data: bytes, off: int) -> int:
    return struct.unpack_from("<Q", data, off)[0]

def _u128(data: bytes, off: int) -> int:
    lo, hi = struct.unpack_from("<QQ", data, off)
    return (hi << 64) | lo


# ─────────────────────────────────────────────────────────────────────────────
#  Minimal async Solana RPC client with endpoint failover
# ─────────────────────────────────────────────────────────────────────────────
class SolRPC:
    def __init__(self, endpoints: List[str], session: aiohttp.ClientSession):
        self._ep  = endpoints
        self._ses = session
        self._idx = 0

    async def _call(self, method: str, params: list, timeout: float = 10.0) -> Any:
        err = None
        # Try primary first, then fallbacks, then repeat once
        for attempt in range(len(self._ep) * 2):
            url = self._ep[attempt % len(self._ep)]
            try:
                async with self._ses.post(
                    url,
                    json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as r:
                    d = await r.json(content_type=None)
                    if "error" in d:
                        raise RuntimeError(d["error"])
                    return d.get("result")
            except Exception as e:
                err = e
                # Only log warnings for the primary endpoint or after multiple failures
                if attempt == 0 or attempt >= len(self._ep):
                    log.warning("RPC %s@%s failed: %s", method, url[:40], e)
                await asyncio.sleep(0.1)
        raise RuntimeError(f"All RPC endpoints exhausted for {method}: {err}")

    async def get_multiple_accounts(self, keys: List[str]) -> List[Optional[dict]]:
        if not keys:
            return []
        res = await self._call(
            "getMultipleAccounts",
            [keys, {"encoding": "base64", "commitment": "processed"}],
        )
        return (res or {}).get("value", [None] * len(keys))

    async def get_multiple_parsed_token_accounts(self, keys: List[str]) -> List[Optional[dict]]:
        if not keys:
            return []
        res = await self._call(
            "getMultipleAccounts",
            [keys, {"encoding": "jsonParsed", "commitment": "processed"}],
        )
        # Convert jsonParsed format into a simplified format similar to getTokenAccountBalance
        out = []
        for val in (res or {}).get("value", [None] * len(keys)):
            if val and val.get("data", {}).get("parsed", {}).get("info"):
                info = val["data"]["parsed"]["info"]
                amt  = info.get("tokenAmount", {})
                out.append({"uiAmount": amt.get("uiAmount", 0), "mint": info.get("mint", "")})
            else:
                out.append(None)
        return out

    async def get_token_balance(self, vault: str) -> Optional[dict]:
        res = await self.get_multiple_parsed_token_accounts([vault])
        return res[0] if res else None

    async def get_token_supply(self, mint: str) -> Optional[dict]:
        res = await self._call("getTokenSupply", [mint, {"commitment": "processed"}])
        return (res or {}).get("value")


# ─────────────────────────────────────────────────────────────────────────────
#  SOL/USD price cache (fetched directly from Raydium WSOL/USDC pool)
# ─────────────────────────────────────────────────────────────────────────────
# Most liquid Raydium V4 pool for WSOL/USDC (58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2)
WSOL_VAULT_ADDR = "DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz"
USDC_VAULT_ADDR = "HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz"

class SolPrice:
    TTL = 0.5  # Hyper-fast 500ms update for real-time SOL/USD peg

    def __init__(self, rpc: SolRPC):
        self._rpc   = rpc
        self._price = 0.0
        self._ts    = 0.0
        self._lock  = asyncio.Lock()

    async def get(self) -> float:
        if time.time() - self._ts < self.TTL and self._price > 0:
            return self._price
        async with self._lock:
            if time.time() - self._ts < self.TTL and self._price > 0:
                return self._price
            try:
                # Fetch Raydium WSOL and USDC vault balances simultaneously (1 RPC call)
                bals = await self._rpc.get_multiple_parsed_token_accounts([WSOL_VAULT_ADDR, USDC_VAULT_ADDR])
                if len(bals) == 2 and bals[0] and bals[1]:
                    sol_amt = float(bals[0].get("uiAmount") or 0)
                    usdc_amt = float(bals[1].get("uiAmount") or 0)
                    if sol_amt > 0:
                        self._price = usdc_amt / sol_amt
                        self._ts    = time.time()
                        log.debug("SOL/USD (On-Chain) = $%.2f", self._price)
            except Exception as e:
                log.warning("SOL/USD on-chain fetch failed: %s (using $%.2f)", e, self._price)
        return self._price


# ─────────────────────────────────────────────────────────────────────────────
#  Pool-type detection
# ─────────────────────────────────────────────────────────────────────────────
def _pool_type(owner: str) -> str:
    if owner == PUMPFUN_PROGRAM_ID:    return "pumpfun"
    if owner == PUMPSWAP_PROGRAM_ID:   return "pumpswap"
    if owner == RAYDIUM_CPMM_PROGRAM:  return "raydium_cpmm"
    if owner == RAYDIUM_CLMM_PROGRAM:  return "raydium_clmm"
    if owner == RAYDIUM_AMM_V4:        return "raydium_v4"
    return "unknown"


def _detect_sol_pair(raw: bytes, pool_type: str) -> Optional[dict]:
    """
    Exhaustively scan account data for SOL_MINT and derive the full layout
    (mints + vaults) based on the program's structural patterns.
    """
    for i in range(len(raw) - 31):
        if _pubkey(raw, i) == SOL_MINT:
            # We found SOL! Now derive everything else based on the pool type.
            if pool_type == "raydium_cpmm":
                # Layout: [Vault0][Vault1] ... [Mint0][Mint1]
                # Gap between Vaults/Mints is usually 96 bytes.
                # If SOL is Mint0 (i=168), others are at 200 (M1), 72 (V0), 104 (V1).
                # If SOL is Mint1 (i=200), others are at 168 (M0), 72 (V0), 104 (V1).
                is_mint0 = (i % 32 == 8) # simplistic alignment check
                m0_off, m1_off = (i, i + 32) if is_mint0 else (i - 32, i)
                v0_off, v1_off = m0_off - 96, m1_off - 96

                # Validation: check if derived offsets stay inside bounds
                if v0_off < 0 or m1_off + 32 > len(raw):
                    continue

                return {
                    "mint0": _pubkey(raw, m0_off), "mint1": _pubkey(raw, m1_off),
                    "vault0": _pubkey(raw, v0_off), "vault1": _pubkey(raw, v1_off),
                    "sol_side": 0 if is_mint0 else 1
                }

            if pool_type == "raydium_v4":
                # Layout: [Vault0][Vault1][Mint0][Mint1] (shifted versions exist)
                # Fixed gap: Mints are 64 bytes after Vaults.
                is_mint0 = True # V4 is often coin/pc
                m0_off, m1_off = i, i + 32
                v0_off, v1_off = m0_off - 64, m1_off - 64

                # Check for secondary alignment (SOL at Mint1)
                if v0_off < 0 or _pubkey(raw, m0_off) != SOL_MINT:
                    is_mint0 = False
                    m0_off, m1_off = i - 32, i
                    v0_off, v1_off = m0_off - 64, m1_off - 64

                if v0_off >= 0 and m1_off + 32 <= len(raw):
                    return {
                        "mint0": _pubkey(raw, m0_off), "mint1": _pubkey(raw, m1_off),
                        "vault0": _pubkey(raw, v0_off), "vault1": _pubkey(raw, v1_off),
                        "sol_side": 0 if is_mint0 else 1
                    }

            if pool_type == "pumpswap":
                # Layout: [M0][M1][LP][V0][V1]
                # Gap: Vaults are 96 bytes after Mints.
                is_mint0 = True
                m0_off, m1_off = i, i + 32
                v0_off, v1_off = m0_off + 96, m1_off + 96

                if v0_off < 0 or _pubkey(raw, m0_off) != SOL_MINT:
                    is_mint0 = False
                    m0_off, m1_off = i - 32, i
                    v0_off, v1_off = m0_off + 96, m1_off + 96

                if m0_off >= 0 and v1_off + 32 <= len(raw):
                    return {
                        "mint0": _pubkey(raw, m0_off), "mint1": _pubkey(raw, m1_off),
                        "vault0": _pubkey(raw, v0_off), "vault1": _pubkey(raw, v1_off),
                        "sol_side": 0 if is_mint0 else 1
                    }
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  Per-type parsers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_pumpfun(raw: bytes, sol_usd: float) -> dict:
    """Read bonding-curve account: price = vSol / vToken (adjusted for decimals)."""
    if len(raw) < 49:
        return {"error": "data too short"}
    v_token  = _u64(raw, PUMPFUN_VTOKEN_OFF)
    v_sol    = _u64(raw, PUMPFUN_VSOL_OFF)
    complete = bool(raw[PUMPFUN_COMPLETE_OFF]) if len(raw) > PUMPFUN_COMPLETE_OFF else False
    if v_token == 0:
        return {"error": "zero virtual token reserves"}

    price_sol = (v_sol / 1e9) / (v_token / 10**PUMPFUN_TOKEN_DEC)
    price_usd = price_sol * sol_usd
    mcap_usd = price_usd * PUMPFUN_TOTAL_SUPPLY

    # Liquidity for PumpFun is the virtual SOL reserves converted to USD.
    # Usually represented as 2x the base currency side in AMMs.
    v_sol_val = v_sol / 1e9
    liquidity_usd = v_sol_val * sol_usd * 2

    return {
        "pool_type":       "pumpfun",
        "price_sol":       price_sol,
        "price_usd":       price_usd,
        "mcap_usd":        mcap_usd,
        "liquidity_usd":   liquidity_usd,
        "virtual_sol":     v_sol_val,
        "virtual_tokens":  v_token / 10**PUMPFUN_TOKEN_DEC,
        "bonding_complete": complete,
        "sol_usd":         sol_usd,
    }


async def _cpmm_price(
    pool_type: str, raw: bytes, rpc: SolRPC, sol_usd: float,
    cache: dict, addr: str
) -> dict:
    """Raydium CPMM: read vault token-account balances, compute AMM price."""
    if not cache.get("vault0"):
        # Try standard first
        if len(raw) >= CPMM_MINT1_OFF + 32:
            m0, m1 = _pubkey(raw, CPMM_MINT0_OFF), _pubkey(raw, CPMM_MINT1_OFF)
            if m0 == SOL_MINT or m1 == SOL_MINT:
                cache.update({
                    "mint0": m0, "mint1": m1,
                    "vault0": _pubkey(raw, CPMM_VAULT0_OFF),
                    "vault1": _pubkey(raw, CPMM_VAULT1_OFF),
                    "sol_side": 0 if m0 == SOL_MINT else 1
                })

        # If standard failed or we haven't found SOL, try dynamic
        if not cache.get("vault0"):
            layout = _detect_sol_pair(raw, "raydium_cpmm")
            if layout:
                cache.update(layout)
                log.info("Dynamic discovered Raydium CPMM: SOL Side=%d", layout["sol_side"])

        if not cache.get("vault0"):
            return {"pool_type": pool_type, "error": "SOL_MINT not found in CPMM account"}

    return await _vaults_to_price(pool_type, rpc, sol_usd, cache)


async def _pumpswap_price(
    raw: bytes, rpc: SolRPC, sol_usd: float, cache: dict
) -> dict:
    """PumpSwap AMM price calculation.

    PumpSwap pool account layout (301 bytes, Anchor-style):
      [ 0- 7]  8-byte Anchor discriminator
      [ 8   ]  pool_bump (u8)
      [ 9-10]  index (u16)
      [11-42]  creator (Pubkey, 32 bytes)
      [43-74]  base_mint (Pubkey) — the token mint
      [75-106] quote_mint (Pubkey) — WSOL mint (always So111...2)
      [107-138] lp_mint (Pubkey)
      [139-170] pool_base_token_account (Pubkey) — token vault
      [171-202] pool_quote_token_account (Pubkey) — SOL vault
      [203+]   lp_supply (u64), fees, padding

    PumpSwap pools are ALWAYS token/SOL pairs.
    """
    # Anchor-style offsets (8-byte discriminator + 3 bytes bump/index)
    PS_BASE_MINT  = 43
    PS_QUOTE_MINT = 75
    PS_VAULT0     = 139   # pool_base_token_account (token vault)
    PS_VAULT1     = 171   # pool_quote_token_account (SOL vault)

    if not cache.get("vault0"):
        # Try standard first
        m0, m1 = _pubkey(raw, PS_BASE_MINT), _pubkey(raw, PS_QUOTE_MINT)
        if m0 == SOL_MINT or m1 == SOL_MINT:
            cache.update({
                "mint0": m0, "mint1": m1,
                "vault0": _pubkey(raw, PS_VAULT0),
                "vault1": _pubkey(raw, PS_VAULT1),
                "sol_side": 0 if m0 == SOL_MINT else 1
            })
        else:
            # Dynamic discovery
            layout = _detect_sol_pair(raw, "pumpswap")
            if layout:
                cache.update(layout)
                log.info("Dynamic discovered PumpSwap: SOL Side=%d", layout["sol_side"])

        if not cache.get("vault0"):
            return {"pool_type": "pumpswap", "error": "SOL_MINT not found in PumpSwap account"}
        log.info(
            "PumpSwap decoded: base_mint=%s tok_vault=%s sol_vault=%s",
            cache["mint0"][:12], cache["vault0"][:12], cache["vault1"][:12],
        )

    return await _vaults_to_price("pumpswap", rpc, sol_usd, cache)


async def _vaults_to_price(
    pool_type: str, rpc: SolRPC, sol_usd: float, cache: dict
) -> dict:
    """Shared helper: given vault0/vault1/mint0/mint1 in cache, fetch balances and compute price."""
    try:
        bals = await rpc.get_multiple_parsed_token_accounts([cache["vault0"], cache["vault1"]])
        if len(bals) != 2:
            return {"pool_type": pool_type, "error": "vault balance fetch failed"}
        b0, b1 = bals[0], bals[1]
    except Exception as e:
        return {"pool_type": pool_type, "error": f"vault fetch: {e}"}

    if not b0 or not b1:
        return {"pool_type": pool_type, "error": "vault balance unavailable"}

    amt0 = float(b0.get("uiAmount") or 0)
    amt1 = float(b1.get("uiAmount") or 0)

    # Determine which side is SOL
    sol_side = cache.get("sol_side")  # 0 = vault0 is SOL, 1 = vault1 is SOL
    if sol_side == 0 or cache["mint0"] == SOL_MINT:
        sol_amt, tok_amt, tok_mint = amt0, amt1, cache["mint1"]
    elif sol_side == 1 or cache["mint1"] == SOL_MINT:
        sol_amt, tok_amt, tok_mint = amt1, amt0, cache["mint0"]
    else:
        return {"pool_type": pool_type, "error": "non-SOL pair — cannot compute USD price"}

    if tok_amt == 0:
        return {"pool_type": pool_type, "error": "zero token reserves"}

    price_sol = sol_amt / tok_amt
    price_usd = price_sol * sol_usd

    # Liquidity for AMM pools is 2x the SOL/Quote reserves in USD
    liquidity_usd = sol_amt * sol_usd * 2

    if not cache.get("total_supply"):
        try:
            sup = await rpc.get_token_supply(tok_mint)
            cache["total_supply"] = float((sup or {}).get("uiAmount") or 0)
            cache["token_mint"]   = tok_mint
        except Exception:
            cache["total_supply"] = 0.0

    mcap_usd = price_usd * cache["total_supply"]

    return {
        "pool_type":      pool_type,
        "price_sol":      price_sol,
        "price_usd":      price_usd,
        "mcap_usd":       mcap_usd,
        "liquidity_usd":  liquidity_usd,
        "sol_reserves":   sol_amt,
        "token_reserves": tok_amt,
        "token_mint":     cache.get("token_mint", tok_mint),
        "total_supply":   cache["total_supply"],
        "sol_usd":        sol_usd,
    }


async def _clmm_price(raw: bytes, rpc: SolRPC, sol_usd: float, cache: dict) -> dict:
    """CLMM: derive price from sqrtPriceX64 (Q64.64 fixed-point)."""
    if len(raw) < CLMM_SQRT_PRICE_OFF + 16:
        return {"pool_type": "raydium_clmm", "error": "data too short"}

    sqrt_x64 = _u128(raw, CLMM_SQRT_PRICE_OFF)
    mint_a   = _pubkey(raw, CLMM_MINT_A_OFF)
    mint_b   = _pubkey(raw, CLMM_MINT_B_OFF)
    price_r  = (sqrt_x64 / 2**64) ** 2   # raw price (no decimal adjustment)

    if mint_a == SOL_MINT:
        price_usd = price_r * sol_usd
        tok_mint  = mint_b
    elif mint_b == SOL_MINT:
        price_usd = (1.0 / price_r * sol_usd) if price_r else 0.0
        tok_mint  = mint_a
    else:
        return {"pool_type": "raydium_clmm", "error": "non-SOL CLMM pair"}

    if not cache.get("total_supply"):
        try:
            sup = await rpc.get_token_supply(tok_mint)
            cache["total_supply"] = float((sup or {}).get("uiAmount") or 0)
        except Exception:
            cache["total_supply"] = 0.0

    return {
        "pool_type":     "raydium_clmm",
        "price_usd":     price_usd,
        "mcap_usd":      price_usd * cache["total_supply"],
        "sqrt_price_x64": sqrt_x64,
        "token_mint":    tok_mint,
        "total_supply":  cache["total_supply"],
        "sol_usd":       sol_usd,
    }


async def _v4_price(raw: bytes, rpc: SolRPC, sol_usd: float, cache: dict) -> dict:
    """Raydium AMM V4: auto-detect layout by trying multiple offset variants.

    Standard: baseVault=336, quoteVault=368, baseMint=400, quoteMint=432
    Some V4 pools have shifted layouts (+4, +8, etc.). We try each.
    """
    if not cache.get("vault0"):
        # Try standard and shifted layouts first (legacy logic)
        found = False
        for shift in [0, 4, 8, -4, 16, -8]:
            bv, qv, bm, qm = V4_COIN_VAULT_OFF+shift, V4_PC_VAULT_OFF+shift, V4_COIN_MINT_OFF+shift, V4_PC_MINT_OFF+shift
            if qm+32 <= len(raw) and bv >= 0:
                m0, m1 = _pubkey(raw, bm), _pubkey(raw, qm)
                if m0 == SOL_MINT or m1 == SOL_MINT:
                    cache.update({
                        "mint0": m0, "mint1": m1, "vault0": _pubkey(raw, bv), "vault1": _pubkey(raw, qv),
                        "sol_side": 0 if m0 == SOL_MINT else 1
                    })
                    found = True; break

        if not found:
            # Exhaustive dynamic scan
            layout = _detect_sol_pair(raw, "raydium_v4")
            if layout:
                cache.update(layout); found = True
                log.info("Dynamic discovered Raydium V4: SOL Side=%d", layout["sol_side"])

        if not found:
            return {"pool_type": "raydium_v4", "error": "SOL_MINT not found in V4 account"}

    return await _vaults_to_price("raydium_v4", rpc, sol_usd, cache)


# ─────────────────────────────────────────────────────────────────────────────
#  Main Monitor
# ─────────────────────────────────────────────────────────────────────────────
class OnChainPriceMonitor:
    """
    Polls an array of Solana pair/pool/bonding-curve addresses every
    `poll_interval_ms` milliseconds and reports price + mcap.

    Parameters
    ----------
    pair_addresses   : list[str]   base58 addresses to watch
    poll_interval_ms : int         polling frequency (default 100 → 10 fps)
    on_update        : async callable(addr: str, data: dict) | None
    rpc_endpoints    : list[str]   override default RPC list
    """

    def __init__(
        self,
        pair_addresses: List[str],
        poll_interval_ms: int = 100,
        on_update: Optional[Callable] = None,
        rpc_endpoints: Optional[List[str]] = None,
    ):
        self.addresses     = list(pair_addresses)
        self.interval      = poll_interval_ms / 1000.0
        self.on_update     = on_update
        self._endpoints    = rpc_endpoints or RPC_ENDPOINTS

        # Public state — read from outside
        self.latest: Dict[str, dict] = {a: {} for a in self.addresses}
        self.stats = {"fetches": 0, "errors": 0, "started_at": 0.0}

        # Internal
        self._running  = False
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._rpc: Optional[SolRPC] = None
        self._sol: Optional[SolPrice] = None
        self._cache: Dict[str, dict] = {}   # per-address metadata cache

    # ── lifecycle ─────────────────────────────────────────────────────────────

    async def start(self):
        if self._running:
            return
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=64, ttl_dns_cache=300)
        )
        self._rpc     = SolRPC(self._endpoints, self._session)
        self._sol     = SolPrice(self._rpc)
        self._running = True
        self.stats["started_at"] = time.time()
        self._task = asyncio.create_task(self._loop())
        log.debug(
            "Started — %d address(es) @ %dms interval",
            len(self.addresses), poll_interval_ms := int(self.interval * 1000)
        )

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session:
            await self._session.close()
        uptime = time.time() - self.stats["started_at"]
        log.debug(
            "Stopped — %d fetches | %d errors | %.1fs uptime",
            self.stats["fetches"], self.stats["errors"], uptime,
        )

    # ── dynamic pair management ───────────────────────────────────────────────

    def add(self, addr: str):
        """Add a pair address to the watch list at runtime."""
        if addr not in self.addresses:
            self.addresses.append(addr)
            self.latest[addr] = {}

    def remove(self, addr: str):
        """Remove a pair address from the watch list at runtime."""
        if addr in self.addresses:
            self.addresses.remove(addr)
            self.latest.pop(addr, None)
            self._cache.pop(addr, None)

    # ── one-shot fetch ────────────────────────────────────────────────────────

    async def fetch_one(self, addr: str) -> dict:
        """
        Fetch price/mcap for a single address without starting the loop.
        Creates a temporary session if the monitor hasn't been started.
        """
        owned = self._session is None
        if owned:
            self._session = aiohttp.ClientSession()
            self._rpc     = SolRPC(self._endpoints, self._session)
            self._sol     = SolPrice(self._rpc)
        try:
            sol_usd  = await self._sol.get()
            accounts = await self._rpc.get_multiple_accounts([addr])
            result   = await self._process(addr, accounts[0] if accounts else None, sol_usd)
            result.update({"timestamp": time.time(), "pair_address": addr})
            return result
        finally:
            if owned and self._session:
                await self._session.close()
                self._session = None
                self._rpc     = None
                self._sol     = None

    # ── internals ─────────────────────────────────────────────────────────────

    async def _loop(self):
        while self._running:
            t0 = time.perf_counter()
            try:
                await self._tick()
            except Exception as e:
                self.stats["errors"] += 1
                log.error("Poll error: %s", e)
                await asyncio.sleep(1.0)   # backoff on failure
            sleep = max(0.0, self.interval - (time.perf_counter() - t0))
            if sleep:
                await asyncio.sleep(sleep)

    async def _tick(self):
        if not self.addresses:
            await asyncio.sleep(0.1)
            return

        sol_usd  = await self._sol.get()
        # Batch-fetch all pair accounts in a single RPC round-trip
        accounts = await self._rpc.get_multiple_accounts(self.addresses)

        coros = [
            self._process(addr, acct, sol_usd)
            for addr, acct in zip(self.addresses, accounts)
        ]
        results = await asyncio.gather(*coros, return_exceptions=True)

        now = time.time()
        for addr, result in zip(self.addresses, results):
            if isinstance(result, Exception):
                self.stats["errors"] += 1
                log.warning("Error on %s: %s", addr[:8], result)
                continue
            result["timestamp"]    = now
            result["pair_address"] = addr
            self.latest[addr]      = result
            self.stats["fetches"] += 1
            if self.on_update:
                try:
                    await self.on_update(addr, result)
                except Exception as e:
                    log.warning("on_update error: %s", e)

    async def _process(self, addr: str, acct: Optional[dict], sol_usd: float) -> dict:
        if acct is None:
            return {"error": "account not found"}

        owner = acct.get("owner", "")
        ptype = _pool_type(owner)

        # Decode base64 account data
        raw_list = acct.get("data", [])
        raw_b64  = raw_list[0] if isinstance(raw_list, list) and raw_list else b""
        try:
            raw = base64.b64decode(raw_b64)
        except Exception:
            raw = b""

        cache = self._cache.setdefault(addr, {})

        if ptype == "pumpfun":
            return _parse_pumpfun(raw, sol_usd)

        if ptype == "pumpswap":
            return await _pumpswap_price(raw, self._rpc, sol_usd, cache)

        if ptype == "raydium_cpmm":
            return await _cpmm_price(ptype, raw, self._rpc, sol_usd, cache, addr)

        if ptype == "raydium_clmm":
            return await _clmm_price(raw, self._rpc, sol_usd, cache)

        if ptype == "raydium_v4":
            return await _v4_price(raw, self._rpc, sol_usd, cache)

        return {
            "pool_type": "unknown",
            "owner":     owner,
            "error":     (
                f"Unrecognised program owner '{owner[:20]}'. "
                "This may be a token mint address rather than a pair/pool address. "
                "Use the pair address from DexScreener URL: dexscreener.com/solana/<PAIR_ADDRESS>"
            ),
        }


# ─────────────────────────────────────────────────────────────────────────────
#  CLI demo
# ─────────────────────────────────────────────────────────────────────────────
def _fmt_price(p) -> str:
    if not p:          return "$0"
    if p < 0.0000001:  return f"${p:.3e}"
    if p < 0.001:      return f"${p:.9f}"
    if p < 1:          return f"${p:.6f}"
    return f"${p:,.4f}"

def _fmt_mcap(m) -> str:
    if not m:       return "$0"
    if m >= 1e6:    return f"${m/1e6:.2f}M"
    if m >= 1e3:    return f"${m/1e3:.2f}K"
    return f"${m:.0f}"

def _fmt_sol_price(p) -> str:
    if not p:          return "0.00"
    if p < 0.0000001:  return f"{p:.3e}"
    if p < 0.001:      return f"{p:.9f}"
    if p < 1:          return f"{p:.6f}"
    return f"{p:,.4f}"


async def _cli_demo(addresses: List[str], duration_s: int):
    counter: Dict[str, int] = {a: 0 for a in addresses}

    sys.stdout.write("════════════════════════════════════════════════════════════════════════\n")
    sys.stdout.write("  ⚡ OnChain Price Monitor  —  Direct Solana RPC\n")
    sys.stdout.write("════════════════════════════════════════════════════════════════════════\n\n")

    for a in addresses:
        sys.stdout.write(f"  📍 [{a}]\n")
    sys.stdout.write(f"\n  Interval : 100ms   |   Duration : {duration_s}s\n\n")

    async def on_update(addr: str, data: dict):
        counter[addr] = counter.get(addr, 0) + 1
        err   = data.get("error")
        ptype = data.get("pool_type", "?")
        tick  = counter[addr]
        short = addr[:10] + "…"

        # Get live sol price from monitor state
        sol_usd_val = monitor._sol._price if monitor._sol else 0.0

        if err:
            sys.stdout.write(f"[{time.strftime('%H:%M:%S')}] [{short}] [{ptype:<13}] ❌ {err}\n")
        else:
            price = _fmt_price(data.get("price_usd"))
            mcap  = _fmt_mcap(data.get("mcap_usd"))
            liq   = _fmt_mcap(data.get("liquidity_usd"))
            sol   = data.get("price_sol", 0)
            sol_str = _fmt_sol_price(sol)
            sys.stdout.write(
                f"[{time.strftime('%H:%M:%S')}] [{short}] [{ptype:<13}]  Price: {price:<12} "
                f"Mcap: {mcap:<8}  Liq: {liq:<8}  ({sol_str} SOL)  [SOL=${sol_usd_val:.2f}]  [Tick: {tick}]\n"
            )
        sys.stdout.flush()

    monitor = OnChainPriceMonitor(
        pair_addresses=addresses,
        poll_interval_ms=100,
        on_update=on_update,
    )

    try:
        await monitor.start()
        await asyncio.sleep(duration_s)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        pass
    finally:
        await monitor.stop()

    sys.stdout.write("\n════════════════════════════════════════════════════════════════════════\n")
    sys.stdout.write(f"  ✅  Done — {monitor.stats['fetches']} successful fetches | "
          f"{monitor.stats['errors']} errors\n")
    sys.stdout.write("════════════════════════════════════════════════════════════════════════\n\n")


# ── Default test addresses ────────────────────────────────────────────────────
DEFAULT_TEST_PAIRS = [
    "42BPkG5SPVExcCdvAVBkcpL5aCz2y6FqQ9E82FwiH3sS",
    "6gL9ZmVqZd5SLFigakhFVejotwM7yWBEvsEfEN6QEm54",
]

if __name__ == "__main__":
    _args = sys.argv[1:]

    _duration = 30
    _addresses = []
    _i = 0
    while _i < len(_args):
        if _args[_i] in ("--duration", "-d") and _i + 1 < len(_args):
            _duration = int(_args[_i + 1])
            _i += 2
        else:
            _addresses.append(_args[_i])
            _i += 1

    # Interactive prompt if no addresses provided via CLI
    if not _addresses:
        print("\n" + "═" * 72)
        print("  ⚡ OnChain Price Monitor")
        print("═" * 72)
        raw = input("\n  Paste pair address(es) separated by commas:\n  > ").strip()
        if not raw:
            print("  ❌ No addresses provided. Exiting.")
            sys.exit(1)
        _addresses = [a.strip() for a in raw.split(",") if a.strip()]
        if not _addresses:
            print("  ❌ No valid addresses found. Exiting.")
            sys.exit(1)

        dur_input = input(f"\n  Duration in seconds (default {_duration}): ").strip()
        if dur_input.isdigit() and int(dur_input) > 0:
            _duration = int(dur_input)

    # Validate addresses (Base58 check)
    _B58_CHARS = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    _valid = []
    for addr in _addresses:
        if len(addr) < 32 or len(addr) > 44:
            print(f"  ⚠️  Skipping '{addr}' — wrong length ({len(addr)} chars, expected 32-44)")
        elif not all(c in _B58_CHARS for c in addr):
            bad = [c for c in addr if c not in _B58_CHARS]
            print(f"  ⚠️  Skipping '{addr[:20]}...' — invalid Base58 characters: {bad[:5]}")
            print(f"      Solana addresses are CASE-SENSITIVE! Copy the exact address from DexScreener.")
        else:
            _valid.append(addr)

    if not _valid:
        print("\n  ❌ No valid pair addresses. Exiting.")
        sys.exit(1)

    asyncio.run(_cli_demo(_valid, _duration))

