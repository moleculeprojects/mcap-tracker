"""
candle_tracker.py
-----------------
Fetches 15-minute OHLCV candle data from GeckoTerminal to determine:
  - Birth MCAP  : open of the first candle
  - ATH MCAP    : maximum High across all candles
  - Current MCAP: close of the latest candle

GeckoTerminal is used because its Solana OHLCV bars are the same data that
DexScreener charts render — it is the only source that provides actual candle
Highs (true ATH) without scanning on-chain transactions.

DexScreener's public pairs API only exposes h1/h6/h24 % changes which
cannot reliably capture ATH values that occurred within the first hour of
a token's life.
"""

import requests
import time
import logging
import sys

# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO, format="[CandleTracker] %(message)s")
logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# Play Strategies Configuration
# ──────────────────────────────────────────────────────────────────────────────

# Play 1: Micro Pump Strategy
PLAY_1_LAUNCH_MCAP_RANGE  = (20_000, 45_000)
PLAY_1_ATH_MAX             = 75_000
PLAY_1_REENTRY_MCAP_RANGE  = (7_000, 15_000)

# Play 2: Moderate Pump Strategy
PLAY_2_LAUNCH_MCAP_RANGE  = (20_000, 45_000)
PLAY_2_ATH_RANGE           = (75_000, 140_000)
PLAY_2_REENTRY_MCAP_RANGE  = (15_000, 30_000)

TOTAL_SUPPLY = 1_000_000_000  # pump.fun default

# ── Signal & Logic Constants ──────────────────────────────────────────────────
MIN_PUSH_PCT           = 15      # Push must be at least this % above Birth to be an ATH
RETRACE_DEPTH_PCT      = 50      # Drop must be at least this % from ATH (Choice B: ATH * 0.5)


# ──────────────────────────────────────────────────────────────────────────────
# CandleTracker
# ──────────────────────────────────────────────────────────────────────────────

class CandleTracker:
    """
    Analyses a token pair's MCAP history using GeckoTerminal 15-minute candles.
    GeckoTerminal is the same OHLCV source that DexScreener's chart UI renders.
    """

    GECKO_URL = (
        "https://api.geckoterminal.com/api/v2"
        "/networks/solana/pools/{pair}/ohlcv/minute?aggregate=15&limit=1000"
    )

    def __init__(self):
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        }

    # ── Candle Fetch ───────────────────────────────────────────────────────────

    def fetch_bars(self, pair_address: str) -> list | None:
        """
        Fetch 15-min OHLCV bars from GeckoTerminal.
        Returns list of bars ordered newest → oldest (Gecko default), or None.
        Each bar: [timestamp, open, high, low, close, volume]
        """
        url = self.GECKO_URL.format(pair=pair_address)
        try:
            resp = requests.get(url, headers=self.headers, timeout=15)
            if resp.status_code != 200:
                logger.error(f"GeckoTerminal returned {resp.status_code}")
                return None
            bars = (
                resp.json()
                .get("data", {})
                .get("attributes", {})
                .get("ohlcv_list", [])
            )
            if not bars:
                logger.warning("Empty bar list from GeckoTerminal.")
                return None
            return bars
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            return None

    # ── Analysis ───────────────────────────────────────────────────────────────

    def analyze(self, pair_address: str, name: str = "Unknown") -> dict | None:
        """
        Full Strategy Algorithm:
        1. Setup: Birth MCAP = Open of first bar.
        2. Stage 1 (Floating): Track highest High seen chronologically.
           - Start looking for signal only once highest High >= Birth * 1.15.
        3. Stage 2 (The Lock): Look for the FIRST Red Candle (Close < Open) that:
           - Closes below Birth OR
           - Hits 60% drop from highest High (Low <= High * 0.40)
        4. Stage 3 (Final): Once locked, assign play_ath = max High seen until lock.
           - Check all subsequent candles for ceiling violation.
           - Ceiling is Birth if signal candle was < Birth.
           - Ceiling is ATH if signal candle was > Birth.
        """
        bars = self.fetch_bars(pair_address)
        if not bars:
            return None

        # Gecko returns newest-first; reverse so index 0 = birth
        bars_asc = list(reversed(bars))

        mcap_birth   = bars_asc[0][1] * TOTAL_SUPPLY
        mcap_current = bars_asc[-1][4] * TOTAL_SUPPLY
        
        floating_ath = 0.0
        is_locked = False
        locked_ath = 0.0
        retrace_below_birth = False
        lock_index = -1
        violated_after_lock = False
        
        # Signal buffers to ignore noise wicks
        min_qualifying_high = mcap_birth * (1 + MIN_PUSH_PCT / 100)
        retrace_multiplier  = (100 - RETRACE_DEPTH_PCT) / 100
        
        for i, bar in enumerate(bars_asc):
            b_open  = bar[1] * TOTAL_SUPPLY
            b_high  = bar[2] * TOTAL_SUPPLY
            b_low   = bar[3] * TOTAL_SUPPLY
            b_close = bar[4] * TOTAL_SUPPLY
            
            # Stage 1 & 2: Finding the Lock
            if not is_locked:
                # Update floating peak
                if b_high > floating_ath:
                    floating_ath = b_high
                
                # Signal must be a Red Candle
                is_red = (b_close < b_open)
                
                if is_red and floating_ath >= min_qualifying_high:
                    # Choice B Math: e.g. 60% drop = 40% value remaining
                    drain_limit = floating_ath * retrace_multiplier
                    
                    if b_close <= mcap_birth or b_low <= drain_limit:
                        is_locked = True
                        locked_ath = floating_ath
                        retrace_below_birth = (b_close <= mcap_birth)
                        lock_index = i
            
            # Stage 3: Monitoring After Lock
            elif is_locked:
                ceiling = mcap_birth if retrace_below_birth else locked_ath
                if b_high > ceiling:
                    violated_after_lock = True
        
        # ── Reporting Stats ──
        # Global ATH for display if play is invalid
        global_ath = max(b[2] for b in bars_asc) * TOTAL_SUPPLY
        
        is_play_valid = is_locked and not violated_after_lock
        
        fail_reason = ""
        if not (PLAY_1_LAUNCH_MCAP_RANGE[0] <= mcap_birth <= PLAY_1_LAUNCH_MCAP_RANGE[1]):
            fail_reason = f"Birth MCAP ${mcap_birth:,.0f} out of range ($20k-$45k)"
        elif not is_locked:
            fail_reason = f"No qualifying Red Candle retracement ({RETRACE_DEPTH_PCT}% drop or below birth) found"
        elif violated_after_lock:
            ceiling_type = "Birth" if retrace_below_birth else "ATH"
            fail_reason = f"LATE: Re-pumped above {ceiling_type} after signal lock"
        
        play_1_pattern = False
        play_2_pattern = False
        
        if is_play_valid:
            if locked_ath <= PLAY_1_ATH_MAX:
                play_1_pattern = True
            elif PLAY_2_ATH_RANGE[0] <= locked_ath <= PLAY_2_ATH_RANGE[1]:
                play_2_pattern = True
            else:
                fail_reason = f"Locked ATH ${locked_ath:,.0f} outside Play 1/2 ranges"

        # Reentry entry check
        play_1_entry = play_1_pattern and (
            PLAY_1_REENTRY_MCAP_RANGE[0] <= mcap_current <= PLAY_1_REENTRY_MCAP_RANGE[1]
        )
        play_2_entry = play_2_pattern and (
            PLAY_2_REENTRY_MCAP_RANGE[0] <= mcap_current <= PLAY_2_REENTRY_MCAP_RANGE[1]
        )

        pump_pct = ((locked_ath - mcap_birth) / mcap_birth * 100) if mcap_birth > 0 else 0
        dump_pct = ((locked_ath - mcap_current) / locked_ath * 100) if locked_ath > 0 else 0

        logger.info(
            f"{name} | Birth: ${mcap_birth:,.0f} | LockedATH: ${locked_ath:,.0f} "
            f"| Locked: {is_locked} | Violated: {violated_after_lock}"
        )

        return {
            "name":          name,
            "pair":          pair_address,
            "birth_mcap":    round(mcap_birth,   2),
            "ath_mcap":      round(locked_ath if is_locked else global_ath, 2),
            "current_mcap":  round(mcap_current, 2),
            "pump_pct":      round(pump_pct if is_locked else 0, 2),
            "dump_pct":      round(dump_pct if is_locked else 0, 1),
            "bars_scanned":  len(bars_asc),
            "play_1_match":  play_1_pattern,
            "play_2_match":  play_2_pattern,
            "play_1_entry":  play_1_entry,
            "play_2_entry":  play_2_entry,
            "is_struct_valid": is_play_valid,
            "fail_reason":   fail_reason
        }


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) > 1:
        pair = sys.argv[1].split("/")[-1]
    else:
        raw = input("Enter pair address or DexScreener URL: ").strip()
        pair = raw.split("/")[-1]

    tracker = CandleTracker()
    p = tracker.analyze(pair, name="TEST TOKEN")

    if p:
        print(f"\n{'─'*50}")
        print(f"  Token          : {p['name']}")
        print(f"  Birth MCAP     : ${p['birth_mcap']:>12,.0f}")
        print(f"  ATH MCAP       : ${p['ath_mcap']:>12,.0f}  (+{p['pump_pct']}%)")
        print(f"  Current MCAP   : ${p['current_mcap']:>12,.0f}")
        print(f"  Dump from ATH  : {p['dump_pct']}%")
        print(f"  Bars Scanned   : {p['bars_scanned']}")
        print(f"{'─'*50}")
        if p.get("play_1_match"):
            print("✅ PLAY 1 MATCH (Micro Pump — ATH ≤ $75K, reentry $7K–$15K)")
        elif p.get("play_2_match"):
            print("✅ PLAY 2 MATCH (Moderate Pump — ATH $75K–$140K, reentry $15K–$30K)")
        else:
            print("❌ No play match")
            if p.get("fail_reason"):
                print(f"   Reason: {p['fail_reason']}")
    else:
        logger.error("Could not retrieve candle data.")
