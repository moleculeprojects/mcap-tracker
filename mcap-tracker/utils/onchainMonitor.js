/**
 * 🛰️ MCAP Tracker - On-Chain Monitor (Node.js Port)
 * Unified monitor that handles blockchain interactions, price tracking, and database updates.
 *
 * ─────────────────────────────────────────────────────────────
 * PERFORMANCE ARCHITECTURE (v2 — Parallel + Pre-batched)
 * ─────────────────────────────────────────────────────────────
 * OLD: Sequential for-loop → each Raydium token blocked the loop for a separate
 *      RPC vault-fetch call → with 20 tokens this could take 20–30 seconds per cycle.
 *
 * NEW:
 *   1. Fetch ALL pair account data in ONE batched RPC call (unchanged — already fast).
 *   2. DECODE which vaults each token needs (no I/O here — pure in-memory parsing).
 *   3. Collect ALL required vault addresses in one flat list.
 *   4. Fetch ALL vault balances in ONE SINGLE batched RPC call.
 *   5. Process all tokens in PARALLEL via Promise.all (no I/O blocking any longer).
 *   6. Persist DB updates in parallel.
 *
 * Result: latency goes from O(n * rpc_delay) → O(2 * rpc_delay), regardless of token count.
 * With 30 tokens, expected cycle time drops from ~20s to under 1s.
 */

const axios  = require('axios');
const crypto = require('crypto');
const fs     = require('fs');
const path   = require('path');
const { calculateExitConditions } = require('./tradeLogic');

// --- Reentry Zone Definitions (configurable via .env) ---
const PLAY_1_REENTRY_RANGE = [
    Number(process.env.REENTRY_PLAY1_MIN) || 7_000,
    Number(process.env.REENTRY_PLAY1_MAX) || 15_000
];
const PLAY_2_REENTRY_RANGE = [
    Number(process.env.REENTRY_PLAY2_MIN) || 15_000,
    Number(process.env.REENTRY_PLAY2_MAX) || 30_000
];

// ─────────────────────────────────────────────────────────────
// Telegram
// ─────────────────────────────────────────────────────────────
let _telegramConfig = null;
function loadTelegramConfig() {
    if (_telegramConfig) return _telegramConfig;
    let botToken = process.env.BOT_TOKEN;
    let chatIds  = process.env.CHAT_IDS;

    if (!botToken || !chatIds) {
        const envPath = path.join(__dirname, '..', '..', '.env');
        if (fs.existsSync(envPath)) {
            for (const line of fs.readFileSync(envPath, 'utf8').split('\n')) {
                const trimmed = line.trim();
                if (!trimmed || trimmed.startsWith('#')) continue;
                const eqIdx = trimmed.indexOf('=');
                if (eqIdx < 0) continue;
                const key = trimmed.slice(0, eqIdx).trim();
                const val = trimmed.slice(eqIdx + 1).trim();
                if (key === 'BOT_TOKEN' && !botToken) botToken = val;
                if (key === 'CHAT_IDS'  && !chatIds)  chatIds  = val;
            }
        }
    }
    _telegramConfig = {
        botToken: botToken || null,
        chatIds: chatIds ? chatIds.split(',').map(s => s.trim()).filter(Boolean) : [],
    };
    return _telegramConfig;
}

async function sendTelegram(message) {
    const { botToken, chatIds } = loadTelegramConfig();
    if (!botToken || !chatIds.length) return;
    for (const chatId of chatIds) {
        try {
            await axios.post(`https://api.telegram.org/bot${botToken}/sendMessage`, {
                chat_id:    chatId,
                text:       message,
                parse_mode: 'HTML',
            }, { timeout: 10000 });
        } catch (e) {
            console.error(`[Telegram] Failed to send to ${chatId}: ${e.message}`);
        }
    }
}

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────
const RPC_ENDPOINTS = [
    'https://mainnet.helius-rpc.com/?api-key=1b7550b1-47c2-4a79-83c1-4a88a8dcc2e7',
    'https://api.mainnet-beta.solana.com',
];

const PUMPFUN_PROGRAM_ID    = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const RAYDIUM_AMM_V4        = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
const RAYDIUM_CPMM_PROGRAM  = 'CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C';
const PUMPSWAP_PROGRAM_ID   = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const SOL_MINT              = 'So11111111111111111111111111111111111111112';

// Settings
const ENABLE_TICK_LOGGING      = false;
const ENABLE_HEARTBEAT_LOGGING = false;

// ─────────────────────────────────────────────────────────────
// Base58 helpers
// ─────────────────────────────────────────────────────────────
const _B58 = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
function encodeBase58(buffer) {
    let n   = BigInt('0x' + buffer.toString('hex'));
    let out = [];
    while (n >= 58n) { let r = n % 58n; n = n / 58n; out.push(_B58[Number(r)]); }
    out.push(_B58[Number(n)]);
    for (let i = 0; i < buffer.length; i++) { if (buffer[i] === 0) out.push(_B58[0]); else break; }
    return out.reverse().join('');
}
function getPubkey(buffer, offset) {
    return encodeBase58(buffer.slice(offset, offset + 32));
}

// ─────────────────────────────────────────────────────────────
// RPC Client  — with endpoint rotation and retry
// ─────────────────────────────────────────────────────────────
class SolRPC {
    constructor(endpoints) { this.endpoints = endpoints; }

    async call(method, params) {
        let lastError = null;
        for (let i = 0; i < this.endpoints.length * 2; i++) {
            const url = this.endpoints[i % this.endpoints.length];
            try {
                const res = await axios.post(url, {
                    jsonrpc: '2.0', id: 1, method, params,
                }, { timeout: 8000 });
                if (res.data.error) throw new Error(JSON.stringify(res.data.error));
                return res.data.result;
            } catch (err) { lastError = err; }
        }
        throw new Error(`RPC failed: ${lastError?.message}`);
    }

    /** Fetch raw base64 account data for up to 100 keys in one call. */
    async getMultipleAccounts(keys) {
        if (!keys.length) return [];
        const res = await this.call('getMultipleAccounts', [keys, { encoding: 'base64', commitment: 'processed' }]);
        return res?.value || new Array(keys.length).fill(null);
    }

    /** Fetch parsed token-account data (uiAmount) for multiple keys in one call. */
    async getParsedTokenAccounts(keys) {
        if (!keys.length) return [];
        const res = await this.call('getMultipleAccounts', [keys, { encoding: 'jsonParsed', commitment: 'processed' }]);
        return (res?.value || []).map(val => {
            if (val?.data?.parsed?.info) {
                const info = val.data.parsed.info;
                return { uiAmount: info.tokenAmount?.uiAmount || 0, mint: info.mint || '' };
            }
            return null;
        });
    }

    async getTokenSupply(mint) {
        const res = await this.call('getTokenSupply', [mint, { commitment: 'processed' }]);
        return res?.value;
    }
}

// ─────────────────────────────────────────────────────────────
// SOL/USD price cache
// ─────────────────────────────────────────────────────────────
class SolPrice {
    static WSOL_VAULT = 'DQyrAcCrDXQ7NeoqGgDCZwBvWDcYmFCjSb9JtteuvPpz';
    static USDC_VAULT = 'HLmqeL62xR1QoZ1HKKbXRrdN1p3phKpxRMb2VVopvBBz';

    constructor(rpc) { this.rpc = rpc; this.price = 220.0; this.ts = 0; }

    async get() {
        if (Date.now() - this.ts < 500) return this.price;
        try {
            const bals = await this.rpc.getParsedTokenAccounts([SolPrice.WSOL_VAULT, SolPrice.USDC_VAULT]);
            if (bals.length === 2 && bals[0] && bals[1]) {
                const s = bals[0].uiAmount, u = bals[1].uiAmount;
                if (s > 0) { this.price = u / s; this.ts = Date.now(); }
            }
        } catch (e) { console.error('SOL Price Error:', e.message); }
        return this.price;
    }
}

// ─────────────────────────────────────────────────────────────
// Layout parsing — PURE / synchronous (no I/O)
// Decode where vaults live for each AMM type.
// ─────────────────────────────────────────────────────────────

/**
 * Decode a PumpFun bonding-curve account synchronously.
 * Returns { mcapUsd, priceUsd, priceSol, totalSupply } or null on failure.
 */
function decodePumpFun(buffer, solUsd) {
    if (buffer.length < 49) return null;
    const vToken = buffer.readBigUInt64LE(8);
    const vSol   = buffer.readBigUInt64LE(16);
    if (vToken === 0n) return null;
    const priceSol   = (Number(vSol) / 1e9) / (Number(vToken) / 1e6);
    const priceUsd   = priceSol * solUsd;
    const totalSupply = 1e9;
    return { mcapUsd: priceUsd * totalSupply, priceUsd, priceSol, totalSupply, needsVaults: false };
}

/**
 * For AMM types (Raydium, PumpSwap), parse address metadata from the raw
 * account buffer and return { vault0, vault1, mint0, mint1, solSide } so the
 * caller can batch-fetch all vault balances together.
 *
 * Returns null if vaults cannot be determined.
 */
function extractVaultAddresses(owner, buffer, tCache) {
    // Return cached result immediately
    if (tCache.vault0) return { vault0: tCache.vault0, vault1: tCache.vault1,
                                 mint0: tCache.mint0,   mint1: tCache.mint1,
                                 solSide: tCache.solSide };

    if (owner === RAYDIUM_AMM_V4) {
        for (const shift of [0, 4, 8, -4, 16, -8]) {
            const bv = 336 + shift, qv = 368 + shift, bm = 400 + shift, qm = 432 + shift;
            if (qm + 32 > buffer.length || bv < 0) continue;
            const m0 = getPubkey(buffer, bm), m1 = getPubkey(buffer, qm);
            if (m0 === SOL_MINT || m1 === SOL_MINT) {
                const layout = {
                    mint0: m0, mint1: m1,
                    vault0: getPubkey(buffer, bv), vault1: getPubkey(buffer, qv),
                    solSide: m0 === SOL_MINT ? 0 : 1,
                };
                Object.assign(tCache, layout);
                return layout;
            }
        }
        return null;
    }

    if (owner === RAYDIUM_CPMM_PROGRAM) {
        const m0 = getPubkey(buffer, 72), m1 = getPubkey(buffer, 104);
        if (m0 === SOL_MINT || m1 === SOL_MINT) {
            const layout = {
                mint0: m0, mint1: m1,
                vault0: getPubkey(buffer, 168), vault1: getPubkey(buffer, 200),
                solSide: m0 === SOL_MINT ? 0 : 1,
            };
            Object.assign(tCache, layout);
            return layout;
        }
        return null;
    }

    if (owner === PUMPSWAP_PROGRAM_ID) {
        // Try standard Anchor offsets first
        for (const [b, q, v0, v1] of [[43, 75, 139, 171]]) {
            if (q + 32 > buffer.length) continue;
            const m0 = getPubkey(buffer, b), m1 = getPubkey(buffer, q);
            if (m0 === SOL_MINT || m1 === SOL_MINT) {
                const layout = {
                    mint0: m0, mint1: m1,
                    vault0: getPubkey(buffer, v0), vault1: getPubkey(buffer, v1),
                    solSide: m0 === SOL_MINT ? 0 : 1,
                };
                Object.assign(tCache, layout);
                return layout;
            }
        }
        return null;
    }

    return null;
}

/**
 * Compute mcap from vault balances (already fetched).
 * b0/b1 are parsed token-account objects { uiAmount }.
 */
function computeMcapFromVaults(layout, b0, b1, solUsd, tCache, totalSupplyFallback) {
    if (!b0 || !b1) return null;
    const amt0 = b0.uiAmount || 0;
    const amt1 = b1.uiAmount || 0;

    let solAmt, tokAmt, tokMint;
    if (layout.solSide === 0 || layout.mint0 === SOL_MINT) {
        [solAmt, tokAmt, tokMint] = [amt0, amt1, layout.mint1];
    } else {
        [solAmt, tokAmt, tokMint] = [amt1, amt0, layout.mint0];
    }

    if (tokAmt === 0) return null;

    const priceSol  = solAmt / tokAmt;
    const priceUsd  = priceSol * solUsd;
    const supply    = tCache.totalSupply || totalSupplyFallback || 1e9;
    const mcapUsd   = priceUsd * supply;

    return { mcapUsd, priceUsd, priceSol, totalSupply: supply, needsVaults: true };
}

// ─────────────────────────────────────────────────────────────
// DB helpers
// ─────────────────────────────────────────────────────────────
function dbRun(db, sql, params) {
    return new Promise((resolve, reject) =>
        db.run(sql, params, err => err ? reject(err) : resolve()));
}
function dbAll(db, sql, params) {
    return new Promise((resolve, reject) =>
        db.all(sql, params, (err, rows) => err ? reject(err) : resolve(rows)));
}

// ─────────────────────────────────────────────────────────────
// Main monitor
// ─────────────────────────────────────────────────────────────
async function startMonitor(db) {
    const rpc      = new SolRPC(RPC_ENDPOINTS);
    const solPrice = new SolPrice(rpc);
    const cache    = {};   // per-token-address vault metadata cache

    console.log('🛰️ JS On-Chain Monitor Initialized (v2 — Parallel Execution)');

    // Per-token supply cache to avoid repeated getTokenSupply calls
    const supplyCache = {};    // mint → totalSupply (float)

    let loopCount  = 0;
    let cycleMs    = 0;        // rolling cycle time for diagnostics

    while (true) {
        const cycleStart = Date.now();
        try {
            // ── 1. Load active tokens ───────────────────────────────────────
            const tokens = await dbAll(db,
                `SELECT id, name, link, address, pair_address,
                        captured_mcap, current_mcap, highest_mcap,
                        captured_mcap_sol, current_mcap_sol, highest_mcap_sol,
                        post_exit_highest_mcap, post_exit_lowest_mcap,
                        post_exit_highest_mcap_sol, post_exit_lowest_mcap_sol,
                        trailing_sl_sol, total_supply, play, reentry_alerted, status
                 FROM tokens WHERE status IN ('active', 'stopped')`, []);

            if (tokens.length === 0) {
                await new Promise(r => setTimeout(r, 3000));
                continue;
            }

            // ── 2. Heartbeat ────────────────────────────────────────────────
            if (loopCount % 20 === 0 && ENABLE_HEARTBEAT_LOGGING) {
                console.log(`📡 [Monitor] Active: ${tokens.length} tokens | last cycle: ${cycleMs}ms`);
            }
            loopCount++;

            // ── 3. Fetch SOL price ──────────────────────────────────────────
            const solUsd = await solPrice.get();

            // ── 4. Batch-fetch ALL pair accounts in ONE RPC call ────────────
            const monitorAddresses = tokens.map(t => t.pair_address || t.address);
            const accounts         = await rpc.getMultipleAccounts(monitorAddresses);

            // ── 5. Decode vault addresses (pure, no I/O) ────────────────────
            //    Collect every unique vault address needed across all tokens.
            const vaultMap    = {};   // vault address → index in flat list
            const tokenLayouts = []; // parallel to tokens[]

            for (let i = 0; i < tokens.length; i++) {
                const token  = tokens[i];
                const acct   = accounts[i];
                tokenLayouts.push(null);

                if (!acct) continue;

                const owner  = String(acct.owner);
                const buffer = Buffer.from(acct.data[0], 'base64');
                const tCache = cache[token.address] || (cache[token.address] = {});

                if (owner === PUMPFUN_PROGRAM_ID) {
                    // PumpFun computes mcap inline — no vaults needed
                    tokenLayouts[i] = { type: 'pumpfun', buffer, owner };
                } else {
                    const layout = extractVaultAddresses(owner, buffer, tCache);
                    if (layout) {
                        tokenLayouts[i] = { type: 'amm', buffer, owner, layout, tCache };
                        // Register vault addresses for batch fetch
                        for (const vaultAddr of [layout.vault0, layout.vault1]) {
                            if (!(vaultAddr in vaultMap)) {
                                vaultMap[vaultAddr] = Object.keys(vaultMap).length;
                            }
                        }
                    } else if (owner !== '' && !owner.includes('TokenkegQ')) {
                        if (ENABLE_TICK_LOGGING) {
                            const ts = new Date().toTimeString().slice(0, 8);
                            console.log(`[${ts}] ⚠️ Unrecognised owner "${owner}" for ${token.name}`);
                        }
                    }
                }
            }

            // ── 6. Batch-fetch ALL vault balances in ONE RPC call ───────────
            const vaultAddresses = Object.keys(vaultMap);
            let   vaultBalances  = [];
            if (vaultAddresses.length > 0) {
                vaultBalances = await rpc.getParsedTokenAccounts(vaultAddresses);
            }

            // ── 7. Process all tokens IN PARALLEL ───────────────────────────
            const now   = new Date();
            const dbNow = now.toISOString().replace('T', ' ').split('.')[0];

            const updates = await Promise.all(tokens.map(async (token, i) => {
                const layout = tokenLayouts[i];
                if (!layout) return null;   // account not found / unrecognised

                let priceInfo = null;

                if (layout.type === 'pumpfun') {
                    priceInfo = decodePumpFun(layout.buffer, solUsd);
                } else if (layout.type === 'amm') {
                    const { vault0, vault1 } = layout.layout;
                    const b0 = vaultBalances[vaultMap[vault0]] || null;
                    const b1 = vaultBalances[vaultMap[vault1]] || null;

                    // Handle total supply (cache it to avoid repeated RPC calls)
                    const tCache = layout.tCache;
                    if (!tCache.totalSupply) {
                        const tokMint = layout.layout.solSide === 0
                            ? layout.layout.mint1
                            : layout.layout.mint0;
                        if (!supplyCache[tokMint]) {
                            try {
                                const sup = await rpc.getTokenSupply(tokMint);
                                supplyCache[tokMint] = parseFloat(sup?.uiAmount || 0);
                            } catch (_) { supplyCache[tokMint] = 0; }
                        }
                        tCache.totalSupply = supplyCache[tokMint];
                    }

                    priceInfo = computeMcapFromVaults(layout.layout, b0, b1, solUsd, tCache, token.total_supply);
                }

                if (!priceInfo || priceInfo.mcapUsd == null) return null;

                const { mcapUsd, priceUsd, priceSol, totalSupply } = priceInfo;
                const mcapSol   = mcapUsd / solUsd;
                const mcapK     = (mcapUsd / 1000).toFixed(2);

                if (ENABLE_TICK_LOGGING) {
                    const ts = now.toTimeString().slice(0, 8);
                    const usedAddr = monitorAddresses[i];
                    console.log(`[${ts}] [${usedAddr.slice(0, 10)}…]  Mcap: $${mcapK}K`);
                }

                // -- SOL-based values --
                const capturedSol = token.captured_mcap_sol || (token.captured_mcap / solUsd);
                const peakSol     = Math.max(token.highest_mcap_sol || 0, mcapSol);
                const peakUsd     = Math.max(token.highest_mcap    || 0, mcapUsd);

                // -- Reentry zone check --
                const tokenPlay = token.play || 'No';
                if (tokenPlay !== 'No' && token.reentry_alerted === 0) {
                    let inReentry = false, isBelowEntry = false, reentryRange = null, minEntry = 0;
                    if (tokenPlay === 'Play 1') {
                        minEntry = PLAY_1_REENTRY_RANGE[0];
                        inReentry = mcapUsd >= minEntry && mcapUsd <= PLAY_1_REENTRY_RANGE[1];
                        isBelowEntry = mcapUsd < minEntry;
                        reentryRange = `$${(minEntry/1000).toFixed(0)}K–$${(PLAY_1_REENTRY_RANGE[1]/1000).toFixed(0)}K`;
                    } else if (tokenPlay === 'Play 2') {
                        minEntry = PLAY_2_REENTRY_RANGE[0];
                        inReentry = mcapUsd >= minEntry && mcapUsd <= PLAY_2_REENTRY_RANGE[1];
                        isBelowEntry = mcapUsd < minEntry;
                        reentryRange = `$${(minEntry/1000).toFixed(0)}K–$${(PLAY_2_REENTRY_RANGE[1]/1000).toFixed(0)}K`;
                    }

                    if (inReentry) {
                        console.log(`🎯 [REENTRY] ${token.name} (${tokenPlay}) hit reentry zone @ $${mcapK}K`);
                        db.run(`UPDATE tokens SET reentry_alerted = 1 WHERE id = ?`, [token.id]);
                        const msg =
                            `🎯 <b>REENTRY ZONE HIT</b>\n` +
                            `<b>${token.name}</b> — ${tokenPlay}\n` +
                            `Current Mcap: <b>$${mcapK}K</b>\n` +
                            `Reentry Range: <b>${reentryRange}</b>\n` +
                            `\n${token.link || ''}`;
                        sendTelegram(msg).catch(e => console.error('[Telegram] Reentry alert error:', e.message));
                    } else if (isBelowEntry) {
                        console.log(`🔫 [SNIPER] ${token.name} (${tokenPlay}) below reentry zone @ $${mcapK}K`);
                        db.run(`UPDATE tokens SET reentry_alerted = 2 WHERE id = ?`, [token.id]);
                        const msg =
                            `🔫 <b>SNIPER ALERT</b>\n` +
                            `<b>${token.name}</b> — ${tokenPlay}\n` +
                            `Token is <b>BELOW</b> Reentry Range!\n` +
                            `Current Mcap: <b>$${mcapK}K</b>\n` +
                            `Min Entry: <b>$${(minEntry/1000).toFixed(0)}K</b>\n` +
                            `\n${token.link || ''}`;
                        sendTelegram(msg).catch(e => console.error('[Telegram] Sniper alert error:', e.message));
                    }
                }

                // -- Exit conditions (SL / trailing SL) --
                let shouldExit = false, reason = "", newTrailSol = token.trailing_sl_sol || 0;
                if (token.status === 'active') {
                    const exitCheck = calculateExitConditions(mcapSol, capturedSol, peakSol, token.trailing_sl_sol || 0);
                    shouldExit = exitCheck.shouldExit;
                    reason = exitCheck.reason;
                    newTrailSol = exitCheck.newTrailingSl;
                }

                // -- Post-exit calculations --
                let postExitHighest = token.post_exit_highest_mcap;
                let postExitLowest = token.post_exit_lowest_mcap;
                let postExitHighestSol = token.post_exit_highest_mcap_sol;
                let postExitLowestSol = token.post_exit_lowest_mcap_sol;

                if (token.status === 'stopped' || shouldExit) {
                    // Update post-exit metrics
                    postExitHighest = Math.max(postExitHighest || 0, mcapUsd);
                    postExitLowest = (postExitLowest === null || postExitLowest === 0) ? mcapUsd : Math.min(postExitLowest, mcapUsd);
                    postExitHighestSol = Math.max(postExitHighestSol || 0, mcapSol);
                    postExitLowestSol = (postExitLowestSol === null || postExitLowestSol === 0) ? mcapSol : Math.min(postExitLowestSol, mcapSol);
                }

                return {
                    token, shouldExit, reason, newTrailSol,
                    mcapUsd, mcapSol, peakUsd, peakSol, capturedSol,
                    postExitHighest, postExitLowest, postExitHighestSol, postExitLowestSol,
                    totalSupply: totalSupply || Math.pow(10, 9),
                    dbNow, mcapK,
                };
            }));

            // ── 8. Persist DB updates ────────────────────────────────────────
            //    Fire all DB writes in parallel (SQLite handles the queue).
            const dbOps = updates
                .filter(u => u !== null)
                .map(u => {
                    const { token, shouldExit, reason, newTrailSol,
                            mcapUsd, mcapSol, peakUsd, peakSol,
                            capturedSol, totalSupply, dbNow, mcapK } = u;

                    if (shouldExit) {
                        console.log(`📡 [EXIT] ${token.name}: ${reason} @ ${mcapSol.toFixed(2)} SOL ($${mcapK}K)`);
                        return dbRun(db,
                            `UPDATE tokens SET
                                current_mcap = ?, current_mcap_sol = ?,
                                highest_mcap = ?, highest_mcap_sol = ?,
                                post_exit_highest_mcap = ?, post_exit_lowest_mcap = ?,
                                post_exit_highest_mcap_sol = ?, post_exit_lowest_mcap_sol = ?,
                                captured_mcap_sol = ?, total_supply = ?,
                                status = 'stopped', exit_reason = ?, updated_at = ?
                             WHERE id = ?`,
                            [mcapUsd, mcapSol, peakUsd, peakSol,
                             u.postExitHighest, u.postExitLowest,
                             u.postExitHighestSol, u.postExitLowestSol,
                             capturedSol, totalSupply, reason, dbNow, token.id]);
                    } else {
                        return dbRun(db,
                            `UPDATE tokens SET
                                current_mcap = ?, current_mcap_sol = ?,
                                highest_mcap = ?, highest_mcap_sol = ?,
                                post_exit_highest_mcap = ?, post_exit_lowest_mcap = ?,
                                post_exit_highest_mcap_sol = ?, post_exit_lowest_mcap_sol = ?,
                                captured_mcap_sol = ?, total_supply = ?,
                                trailing_sl_sol = ?, updated_at = ?
                             WHERE id = ?`,
                            [mcapUsd, mcapSol, peakUsd, peakSol,
                             u.postExitHighest, u.postExitLowest,
                             u.postExitHighestSol, u.postExitLowestSol,
                             capturedSol, totalSupply, newTrailSol, dbNow, token.id]);
                    }
                });

            await Promise.all(dbOps);

        } catch (e) {
            console.error('🛰️ Monitor Loop Exception:', e.message);
        }

        cycleMs = Date.now() - cycleStart;
        // Aim for a 500ms cycle; subtract processing time already spent
        const sleep = Math.max(0, 500 - cycleMs);
        await new Promise(r => setTimeout(r, sleep));
    }
}

module.exports = { startMonitor };
