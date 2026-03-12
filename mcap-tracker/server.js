const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const cron = require('node-cron');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;
const DB_PATH = process.env.DATABASE_PATH || path.join(__dirname, 'tokens.db');

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Initialize database
const db = new sqlite3.Database(DB_PATH, (err) => {
    if (err) {
        console.error('Error opening database:', err);
    } else {
        console.log('Connected to SQLite database');
        // Enable WAL mode for better concurrent write performance
        // and set auto-checkpoint to truncate WAL file frequently (every 100 pages)
        db.serialize(() => {
            db.run('PRAGMA journal_mode=WAL');
            db.run('PRAGMA wal_autocheckpoint=100');
            db.run('PRAGMA synchronous=NORMAL');  // Faster writes, still safe
            db.run('PRAGMA cache_size=-8000');     // 8MB page cache
        });
        initDatabase();
    }
});

function initDatabase() {
    db.run(`
        CREATE TABLE IF NOT EXISTS tokens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            link TEXT NOT NULL UNIQUE,
            address TEXT,
            captured_mcap REAL NOT NULL,
            current_mcap REAL,
            highest_mcap REAL NOT NULL,
            captured_timestamp INTEGER NOT NULL,
            liquidity TEXT,
            narrative TEXT,
            is_dumped INTEGER DEFAULT 0,
            source TEXT DEFAULT 'fresh',
            status TEXT DEFAULT 'active',
            exit_reason TEXT,
            trailing_sl REAL,
            pair_address TEXT,
            play TEXT DEFAULT 'No',
            reentry_alerted INTEGER DEFAULT 0,
            post_exit_highest_mcap REAL,
            post_exit_lowest_mcap REAL,
            post_exit_highest_mcap_sol REAL,
            post_exit_lowest_mcap_sol REAL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `, (err) => {
        if (err) {
            console.error('Error creating table:', err);
        } else {
            console.log('Database initialized');
            // Migrations for existing databases
            db.run(`ALTER TABLE tokens ADD COLUMN is_dumped INTEGER DEFAULT 0`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN source TEXT DEFAULT 'fresh'`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN status TEXT DEFAULT 'active'`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN exit_reason TEXT`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN trailing_sl REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN pair_address TEXT`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN captured_mcap_sol REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN highest_mcap_sol REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN trailing_sl_sol REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN current_mcap_sol REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN total_supply REAL DEFAULT 1000000000`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN play TEXT DEFAULT 'No'`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN reentry_alerted INTEGER DEFAULT 0`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN post_exit_highest_mcap REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN post_exit_lowest_mcap REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN post_exit_highest_mcap_sol REAL`, () => {});
            db.run(`ALTER TABLE tokens ADD COLUMN post_exit_lowest_mcap_sol REAL`, () => {});

            // Run initial cleanup on startup
            setTimeout(() => runMaintenance('startup'), 5000);
        }
    });
}

// Helper function to extract token address from DexScreener URL
function extractTokenAddress(link) {
    try {
        if (link.includes('/solana/')) {
            const parts = link.split('/solana/');
            if (parts.length > 1) {
                return parts[1].split('?')[0].split('#')[0].trim();
            }
        }
    } catch (e) {
        console.error('Error extracting address:', e);
    }
    return null;
}

// Helper function to parse market cap from string like "$2.4K" or "$19K"
function parseMarketCap(mcapStr) {
    if (!mcapStr || mcapStr === 'N/A') return null;

    try {
        // Remove $ and commas
        let cleaned = mcapStr.replace('$', '').replace(',', '').trim();

        // Handle K, M, B suffixes
        const multipliers = {
            'K': 1000,
            'M': 1000000,
            'B': 1000000000
        };

        const match = cleaned.match(/^([\d.]+)([KMB])?$/i);
        if (match) {
            let value = parseFloat(match[1]);
            const suffix = match[2]?.toUpperCase();
            if (suffix && multipliers[suffix]) {
                value *= multipliers[suffix];
            }
            return value;
        }
    } catch (e) {
        console.error('Error parsing market cap:', e);
    }
    return null;
}

// Fetch market cap from DexScreener API
async function fetchMarketCapFromDexScreener(address) {
    if (!address) return null;

    try {
        // Use pairs endpoint since the address from DexScreener URLs is a pair address
        const url = `https://api.dexscreener.com/latest/dex/pairs/solana/${address}`;
        const response = await axios.get(url, { timeout: 10000 });

        // The pairs endpoint returns a single pair object (or array)
        const pair = response.data?.pair || (response.data?.pairs && response.data.pairs[0]);
        if (pair) {
            const mcap = parseFloat(pair.fdv || pair.marketCap || 0);
            return mcap > 0 ? mcap : null;
        }
    } catch (error) {
        console.error(`Error fetching market cap for ${address}:`, error.message);
    }
    return null;
}

// Update market cap for a single token
async function updateTokenMarketCap(token) {
    const { id, address, captured_mcap, highest_mcap } = token;

    if (!address) {
        console.log(`Skipping token ${token.name} - no address`);
        return;
    }

    try {
        const currentMcap = await fetchMarketCapFromDexScreener(address);

        if (currentMcap !== null) {
            const newHighestMcap = Math.max(highest_mcap, currentMcap);

            db.run(
                `UPDATE tokens
                 SET current_mcap = ?,
                     highest_mcap = ?,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = ?`,
                [currentMcap, newHighestMcap, id],
                (err) => {
                    if (err) {
                        console.error(`Error updating token ${token.name}:`, err);
                    } else {
                        const percentChange = ((newHighestMcap - captured_mcap) / captured_mcap) * 100;
                        console.log(
                            `Updated ${token.name}: Current=${currentMcap.toFixed(2)}, ` +
                            `Highest=${newHighestMcap.toFixed(2)}, ` +
                            `Change=${percentChange.toFixed(2)}%`
                        );
                    }
                }
            );
        } else {
            console.log(`Could not fetch market cap for ${token.name} (${address})`);
        }
    } catch (error) {
        console.error(`Error updating token ${token.name}:`, error);
    }
}

// Cron job: Update all tokens every 5 seconds (DISABLED: Replaced by onchain_monitor.py)
// cron.schedule('*/5 * * * * *', async () => {
//     console.log(`[Cron] Fetching market caps at ${new Date().toISOString()}`);
//
//     db.all('SELECT id, name, address, captured_mcap, highest_mcap FROM tokens WHERE address IS NOT NULL',
//         async (err, tokens) => {
//             if (err) {
//                 console.error('Error fetching tokens:', err);
//                 return;
//             }
//
//             if (tokens.length === 0) {
//                 return;
//             }
//
//             // Update tokens sequentially to avoid rate limiting
//             for (const token of tokens) {
//                 await updateTokenMarketCap(token);
//                 // Small delay between requests
//                 await new Promise(resolve => setTimeout(resolve, 200));
//             }
//         }
//     );
// });

// POST /add-token - Add a new token
app.post('/add-token', (req, res) => {
    const { name, link, address, pairAddress, timestamp, liquidity, market_cap, narrative, is_dumped, source, play } = req.body;

    if (!name || !link) {
        return res.status(400).json({ error: 'name and link are required' });
    }

    const tokenAddress = address || extractTokenAddress(link);
    const capturedMcap = parseMarketCap(market_cap);

    if (!capturedMcap) {
        return res.status(400).json({ error: 'Could not parse market_cap. Provide a valid value like "$2.4K" or "$19K"' });
    }

    const capturedTimestamp = timestamp || Math.floor(Date.now() / 1000);
    const isDumpedVal = is_dumped ? 1 : 0;
    const sourceVal = source || 'fresh';
    const playVal = play || 'No';

    db.run(
        `INSERT INTO tokens (name, link, address, pair_address, captured_mcap, highest_mcap, captured_timestamp, liquidity, narrative, is_dumped, source, status, play)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?)
         ON CONFLICT(link) DO UPDATE SET
            current_mcap = ?,
            highest_mcap = MAX(highest_mcap, ?),
            status = 'active',
            exit_reason = NULL,
            trailing_sl = NULL,
            trailing_sl_sol = NULL,
            highest_mcap_sol = NULL,
            captured_mcap_sol = NULL,
            play = ?,
            reentry_alerted = 0,
            updated_at = CURRENT_TIMESTAMP`,
        [name, link, tokenAddress, pairAddress || null, capturedMcap, capturedMcap, capturedTimestamp, liquidity || null, narrative || null, isDumpedVal, sourceVal, playVal, capturedMcap, capturedMcap, playVal],
        function(err) {
            if (err) {
                console.error('Error inserting token:', err);
                return res.status(500).json({ error: 'Failed to add token' });
            }

            console.log(`✅ Added token: ${name} (MCap: $${capturedMcap.toFixed(2)})`);
            res.status(201).json({
                success: true,
                id: this.lastID,
                message: 'Token added successfully'
            });
        }
    );
});

// GET /tokens - Get all tokens with calculated percentages
app.get('/tokens', (req, res) => {
    db.all(
        `SELECT
            id,
            name,
            link,
            address,
            captured_mcap,
            current_mcap,
            highest_mcap,
            captured_mcap_sol,
            current_mcap_sol,
            highest_mcap_sol,
            trailing_sl_sol,
            captured_timestamp,
            liquidity,
            narrative,
            is_dumped,
            source,
            status,
            play,
            reentry_alerted,
            exit_reason,
            post_exit_highest_mcap,
            post_exit_lowest_mcap,
            post_exit_highest_mcap_sol,
            post_exit_lowest_mcap_sol,
            (current_mcap / current_mcap_sol) as sol_price,
            total_supply,
            created_at,
            updated_at,
            CASE
                WHEN captured_mcap_sol > 0 THEN
                    ROUND(((current_mcap_sol - captured_mcap_sol) / captured_mcap_sol * 100), 2)
                WHEN captured_mcap > 0 THEN
                    ROUND(((current_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as percent_change,
            CASE
                WHEN captured_mcap_sol > 0 THEN
                    ROUND(((highest_mcap_sol - captured_mcap_sol) / captured_mcap_sol * 100), 2)
                WHEN captured_mcap > 0 THEN
                    ROUND(((highest_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as highest_percent_change
         FROM tokens
         ORDER BY captured_timestamp DESC`,
        (err, tokens) => {
            if (err) {
                console.error('Error fetching tokens:', err);
                return res.status(500).json({ error: 'Failed to fetch tokens' });
            }

            // Format the response
            const formattedTokens = tokens.map(token => ({
                id: token.id,
                name: token.name,
                link: token.link,
                address: token.address,
                pairAddress: token.pair_address || null,
                capturedMcap: token.captured_mcap,
                currentMcap: token.current_mcap,
                highestMcap: token.highest_mcap,
                capturedMcapSol: token.captured_mcap_sol,
                currentMcapSol: token.current_mcap_sol,
                highestMcapSol: token.highest_mcap_sol,
                trailingSlSol: token.trailing_sl_sol,
                solPrice: token.sol_price,
                percentChange: token.percent_change,
                highestPercentChange: token.highest_percent_change,
                capturedTimestamp: token.captured_timestamp,
                liquidity: token.liquidity,
                narrative: token.narrative,
                isDumped: token.is_dumped === 1,
                source: token.source || 'fresh',
                status: token.status || 'active',
                play: token.play || 'No',
                reentryAlerted: token.reentry_alerted,
                exitReason: token.exit_reason,
                trailingSL: token.trailing_sl,
                postExitHighestMcap: token.post_exit_highest_mcap,
                postExitLowestMcap: token.post_exit_lowest_mcap,
                postExitHighestMcapSol: token.post_exit_highest_mcap_sol,
                postExitLowestMcapSol: token.post_exit_lowest_mcap_sol,
                totalSupply: token.total_supply,
                createdAt: token.created_at,
                updatedAt: token.updated_at
            }));

            res.json({ tokens: formattedTokens });
        }
    );
});

// GET /tokens/:id - Get a single token
app.get('/tokens/:id', (req, res) => {
    const id = parseInt(req.params.id);

    db.get(
        `SELECT
            id,
            name,
            link,
            address,
            captured_mcap,
            current_mcap,
            highest_mcap,
            captured_timestamp,
            liquidity,
            narrative,
            is_dumped,
            status,
            exit_reason,
            trailing_sl,
            created_at,
            updated_at,
            CASE
                WHEN captured_mcap > 0 THEN
                    ROUND(((current_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as percent_change,
            CASE
                WHEN captured_mcap > 0 THEN
                    ROUND(((highest_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as highest_percent_change
         FROM tokens
         WHERE id = ?`,
        [id],
        (err, token) => {
            if (err) {
                console.error('Error fetching token:', err);
                return res.status(500).json({ error: 'Failed to fetch token' });
            }

            if (!token) {
                return res.status(404).json({ error: 'Token not found' });
            }

            res.json({
                id: token.id,
                name: token.name,
                link: token.link,
                address: token.address,
                capturedMcap: token.captured_mcap,
                currentMcap: token.current_mcap,
                highestMcap: token.highest_mcap,
                percentChange: token.percent_change,
                highestPercentChange: token.highest_percent_change,
                capturedTimestamp: token.captured_timestamp,
                liquidity: token.liquidity,
                narrative: token.narrative,
                isDumped: token.is_dumped === 1,
                status: token.status || 'active',
                exitReason: token.exit_reason,
                trailingSL: token.trailing_sl,
                createdAt: token.created_at,
                updatedAt: token.updated_at
            });
        }
    );
});

// PATCH /tokens/:id - Update token addresses
app.patch('/tokens/:id', (req, res) => {
    const id = parseInt(req.params.id);
    const { address, pairAddress } = req.body;

    if (!address && !pairAddress) {
        return res.status(400).json({ error: 'Provide at least one of: address, pairAddress' });
    }

    const updates = [];
    const values = [];
    if (address) { updates.push('address = ?'); values.push(address); }
    if (pairAddress !== undefined) { updates.push('pair_address = ?'); values.push(pairAddress || null); }
    updates.push('updated_at = CURRENT_TIMESTAMP');
    values.push(id);

    db.run(`UPDATE tokens SET ${updates.join(', ')} WHERE id = ?`, values, function(err) {
        if (err) {
            console.error('Error updating token:', err);
            return res.status(500).json({ error: 'Failed to update token' });
        }
        if (this.changes === 0) return res.status(404).json({ error: 'Token not found' });
        console.log(`✏️ Updated token #${id}: address=${address || '-'} pairAddress=${pairAddress || '-'}`);
        res.json({ success: true, message: `Token #${id} updated`, changes: this.changes });
    });
});

// DELETE /tokens/clear - Remove all tokens from the database
app.delete('/tokens/clear', (req, res) => {
    db.run('DELETE FROM tokens', function(err) {
        if (err) {
            console.error('Error clearing tokens:', err);
            return res.status(500).json({ error: 'Failed to clear tokens' });
        }
        console.log(`🗑️ Cleared all tokens (${this.changes} rows deleted)`);
        res.json({ success: true, message: `Cleared ${this.changes} tokens` });
    });
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// ─────────────────────────────────────────────────────────────
// DB Maintenance — pruning + VACUUM to keep volume size down
// ─────────────────────────────────────────────────────────────
const PRUNE_STOPPED_AFTER_DAYS = 30;  // Delete stopped tokens older than this

async function runMaintenance(trigger = 'scheduled') {
    return new Promise((resolve) => {
        db.serialize(() => {
            // 1. Prune old stopped tokens
            const cutoff = new Date();
            cutoff.setDate(cutoff.getDate() - PRUNE_STOPPED_AFTER_DAYS);
            const cutoffStr = cutoff.toISOString().replace('T', ' ').split('.')[0];

            db.run(
                `DELETE FROM tokens WHERE status = 'stopped' AND updated_at < ?`,
                [cutoffStr],
                function(err) {
                    if (err) {
                        console.error('[Maintenance] Prune error:', err.message);
                    } else if (this.changes > 0) {
                        console.log(`[Maintenance] 🗑️ Pruned ${this.changes} old stopped tokens (>${PRUNE_STOPPED_AFTER_DAYS}d)`);
                    }
                }
            );

            // 2. Checkpoint WAL file (flush to main DB, truncate WAL)
            db.run('PRAGMA wal_checkpoint(TRUNCATE)', (err) => {
                if (err) console.error('[Maintenance] WAL checkpoint error:', err.message);
                else console.log('[Maintenance] ✅ WAL checkpointed and truncated');
            });

            // 3. VACUUM to reclaim dead pages (runs after prune)
            db.run('VACUUM', (err) => {
                if (err) {
                    console.error('[Maintenance] VACUUM error:', err.message);
                } else {
                    console.log(`[Maintenance] ✅ VACUUM complete [trigger: ${trigger}]`);
                }
                resolve();
            });
        });
    });
}

// Run maintenance every 6 hours
setInterval(() => {
    console.log('[Maintenance] ⏰ Running scheduled maintenance...');
    runMaintenance('6h-cron').catch(e => console.error('[Maintenance] Error:', e.message));
}, 6 * 60 * 60 * 1000);

// POST /admin/vacuum — manually trigger maintenance + VACUUM
app.post('/admin/vacuum', async (req, res) => {
    try {
        await runMaintenance('manual');
        // Report DB file size after vacuum
        const fs = require('fs');
        const stats = fs.existsSync(DB_PATH) ? fs.statSync(DB_PATH) : null;
        const sizeMB = stats ? (stats.size / 1024 / 1024).toFixed(2) : 'unknown';
        res.json({ success: true, message: `Maintenance complete. DB size: ${sizeMB} MB` });
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// GET /admin/db-stats — show DB file size and token counts
app.get('/admin/db-stats', (req, res) => {
    const fs = require('fs');
    const stats = fs.existsSync(DB_PATH) ? fs.statSync(DB_PATH) : null;
    const sizeMB = stats ? (stats.size / 1024 / 1024).toFixed(2) : 'unknown';

    // Check WAL file too
    const walPath = DB_PATH + '-wal';
    const walStats = fs.existsSync(walPath) ? fs.statSync(walPath) : null;
    const walMB = walStats ? (walStats.size / 1024 / 1024).toFixed(2) : '0';

    db.all(`SELECT status, COUNT(*) as count FROM tokens GROUP BY status`, (err, rows) => {
        res.json({
            db_size_mb: sizeMB,
            wal_size_mb: walMB,
            total_mb: stats && walStats ? ((stats.size + walStats.size) / 1024 / 1024).toFixed(2) : sizeMB,
            token_counts: err ? 'error' : rows,
            prune_policy: `Stopped tokens older than ${PRUNE_STOPPED_AFTER_DAYS} days are auto-deleted`,
        });
    });
});

// --- On-Chain Monitor (Native JS) ---
const { startMonitor } = require('./utils/onchainMonitor');

// Start server
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Database: ${DB_PATH}`);

    // Start the native JS monitor loop
    startMonitor(db).catch(err => {
        console.error('❌ Failed to start native monitor:', err);
    });
});

// Graceful shutdown
process.on('SIGINT', () => {
    console.log('\nShutting down gracefully...');
    db.close((err) => {
        if (err) {
            console.error('Error closing database:', err);
        } else {
            console.log('Database closed');
        }
        process.exit(0);
    });
});

