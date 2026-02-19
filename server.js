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
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `, (err) => {
        if (err) {
            console.error('Error creating table:', err);
        } else {
            console.log('Database initialized');
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

// Cron job: Update all tokens every 5 seconds
cron.schedule('*/5 * * * * *', async () => {
    console.log(`[Cron] Fetching market caps at ${new Date().toISOString()}`);

    db.all('SELECT id, name, address, captured_mcap, highest_mcap FROM tokens WHERE address IS NOT NULL',
        async (err, tokens) => {
            if (err) {
                console.error('Error fetching tokens:', err);
                return;
            }

            if (tokens.length === 0) {
                return;
            }

            // Update tokens sequentially to avoid rate limiting
            for (const token of tokens) {
                await updateTokenMarketCap(token);
                // Small delay between requests
                await new Promise(resolve => setTimeout(resolve, 200));
            }
        }
    );
});

// POST /add-token - Add a new token
app.post('/add-token', (req, res) => {
    const { name, link, address, timestamp, liquidity, market_cap, narrative } = req.body;

    if (!name || !link) {
        return res.status(400).json({ error: 'name and link are required' });
    }

    const tokenAddress = address || extractTokenAddress(link);
    const capturedMcap = parseMarketCap(market_cap);

    if (!capturedMcap) {
        return res.status(400).json({ error: 'Could not parse market_cap. Provide a valid value like "$2.4K" or "$19K"' });
    }

    const capturedTimestamp = timestamp || Math.floor(Date.now() / 1000);

    db.run(
        `INSERT INTO tokens (name, link, address, captured_mcap, highest_mcap, captured_timestamp, liquidity, narrative)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(link) DO UPDATE SET
            current_mcap = ?,
            highest_mcap = MAX(highest_mcap, ?),
            updated_at = CURRENT_TIMESTAMP`,
        [name, link, tokenAddress, capturedMcap, capturedMcap, capturedTimestamp, liquidity || null, narrative || null, capturedMcap, capturedMcap],
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
            captured_timestamp,
            liquidity,
            narrative,
            created_at,
            updated_at,
            CASE
                WHEN captured_mcap > 0 THEN
                    ROUND(((highest_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as percent_change
         FROM tokens
         ORDER BY created_at DESC`,
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
                capturedMcap: token.captured_mcap,
                currentMcap: token.current_mcap,
                highestMcap: token.highest_mcap,
                percentChange: token.percent_change,
                capturedTimestamp: token.captured_timestamp,
                liquidity: token.liquidity,
                narrative: token.narrative,
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
            created_at,
            updated_at,
            CASE
                WHEN captured_mcap > 0 THEN
                    ROUND(((highest_mcap - captured_mcap) / captured_mcap * 100), 2)
                ELSE 0
            END as percent_change
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
                capturedTimestamp: token.captured_timestamp,
                liquidity: token.liquidity,
                narrative: token.narrative,
                createdAt: token.created_at,
                updatedAt: token.updated_at
            });
        }
    );
});

// Health check endpoint
app.get('/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Start server
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
    console.log(`📊 Database: ${DB_PATH}`);
    console.log(`⏰ Cron job: Every 5 seconds`);
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

