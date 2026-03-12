/**
 * 📈 MCAP Tracker - Trade Logic
 * Encapsulates Stop Loss (-25%) and dynamic Trailing Stop mathematical calculations.
 */

function calculateExitConditions(currentMcap, capturedMcap, highestMcap, currentTrailingSl) {
    const profitPct = ((currentMcap - capturedMcap) / capturedMcap) * 100;
    const peakProfitPct = ((highestMcap - capturedMcap) / capturedMcap) * 100;

    // 1. Stop Loss Check (-25%)
    if (profitPct <= -25) {
        return { shouldExit: true, reason: "SL Hit (-25%)", newTrailingSl: currentTrailingSl };
    }

    // 2. Trailing SL Logic
    let newTrail = currentTrailingSl;

    // A. Breakeven Lock at +15% profit
    if (peakProfitPct >= 15) {
        newTrail = Math.max(newTrail, capturedMcap * 1.03);
    }

    // B. Dynamic Trailing Activation at +50% profit
    if (peakProfitPct >= 50) {
        // Gap calculation: matching UI settings
        const gapPct = peakProfitPct < 100 ? 50 : 25;
        const calculatedSl = capturedMcap + (highestMcap - capturedMcap) * (1 - gapPct / 100);
        newTrail = Math.max(newTrail, calculatedSl);
    }

    // 3. Exit check for any active Trailing SL
    if (newTrail > 0 && currentMcap <= newTrail) {
        const reason = (newTrail === capturedMcap * 1.03) ? "Breakeven Hit (+3%)" : "Trailing SL Hit";
        return { shouldExit: true, reason: reason, newTrailingSl: newTrail };
    }

    return { shouldExit: false, reason: "", newTrailingSl: newTrail };
}

module.exports = { calculateExitConditions };
