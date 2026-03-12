import asyncio
import re
from typing import Optional, Union

from playwright.async_api import Page, TimeoutError as PlaywrightTimeoutError

GROK_URL = "https://x.com/i/grok"
#VALID NARRATIVE PATTERNS (must match ≥2 repeatable high-WR patterns, with at least one being subculture-related)
# Full narrative / trend evaluation rules — FRONT-RUNNING focus.
GROK_NARRATIVE_RULES = """CRITICAL INSTRUCTION: Your response must be concise — ideally ONE LINE, maximum TWO LINES. Do NOT write long analysis, explanations or reasoning outside the verdict format. Output ONLY the final verdict line(s).

OBJECTIVE
You are a Solana memecoin trend analyst. Identify tokens with genuine, active cultural/social/event momentum that are EITHER early / about to explode OR still in a strong accelerating uptrend with room left to run. Reject only if the narrative is clearly exhausted, played out, or a generic copycat with no original hook.

CRITICAL TREND CHECK
- Is the narrative BUILDING or still ACCELERATING?
- Does it have a strong meta or cultural anchor?
- If >24h old, is it still dominant or pulling fresh attention?
- Is there a clear reason for new buyers (event, viral spread, rotation, cult community)?
- If X buzz is low but TikTok/Instagram/Reels volume is surging (with crypto/Solana tags, wallet mentions, or cross-posting signs), treat as EMERGING with high potential.

VALID NARRATIVE PATTERNS (must match at least ONE strongly)
- Event-Driven: tied to upcoming or unfolding real-world event
- Viral/Attention Spike: fresh viral moment, celebrity, news — momentum still rising
- Strong Meta/Subculture: fits currently hot narrative still drawing liquidity
- Novel Idea/Cult: unique concept or real cult-like community (not obvious 5-min pump)
- Catalyst Front-Run: positioned meaningfully before a known upcoming catalyst
- Cross-Platform Viral: strong early traction on short-form video platforms (TikTok/IG/Reels) with signs of rotating to X/crypto communities

APPROVE (YES) ONLY IF:
- Maps clearly to ≥1 pattern above
- Narrative is EMERGING or still ACTIVE with runway left
- Verifiable, growing or very strong social buzz exists right now (on X and/or TikTok/Instagram/Reels)

REJECT (NO) IF:
- Narrative is dead/peaked, meta has rotated away
- Generic "nothing" token with no story/community
- Late copycat of a 48h+ old runner that already peaked and faded

OUTPUT FORMAT — STRICT
Use EXACTLY one of these two formats (1–2 lines max):
YES - [patterns]; [why this has momentum/potential]; [narrative summary]; [Social buzz: high/medium/low — specify platforms e.g. TikTok high + X building]; [expected runway e.g. 24-72h or 1-2 weeks]; [leader: none / established leader] [optional: confidence high/medium/low]
NO - [reason — EXHAUSTED / GENERIC / NO CATALYST / TOO LATE]; [Social buzz: high/medium/low — specify platforms]; [timing assessment] [optional: confidence high/medium/low]

Examples:
YES - Cross-Platform Viral, Viral/Attention Spike; exploding TikTok dance trend with Solana wallet tags now hitting X; absurd lobster AI revival; high (TikTok surging + X building); 48-96 hours; leader: none; confidence: high
NO - Exhausted narrative, animal meta already rotated to AI agents; low (X faded, TikTok quiet); too late; confidence: medium """


DIP_GROK_RULES = """
CRITICAL INSTRUCTION: Your ENTIRE response must be EXACTLY ONE LINE. Do NOT write any analysis, explanation, reasoning, or commentary. Output ONLY the final verdict line. Any response longer than one line is a failure.

OBJECTIVE
This token has already launched, pumped, and dumped. It is now at a very low market cap but is TRENDING RIGHT NOW on a 5-minute momentum score. Evaluate REVIVAL POTENTIAL — is there genuine reason for a credible second wave, or is this just noise / bots? Do NOT penalize for past dump — focus only on whether the narrative is culturally re-igniting with organic signs.

APPROVE (YES) IF ALL OF THESE:
- Token name/narrative has a real cultural, meme, or thematic anchor (not random/generic)
- Narrative is STILL culturally alive or re-emerging right now (memes, discussions, variants, or related events ongoing)
- Current 5-min trending / volume spike shows plausible organic momentum (not obvious wash trading or pure bot volume)
- Credible revival catalyst exists: renewed attention (cross-platform buzz), lingering meta, fresh community push, or related unfolding event

REJECT (NO) IF ANY OF THESE:
- Narrative is fully dead — zero cultural relevance, moment completely passed with no revival signals
- Token name is meaningless/generic with no identifiable meme/community/history
- Trending is almost certainly wash trading / bot-driven with no organic social backing
- Meta is 100% over with no plausible second-wave path (e.g. rotated away permanently)

VALID REVIVAL SIGNALS (look for at least one strong)
- Renewed X / TikTok / Instagram / Reels activity (memes, remixes, tags, cross-posts)
- Related meta still active (e.g. animal variants, AI spin-offs, holiday callbacks)
- Community re-activation (new holders, Telegram/Discord spikes, KOL mentions)
- External catalyst tie-in (news, celeb, event, or cultural moment re-sparking old narrative)

OUTPUT FORMAT — STRICT — EXACTLY ONE LINE — NO EXCEPTIONS
YES - [revival reason / catalyst]; [why narrative still alive or re-igniting]; [buzz level: high/medium/low — platforms e.g. TikTok medium + X building]; [second wave timeline e.g. 24-72h or 3-7 days]
NO - [reason: DEAD NARRATIVE / WASH TRADE / NO ANCHOR / META OVER]; [buzz level: high/medium/low — platforms]

Examples:
YES - Lobster meme remixing on TikTok with new AI variants; original absurd lobster narrative re-igniting via short-form video rotation; medium (TikTok surging + X pickup); 48-96 hours
NO - DEAD NARRATIVE; was a 12-hour viral pump 10 days ago, zero residual memes or buzz; low (all platforms quiet)
"""


async def ensure_grok_loaded(page: Page) -> None:
    """
    Ensure Grok UI is loaded and the input area is ready.
    Mirrors the robust logic from the thumbnail project, focused on text input.
    """
    print("[Grok] Waiting for Grok header SVG...")
    try:
        await page.wait_for_selector('svg[viewBox="0 0 88 32"]', timeout=60000)
        print("[Grok] ✅ Grok header detected")
    except PlaywrightTimeoutError:
        print("[Grok] ⚠️ Grok header SVG not found within 60s, falling back to <body>")
        await page.wait_for_selector("body", timeout=30000)

    print('[Grok] Waiting for textarea[placeholder="Ask anything"]...')
    try:
        await page.wait_for_selector('textarea[placeholder="Ask anything"]', timeout=30000)
        print("[Grok] ✅ Grok textarea detected")
    except PlaywrightTimeoutError:
        print("[Grok] ⚠️ Textarea not found, falling back to contenteditable")
        await page.wait_for_selector('div[contenteditable="true"], body', timeout=30000)
        print("[Grok] ✅ Contenteditable input available")


async def _send_prompt(page: Page, prompt_text: str) -> None:
    """
    Send a text-only prompt to Grok and wait briefly for response to start.
    Reuses the robust typing / submission pattern from the thumbnail script.
    """
    selector = 'textarea[placeholder="Ask anything"]'
    target = await page.query_selector(selector)
    if target:
        print("[Grok] ✍️ Using textarea selector to input prompt")
    if not target:
        target = await page.query_selector('div[contenteditable="true"]')
        if target:
            print("[Grok] ✍️ Using contenteditable input to type prompt")

    if target:
        try:
            await target.scroll_into_view_if_needed()
        except Exception:
            pass
        try:
            await target.click()
            await page.evaluate(
                "(el) => { el.focus(); if ('value' in el) { el.value=''; } }", target
            )
            await page.keyboard.down("Control")
            await page.keyboard.press("KeyA")
            await page.keyboard.up("Control")
            await page.keyboard.press("Backspace")
            print("[Grok] 🧹 Cleared existing input")
        except Exception as e:
            print(f"[Grok] ⚠️ Could not clear input: {e}")

        try:
            await page.keyboard.insert_text(prompt_text)
            print(f"[Grok] 📋 Inserted prompt via keyboard.insert_text ({len(prompt_text)} chars)")
        except Exception:
            try:
                await target.type(prompt_text, delay=5)
                print(f"[Grok] ⌨️ Typed prompt ({len(prompt_text)} chars)")
            except Exception as e:
                print(f"[Grok] ❌ Failed to input prompt: {e}")

        submitted = False
        try:
            await target.press("Enter")
            submitted = True
        except Exception:
            try:
                await page.keyboard.press("Enter")
                submitted = True
            except Exception:
                try:
                    btn = await page.query_selector(
                        'button[aria-label="Send"], '
                        '[data-testid="send-button"], '
                        'div[role="button"]:has-text("Send")'
                    )
                    if btn:
                        await btn.click()
                        submitted = True
                except Exception:
                    pass
        print(
            "[Grok] 📨 Submitted prompt"
            + ("" if submitted else " (submission may have failed)")
        )
    else:
        print("[Grok] ⚠️ No input selector found; typing at page level")
        await page.keyboard.type(prompt_text, delay=5)
        await page.keyboard.press("Enter")

    # Do not assume fixed delay; actual readiness is detected by _extract_yes_no
    print("[Grok] ⏳ Prompt sent; waiting for Grok to think and respond...")


def _extract_decision_line(text: str) -> Optional[str]:
    """
    From a potentially verbose Grok response, extract ONLY the YES/NO decision line.
    Grok sometimes writes paragraphs of analysis before the final verdict.
    This function finds and returns just the clean decision line.
    """
    if not text:
        return None

    # Split into lines and look for the decision line
    lines = text.split('\n')
    for line in lines:
        stripped = line.strip()
        upper = stripped.upper()
        # Match lines that start with "YES -" or "NO -" (the expected format)
        if re.match(r'^YES\s*[-–—]', upper) or re.match(r'^NO\s*[-–—]', upper):
            return stripped

    # If no newline-separated decision line found, try to find it in continuous text
    # Look for "YES - ..." or "NO - ..." pattern anywhere in the text
    yes_match = re.search(r'(YES\s*[-–—]\s*[^\n]+)', text, re.IGNORECASE)
    no_match = re.search(r'(NO\s*[-–—]\s*[^\n]+)', text, re.IGNORECASE)

    # If both found, use the last occurrence (likely the final verdict)
    if yes_match and no_match:
        # Use whichever appears later in the text (the final verdict)
        if yes_match.start() > no_match.start():
            return yes_match.group(1).strip()
        else:
            return no_match.group(1).strip()
    elif yes_match:
        return yes_match.group(1).strip()
    elif no_match:
        return no_match.group(1).strip()

    return None


async def _extract_yes_no(
    page: Page,
    initial_timeout: int = 90,
    extended_total_timeout: Optional[int] = None,
) -> tuple[Union[Optional[bool], str], Optional[str]]:
    """
    Inspect Grok's rendered text and extract the last YES/NO decision.
    Returns:
      True  -> YES
      False -> NO
      None  -> could not confidently parse

    Dynamic timeout behavior:
      - We always start with an initial window (initial_timeout seconds) after the
        prompt is sent.
      - If we *never* see any "thinking" / loader indicators, we stop at that
        initial timeout.
      - If we *do* see thinking/loader, we extend the overall allowance up to
        extended_total_timeout seconds from the start, minus whatever portion of
        the initial window was already used.
      - If extended_total_timeout is None, this behaves like a simple fixed timeout.
    """
    loop = asyncio.get_event_loop()
    start_time = loop.time()

    # When extended_total_timeout is provided, we distinguish between the short
    # and long windows. Otherwise this is just a single fixed timeout.
    hard_deadline = start_time + (extended_total_timeout or initial_timeout)
    short_deadline = start_time + initial_timeout

    seen_indicator = False
    was_thinking = False  # Track if Grok was thinking in previous iteration
    thinking_stopped_time = None  # Track when thinking stopped

    while loop.time() < hard_deadline:
        try:
            state = await page.evaluate(
                """
                () => {
                    const result = {
                        thinking: false,
                        loaderActive: false,
                        thoughtSpanVisible: false,
                        decision: null,
                        explanationSpan: null,
                        firstParagraph: null,
                        rateLimitDetected: false,
                        grokErrorDetected: false,
                    };

                    // --- Primary extraction: target the exact Grok response container ---
                    // Structure: div.r-3pj75a > div[dir="ltr"] contains ONLY the response text
                    try {
                        const responseContainers = document.querySelectorAll('div.r-3pj75a div[dir="ltr"]');
                        for (const container of responseContainers) {
                            const text = (container.textContent || '').trim();
                            if (!text) continue;
                            const upper = text.toUpperCase();

                            // Check for YES/NO decision in this container
                            if (upper.startsWith("YES") && (upper.length === 3 || !/[A-Z0-9]/.test(upper[3]))) {
                                result.decision = "YES";
                                result.explanationSpan = text;
                            } else if (upper.startsWith("NO") && (upper.length === 2 || !/[A-Z0-9]/.test(upper[2]))) {
                                result.decision = "NO";
                                result.explanationSpan = text;
                            }
                        }
                    } catch (e) {
                        // ignore, fall through to span scanning
                    }

                    // --- Scan spans for status indicators (thinking, rate limit, etc.) ---
                    const spans = Array.from(document.querySelectorAll('span.css-1jxf684'));
                    for (const el of spans) {
                        const raw = (el.textContent || '').trim();
                        if (!raw) continue;

                        // Check for rate limit message
                        if (raw.includes("You've reached your limit") ||
                            raw.includes("20 Grok Auto questions per 2 hours") ||
                            raw.includes("sign up for Premium+")) {
                            result.rateLimitDetected = true;
                        }

                        // Check for Grok generic error ("was unable to reply" / "Something went wrong")
                        if (raw.includes("was unable to reply") ||
                            raw.includes("Grok was unable to reply") ||
                            raw.includes("Something went wrong, please refresh")) {
                            result.grokErrorDetected = true;
                        }

                        if (raw.includes("Thinking about the user's")) {
                            result.thinking = true;
                        }

                        if (raw.startsWith("Thought for ")) {
                            result.thoughtSpanVisible = true;
                        }
                    }

                    // --- Fallback: scan spans for YES/NO if container method didn't find it ---
                    if (!result.decision) {
                        for (const el of spans) {
                            const raw = (el.textContent || '').trim();
                            if (!raw) continue;
                            const upper = raw.toUpperCase();

                            if (
                                upper.startsWith("YES")
                                && (upper.length === 3 || !/[A-Z0-9]/.test(upper[3]))
                            ) {
                                result.decision = "YES";
                                result.explanationSpan = raw;
                                break;
                            } else if (
                                upper.startsWith("NO")
                                && (upper.length === 2 || !/[A-Z0-9]/.test(upper[2]))
                            ) {
                                result.decision = "NO";
                                result.explanationSpan = raw;
                                break;
                            }
                        }
                    }

                    // Detect the animated loader SVG (dot grid) as "still thinking"
                    try {
                        const svgs = Array.from(document.querySelectorAll('svg'));
                        for (const svg of svgs) {
                            const html = svg.innerHTML || '';
                            if (html.includes("M0,1.600000023841858")) {
                                result.loaderActive = true;
                                break;
                            }
                        }
                    } catch (e) {
                        // ignore
                    }

                    // --- Ready Indicator: detect footer buttons (Regenerate/Copy) ---
                    // These buttons only appear once the response is fully rendered.
                    result.footerVisible = !!document.querySelector('button[aria-label="Regenerate"], button[aria-label="Copy text"]');

                    // --- Last-resort fallback: body text parsing ---
                    if (!result.decision) {
                        try {
                            const bodyText = (document.body && document.body.innerText) || '';
                            if (bodyText) {
                                // Also catch the error banner via body text
                                if (!result.grokErrorDetected &&
                                    (bodyText.includes("was unable to reply") ||
                                     bodyText.includes("Something went wrong, please refresh"))) {
                                    result.grokErrorDetected = true;
                                }
                                const lines = bodyText.split(/\\n+/).map(l => l.trim()).filter(Boolean);
                                for (let i = 0; i < lines.length; i++) {
                                    const line = lines[i];
                                    const upper = line.toUpperCase();
                                    if (upper.startsWith("YES") && (upper.length === 3 || !/[A-Z0-9]/.test(upper[3]))) {
                                        result.decision = "YES";
                                        result.firstParagraph = line;
                                        break;
                                    } else if (upper.startsWith("NO") && (upper.length === 2 || !/[A-Z0-9]/.test(upper[2]))) {
                                        result.decision = "NO";
                                        result.firstParagraph = line;
                                        break;
                                    }
                                }
                            }
                        } catch (e) {
                            // ignore
                        }
                    }

                    return result;
                }
                """
            )
        except Exception:
            state = None

        # Check for rate limit FIRST - it can appear at any time, even while Grok is thinking
        if state and state.get("rateLimitDetected"):
            print("[Grok] ⚠️ Rate limit detected! Switching profile immediately...")
            return "rate_limit", None

        # Check for generic Grok error ("was unable to reply" / "Something went wrong")
        if state and state.get("grokErrorDetected"):
            print("[Grok] ⚠️ Grok error detected ('unable to reply'). Switching profile immediately...")
            return "grok_error", None

        # Track when thinking stops
        # We are "thinking" if indicators are on, BUT the presence of footer buttons (Regenerate/Copy)
        # acts as a final "Done" signal that overrides the thinking state.
        is_thinking_now = state and (state.get("thinking") or state.get("loaderActive"))
        footer_visible = state.get("footerVisible") if state else False

        # If Grok is still "thinking" (status text or loader SVG) and hasn't shown the footer, wait.
        if is_thinking_now and not footer_visible:
            was_thinking = True
            seen_indicator = True
            thinking_stopped_time = None  # Reset stop time while still thinking
            print("[Grok] 💭 Still thinking / loader active, waiting...")
            await asyncio.sleep(2)
            continue

        # Detect when thinking just stopped (transition from thinking to not thinking, OR footer appearing)
        if (was_thinking and not is_thinking_now) or (not was_thinking and footer_visible and not state.get("decision")):
            thinking_stopped_time = loop.time()
            if footer_visible:
                print("[Grok] ✅ Footer buttons detected, response is finalized...")
            else:
                print("[Grok] ✅ Thinking stopped, waiting 2 seconds for full response to render...")
            await asyncio.sleep(2)  # Wait 2 seconds after thinking stops before extraction
            was_thinking = False
            # Continue to next iteration to get fresh state evaluation with full response
            continue

        if state and state.get("thoughtSpanVisible"):
                print("[Grok] 🧠 Thought span visible, waiting for final YES/NO...")

        # Extract decision (now that we've waited if thinking stopped)
        if state and state.get("decision"):
            word = state["decision"].upper()
            # Prefer the longer/more complete explanation
            expl_span = state.get("explanationSpan")
            first_para = state.get("firstParagraph")
            raw_expl = None
            if expl_span and first_para:
                # Use whichever is longer (more likely to be complete)
                raw_expl = expl_span if len(expl_span) >= len(first_para) else first_para
            else:
                raw_expl = expl_span or first_para

            explanation = None
            if isinstance(raw_expl, str) and raw_expl.strip():
                explanation = raw_expl.strip()

                # Filter out template/prompt text that shouldn't be in Grok's response
                template_patterns = [
                    r'Token name:.*',
                    r'Token link.*',
                    r'Additional context:.*',
                    r'OBJECTIVE.*?OUTPUT FORMAT',
                    r'Check if a Solana memecoin.*?No other text\. Ever\.',
                    r'Check if a Solana memecoin.*?NOTHING ELSE\.',
                    r'CRITICAL INSTRUCTION:.*?NOTHING ELSE\.',
                    r'If approved →.*?If rejected →.*',
                    r'Correct example for.*',
                    r'Do NOT write paragraphs\..*',
                ]

                cleaned_explanation = explanation
                for pattern in template_patterns:
                    cleaned_explanation = re.sub(pattern, '', cleaned_explanation, flags=re.IGNORECASE | re.DOTALL)

                # Remove Grok UI artifacts that get scraped with the response
                # "Quick Answer" label and everything after it
                cleaned_explanation = re.sub(r'\s*Quick Answer.*', '', cleaned_explanation, flags=re.IGNORECASE)
                # Suggested follow-up prompts (appear after the timeline like "24-72 hours")
                # Pattern: after a timeframe like "X-Y hours", strip trailing Grok suggestions
                cleaned_explanation = re.sub(
                    r'(\d+-\d+\s*hours?)\s+(?:Analyze|Details|Similar|Compare|Check|Explore|View|More)\s.*$',
                    r'\1', cleaned_explanation, flags=re.IGNORECASE
                )

                # Also remove angle brackets, square brackets, and placeholder-like text
                cleaned_explanation = re.sub(r'<[^>]+>', '', cleaned_explanation)  # Remove <...> placeholders
                cleaned_explanation = re.sub(r'\[[^\]]+\]', '', cleaned_explanation)  # Remove [...] placeholders
                cleaned_explanation = cleaned_explanation.strip()

                # Detect template remnants: bare semicolons with no real content
                # e.g. "YES - ; ; ;" or "NO - ; ;" left after placeholder removal
                stripped_test = re.sub(r'(YES|NO)\s*-\s*', '', cleaned_explanation, flags=re.IGNORECASE)
                stripped_test = re.sub(r'[;\s\n\ror]', '', stripped_test)
                if not stripped_test:
                    # This is template text, not a real explanation
                    print(f"[Grok] ⚠️ Detected template text as explanation, discarding: '{cleaned_explanation}'")
                    cleaned_explanation = ""

                # If cleaning removed too much or left template-like text, use original
                if cleaned_explanation and len(cleaned_explanation) > 10:
                    # Check if it still looks like template text
                    if '<' not in cleaned_explanation and 'very short' not in cleaned_explanation.lower():
                        explanation = cleaned_explanation

                # Debug: log the length to help troubleshoot
                if len(explanation) < 20:
                    print(f"[Grok] ⚠️ Captured explanation seems short ({len(explanation)} chars): '{explanation}'")
                else:
                    print(f"[Grok] ✅ Captured full explanation ({len(explanation)} chars)")
            if word == "YES":
                return True, explanation
            if word == "NO":
                return False, explanation

        # Fallback: infer YES/NO from the beginning of the first paragraph
        if state and state.get("firstParagraph"):
            fp = (state["firstParagraph"] or "").strip()
            fp_upper = fp.upper()
            if fp_upper.startswith("YES") and (
                len(fp_upper) == 3 or not fp_upper[3].isalnum()
            ):
                return True, fp
            if fp_upper.startswith("NO") and (
                len(fp_upper) == 2 or not fp_upper[2].isalnum()
            ):
                return False, fp

        await asyncio.sleep(2)

        # If we have NOT seen any thinking/loader indicators and we've already
        # consumed the initial window, stop early without waiting for the full
        # extended_total_timeout.
        if not seen_indicator and loop.time() >= short_deadline:
            break

    effective_cap = (
        extended_total_timeout if (extended_total_timeout and seen_indicator) else initial_timeout
    )
    print(f"[Grok] ⏱️ Timeout waiting for YES/NO decision ({effective_cap}s max reached)")
    return None, None


async def check_token_trend_with_grok(
    grok_page: Page,
    token_name: str,
    token_link: str = "",
    extra_context: str = "",
    system_prompt: str = None,
) -> tuple[Union[Optional[bool], str], Optional[str]]:
    """
    Ask Grok whether a token has an active/hot narrative that has NOT died down.

    Returns:
      True  -> Grok answered YES (active, hot narrative still alive)
      False -> Grok answered NO  (no clear or dead narrative)
      None  -> Could not get / parse a reliable answer
    """
    if grok_page is None:
        print("[Grok] ⚠️ grok_page is None; skipping Grok check")
        return None, None

    try:
        await grok_page.goto(GROK_URL, wait_until="domcontentloaded")
    except Exception as e:
        print(f"[Grok] ❌ Failed to navigate to Grok: {e}")
        return None, None

    try:
        await ensure_grok_loaded(grok_page)
    except Exception as e:
        print(f"[Grok] ❌ Grok UI did not fully load: {e}")
        return None, None

    # Build the prompt with token-specific information
    active_rules = system_prompt.strip() if system_prompt else GROK_NARRATIVE_RULES.strip()
    parts = [
        active_rules,
        "",
        f"Token name: {token_name}",
    ]
    if token_link:
        parts.append(f"Token link (for context only): {token_link}")
    if extra_context:
        parts.append(f"Additional context: {extra_context}")

    full_prompt = "\n".join(parts)

    print(f"[Grok] 🔍 Checking narrative/trend for token: {token_name}")
    try:
        await _send_prompt(grok_page, full_prompt)
        # Dynamic timeout strategy:
        #   - Start with a 30s window after the prompt is sent.
        #   - If we never see "thinking"/loader indicators, stop at 30s.
        #   - If we DO see indicators, extend total wait up to 90s from prompt time
        #     (i.e., 90s minus whatever portion of the first 30s was already used).
        decision, explanation = await _extract_yes_no(
            grok_page,
            initial_timeout=30,
            extended_total_timeout=120,
        )
        # Handle rate limit or generic error - both require a profile switch
        if decision == "rate_limit":
            print(f"[Grok] ⚠️ Rate limit detected for {token_name} - profile switch required")
            return "rate_limit", None
        if decision == "grok_error":
            print(f"[Grok] ⚠️ Grok error ('unable to reply') for {token_name} - profile switch required")
            return "grok_error", None
        if decision is None:
            print(f"[Grok] ⚠️ Could not parse a clear YES/NO for {token_name}")
        else:
            print(
                f"[Grok] ✅ Decision for {token_name}: {'YES (active trend)' if decision else 'NO (no active trend)'}"
            )
            if explanation:
                # Log a short version of Grok's narrative explanation for debugging
                short_expl = explanation if len(explanation) <= 200 else explanation[:197] + "..."
                print(f"[Grok] 📄 Explanation: {short_expl}")
        return decision, explanation
    except Exception as e:
        print(f"[Grok] ❌ Error during Grok check for {token_name}: {e}")
        return None, None


