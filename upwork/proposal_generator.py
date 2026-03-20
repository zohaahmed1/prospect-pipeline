"""
Proposal generator — priority order:
  1. Anthropic SDK   (if ANTHROPIC_API_KEY is set)
  2. OAuth API call  (if CLAUDE_CODE_OAUTH_TOKEN is set — uses Claude Max subscription)
  3. claude CLI      (local fallback via multiprocessing spawn)

Agency: Skip the Noise Media
"""

import os
import subprocess
import multiprocessing as mp
import requests as _requests
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ── Load secrets from Streamlit Cloud ─────────────────────────────────────────
try:
    import streamlit as _st
    if hasattr(_st, "secrets"):
        for _key in ["ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN"]:
            try:
                if _key in _st.secrets:
                    os.environ.setdefault(_key, _st.secrets[_key])
            except Exception:
                pass
except Exception:
    pass

# ── Anthropic SDK (optional) ───────────────────────────────────────────────────
try:
    import anthropic as _anthropic
    _sdk_available = True
except ImportError:
    _sdk_available = False

_SYSTEM_PROMPT = """You write Upwork proposals for Zoha at Skip the Noise Media, a Reddit Certified Partner performance marketing agency.

VOICE RULES (non-negotiable):
- NO em-dashes (— or –). Never. Not once.
- Short hyphens "-" are fine where needed: number ranges ($15-20K, 3-4x ROAS), compound terms (B2B, follow-up), and as a separator with spaces ("Flare.io - $75 CPL targeting CTOs in NA").
- Use "&" instead of "and" where it sounds natural.
- Short sentences. One idea per sentence. No setup, just facts.
- State the number, move on. Don't over-explain.
- Never: "I hope this finds you well", "I wanted to reach out", "leverage", "synergy", "delighted", "excited to", "I'd love to", "happy to help", "looking forward"
- Under 150 words total. Shorter is better. Cut anything that doesn't add proof or move the reader.
- No bullet points. Short paragraphs (1-3 sentences each).
- Do NOT start with "I".
- Abbreviations are fine: YoY, NA, B2B, B2C, DTC, CPL, CPA, ROAS, ICP.
- Numbers stay lowercase-ish in context: $35k, $10K/mo, $300-500K/mo, 10%.

STRUCTURE (no headers, natural flow):
1. Hook: Address their specific ask or pain point. 1-2 sentences. Context first, not "I".
2. Proof: One specific result with real numbers that maps to their need.
3. Approach: What you would actually do. 2-3 short, concrete sentences.
4. CTA: One short question. Low pressure. No exclamation marks.

CASE STUDIES BY JOB TYPE — read the job and pick the most relevant block:

REDDIT ADS jobs:
- We're a Reddit Certified Partner (one of ~200 globally)
- Flare.io (B2B cybersecurity): $75 CPL targeting CTOs and Heads of Security in NA
- Restream.io: $15 CPL on Reddit, scaled to $10K/mo
- 3D AI Studio: $7-9K/mo, $25-30 CPA
- StoryKeeper: 4x ROAS
- CIBC: $80K/mo, 21.6M impressions
- CTA: "What does your target audience look like? Share the ICP and I can do a quick audience sizing to see if Reddit makes sense before we scope anything."

META ADS / ECOMMERCE jobs:
- TinyProtectors: 3-4x ROAS at $15-20K/mo
- Noirvere: $50/day to $2K/day, 3-5x ROAS
- At WPP: Adidas $2.1M in spend, 11-30x ROAS
- DrinkPureRose: CPA $77 on $3K spend via UGC hook testing
- AI workflow: 20 ad variants produced in under 2 hours, 30+ creatives/month without burning design sprint capacity (use this when the job mentions creative production, creative fatigue, or creative testing at scale)
- CTA: "What's your current monthly spend and ROAS target? Are you running creative testing or working off one or two hero ads?"

B2B SAAS jobs (lead gen, pipeline, multi-platform):
- Restream.io: $25-75K/mo across Meta, LinkedIn and Reddit, 10% CAC reduction YoY
- Flare.io: $10K/mo, $75 CPL, full pipeline to HubSpot, CTOs and Heads of Security in NA
- Simplebooth: $100/day to $250/day for hardware and software sales
- Dell at WPP: $700K+ LinkedIn ABM, 202K clicks (name-drop only, keep it brief)
- CTA: "What's your monthly ad budget and which platforms are you currently on?"

B2C SAAS jobs:
- 3D AI Studio: $7-9K/mo on Reddit, $25-30 CPA
- Restream.io: $15 CPL on Reddit
- Apolone.com: $0 to $10K/mo in 2 months
- Simplebooth: $100/day to $250/day
- CTA: "What's your current monthly budget and what does your funnel look like — free trial, freemium, or direct purchase?"

LINKEDIN ADS jobs:
- Dell at WPP: $700K+ LinkedIn ABM with first-party company lists, 202K clicks
- Restream.io: $10-25K/mo, 10% CAC reduction, Thought Leader Ads and Sponsored Content
- Flare.io: Full pipeline from LinkedIn to lead magnet to HubSpot to qualification, CTOs in NA
- Van Cleef & Arpels/Cartier: $151K at $40.55 CPM (precision ABM targeting)
- CTA: "What seniority level are you targeting, and do you have lead magnets in place or are we building from scratch?"

TIKTOK ADS jobs:
- Noirvere: $50/day to $2K/day across Meta and TikTok, 3-5x ROAS
- Apolone.com: $500 to $10K/mo in 2 months. Fast creative iteration was the key variable.
- At WPP: TikTok for Adidas at ~$1M/year, 40 campaigns, $1.27 CPC
- CTA: "What's your current creative situation — UGC and video assets in hand, or starting from scratch? And what's the monthly budget?"

EVENT MARKETING jobs:
- Art of Living Canada: 3 sold-out shows (Vancouver, Edmonton, Calgary), 8 weeks, $35K spent, $3K+ tickets per show
- CTA: "What's the event, when is it, and what's the budget? The earlier we start, the more runway to optimize before the date."

CREATIVE STRATEGY jobs (ad creative production, creative testing frameworks, UGC, static/video ads):
- AI-assisted creative workflow: 20 ad variants produced in under 2 hours. 30+ creatives per month. Brief to client-ready in one day vs 1-2 week traditional cycle.
- Process: client brief & angle map (5+ angles) - copy per angle + visual direction brief - UGC/static/motion production - ABO test at $5-10/ad/day - 72hr pulse check (kill CTR <0.5% at day 3, scale ROAS 1.5x+ avg)
- Weekly reporting: CTR/CPC/CPA/ROAS per ad, winner/loser summary, next week's test hypotheses
- DrinkPureRose.com: UGC hook testing brought CPA to $77 on $3K ad spend
- TinyProtectors: Built launch creative for Urban Outfitters in-store activation
- Portfolio: skipthenoisemedia.com/progress
- CTA: "How many ad variants are you running right now, and when did you last refresh a hook or format?"

AD CREATIVE PRODUCTION jobs (specifically asking for someone to make ad creatives / UGC / static ads):
- Use the AI workflow as the main proof point. Lead with speed and volume, not "AI-generated".
- "We run an AI-assisted production workflow. Brief to client-ready in under 2 hours. 20+ variants per sprint, 30+ per month. Each one goes through a creative strategy filter before production."
- Case studies: DrinkPureRose ($77 CPA via hook testing), TinyProtectors (Urban Outfitters activation), portfolio at skipthenoisemedia.com/progress
- CTA: "What formats are you running right now - static, UGC, motion? Do you have existing brand assets or are we starting from scratch?"

CAMPAIGN MANAGEMENT / MULTI-PLATFORM jobs:
- Use whichever case studies match the client's industry and platform mix
- CTA: "Can you share more about your budget, target audience, and which platforms you're currently on?"

EXAMPLE PROPOSALS (match this style exactly - this is Zoha's voice):

Example 1 — Reddit Ads / B2B SaaS:
Most B2B companies skip Reddit before they've tested it. We haven't.

For Flare.io (cybersecurity) - $75 CPL targeting CTOs & Heads of Security in NA. For Restream.io - $15 CPL at $10K/mo. Reddit Certified Partner, one of ~200 globally.

We'd start with audience sizing on your ICP, map the relevant subreddits, then build a test before scaling.

What does your target audience look like? Share the ICP and I can do a quick sizing before we scope anything.

Example 2 — Meta / eCommerce:
On Meta right now, creative is the variable - not targeting or spend.

TinyProtectors hit 3-4x ROAS at $15-20K/mo once the hooks were dialled in. Noirvere went from $50/day to $2K/day at 3-5x ROAS.

We'd audit your current creative library, find the format & angle gaps, then build a testing cadence to find winners fast.

What's your current monthly spend & ROAS target? Running creative testing or working off one or two hero ads?

Example 3 — LinkedIn Ads / B2B:
LinkedIn ABM gets expensive fast without tight targeting.

At WPP, managed Dell at $700K+ using first-party company lists & title stacking. For Flare.io - full pipeline from LinkedIn to lead magnet to HubSpot to qualification, CTOs in NA.

We'd build your company list, layer seniority & function, then test Thought Leader Ads & Document Ads before scaling.

What seniority level are you targeting? Do you have lead magnets in place or are we building from scratch?

Example 4 — Creative Strategy / Ad Creative Production:
Creative fatigue is a volume problem, not a design problem. Most brands test 2-3 variants when you need 15-20 to find a winner.

We run an AI-assisted workflow - brief to 20 client-ready variants in under 2 hours. 30+ creatives per month, no design sprints burned. For DrinkPureRose - UGC hook testing brought CPA to $77 on $3K spend.

How many variants are you running right now? When did you last test a new hook or format?"""


def _detect_job_type(title: str, description: str) -> str:
    """Classify a job into one of the case study categories.

    Priority: specific platform signals first, then industry/context signals.
    Returns the exact category label used in the system prompt.
    """
    text = (title + " " + description).lower()

    # ── Specific platform signals (most reliable) ──────────────────────────────
    if "reddit" in text:
        return "REDDIT ADS"
    if "linkedin" in text and any(k in text for k in ["ads", "advertising", "paid", "campaign", "abm"]):
        return "LINKEDIN ADS"
    if "tiktok" in text:
        return "TIKTOK ADS"

    # ── Event marketing ────────────────────────────────────────────────────────
    if any(k in text for k in ["event marketing", "event promotion", "event tickets", "concert", "sold-out show", "venue", "live event"]):
        return "EVENT MARKETING"

    # ── Creative production / strategy (before SaaS check — overlaps) ─────────
    creative_signals = ["creative strategy", "creative strategist", "creative production", "creative testing", "creative fatigue", "ugc ads", "ugc creative", "ad variants", "ad creative production"]
    if any(k in text for k in creative_signals):
        # If explicitly about making ads (not just strategy), use that block
        make_signals = ["produce ads", "create ads", "make ads", "video ads", "static ads", "design ads", "ad maker", "ad production"]
        if any(k in text for k in make_signals):
            return "AD CREATIVE PRODUCTION"
        return "CREATIVE STRATEGY"

    # ── SaaS (B2B vs B2C) — check before generic eComm ───────────────────────
    saas_signals = ["saas", "software as a service", "b2b software", "b2c software", "platform", "app downloads", "free trial", "freemium", "product-led"]
    b2b_signals = ["b2b", "enterprise", "lead gen", "pipeline", "sql", "mql", "demo", "sales cycle", "account-based", "abm", "decision maker", "cto", "vp of", "head of", "cmo", "cro"]
    b2c_signals = ["b2c", "consumer app", "mobile app", "app store", "google play", "freemium", "downloads"]

    has_saas = any(k in text for k in saas_signals)
    has_b2b = any(k in text for k in b2b_signals)
    has_b2c = any(k in text for k in b2c_signals)

    if has_saas or has_b2b:
        if has_b2c and not has_b2b:
            return "B2C SAAS"
        return "B2B SAAS"

    # ── eComm / DTC ────────────────────────────────────────────────────────────
    ecomm_signals = ["dtc", "d2c", "ecommerce", "e-commerce", "shopify", "direct to consumer", "direct-to-consumer", "amazon", "woocommerce"]
    if any(k in text for k in ecomm_signals):
        return "META ADS / ECOMMERCE"

    # ── Generic Meta/Facebook signals without eComm context ───────────────────
    if any(k in text for k in ["meta ads", "facebook ads", "facebook advertising", "instagram ads"]):
        return "META ADS / ECOMMERCE"

    # ── Ad creative (standalone, no strategy context) ──────────────────────────
    if any(k in text for k in ["ad creative", "create creatives", "design ads", "make ads"]):
        return "AD CREATIVE PRODUCTION"

    # ── Default fallback ───────────────────────────────────────────────────────
    return "CAMPAIGN MANAGEMENT / MULTI-PLATFORM"


_ANGLE_HINTS = {
    "Results-focused": (
        "Lead hard with numbers. Open with the most relevant metric or case study result. "
        "Every sentence earns its place with a fact. Minimal context-setting."
    ),
    "Aggressive": (
        "Be blunt. Call out the gap or problem directly in the first line. Don't hedge. "
        "Confident, not arrogant. Cut any softening language."
    ),
    "Soft sell": (
        "Warmer tone. Start by acknowledging their situation. Lower-pressure CTA. "
        "Frame it as a conversation, not a pitch."
    ),
}


def _build_user_prompt(title, description, budget, skills, client_info, questions=None, angle=None):
    skills_str = ", ".join(skills[:8]) if skills else "not listed"
    client_str = client_info or "no additional client info"

    # ── Detect job type so Claude uses the right case study block ──────────────
    detected_type = _detect_job_type(title, description)
    type_note = (
        f"\n\nDetected job category: {detected_type}. "
        f"Use ONLY proof points and examples from the '{detected_type}' section in the system prompt. "
        "Do not mix in results or case studies from other job type categories."
    )

    angle_note = ""
    if angle and angle in _ANGLE_HINTS:
        angle_note = f"\n\nTone instruction: {_ANGLE_HINTS[angle]}"

    if questions:
        qs_block = "\n".join(f"{i+1}. {q}" for i, q in enumerate(questions))
        questions_section = f"""

Screening Questions from the client:
{qs_block}

After the proposal, write exactly "---" on its own line, then answer each question. Format:
Q: [question]
A: [1-3 sentence answer in Zoha's voice — direct, backed by a specific result or fact, no fluff]

Keep answers short. Use case studies where relevant."""
        output_instruction = "Output the proposal, then --- , then the Q&A answers. No other labels or preamble. Start directly with the hook."
    else:
        questions_section = ""
        output_instruction = "Output the proposal only. No labels, no reasoning, no preamble. Start directly with the hook sentence."

    return f"""Write a proposal for this Upwork job.{type_note}{angle_note}

Job Title: {title}
Budget: {budget}
Required Skills: {skills_str}
Client Context: {client_str}

Job Description:
{description[:2000]}{questions_section}

{output_instruction} Under 150 words for the proposal. Shorter is better. No bullet points. No em-dashes."""


def _via_sdk(user_prompt, max_tokens=512):
    """Generate using Anthropic Python SDK."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None  # fall through
    client = _anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=max_tokens,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return msg.content[0].text.strip()


def _via_oauth(user_prompt):
    """Generate via Anthropic API using Claude Max OAuth token (no API key needed).

    Claude Code stores the OAuth token in CLAUDE_CODE_OAUTH_TOKEN.
    The Anthropic API accepts it as a Bearer token, so Claude Max subscribers
    can make API calls without a separate API key.
    """
    token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
    if not token:
        return None  # fall through
    resp = _requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "anthropic-version": "2023-06-01",
        },
        json={
            "model": "claude-3-5-sonnet-20241022",
            "max_tokens": 900,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_prompt}],
        },
        timeout=30,
    )
    if resp.status_code == 200:
        return resp.json()["content"][0]["text"].strip()
    # Raise so caller can surface the real error instead of falling through to CLI
    raise RuntimeError(f"OAuth API HTTP {resp.status_code}: {resp.text[:300]}")


# ── CLI via multiprocessing spawn ──────────────────────────────────────────────
# The `claude` CLI segfaults (exit -11) when spawned directly inside Claude
# Desktop's process tree — it inherits macOS Mach bootstrap ports and tries to
# connect to Claude Desktop's XPC service.
#
# Fix: use multiprocessing `spawn` context, which starts a fresh Python
# interpreter via exec(). That fresh process has no inherited Mach port context,
# so it can safely spawn `claude` as a child.
#
# This function MUST be module-level (not nested) so multiprocessing can pickle it.
def _spawn_worker(queue, full_prompt, oauth_token, path, home, lang):
    import subprocess, os
    env = {
        "PATH": path,
        "HOME": home,
        "LANG": lang,
        "TERM": "xterm-256color",
    }
    if oauth_token:
        env["CLAUDE_CODE_OAUTH_TOKEN"] = oauth_token
    try:
        r = subprocess.run(
            ["claude", "-p", full_prompt, "--model", "claude-sonnet-4-6"],
            capture_output=True,
            text=True,
            timeout=55,
            env=env,
            stdin=subprocess.DEVNULL,
        )
        queue.put((r.returncode, r.stdout, r.stderr))
    except subprocess.TimeoutExpired:
        queue.put((-2, "", ""))
    except Exception as e:
        queue.put((-1, "", str(e)))


def _via_cli(full_prompt):
    """Generate using claude CLI, via a spawned subprocess to avoid -11 on macOS."""
    ctx = mp.get_context("spawn")
    q = ctx.Queue()
    p = ctx.Process(
        target=_spawn_worker,
        args=(
            q,
            full_prompt,
            os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", ""),
            os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
            os.environ.get("HOME", ""),
            os.environ.get("LANG", "en_US.UTF-8"),
        ),
    )
    p.start()
    p.join(timeout=65)

    if p.is_alive():
        p.terminate()
        p.join(2)
        raise subprocess.TimeoutExpired("claude", 60)

    if not q.empty():
        rc, stdout, stderr = q.get_nowait()
        if rc == -2:
            raise subprocess.TimeoutExpired("claude", 55)
        if rc != 0:
            err = stderr.strip() or stdout.strip() or f"exit code {rc}"
            raise RuntimeError(f"claude CLI failed — {err}")
        return stdout.strip()

    raise RuntimeError(f"claude CLI process exited unexpectedly (code {p.exitcode})")


def generate_proposal(title, description, budget, skills, client_info="", questions=None, angle=None):
    """Generate a tailored Upwork proposal, optionally with screening question answers.

    angle: None | "Results-focused" | "Aggressive" | "Soft sell"

    Priority:
      1. Anthropic SDK    — if ANTHROPIC_API_KEY is set
      2. OAuth API call   — if CLAUDE_CODE_OAUTH_TOKEN is set (Claude Max, no API key needed)
      3. claude CLI       — local fallback via multiprocessing spawn

    Returns proposal text string (with Q&A appended after '---' if questions given),
    or an error message starting with 'Error:'.
    """
    user_prompt = _build_user_prompt(title, description, budget, skills, client_info, questions, angle)
    full_prompt = f"{_SYSTEM_PROMPT}\n\n---\n\n{user_prompt}"
    # Increase token limit when answering screening questions
    _max_tokens = 900 if questions else 512

    # 1. SDK
    if _sdk_available and os.environ.get("ANTHROPIC_API_KEY"):
        try:
            return _via_sdk(user_prompt, max_tokens=_max_tokens)
        except Exception as e:
            return f"Error: Anthropic SDK failed — {e}"

    # 2. OAuth token (Claude Max — works on Streamlit Cloud, no separate API key)
    if os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
        try:
            return _via_oauth(user_prompt)
        except Exception as e:
            return f"Error: OAuth API call failed — {e}"

    # 3. CLI via spawn (local only — avoids macOS Mach port segfault)
    try:
        return _via_cli(full_prompt)
    except FileNotFoundError:
        return "Error: Proposal generation unavailable. Add CLAUDE_CODE_OAUTH_TOKEN to Streamlit secrets."
    except subprocess.TimeoutExpired:
        return "Error: Proposal generation timed out (60s)."
    except Exception as e:
        return f"Error: {e}"
