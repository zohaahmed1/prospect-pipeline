"""
Proposal generator — uses Anthropic SDK if ANTHROPIC_API_KEY is set,
otherwise falls back to the `claude` CLI subprocess.

Agency: Skip the Noise Media
"""

import os
import subprocess
import multiprocessing as mp
from pathlib import Path

# ── Load .env ──────────────────────────────────────────────────────────────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ── Anthropic SDK (optional) ───────────────────────────────────────────────────
try:
    import anthropic as _anthropic
    _sdk_available = True
except ImportError:
    _sdk_available = False

# ── Load ANTHROPIC_API_KEY from Streamlit secrets (Streamlit Cloud) ────────────
try:
    import streamlit as _st
    if hasattr(_st, "secrets") and "ANTHROPIC_API_KEY" in _st.secrets:
        os.environ.setdefault("ANTHROPIC_API_KEY", _st.secrets["ANTHROPIC_API_KEY"])
except Exception:
    pass

_SYSTEM_PROMPT = """You write Upwork proposals for Zoha at Skip the Noise Media, a Reddit Certified Partner performance marketing agency.

Write in Zoha's voice:
- NO em-dashes (— or –) ever. Use a period and new sentence instead.
- Short sentences. Conversational. No corporate jargon.
- Never use: "I hope this finds you well", "I wanted to reach out", "leverage", "synergy", "delighted", "excited to", "I'd love to"
- Under 200 words total
- No bullet points. Plain paragraphs only.
- Do NOT start the proposal with "I".

Agency proof points (use only what's directly relevant to the job):
- Reddit Certified Partner — one of ~200 agencies globally
- $75 CPL for B2B SaaS clients on Reddit Ads
- $15 CPL for B2C SaaS clients on Reddit Ads
- ROAS beating Meta for DTC and eCommerce brands
- 100+ Reddit Ads Playbook downloads

Proposal structure (no headers, just natural flow):
1. Hook — address their specific ask or pain point in 1-2 sentences. Start with context, not "I".
2. Proof — one specific result that directly maps to their need.
3. Approach — what you would do for them in 2-3 short, concrete sentences.
4. CTA — one short sentence. Low pressure. No exclamation marks."""


def _build_user_prompt(title, description, budget, skills, client_info):
    skills_str = ", ".join(skills[:8]) if skills else "not listed"
    client_str = client_info or "no additional client info"
    return f"""Write a proposal for this Upwork job.

Job Title: {title}
Budget: {budget}
Required Skills: {skills_str}
Client Context: {client_str}

Job Description:
{description[:2000]}

Write the proposal now. Under 200 words. No bullet points. No em-dashes. Start with the hook."""


def _via_sdk(user_prompt):
    """Generate using Anthropic Python SDK."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return None  # fall through to CLI
    client = _anthropic.Anthropic(api_key=api_key)
    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
    )
    return msg.content[0].text.strip()


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


def generate_proposal(title, description, budget, skills, client_info=""):
    """Generate a tailored Upwork proposal.

    Tries Anthropic SDK first (if ANTHROPIC_API_KEY set), then falls back
    to the claude CLI (Claude Code subscription).

    Returns proposal text string, or an error message starting with 'Error:'.
    """
    user_prompt = _build_user_prompt(title, description, budget, skills, client_info)
    full_prompt = f"{_SYSTEM_PROMPT}\n\n---\n\n{user_prompt}"

    # 1. Try SDK
    if _sdk_available and os.environ.get("ANTHROPIC_API_KEY"):
        try:
            return _via_sdk(user_prompt)
        except Exception as e:
            return f"Error: Anthropic SDK failed — {e}"

    # 2. Fall back to CLI (via spawn to avoid macOS Mach port segfault)
    try:
        return _via_cli(full_prompt)
    except FileNotFoundError:
        return "Error: `claude` CLI not found. Install Claude Code or set ANTHROPIC_API_KEY in .env."
    except subprocess.TimeoutExpired:
        return "Error: Proposal generation timed out (60s)."
    except Exception as e:
        return f"Error: {e}"
