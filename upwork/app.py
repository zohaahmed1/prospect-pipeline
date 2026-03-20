"""
Upwork Job Researcher + Proposal Generator
Skip the Noise Media — Reddit Certified Partner

Launch: python3 upwork_tool.py
"""

import json
import streamlit as st
import streamlit.components.v1 as components
from datetime import datetime, timezone
from pathlib import Path
from upwork_api import (
    KEYWORD_GROUPS,
    TIER3_COUNTRIES,
    CLIENT_ID,
    CLIENT_SECRET,
    STORED_ACCESS_TOKEN,
    has_client_credentials,
    get_auth_url,
    exchange_code_for_token,
    search_jobs,
    fetch_job_questions,
    get_last_api_error,
    score_breakdown,
    learned_boost,
)
from proposal_generator import generate_proposal

# ── Applied-jobs log (persists across page refreshes) ──────────────────────────
_APPLIED_LOG_PATH = Path(__file__).resolve().parent / "applied_jobs.json"


def _load_applied_log() -> list:
    """Load applied jobs from the JSON log file."""
    try:
        if _APPLIED_LOG_PATH.exists():
            return json.loads(_APPLIED_LOG_PATH.read_text())
    except Exception:
        pass
    return []


def _save_to_applied_log(job: dict, proposal_text: str = "") -> None:
    """Append a job to the applied log (silently ignores write errors)."""
    log = _load_applied_log()
    existing_ids = {e["id"] for e in log}
    if job["id"] in existing_ids:
        return
    log.append({
        "id": job["id"],
        "title": job["title"],
        "url": job.get("url", ""),
        "budget": job.get("budget", ""),
        "score": job.get("score", 0),
        "applied_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "proposal_preview": proposal_text[:300] if proposal_text else "",
    })
    try:
        _APPLIED_LOG_PATH.write_text(json.dumps(log, indent=2))
    except Exception:
        pass  # Streamlit Cloud has ephemeral FS — log stays in session_state only


# ── Liked-jobs log (powers the learning system) ────────────────────────────────
_LIKED_LOG_PATH = Path(__file__).resolve().parent / "liked_jobs.json"


def _load_liked_log() -> list:
    try:
        if _LIKED_LOG_PATH.exists():
            return json.loads(_LIKED_LOG_PATH.read_text())
    except Exception:
        pass
    return []


def _save_to_liked_log(job: dict, keywords_matched: list) -> None:
    log = _load_liked_log()
    existing_ids = {e["id"] for e in log}
    if job["id"] in existing_ids:
        return
    log.append({
        "id": job["id"],
        "title": job["title"],
        "keywords_matched": keywords_matched,
        "budget": job.get("budget", ""),
        "liked_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    })
    try:
        _LIKED_LOG_PATH.write_text(json.dumps(log, indent=2))
    except Exception:
        pass


def _remove_from_liked_log(job_id: str) -> None:
    log = _load_liked_log()
    updated = [e for e in log if e["id"] != job_id]
    try:
        _LIKED_LOG_PATH.write_text(json.dumps(updated, indent=2))
    except Exception:
        pass


# ── Search results + proposals cache (survives page refreshes) ─────────────────
_JOBS_CACHE_PATH = Path(__file__).resolve().parent / "jobs_cache.json"
_PROPOSALS_CACHE_PATH = Path(__file__).resolve().parent / "proposals_cache.json"


def _save_jobs_cache(jobs: list) -> None:
    try:
        _JOBS_CACHE_PATH.write_text(json.dumps(jobs, indent=2))
    except Exception:
        pass


def _load_jobs_cache() -> list:
    try:
        if _JOBS_CACHE_PATH.exists():
            return json.loads(_JOBS_CACHE_PATH.read_text())
    except Exception:
        pass
    return []


def _save_proposals_cache(proposals: dict) -> None:
    try:
        _PROPOSALS_CACHE_PATH.write_text(json.dumps(proposals, indent=2))
    except Exception:
        pass


def _load_proposals_cache() -> dict:
    try:
        if _PROPOSALS_CACHE_PATH.exists():
            return json.loads(_PROPOSALS_CACHE_PATH.read_text())
    except Exception:
        pass
    return {}


st.set_page_config(
    page_title="Upwork Job Finder — Skip the Noise",
    page_icon="🎯",
    layout="wide",
)

# ── Session state init ─────────────────────────────────────────────────────────
if "access_token" not in st.session_state:
    st.session_state.access_token = STORED_ACCESS_TOKEN
if "jobs" not in st.session_state:
    st.session_state.jobs = _load_jobs_cache()
if "proposals" not in st.session_state:
    st.session_state.proposals = _load_proposals_cache()
if "searched" not in st.session_state:
    st.session_state.searched = bool(st.session_state.jobs)
if "dismissed" not in st.session_state:
    st.session_state.dismissed = set()
if "applied" not in st.session_state:
    # Restore from log file so applied state survives page refreshes
    st.session_state.applied = {e["id"] for e in _load_applied_log()}
if "liked" not in st.session_state:
    st.session_state.liked = {e["id"] for e in _load_liked_log()}


def _save_token_to_env(token):
    """Write UPWORK_ACCESS_TOKEN into .env so it persists across restarts."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    lines = env_path.read_text().splitlines()
    new_lines = []
    replaced = False
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("UPWORK_ACCESS_TOKEN=") or stripped.startswith("# UPWORK_ACCESS_TOKEN="):
            new_lines.append(f"UPWORK_ACCESS_TOKEN={token}")
            replaced = True
        else:
            new_lines.append(line)
    if not replaced:
        new_lines.append(f"UPWORK_ACCESS_TOKEN={token}")
    env_path.write_text("\n".join(new_lines) + "\n")


# ── Auto-capture OAuth callback (?code= in URL) ────────────────────────────────
_oauth_code = st.query_params.get("code")
if _oauth_code and not st.session_state.access_token:
    with st.spinner("Completing OAuth connection..."):
        _result = exchange_code_for_token(_oauth_code)
    if _result and "access_token" in _result:
        _token = _result["access_token"]
        st.session_state.access_token = _token
        st.query_params.clear()
        _save_token_to_env(_token)
        st.rerun()
    else:
        st.error(f"OAuth callback failed: {get_last_api_error()}")
        st.query_params.clear()


# ── Helper functions ───────────────────────────────────────────────────────────
def _country_flag(code: str) -> str:
    """Convert a 2-letter ISO country code to its flag emoji (e.g. 'US' → 🇺🇸)."""
    if not code or len(code) != 2:
        return ""
    return "".join(chr(0x1F1E0 + ord(c) - ord("A")) for c in code.upper())


def score_badge(score):
    if score >= 8:
        return f"🟢 {score}/10"
    elif score >= 5:
        return f"🟡 {score}/10"
    else:
        return f"🔴 {score}/10"


def format_client(client):
    parts = []
    country_code = client.get("countryCode", "")
    if country_code:
        flag = _country_flag(country_code)
        country_name = client.get("country", country_code)
        parts.append(f"{flag} {country_name}" if flag else country_name)
    if client.get("paymentVerificationStatus") == "VERIFIED":
        parts.append("✅ Payment verified")
    rating = float(client.get("totalFeedback") or 0)
    if rating:
        parts.append(f"⭐ {rating:.1f}")
    jobs_posted = int(client.get("totalPostedJobs") or 0)
    if jobs_posted:
        parts.append(f"{jobs_posted} jobs posted")
    total_spent = (client.get("totalSpent") or {}).get("amount")
    if total_spent:
        try:
            spent_k = float(total_spent) / 1000
            if spent_k >= 1:
                parts.append(f"${spent_k:.0f}k spent")
        except Exception:
            pass
    return " · ".join(parts) if parts else "No client history"


def format_time_ago(created):
    if not created:
        return ""
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00").replace(" ", "T"))
        hours = (datetime.now(timezone.utc) - dt).total_seconds() / 3600
        if hours < 1:
            return "just now"
        if hours < 24:
            return f"{int(hours)}h ago"
        return f"{int(hours / 24)}d ago"
    except Exception:
        return ""


def _job_hours_old(job):
    """Return hours since job was posted (for recency filtering/sorting)."""
    created = job.get("created", "")
    if not created:
        return 9999
    try:
        dt = datetime.fromisoformat(created.replace("Z", "+00:00").replace(" ", "T"))
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except Exception:
        return 9999


def _job_budget_value(job):
    """Parse budget string into a comparable number for sorting."""
    import re
    budget = str(job.get("budget", ""))
    nums = re.findall(r"[\d,]+", budget)
    if nums:
        try:
            return float(nums[0].replace(",", ""))
        except Exception:
            pass
    return 0


def _passes_budget_filter(job, min_hourly: float, min_fixed: float) -> bool:
    """Return True if the job's budget meets the minimum for its type.

    Hourly jobs are checked against min_hourly, fixed-price against min_fixed.
    Jobs with no parseable budget are always included (don't filter them out).
    """
    if min_hourly == 0 and min_fixed == 0:
        return True
    import re
    budget = str(job.get("budget", ""))
    is_hourly = "/hr" in budget
    nums = re.findall(r"\d[\d,]*", budget)
    if not nums:
        return True  # unknown budget — keep it
    try:
        val = float(nums[0].replace(",", ""))
    except Exception:
        return True
    if is_hourly:
        return min_hourly == 0 or val >= min_hourly
    else:
        return min_fixed == 0 or val >= min_fixed


def _client_spent_value(client: dict) -> float:
    """Parse a client's totalSpent displayValue ('$25K', '$1.2M') → float."""
    display = (client.get("totalSpent") or {}).get("amount", "")
    if not display:
        return 0.0
    s = display.replace("$", "").replace(",", "").strip().upper()
    try:
        if s.endswith("K"):
            return float(s[:-1]) * 1_000
        if s.endswith("M"):
            return float(s[:-1]) * 1_000_000
        return float(s)
    except Exception:
        return 0.0


def _copy_button(text: str, key: str):
    """Browser clipboard copy button via injected JS (works on Streamlit Cloud HTTPS)."""
    safe_id = "".join(c for c in key if c.isalnum() or c == "_")
    safe_text = text.replace("\\", "\\\\").replace("`", "\\`").replace("$", "\\$")
    components.html(
        f"""<button id="cb_{safe_id}"
            onclick="navigator.clipboard.writeText(`{safe_text}`)
                .then(()=>{{
                    var b=document.getElementById('cb_{safe_id}');
                    b.innerText='✅ Copied!';
                    setTimeout(()=>b.innerText='📋 Copy',2000);
                }})
                .catch(()=>document.getElementById('cb_{safe_id}').innerText='Select & copy manually')"
            style="background:#f0f2f6;border:1px solid #d0d3da;border-radius:6px;
                   padding:5px 14px;cursor:pointer;font-size:12px;
                   font-family:sans-serif;color:#31333F;white-space:nowrap;">
            📋 Copy
        </button>""",
        height=38,
    )


# ── Header ─────────────────────────────────────────────────────────────────────
st.title("🎯 Upwork Job Finder")
st.caption("Skip the Noise Media — Reddit Certified Partner")

# ── Auth check ────────────────────────────────────────────────────────────────
is_authed = bool(st.session_state.access_token)

if not is_authed:
    with st.container(border=True):
        st.subheader("Connect Your Upwork Account")
        if not has_client_credentials():
            st.error("Add `UPWORK_CLIENT_ID` and `UPWORK_CLIENT_SECRET` to your `.env` file first.")
            st.stop()
        auth_url = get_auth_url()
        st.markdown(f"**[Click here to authorize on Upwork]({auth_url})**")
        st.info("After approving, Upwork will redirect you back here automatically and connect.")
    st.stop()

# ── Sidebar: Search Controls ───────────────────────────────────────────────────
with st.sidebar:
    st.header("Search Jobs")

    # Service areas with Select All / None
    st.subheader("Service Areas")
    col_all, col_none = st.columns(2)
    with col_all:
        if st.button("All", use_container_width=True, key="select_all"):
            for g in KEYWORD_GROUPS:
                st.session_state[f"kw_{g}"] = True
    with col_none:
        if st.button("None", use_container_width=True, key="select_none"):
            for g in KEYWORD_GROUPS:
                st.session_state[f"kw_{g}"] = False

    selected_groups = []
    for group_name in KEYWORD_GROUPS:
        key = f"kw_{group_name}"
        if key not in st.session_state:
            st.session_state[key] = True  # all selected by default
        if st.checkbox(group_name, value=st.session_state[key], key=key):
            selected_groups.append(group_name)

    custom_kw = st.text_input(
        "Custom keywords (comma-separated)",
        placeholder="e.g. affiliate marketing, influencer",
    )

    st.subheader("Filters")
    job_type = st.radio("Job Type", ["All", "Fixed-Price", "Hourly"], index=0)
    job_type_param = {"All": "all", "Fixed-Price": "fixed", "Hourly": "hourly"}[job_type]

    posted_within = st.selectbox(
        "Posted within",
        ["All time", "Last 24h", "Last 48h", "Last 7 days"],
        index=0,
    )
    posted_hours = {"All time": None, "Last 24h": 24, "Last 48h": 48, "Last 7 days": 168}[posted_within]

    min_score = st.slider("Minimum Relevance Score", 0, 10, 4)

    hide_tier3 = st.checkbox(
        "🌎 Hide Tier-3 country jobs",
        value=True,
        help="Filter out jobs from clients in India, Pakistan, Philippines, Nigeria, Bangladesh, etc.",
    )

    st.caption("💵 Minimum budget")
    _hourly_opts = ["Any", "$25/hr", "$30/hr", "$40/hr", "$50/hr", "$75/hr", "$100/hr"]
    _fixed_opts  = ["Any", "$500", "$1k", "$2.5k", "$5k", "$10k"]
    col_h, col_f = st.columns(2)
    with col_h:
        min_hourly_label = st.selectbox("Hourly", _hourly_opts, index=0, key="min_hourly",
                                        label_visibility="collapsed")
    with col_f:
        min_fixed_label  = st.selectbox("Fixed",  _fixed_opts,  index=0, key="min_fixed",
                                        label_visibility="collapsed")
    _hourly_map = {"Any": 0, "$25/hr": 25, "$30/hr": 30, "$40/hr": 40,
                   "$50/hr": 50, "$75/hr": 75, "$100/hr": 100}
    _fixed_map  = {"Any": 0, "$500": 500, "$1k": 1_000, "$2.5k": 2_500,
                   "$5k": 5_000, "$10k": 10_000}
    min_hourly_val = _hourly_map[min_hourly_label]
    min_fixed_val  = _fixed_map[min_fixed_label]

    min_spent_label = st.selectbox(
        "💰 Min client total spent",
        ["Any", "$1k+", "$5k+", "$10k+", "$20k+", "$50k+"],
        index=0,
    )
    _spent_map = {"Any": 0, "$1k+": 1_000, "$5k+": 5_000, "$10k+": 10_000,
                  "$20k+": 20_000, "$50k+": 50_000}
    min_spent_val = _spent_map[min_spent_label]

    sort_by = st.selectbox(
        "Sort by",
        ["Score (high to low)", "Budget (high to low)", "Newest first"],
        index=0,
    )

    search_clicked = st.button("🔍 Search Jobs", type="primary", use_container_width=True)

    if st.session_state.searched and st.session_state.jobs:
        st.divider()
        visible = sum(
            1 for j in st.session_state.jobs
            if j["score"] >= min_score
            and (posted_hours is None or _job_hours_old(j) <= posted_hours)
        )
        applied_count = len(st.session_state.applied)
        liked_count = len(st.session_state.liked)
        st.caption(
            f"{len(st.session_state.jobs)} found · {visible} shown"
            + (f" · {applied_count} applied" if applied_count else "")
        )
        if liked_count:
            st.caption(f"🧠 Learning from {liked_count} liked job{'s' if liked_count != 1 else ''}")

    st.divider()

    # ── Export proposals ───────────────────────────────────────────────────────
    if st.session_state.proposals:
        export_lines = []
        for _j in st.session_state.jobs:
            _pid = _j["id"]
            if _pid in st.session_state.proposals:
                export_lines += [
                    f"=== {_j['title']} ===",
                    f"Budget: {_j['budget']} | Score: {_j['score']}/10",
                    f"URL: {_j.get('url', '')}",
                    "",
                    st.session_state.proposals[_pid],
                    "\n" + "─" * 60 + "\n",
                ]
        st.download_button(
            "⬇️ Export Proposals",
            data="\n".join(export_lines),
            file_name=f"proposals_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}.txt",
            mime="text/plain",
            use_container_width=True,
        )

    # ── Clear results ──────────────────────────────────────────────────────────
    if st.session_state.jobs:
        if st.button("🗑️ Clear Results", use_container_width=True):
            st.session_state.jobs = []
            st.session_state.proposals = {}
            st.session_state.searched = False
            st.session_state.applied = set()
            st.session_state.dismissed = set()
            _save_jobs_cache([])
            _save_proposals_cache({})
            st.rerun()

    if st.button("Disconnect", use_container_width=True):
        st.session_state.access_token = ""
        st.session_state.jobs = []
        st.session_state.proposals = {}
        st.session_state.searched = False
        st.session_state.applied = set()
        st.session_state.dismissed = set()
        _save_jobs_cache([])
        _save_proposals_cache({})
        st.rerun()

# ── Search Logic ───────────────────────────────────────────────────────────────
if search_clicked:
    keywords = []
    for group in selected_groups:
        keywords.extend(KEYWORD_GROUPS[group])
    if custom_kw:
        keywords.extend([k.strip() for k in custom_kw.split(",") if k.strip()])

    if not keywords:
        st.warning("Select at least one service area or enter custom keywords.")
    else:
        with st.spinner(f"Searching {len(keywords)} keyword(s)..."):
            jobs = search_jobs(
                keywords,
                job_type=job_type_param,
                limit=50,
                token=st.session_state.access_token,
            )
        err = get_last_api_error()
        if err and not jobs:
            st.error(f"Search failed: {err}")
            if "401" in str(err) or "403" in str(err):
                st.warning("⚠️ Token expired. Click **Disconnect** in the sidebar and reconnect to Upwork.")
        else:
            # Apply learned boost from liked jobs before caching
            _liked = _load_liked_log()
            if _liked:
                for j in jobs:
                    boost = learned_boost(j, _liked)
                    if boost:
                        j["score"] = min(10, j["score"] + boost)
                        j["score_boosted"] = True
            st.session_state.jobs = jobs
            st.session_state.searched = True
            _save_jobs_cache(jobs)
            if err:
                st.warning(f"Some searches failed: {err}")

# ── Tabs: Search Results | Paste a Job ────────────────────────────────────────
tab_search, tab_paste = st.tabs(["🔍 Search Results", "📋 Paste a Job"])

with tab_paste:
    st.subheader("Generate a proposal from any job posting")
    st.caption("Paste a job description directly — no search needed.")
    paste_title = st.text_input("Job title", placeholder="e.g. Meta Ads Manager for DTC Brand")
    paste_budget = st.text_input("Budget", placeholder="e.g. $50/hr or $2,500 fixed")
    paste_desc = st.text_area(
        "Job description",
        placeholder="Paste the full job post here...",
        height=280,
        label_visibility="collapsed" if False else "visible",
    )
    col_paste_gen, col_paste_angle, _ = st.columns([2, 2, 4])
    with col_paste_angle:
        paste_angle_label = st.selectbox(
            "Tone",
            ["Default", "Results-focused", "Aggressive", "Soft sell"],
            key="paste_angle",
            label_visibility="collapsed",
        )
    paste_angle = None if paste_angle_label == "Default" else paste_angle_label
    paste_manual_q = st.text_area(
        "Screening questions (optional — one per line)",
        height=80,
        placeholder="What's your experience with Reddit Ads?\nHow do you measure ROAS?",
        label_visibility="visible",
    )
    with col_paste_gen:
        if st.button("✍️ Generate Proposal", key="paste_gen", type="primary"):
            if paste_desc.strip():
                qs = [q.strip() for q in paste_manual_q.splitlines() if q.strip()]
                spinner_msg = "Writing proposal & answers..." if qs else "Writing proposal..."
                with st.spinner(spinner_msg):
                    paste_proposal = generate_proposal(
                        title=paste_title or "Job Posting",
                        description=paste_desc,
                        budget=paste_budget or "Not specified",
                        skills=[],
                        client_info="",
                        questions=qs or None,
                        angle=paste_angle,
                    )
                st.session_state["paste_proposal"] = paste_proposal
            else:
                st.warning("Paste a job description first.")

    if "paste_proposal" in st.session_state:
        paste_text = st.session_state["paste_proposal"]
        if paste_text.startswith("Error:"):
            st.error(paste_text)
        else:
            if "\n---\n" in paste_text:
                p_prop, p_qa = paste_text.split("\n---\n", 1)
            else:
                p_prop, p_qa = paste_text, ""
            wc = len(p_prop.split())
            wc_lbl = f"📝 {wc} words" + (" ⚠️ over 150 — trim before sending" if wc > 150 else "")
            st.caption(wc_lbl)
            edited_paste = st.text_area(
                "Proposal", value=p_prop.strip(), height=220,
                key="paste_proposal_box", label_visibility="collapsed",
            )
            _copy_button(edited_paste, "paste_prop")
            if p_qa.strip():
                st.caption("📋 Screening question answers")
                edited_paste_qa = st.text_area(
                    "Q&A", value=p_qa.strip(), height=150,
                    key="paste_qa_box", label_visibility="collapsed",
                )
                _copy_button(edited_paste_qa, "paste_qa")

with tab_search:
  if st.session_state.searched:
    # Filter — applied jobs move to their own section below
    jobs = [
        j for j in st.session_state.jobs
        if j["score"] >= min_score
        and j["id"] not in st.session_state.dismissed
        and j["id"] not in st.session_state.applied
        and (posted_hours is None or _job_hours_old(j) <= posted_hours)
        and (not hide_tier3
             or (j.get("client") or {}).get("countryCode", "") not in TIER3_COUNTRIES)
        and _passes_budget_filter(j, min_hourly_val, min_fixed_val)
        and _client_spent_value(j.get("client") or {}) >= min_spent_val
    ]

    # Sort
    if sort_by == "Score (high to low)":
        jobs = sorted(jobs, key=lambda j: j["score"], reverse=True)
    elif sort_by == "Budget (high to low)":
        jobs = sorted(jobs, key=_job_budget_value, reverse=True)
    elif sort_by == "Newest first":
        jobs = sorted(jobs, key=_job_hours_old)

    total_found = len(st.session_state.jobs)
    dismissed_count = len(st.session_state.dismissed)

    if not jobs:
        st.info("No jobs matched your filters. Try lowering the score, broadening the recency window, or adding more keywords.")
        if dismissed_count:
            if st.button("Restore dismissed jobs"):
                st.session_state.dismissed = set()
                st.rerun()
    else:
        st.caption(
            f"Showing {len(jobs)} of {total_found} jobs · sorted by {sort_by.lower()}"
            + (f" · {dismissed_count} dismissed" if dismissed_count else "")
        )

        for job in jobs:
            jid = job["id"]

            with st.container(border=True):
                # ── Title row ──────────────────────────────────────────────────
                col1, col2, col3, col4, col5, col6 = st.columns([5, 2, 1, 1, 1, 1])
                with col1:
                    title_md = f"**{job['title']}**"
                    if job.get("url"):
                        title_md = f"**[{job['title']}]({job['url']})**"
                    st.markdown(title_md)
                with col2:
                    st.markdown(f"💰 `{job['budget']}`")
                with col3:
                    badge = score_badge(job["score"])
                    if job.get("score_boosted"):
                        badge += " ⚡"
                    st.markdown(badge)
                with col4:
                    # 👍 Like = teach the algorithm, stays visible in main results
                    is_liked = jid in st.session_state.liked
                    like_label = "👍" if not is_liked else "💛"
                    like_help = "Teach the algorithm (more like this)" if not is_liked else "Unlike"
                    if st.button(like_label, key=f"like_{jid}", help=like_help):
                        if not is_liked:
                            st.session_state.liked.add(jid)
                            bd = score_breakdown(job)
                            _save_to_liked_log(job, [kw for kw, _ in bd["matched_pos"]])
                        else:
                            st.session_state.liked.discard(jid)
                            _remove_from_liked_log(jid)
                        st.rerun()
                with col5:
                    if st.button("✅", key=f"apply_{jid}", help="Mark as applied"):
                        st.session_state.applied.add(jid)
                        _save_to_applied_log(
                            job,
                            st.session_state.proposals.get(jid, ""),
                        )
                        st.rerun()
                with col6:
                    if st.button("✕", key=f"dismiss_{jid}", help="Dismiss this job"):
                        st.session_state.dismissed.add(jid)
                        st.rerun()

                # ── Client + time row ──────────────────────────────────────────
                time_str = format_time_ago(job["created"])
                client_str = format_client(job["client"])
                caption_parts = [client_str]
                if time_str:
                    caption_parts.append(time_str)
                st.caption(" · ".join(caption_parts))

                # ── Skills ─────────────────────────────────────────────────────
                if job["skills"]:
                    st.markdown(" ".join(f"`{s}`" for s in job["skills"][:8]))

                # ── Description preview ────────────────────────────────────────
                desc = (job["description"] or "").replace("\n", " ").strip()
                preview = desc[:400] + ("..." if len(desc) > 400 else "")
                st.caption(preview)

                # ── Score breakdown ────────────────────────────────────────────
                with st.expander(f"🔍 Score breakdown ({job['score']}/10)"):
                    bd = score_breakdown(job)
                    if bd["gated"]:
                        st.caption("⚠️ Keyword signal < 2pts — budget & client bonuses not applied.")
                    lines = []
                    if bd["matched_pos"]:
                        kw_parts = [f"+{pts} `{kw}`" for kw, pts in bd["matched_pos"]]
                        lines.append(f"**Keywords ({bd['kw_score']}/6):** {' · '.join(kw_parts)}")
                    else:
                        lines.append("**Keywords:** no positive signals matched")
                    if bd["matched_neg"]:
                        neg_parts = [f"{pts} `{kw}`" for kw, pts in bd["matched_neg"]]
                        lines.append(f"**Negatives ({bd['neg_total']}):** {' · '.join(neg_parts)}")
                    if bd["budget_score"]:
                        lines.append(f"**Budget:** +{bd['budget_score']} (hourly ≥$30/hr · fixed ≥$1k)")
                    if bd["client_score"]:
                        spend_note = f" · {bd['spend_str']} spent ✓" if bd["spent_ok"] and bd["spend_str"] else ""
                        lines.append(f"**Client:** +{bd['client_score']}{spend_note}")
                    if bd["recency"]:
                        lines.append("**Recency:** +1 (posted < 48h)")
                    if bd.get("geo_score"):
                        _geo_s = bd["geo_score"]
                        _geo_flag = _country_flag(bd.get("country_code", ""))
                        _geo_name = bd.get("country", bd.get("country_code", ""))
                        _geo_label = f"{_geo_flag} {_geo_name}".strip() or "unknown"
                        lines.append(f"**Geo:** {'+' if _geo_s > 0 else ''}{_geo_s} ({_geo_label})")
                    st.markdown("  \n".join(lines))

                # ── Proposal controls ──────────────────────────────────────────
                col_gen, col_angle, col_spacer = st.columns([2, 2, 4])
                with col_angle:
                    angle_label = st.selectbox(
                        "Tone",
                        ["Default", "Results-focused", "Aggressive", "Soft sell"],
                        key=f"angle_{jid}",
                        label_visibility="collapsed",
                    )
                _angle = None if angle_label == "Default" else angle_label

                with col_gen:
                    if st.button("✍️ Generate Proposal", key=f"gen_{jid}"):
                        # Manual questions take priority over auto-fetch
                        manual_raw = st.session_state.get(f"manual_q_{jid}", "")
                        manual_qs = [q.strip() for q in manual_raw.splitlines() if q.strip()]
                        with st.spinner("Checking for screening questions..."):
                            auto_qs, q_err = fetch_job_questions(
                                jid,
                                ciphertext=job.get("ciphertext"),
                                token=st.session_state.access_token,
                            )
                        questions = manual_qs or auto_qs or []
                        job["questions"] = questions
                        job["questions_err"] = q_err if not manual_qs else None
                        spinner_msg = "Writing proposal & answers..." if questions else "Writing proposal..."
                        with st.spinner(spinner_msg):
                            proposal = generate_proposal(
                                title=job["title"],
                                description=job["description"],
                                budget=job["budget"],
                                skills=job["skills"],
                                client_info=format_client(job["client"]),
                                questions=questions or None,
                                angle=_angle,
                            )
                        st.session_state.proposals[jid] = proposal
                        _save_proposals_cache(st.session_state.proposals)

                # ── Manual screening questions input ───────────────────────────
                with st.expander("📋 Add screening questions manually"):
                    manual_qs_raw = st.text_area(
                        "Questions",
                        placeholder="Paste each question on a new line, e.g.\nWhat's your experience with Reddit Ads?\nHow do you approach creative testing?",
                        height=100,
                        key=f"manual_q_{jid}",
                        label_visibility="collapsed",
                    )
                    if st.button("💬 Answer These Questions", key=f"answer_q_{jid}"):
                        qs_list = [q.strip() for q in manual_qs_raw.splitlines() if q.strip()]
                        if qs_list:
                            with st.spinner("Writing proposal & answers..."):
                                result = generate_proposal(
                                    title=job["title"],
                                    description=job["description"],
                                    budget=job["budget"],
                                    skills=job["skills"],
                                    client_info=format_client(job["client"]),
                                    questions=qs_list,
                                    angle=_angle,
                                )
                            st.session_state.proposals[jid] = result
                            _save_proposals_cache(st.session_state.proposals)
                            st.rerun()
                        else:
                            st.caption("Paste at least one question first.")

                if jid in st.session_state.proposals:
                    proposal_text = st.session_state.proposals[jid]

                    # Regenerate button (outside col_gen so it renders after proposal exists)
                    col_regen, col_spacer2 = st.columns([2, 6])
                    with col_regen:
                        if st.button("🔄 Regenerate", key=f"regen_{jid}"):
                            cached_questions = job.get("questions") or []
                            with st.spinner("Rewriting..."):
                                proposal_text = generate_proposal(
                                    title=job["title"],
                                    description=job["description"],
                                    budget=job["budget"],
                                    skills=job["skills"],
                                    client_info=format_client(job["client"]),
                                    questions=cached_questions or None,
                                    angle=_angle,
                                )
                            st.session_state.proposals[jid] = proposal_text
                            _save_proposals_cache(st.session_state.proposals)

                    # Show warning if question auto-fetch failed
                    if job.get("questions_err"):
                        st.warning("⚠️ Couldn't auto-fetch screening questions. Paste them in the box above.")

                    if proposal_text.startswith("Error:"):
                        st.error(proposal_text)
                    else:
                        # Split proposal from Q&A answers (separated by ---)
                        if "\n---\n" in proposal_text:
                            proposal_part, qa_part = proposal_text.split("\n---\n", 1)
                        else:
                            proposal_part = proposal_text
                            qa_part = ""

                        # ── Proposal box ──────────────────────────────────────
                        word_count = len(proposal_part.split())
                        wc_label = f"📝 {word_count} words"
                        if word_count > 150:
                            wc_label += " ⚠️ over 150 — trim before sending"
                        st.caption(wc_label)
                        edited_proposal = st.text_area(
                            "Proposal",
                            value=proposal_part.strip(),
                            height=200,
                            key=f"proposal_text_{jid}",
                            label_visibility="collapsed",
                        )
                        # ── Copy + Quick Apply row ────────────────────────────
                        _ca_url = (
                            f"https://www.upwork.com/ab/proposals/job/{job['ciphertext']}/apply/"
                            if job.get("ciphertext") else job.get("url", "")
                        )
                        col_copy, col_apply, col_pad = st.columns([1, 1, 4])
                        with col_copy:
                            _copy_button(edited_proposal, f"prop_{jid}")
                        with col_apply:
                            if _ca_url:
                                st.link_button("🚀 Quick Apply", _ca_url, use_container_width=True)

                        # ── Screening Q&A box (shown only when present) ───────
                        if qa_part.strip():
                            st.caption("📋 Screening question answers — paste each into Upwork's question fields")
                            edited_qa = st.text_area(
                                "Screening answers",
                                value=qa_part.strip(),
                                height=150,
                                key=f"qa_text_{jid}",
                                label_visibility="collapsed",
                            )
                            _copy_button(edited_qa, f"qa_{jid}")
                            # Persist edits
                            st.session_state.proposals[jid] = edited_proposal + "\n---\n" + edited_qa
                        else:
                            if edited_proposal != proposal_part.strip():
                                st.session_state.proposals[jid] = edited_proposal

    # ── Applied Jobs section ───────────────────────────────────────────────────
    applied_log = _load_applied_log()
    # Also include any in-session applied jobs not yet written to file
    session_applied_ids = st.session_state.applied
    logged_ids = {e["id"] for e in applied_log}
    # Build applied cards from current search results (for in-session applies)
    in_session_only = [
        j for j in st.session_state.jobs
        if j["id"] in session_applied_ids and j["id"] not in logged_ids
    ]

    if applied_log or in_session_only:
        st.divider()
        total_applied = len(applied_log) + len(in_session_only)
        with st.expander(f"✅ Applied Jobs ({total_applied})", expanded=False):
            # Show log entries (persistent)
            for entry in reversed(applied_log):
                col_a, col_b, col_c = st.columns([5, 2, 1])
                with col_a:
                    url = entry.get("url", "")
                    title_md = f"**[{entry['title']}]({url})**" if url else f"**{entry['title']}**"
                    st.markdown(title_md)
                with col_b:
                    st.caption(f"💰 {entry.get('budget', '')} · {entry.get('applied_at', '')}")
                with col_c:
                    if st.button("↩️", key=f"unapply_log_{entry['id']}", help="Remove from applied"):
                        st.session_state.applied.discard(entry["id"])
                        # Rewrite log without this entry
                        updated = [e for e in applied_log if e["id"] != entry["id"]]
                        try:
                            _APPLIED_LOG_PATH.write_text(json.dumps(updated, indent=2))
                        except Exception:
                            pass
                        st.rerun()
                if entry.get("proposal_preview"):
                    st.caption(f"_{entry['proposal_preview'][:150]}..._")
                st.divider()

            # Show in-session applies not yet in the log
            for job in in_session_only:
                col_a, col_b, col_c = st.columns([5, 2, 1])
                with col_a:
                    url = job.get("url", "")
                    title_md = f"**[{job['title']}]({url})**" if url else f"**{job['title']}**"
                    st.markdown(title_md)
                with col_b:
                    st.caption(f"💰 {job.get('budget', '')} · this session")
                with col_c:
                    if st.button("↩️", key=f"unapply_sess_{job['id']}", help="Remove from applied"):
                        st.session_state.applied.discard(job["id"])
                        st.rerun()
                st.divider()

  else:
    st.markdown("""
### How to use

1. Select service areas in the sidebar (use **All / None** to toggle quickly)
2. Set filters: job type, posted within, minimum score
3. Hit **🔍 Search Jobs**
4. Click **✍️ Generate Proposal** on any job that looks good

Proposals are written in your voice. Edit directly in the text box before sending.
""")
