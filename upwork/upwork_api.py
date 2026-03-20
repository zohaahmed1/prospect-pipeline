"""
Upwork API client — OAuth 2.0 Authorization Code flow + GraphQL job search.

Flow:
  1. get_auth_url()  → user opens in browser, approves
  2. exchange_code_for_token(code)  → get access_token
  3. search_jobs(...)  → signed Bearer requests to GraphQL

Agency: Skip the Noise Media
"""

import os
import requests
from pathlib import Path
from datetime import datetime, timezone

# ── Load .env (local dev) ──────────────────────────────────────────────────────
_env_path = Path(__file__).resolve().parent.parent / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

# ── Load Streamlit secrets ─────────────────────────────────────────────────────
_st_secrets = {}
try:
    import streamlit as st
    if hasattr(st, "secrets"):
        for _key in ["UPWORK_CLIENT_ID", "UPWORK_CLIENT_SECRET", "UPWORK_ACCESS_TOKEN"]:
            try:
                if _key in st.secrets:
                    _st_secrets[_key] = st.secrets[_key]
            except Exception:
                pass
except Exception:
    pass


def _env(key, default=""):
    return _st_secrets.get(key) or os.environ.get(key, default)


# ── Credentials ───────────────────────────────────────────────────────────────
CLIENT_ID = _env("UPWORK_CLIENT_ID")
CLIENT_SECRET = _env("UPWORK_CLIENT_SECRET")
STORED_ACCESS_TOKEN = _env("UPWORK_ACCESS_TOKEN")  # cached token from previous OAuth flow

# ── Endpoints ─────────────────────────────────────────────────────────────────
GRAPHQL_URL = "https://api.upwork.com/graphql"
TOKEN_URL = "https://www.upwork.com/api/v3/oauth2/token"
AUTH_URL = "https://www.upwork.com/ab/account-security/oauth2/authorize"
REDIRECT_URI = "http://localhost:8502"

_HEADERS_BASE = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.upwork.com",
    "Referer": "https://www.upwork.com/",
    "Content-Type": "application/json",
}

# ── Keyword groups — refined from 1225 proposal history (25.5% win rate) ──────
# Winning patterns: "management" 41%, "strategy/creative" 37-38%, "setup" 35%,
# "dtc" 35%, "campaign" 32%, "google" 31%, "reddit/meta/facebook" 28-30%
KEYWORD_GROUPS = {
    "Reddit Ads": ["reddit ads", "reddit advertising"],
    "Meta / Facebook Ads": ["meta ads", "facebook ads", "facebook advertising"],
    "LinkedIn Ads": ["linkedin ads", "linkedin advertising", "linkedin paid"],
    "Pinterest Ads": ["pinterest ads", "pinterest advertising"],
    "Snapchat Ads": ["snapchat ads", "snapchat advertising"],
    "Campaign Management": ["campaign management", "ads manager", "media buyer"],
    "Creative Strategist": ["creative strategist", "ad creative", "ugc creative"],
    "B2B SaaS Paid": ["b2b saas ads", "saas paid media", "b2b paid ads"],
    "Google + Meta": ["google meta ads", "google facebook ads", "ppc meta"],
    "DTC / eComm Ads": ["dtc ads", "ecommerce ads", "shopify ads"],
    "Performance Marketing": ["performance marketing", "paid media specialist"],
}

# Positive keyword scores — specific paid-ads signals only.
_SCORE_KEYWORDS = {
    # Core services — highest specificity (4 pts)
    "reddit ads": 4,
    "reddit advertising": 4,
    # Strong paid-ads role signals (3 pts)
    "meta ads": 3,
    "facebook ads": 3,
    "facebook advertising": 3,
    "media buyer": 3,
    "paid social": 3,
    "performance marketing": 3,
    "paid media": 3,
    "ppc": 3,               # pay-per-click — strong paid signal
    "paid search": 3,       # SEM / Google Ads context
    # Specific paid-ads tactics (2 pts)
    "creative strategist": 2,
    "creative strategy": 2,
    "campaign management": 2,  # dropped from 3 — too generic (email, SEO also use it)
    "ads manager": 2,          # role-specific
    "social ads": 2,           # paid social shorthand
    "ad creative": 2,
    "campaign setup": 2,
    "roas": 2,
    "google ads": 2,
    "tiktok ads": 2,
    "sem": 2,
    "retargeting": 2,
    "ugc ads": 2,
    "dtc ads": 2,
    "ecommerce ads": 2,
    "b2b saas ads": 2,
    "paid advertising": 2,
    "cpc": 2,
    # Contextual (1 pt) — only useful when other signals already present
    "dtc": 1,
    "ecommerce": 1,
    "shopify ads": 1,
    "b2b paid": 1,
    "lookalike": 1,
    "conversion rate": 1,
}

# Negative signals — deduct for clear wrong-fits
_NEGATIVE_KEYWORDS = {
    # Completely wrong service type
    "seo": -3,
    "search engine optimization": -3,
    # Agency / subcontractor work (Zoha wants direct clients, not agency subcontracts)
    "white label": -4,
    "subcontract": -3,
    "for our clients": -2,    # agency posting on behalf of their clients
    "our agency": -2,
    "digital agency": -2,
    # Organic / non-paid work
    "social media management": -2,
    "organic social": -2,
    "content writing": -2,
    "web design": -2,
    "website development": -2,
    "wordpress": -2,
    "influencer marketing": -2,
    "email marketing": -1,
    "email campaign": -1,
    "graphic design": -1,
    "copywriting": -1,        # reduced from -2 — appears in legit ad creative jobs
    "content creator": -1,
}

# ── Geo tiers ──────────────────────────────────────────────────────────────────
# Tier-1: high-budget direct clients, Zoha's primary market
_TIER1_COUNTRIES = frozenset({
    "US", "CA",                                           # North America
    "GB", "AU", "NZ", "IE",                               # UK + Anglosphere
    "DE", "FR", "NL", "SE", "NO", "DK", "FI",            # Northern/Western EU
    "CH", "AT", "BE", "ES", "IT", "PT", "PL", "CZ",      # Rest of EU
    "SG", "JP", "KR", "HK", "AE", "IL",                  # High-income APAC + ME
})

# Tier-3: low-budget / high-volume markets — deprioritize
TIER3_COUNTRIES = frozenset({
    "IN", "PK", "BD", "PH", "NG", "EG", "MM", "LK",
    "NP", "GH", "KE", "ET", "TZ", "UG", "ZM", "RW",
    "VN", "KH", "ID",
})

_COUNTRY_NAME_TO_CODE: dict[str, str] = {
    "United States": "US", "Canada": "CA", "United Kingdom": "GB",
    "Australia": "AU", "New Zealand": "NZ", "Ireland": "IE",
    "Germany": "DE", "France": "FR", "Netherlands": "NL",
    "Sweden": "SE", "Norway": "NO", "Denmark": "DK", "Finland": "FI",
    "Switzerland": "CH", "Austria": "AT", "Belgium": "BE",
    "Spain": "ES", "Italy": "IT", "Portugal": "PT", "Poland": "PL",
    "Czech Republic": "CZ", "Czechia": "CZ",
    "Singapore": "SG", "Japan": "JP", "South Korea": "KR",
    "Hong Kong": "HK", "United Arab Emirates": "AE", "Israel": "IL",
    "India": "IN", "Pakistan": "PK", "Bangladesh": "BD",
    "Philippines": "PH", "Nigeria": "NG", "Egypt": "EG",
    "Myanmar": "MM", "Sri Lanka": "LK", "Nepal": "NP",
    "Ghana": "GH", "Kenya": "KE", "Vietnam": "VN",
    "Cambodia": "KH", "Indonesia": "ID",
    "Ukraine": "UA", "Russia": "RU", "Brazil": "BR", "Mexico": "MX",
}


def _to_country_code(country_str: str) -> str:
    """Normalize a country name or 2-letter code to uppercase ISO 3166-1 alpha-2."""
    if not country_str:
        return ""
    s = country_str.strip()
    if len(s) == 2:
        return s.upper()
    return _COUNTRY_NAME_TO_CODE.get(s, "")


def _geo_score(country_code: str) -> int:
    """Return +1 for Tier-1 countries, -3 for Tier-3, 0 otherwise."""
    if not country_code:
        return 0
    code = country_code.upper()
    if code in _TIER1_COUNTRIES:
        return 1
    if code in TIER3_COUNTRIES:
        return -3
    return 0


_last_api_error = None


def get_last_api_error():
    return _last_api_error


def has_client_credentials():
    return bool(CLIENT_ID and CLIENT_SECRET)


def get_auth_url():
    """Build the OAuth 2.0 authorization URL for the user to visit."""
    return (
        f"{AUTH_URL}"
        f"?response_type=code"
        f"&client_id={CLIENT_ID}"
        f"&redirect_uri={REDIRECT_URI}"
    )


def exchange_code_for_token(code):
    """Exchange an authorization code for an access token.

    Returns dict with 'access_token' key on success, None on failure.
    """
    global _last_api_error
    try:
        resp = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "code": code.strip(),
                "redirect_uri": REDIRECT_URI,
            },
            auth=(CLIENT_ID, CLIENT_SECRET),
            timeout=30,
        )
        resp.raise_for_status()
        _last_api_error = None
        return resp.json()
    except requests.HTTPError as e:
        _last_api_error = f"Token exchange failed ({e.response.status_code}): {e.response.text[:400]}"
        return None
    except Exception as e:
        _last_api_error = f"Token exchange error: {e}"
        return None


def _gql(query, variables=None, token=None):
    """Execute a GraphQL query. Returns data dict or None on error."""
    global _last_api_error
    tok = token or STORED_ACCESS_TOKEN
    if not tok:
        _last_api_error = "No access token. Complete OAuth setup first."
        return None
    payload = {"query": query}
    if variables:
        payload["variables"] = variables
    try:
        resp = requests.post(
            GRAPHQL_URL,
            headers={
                **_HEADERS_BASE,
                "Authorization": f"Bearer {tok}",
            },
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        if "errors" in result:
            _last_api_error = f"GraphQL error: {result['errors'][0].get('message', str(result['errors']))}"
            return None
        _last_api_error = None
        return result.get("data")
    except requests.HTTPError as e:
        _last_api_error = f"HTTP {e.response.status_code}: {e.response.text[:300]}"
        return None
    except Exception as e:
        _last_api_error = str(e)
        return None


_JOB_SEARCH_QUERY = """
query SearchJobs($searchExpr: String!) {
  marketplaceJobPostingsSearch(
    marketPlaceJobFilter: {
      searchExpression_eq: $searchExpr
      verifiedPaymentOnly_eq: true
    }
    paging: { first: 50, offset: 0 }
  ) {
    totalCount
    edges {
      node {
        id
        ciphertext
        title
        description
        createdDateTime
        engagement
        hourlyBudgetType
        amount { displayValue rawValue }
        hourlyBudgetMin { displayValue rawValue }
        hourlyBudgetMax { displayValue rawValue }
        skills { name }
        client {
          totalFeedback
          totalPostedJobs
          totalSpent { displayValue }
          verificationStatus
          location { country }
        }
      }
    }
  }
}
"""

_REST_JOB_BASE = "https://api.upwork.com/api/profiles/v2/jobs"


def fetch_job_questions(job_id, ciphertext=None, token=None):
    """Fetch screening questions via Upwork REST API.

    GraphQL does not expose the questions field on MarketplaceJobPosting.
    Falls back to REST: tries ciphertext first, then internal node ID.
    Returns (questions_list, error_str). error_str is None on success.
    """
    tok = token or STORED_ACCESS_TOKEN
    if not tok:
        return [], "No access token available."

    for lookup in filter(None, [ciphertext, job_id]):
        try:
            resp = requests.get(
                f"{_REST_JOB_BASE}/{lookup}.json",
                headers={**_HEADERS_BASE, "Authorization": f"Bearer {tok}"},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                # Questions can live under several field names depending on API version
                for field in ("questions", "screeningQuestions", "clientQuestions", "job_questions"):
                    raw = data.get(field) or []
                    if raw:
                        return [
                            (q if isinstance(q, str) else q.get("question") or q.get("text") or str(q)).strip()
                            for q in raw if q
                        ], None
                return [], None  # 200 but no questions on this job
        except Exception as e:
            continue  # try next lookup key

    return [], "Screening questions aren't accessible via Upwork's API for this job."


def _fmt_money(val):
    """Format '100.0' → '$100', '15.5' → '$16'."""
    try:
        n = float(val)
        if n == 0:
            return ""
        return f"${int(round(n))}"
    except Exception:
        return str(val) if val else ""


def _parse_spent(display_value):
    """Parse Upwork's totalSpent displayValue ('$25K', '$1.2M', '$500') → float."""
    try:
        s = (display_value or "").replace("$", "").replace(",", "").strip().upper()
        if not s or s in ("+", ""):
            return 0.0
        if s.endswith("K"):
            return float(s[:-1]) * 1_000
        if s.endswith("M"):
            return float(s[:-1]) * 1_000_000
        return float(s)
    except Exception:
        return 0.0


def _budget_score(budget_str, engagement):
    """Return 0-2 budget score.

    Thresholds:
      Hourly : >= $50/hr = 2,  >= $30/hr = 1
      Fixed  : >= $2500   = 2, >= $1000  = 1
    """
    is_hourly = "/hr" in budget_str or "hourly" in (engagement or "").lower()
    try:
        num = float(
            budget_str.replace("$", "").replace(",", "").replace("/hr", "")
            .strip().split("-")[0].strip()
        )
        if is_hourly:
            return 2 if num >= 50 else (1 if num >= 30 else 0)
        else:
            return 2 if num >= 2500 else (1 if num >= 1000 else 0)
    except Exception:
        return 0


def _client_score(client, gated):
    """Return 0-3 client quality score.

    +1 rating >= 4.5
    +1 jobs posted >= 5
    +1 total platform spend >= $20k  (signals serious, repeat buyer)
    """
    if gated:
        return 0
    score = 0
    if float(client.get("totalFeedback") or 0) >= 4.5:
        score += 1
    if int(client.get("totalPostedJobs") or 0) >= 5:
        score += 1
    spent_str = (client.get("totalSpent") or {}).get("amount", "")
    if _parse_spent(spent_str) >= 20_000:
        score += 1
    return score


def _score_job(job):
    text = (job.get("title", "") + " " + job.get("description", "")).lower()

    # ── Keyword relevance (0–6) ────────────────────────────────────────────────
    kw_raw = sum(pts for kw, pts in _SCORE_KEYWORDS.items() if kw in text)
    kw_score = min(kw_raw, 6)

    # ── Negative signals ──────────────────────────────────────────────────────
    neg = sum(pts for kw, pts in _NEGATIVE_KEYWORDS.items() if kw in text)

    # ── Geo score (always applied — not gated by keyword signal) ─────────────
    geo = _geo_score((job.get("client") or {}).get("countryCode", ""))

    # ── Gate: if paid-ads signal is weak, budget/client bonuses don't apply ───
    if kw_score < 2:
        return max(0, min(kw_score + neg + geo, 4))

    # ── Budget (0–2) ──────────────────────────────────────────────────────────
    budget_score = _budget_score(job.get("budget", ""), job.get("engagement", ""))

    # ── Client quality (0–3) ──────────────────────────────────────────────────
    client_score = _client_score(job.get("client") or {}, gated=False)

    # ── Recency (0–1) ─────────────────────────────────────────────────────────
    recency = 0
    created = job.get("created", "")
    if created:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - dt).total_seconds() / 3600 <= 48:
                recency = 1
        except Exception:
            pass

    total = kw_score + budget_score + client_score + recency + neg + geo
    return max(0, min(total, 10))


def score_breakdown(job):
    """Return scoring components for a job (mirrors _score_job logic).

    Returns dict:
        kw_score, budget_score, client_score, recency, neg_total,
        gated (bool), matched_pos [(kw, pts)], matched_neg [(kw, pts)],
        spent_ok (bool), spend_str (str)
    """
    text = (job.get("title", "") + " " + job.get("description", "")).lower()

    matched_pos = [(kw, pts) for kw, pts in _SCORE_KEYWORDS.items() if kw in text]
    matched_neg = [(kw, pts) for kw, pts in _NEGATIVE_KEYWORDS.items() if kw in text]
    kw_raw = sum(pts for _, pts in matched_pos)
    kw_score = min(kw_raw, 6)
    neg_total = sum(pts for _, pts in matched_neg)
    gated = kw_score < 2

    budget_score = 0 if gated else _budget_score(job.get("budget", ""), job.get("engagement", ""))
    client = job.get("client") or {}
    client_score = _client_score(client, gated)

    spend_str = (client.get("totalSpent") or {}).get("amount", "")
    spent_ok = _parse_spent(spend_str) >= 20_000

    recency = 0
    created = job.get("created", "")
    if created and not gated:
        try:
            dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            if (datetime.now(timezone.utc) - dt).total_seconds() / 3600 <= 48:
                recency = 1
        except Exception:
            pass

    geo = _geo_score(client.get("countryCode", ""))

    return {
        "kw_score": kw_score,
        "budget_score": budget_score,
        "client_score": client_score,
        "recency": recency,
        "neg_total": neg_total,
        "gated": gated,
        "matched_pos": matched_pos,
        "matched_neg": matched_neg,
        "spent_ok": spent_ok,
        "spend_str": spend_str,
        "geo_score": geo,
        "country": client.get("country", ""),
        "country_code": client.get("countryCode", ""),
    }


def search_jobs(keywords, job_type="all", limit=30, token=None):
    """Search Upwork jobs across a list of keywords.

    Returns deduplicated, score-sorted list of job dicts:
      id, title, description, budget, engagement, skills, client, score, created
    """
    seen = {}
    for kw in keywords:
        data = _gql(
            _JOB_SEARCH_QUERY,
            {"searchExpr": kw},
            token=token,
        )
        if not data:
            continue
        postings = data.get("marketplaceJobPostingsSearch") or {}
        for edge in postings.get("edges") or []:
            node = edge.get("node") or {}
            jid = node.get("id")
            if not jid or jid in seen:
                continue

            # Budget: prefer hourly range, fall back to fixed amount
            engagement = node.get("engagement") or ""
            is_hourly = bool(node.get("hourlyBudgetType")) or bool(node.get("hourlyBudgetMin"))
            if is_hourly:
                lo_raw = (node.get("hourlyBudgetMin") or {}).get("rawValue", "")
                hi_raw = (node.get("hourlyBudgetMax") or {}).get("rawValue", "")
                lo = _fmt_money(lo_raw)
                hi = _fmt_money(hi_raw)
                if lo and hi:
                    budget = f"{lo}-{hi}/hr"
                elif lo:
                    budget = f"{lo}+/hr"
                else:
                    budget = "Hourly"
            else:
                raw = (node.get("amount") or {}).get("rawValue", "")
                budget = _fmt_money(raw) or "N/A"

            # Client info — normalise field names
            raw_client = node.get("client") or {}
            _raw_country = (raw_client.get("location") or {}).get("country", "")
            _country_code = _to_country_code(_raw_country)
            client = {
                "paymentVerificationStatus": "VERIFIED" if raw_client.get("verificationStatus") == "VERIFIED" else "",
                "totalFeedback": raw_client.get("totalFeedback", 0),
                "totalPostedJobs": raw_client.get("totalPostedJobs", 0),
                "totalSpent": {"amount": (raw_client.get("totalSpent") or {}).get("displayValue", "")},
                "country": _raw_country,
                "countryCode": _country_code,
            }

            ciphertext = node.get("ciphertext", "")
            job = {
                "id": jid,
                "ciphertext": ciphertext,  # ~022... format used for job detail lookups
                "title": node.get("title", ""),
                "description": node.get("description", ""),
                "budget": budget,
                "engagement": engagement,
                "skills": [s.get("name", "") for s in (node.get("skills") or [])],
                "client": client,
                "created": node.get("createdDateTime", ""),
                "url": f"https://www.upwork.com/jobs/{ciphertext}" if ciphertext else "",
                "questions": [],  # fetched on-demand via fetch_job_questions()
            }

            if job_type == "hourly" and not is_hourly:
                continue
            if job_type == "fixed" and is_hourly:
                continue

            job["score"] = _score_job(job)
            seen[jid] = job

    return sorted(seen.values(), key=lambda j: j["score"], reverse=True)[:limit]


def learned_boost(job, liked_jobs):
    """Return 0-2 extra score points based on patterns from jobs the user liked.

    Algorithm: count keyword frequency across all liked jobs. For each keyword
    that also appears in this job, weight its contribution by how often it
    appeared in liked jobs (frequency ratio). Cap final boost at 2.
    """
    if not liked_jobs:
        return 0
    from collections import Counter
    text = (job.get("title", "") + " " + job.get("description", "")).lower()
    kw_freq = Counter()
    for lj in liked_jobs:
        for kw in lj.get("keywords_matched", []):
            kw_freq[kw] += 1
    total = len(liked_jobs)
    boost_raw = sum(
        (count / total) * _SCORE_KEYWORDS.get(kw, 1)
        for kw, count in kw_freq.items()
        if kw in text
    )
    return min(2, int(boost_raw))
