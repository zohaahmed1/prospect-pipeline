#!/usr/bin/env python3
"""
Prospect Pipeline: Apollo Email Enrichment → Million Verifier → Instantly Push
Agency: Skip the Noise Media (Reddit Certified Partner)

Usage:
    python3 prospect_pipeline.py                    # Run full pipeline on all contacts
    python3 prospect_pipeline.py --verify-only      # Only verify emails (skip Apollo)
    python3 prospect_pipeline.py --push-only        # Only push to Instantly (skip Apollo + MV)
    python3 prospect_pipeline.py --dry-run           # Show what would happen, no API calls
"""

import requests, json, time, random, argparse, sys, os
from datetime import datetime
from pathlib import Path

# ── Load .env file if present (for local dev) ──
_env_path = Path(__file__).parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# ── API Keys (from env vars — set in .env locally or GitHub Secrets in CI) ──
INSTANTLY_API_KEY = os.environ.get("INSTANTLY_API_KEY", "")
MILLION_VERIFIER_KEY = os.environ.get("MILLION_VERIFIER_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")

# ── API Bases ──
APOLLO_BASE = "https://api.apollo.io/api/v1"
MV_BASE = "https://api.millionverifier.com/api/v3"
INSTANTLY_BASE = "https://api.instantly.ai/api/v2"


def get_instantly_headers():
    return {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {INSTANTLY_API_KEY}",
    }

CAMPAIGN_NAME = "SaaS Reddit Ads - Initial Prospecting (March 2026)"

# ── MV Result Codes ──
MV_RESULTS = {1: "ok", 2: "catch_all", 3: "unknown", 4: "error", 5: "disposable", 6: "invalid"}
MV_SAFE = {"ok", "catch_all"}  # Safe to send
MV_RISKY = {"unknown"}         # Send with caution
MV_UNSAFE = {"error", "disposable", "invalid"}  # Do not send


# ---------------------------------------------------------------------------
# Load contacts from contacts.json
# ---------------------------------------------------------------------------

def load_contacts():
    """Load contacts from contacts.json (same directory as this script)."""
    json_path = Path(__file__).parent / "contacts.json"
    if not json_path.exists():
        print(f"ERROR: {json_path} not found. Create it with your prospect list.")
        sys.exit(1)
    with open(json_path) as f:
        data = json.load(f)
    print(f"  Loaded {len(data)} contacts from {json_path.name}")
    return data


# ---------------------------------------------------------------------------
# Step 1: Apollo Email Enrichment
# ---------------------------------------------------------------------------

def apollo_enrich_email(contact):
    """Use Apollo People Match to find/verify email for a contact."""
    url = f"{APOLLO_BASE}/people/match"
    payload = {
        "api_key": INSTANTLY_API_KEY,  # Apollo uses same org key
        "first_name": contact["first_name"],
        "last_name": contact["last_name"],
        "domain": contact["domain"],
        "organization_name": contact["company"],
    }
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        person = data.get("person", {})
        if person and person.get("email"):
            return {
                "email": person["email"],
                "status": person.get("email_status", "unknown"),
                "catchall": person.get("email_domain_catchall", False),
            }
    except Exception as e:
        print(f"  Apollo error for {contact['first_name']} {contact['last_name']}: {e}")
    return None


def enrich_emails(contacts, dry_run=False):
    """Enrich missing emails via Apollo. Returns updated contacts."""
    print("\n" + "=" * 60)
    print("STEP 1: Apollo Email Enrichment")
    print("=" * 60)

    enriched = 0
    skipped = 0
    failed = 0

    for c in contacts:
        if c.get("email"):
            print(f"  [SKIP] {c['first_name']} {c['last_name']} @ {c['company']} — email already set")
            skipped += 1
            continue

        if dry_run:
            print(f"  [DRY] Would enrich {c['first_name']} {c['last_name']} @ {c['company']}")
            continue

        print(f"  [ENRICH] {c['first_name']} {c['last_name']} @ {c['company']}...", end=" ")
        result = apollo_enrich_email(c)
        if result:
            c["email"] = result["email"]
            c["apollo_status"] = result["status"]
            c["apollo_catchall"] = result["catchall"]
            print(f"-> {result['email']} ({result['status']})")
            enriched += 1
        else:
            print("-> NOT FOUND")
            failed += 1
        time.sleep(0.5)  # Rate limit

    print(f"\n  Summary: {enriched} enriched, {skipped} already had emails, {failed} failed")
    return contacts


# ---------------------------------------------------------------------------
# Step 2: Million Verifier Email Verification
# ---------------------------------------------------------------------------

def mv_verify_email(email):
    """Verify a single email via Million Verifier API."""
    params = {
        "api": MILLION_VERIFIER_KEY,
        "email": email,
        "timeout": 10,
    }
    try:
        r = requests.get(MV_BASE, params=params, timeout=15)
        r.raise_for_status()
        data = r.json()
        # API returns: result (text), resultcode (int), quality (text)
        return {
            "result": data.get("result", "error"),
            "result_code": data.get("resultcode", 4),
            "quality": data.get("quality", ""),
            "free": data.get("free", False),
            "role": data.get("role", False),
            "subresult": data.get("subresult", ""),
            "credits": data.get("credits", 0),
            "raw": data,
        }
    except Exception as e:
        return {"result": "error", "result_code": 4, "error": str(e)}


def verify_emails(contacts, dry_run=False):
    """Verify all contact emails via Million Verifier. Returns categorized results."""
    print("\n" + "=" * 60)
    print("STEP 2: Million Verifier Email Verification")
    print("=" * 60)

    verified = []
    risky = []
    invalid = []
    no_email = []

    for c in contacts:
        if not c.get("email"):
            print(f"  [SKIP] {c['first_name']} {c['last_name']} — no email")
            no_email.append(c)
            continue

        if dry_run:
            print(f"  [DRY] Would verify {c['email']}")
            verified.append(c)
            continue

        print(f"  [VERIFY] {c['email']}...", end=" ")
        result = mv_verify_email(c["email"])
        c["mv_result"] = result["result"]
        c["mv_quality"] = result.get("quality")
        c["mv_subresult"] = result.get("subresult", "")

        if result["result"] in MV_SAFE:
            status_extra = f" (quality: {result.get('quality')})" if result.get("quality") else ""
            print(f"-> OK {result['result']}{status_extra}")
            verified.append(c)
        elif result["result"] in MV_RISKY:
            print(f"-> RISKY {result['result']}")
            risky.append(c)
        else:
            print(f"-> INVALID {result['result']} — WILL NOT PUSH")
            invalid.append(c)

        time.sleep(0.1)  # MV allows 160/sec but let's be safe

    print(f"\n  Summary: {len(verified)} verified, {len(risky)} risky, {len(invalid)} invalid, {len(no_email)} no email")

    if invalid:
        print("\n  INVALID EMAILS (will not be pushed):")
        for c in invalid:
            print(f"    - {c['email']} ({c['first_name']} {c['last_name']} @ {c['company']}) — {c['mv_result']}")

    if risky:
        print("\n  RISKY EMAILS (will be pushed with caution):")
        for c in risky:
            print(f"    - {c['email']} ({c['first_name']} {c['last_name']} @ {c['company']}) — {c['mv_result']}")

    return verified, risky, invalid, no_email


# ---------------------------------------------------------------------------
# Step 3: Push to Instantly
# ---------------------------------------------------------------------------

# Touch sequences
def touch1(c):
    return (
        f"{c['hook']}.\n\n"
        f"We only do one thing - Reddit Ads for SaaS. "
        f"I can put together a quick Reddit opportunity audit for {c['company']} if that's useful."
    )

def touch2(c):
    return (
        f"Quick data point - SaaS brands running Reddit Ads with proper subreddit targeting "
        f"are seeing 30-50% lower cost per lead compared to Meta and LinkedIn.\n\n"
        f"For {c['company']}, {c['reddit_fit']}. "
        f"Happy to map those out - takes about 5 minutes on our end."
    )

def touch3(c):
    return (
        f"No worries if the timing isn't right - I won't keep following up.\n\n"
        f"We put together a Reddit Ads Playbook that breaks down targeting, creative, and bidding "
        f"specifically for SaaS brands. Might be useful for {c['company']} down the line - happy to send it over if you're interested."
    )

subject_templates = [
    "reddit ads for {company}",
    "{company}'s buyers are already on reddit",
    "the paid channel {company} probably isn't running yet",
]


def instantly_api(method, endpoint, payload=None, params=None):
    url = f"{INSTANTLY_BASE}/{endpoint}"
    r = getattr(requests, method)(url, headers=get_instantly_headers(), json=payload, params=params, timeout=30)
    r.raise_for_status()
    return r.json() if r.text else {}


def get_or_create_campaign():
    qp = {"limit": 100}
    while True:
        camps = instantly_api("get", "campaigns", params=qp)
        for c in camps.get("items", []):
            if c.get("name") == CAMPAIGN_NAME:
                print(f"  Found campaign: {c['id']}")
                return c["id"]
        nxt = camps.get("next_starting_after")
        if not nxt or not camps.get("items"):
            break
        qp["starting_after"] = nxt
    payload = {
        "name": CAMPAIGN_NAME,
        "campaign_schedule": {
            "schedules": [{
                "name": "Default",
                "days": {"1": True, "2": True, "3": True, "4": True, "5": True},
                "timezone": "America/Chicago",
                "timing": {"from": "09:00", "to": "17:00"},
            }]
        },
    }
    result = instantly_api("post", "campaigns", payload)
    cid = result.get("id")
    print(f"  Created campaign: {cid}")
    return cid


def push_to_instantly(contacts_to_push, dry_run=False):
    """Push verified contacts to Instantly campaign."""
    print("\n" + "=" * 60)
    print("STEP 3: Push to Instantly")
    print("=" * 60)

    if not contacts_to_push:
        print("  No contacts to push.")
        return

    if dry_run:
        print(f"  [DRY] Would push {len(contacts_to_push)} contacts:")
        for c in contacts_to_push:
            print(f"    - {c['email']} ({c['first_name']} {c['last_name']} @ {c['company']})")
        return

    campaign_id = get_or_create_campaign()
    ok_count = 0
    err_count = 0

    for c in contacts_to_push:
        subs = [s.format(**c) for s in subject_templates]
        lead = {
            "email": c["email"],
            "first_name": c["first_name"],
            "last_name": c["last_name"],
            "company_name": c["company"],
            "campaign": campaign_id,
            "custom_variables": {
                "title": c["title"],
                "company": c["company"],
                "subject_line_a": subs[0],
                "subject_line_b": subs[1],
                "subject_line_c": subs[2],
                "touch1_body": touch1(c),
                "touch2_body": touch2(c),
                "touch3_body": touch3(c),
            },
        }
        try:
            instantly_api("post", "leads", lead)
            print(f"  [OK] {c['email']}")
            ok_count += 1
        except requests.exceptions.HTTPError as e:
            print(f"  [ERR] {c['email']} — {e.response.status_code}: {e.response.text[:100]}")
            err_count += 1
        time.sleep(0.3)

    print(f"\n  Summary: {ok_count} pushed, {err_count} errors")


# ---------------------------------------------------------------------------
# Slack notifications
# ---------------------------------------------------------------------------

def post_to_slack(summary):
    """Post pipeline results to Slack via incoming webhook."""
    if not SLACK_WEBHOOK_URL:
        print("  [SLACK] No webhook URL configured, skipping notification")
        return
    try:
        payload = {"text": summary}
        r = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        r.raise_for_status()
        print("  [SLACK] Notification sent")
    except Exception as e:
        print(f"  [SLACK] Failed to send notification: {e}")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Prospect Pipeline: Apollo -> Million Verifier -> Instantly")
    parser.add_argument("--verify-only", action="store_true", help="Skip Apollo enrichment, only verify + push")
    parser.add_argument("--push-only", action="store_true", help="Skip Apollo + MV, only push to Instantly")
    parser.add_argument("--dry-run", action="store_true", help="Show what would happen without making API calls")
    args = parser.parse_args()

    # Load contacts from JSON
    contacts = load_contacts()

    print(f"{'=' * 60}")
    print(f"  Prospect Pipeline — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Contacts: {len(contacts)}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"{'=' * 60}")

    work = list(contacts)  # Copy

    # Step 1: Apollo enrichment (skip if emails already filled)
    if not args.verify_only and not args.push_only:
        work = enrich_emails(work, dry_run=args.dry_run)

    # Step 2: Million Verifier
    if not args.push_only:
        verified, risky, invalid, no_email = verify_emails(work, dry_run=args.dry_run)
        to_push = verified + risky  # Push verified + risky (catch_all/unknown)
    else:
        to_push = [c for c in work if c.get("email")]
        invalid = []

    # Step 3: Push to Instantly
    push_to_instantly(to_push, dry_run=args.dry_run)

    # Final summary
    print(f"\n{'=' * 60}")
    print(f"  PIPELINE COMPLETE")
    if not args.push_only and not args.dry_run:
        print(f"  Verified: {len(verified)} | Risky: {len(risky)} | Invalid: {len(invalid)} | No email: {len(no_email)}")
        print(f"  Pushed: {len(to_push)}")
        if invalid:
            print(f"\n  ACTION NEEDED — Replace these invalid contacts:")
            for c in invalid:
                print(f"    {c['first_name']} {c['last_name']} @ {c['company']} ({c['email']}) — {c['mv_result']}")

        # Post to Slack
        ts = datetime.now().strftime("%Y-%m-%d %H:%M CT")
        slack_msg = (
            f"*Prospect Pipeline Complete* ({ts})\n"
            f"Verified: {len(verified)} | Risky: {len(risky)} | Invalid: {len(invalid)}\n"
            f"Pushed to Instantly: {len(to_push)}"
        )
        if invalid:
            invalid_names = ", ".join(f"{c['first_name']} {c['last_name']}" for c in invalid)
            slack_msg += f"\n:warning: Invalid emails: {invalid_names}"
        post_to_slack(slack_msg)

    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
