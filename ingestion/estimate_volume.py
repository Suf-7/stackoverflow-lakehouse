"""Estimate full-load and incremental volume using count-only requests.

Uses the API's `total` filter (one cheap request per count) instead of
downloading data. Writes data/samples/volume_estimates.json.
"""
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import ROOT, load_config, to_epoch, utc_now_iso  # noqa: E402
from so_api import StackExchangeClient  # noqa: E402


def main():
    cfg = load_config()
    client = StackExchangeClient(cfg["site"], cfg["min_quota_reserve"])
    now = int(time.time())
    start = to_epoch(cfg["history_start_date"])
    last30 = now - 30 * 86400

    # 1) validate tag names (one request for all tags)
    info = client.get("/tags/" + ";".join(cfg["tags"]) + "/info", pagesize=100)
    found = {t["name"]: t["count"] for t in info["items"]}
    missing = [t for t in cfg["tags"] if t not in found]

    # 2) per-tag counts: since history start, and last 30 days
    per_tag = {}
    for tag in cfg["tags"]:
        per_tag[tag] = {
            "all_time_questions": found.get(tag),
            "questions_since_start": client.count("/questions", tagged=tag, fromdate=start, todate=now),
            "questions_last_30_days": client.count("/questions", tagged=tag, fromdate=last30, todate=now),
            "questions_with_activity_last_30_days": client.count("/questions", tagged=tag, sort="activity", min=last30),
        }
        print(tag, per_tag[tag])

    # 3) site-wide questions per year, for context on the AI-era decline
    site_per_year = {}
    for year in range(2018, 2027):
        frm = to_epoch(f"{year}-01-01")
        to = min(to_epoch(f"{year + 1}-01-01"), now)
        site_per_year[str(year)] = client.count("/questions", fromdate=frm, todate=to)
        print(year, site_per_year[str(year)])

    out = {
        "generated_at_utc": utc_now_iso(),
        "history_start_date": cfg["history_start_date"],
        "tags_not_found": missing,
        "per_tag": per_tag,
        "sum_questions_since_start_before_dedup": sum(v["questions_since_start"] for v in per_tag.values()),
        "sum_questions_last_30_days_before_dedup": sum(v["questions_last_30_days"] for v in per_tag.values()),
        "sum_active_questions_last_30_days_before_dedup": sum(v["questions_with_activity_last_30_days"] for v in per_tag.values()),
        "site_wide_questions_per_year": site_per_year,
        "note": "2026 is year-to-date. Counts exclude deleted posts.",
        "api_requests": client.requests_made,
        "quota_remaining": client.quota_remaining,
    }
    path = os.path.join(ROOT, "data", "samples", "volume_estimates.json")
    with open(path, "w") as f:
        json.dump(out, f, indent=2)
    print("wrote", path, "| requests:", client.requests_made, "| quota left:", client.quota_remaining)


if __name__ == "__main__":
    main()
