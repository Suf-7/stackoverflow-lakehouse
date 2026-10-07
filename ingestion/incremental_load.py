"""INCREMENTAL LOAD: everything that changed since the last watermark.

Captures three kinds of change:
  * NEW      questions/answers created after the watermark
  * UPDATED  questions whose last_activity_date moved (new answer, edit, vote,
             closure). sort=activity + min=<watermark> returns exactly these.
  * DELETED  the API never returns deleted posts, so we re-request a list of
             known recent ids; ids missing from the response are deletions.
  * METRICS  the same re-request gives fresh score/view counts (snapshots),
             because votes and views do not change last_activity_date.

The watermark is stored in data/state/watermark.json. A 1-hour overlap is
subtracted on each run so late-arriving activity is not missed; duplicates
are harmless because Silver MERGEs on question_id / answer_id.

Examples
  python ingestion/incremental_load.py --since 2026-09-24T00:00:00 --out data/samples/incremental
  python ingestion/incremental_load.py --landing-root data/landing   # uses watermark
  python ingestion/incremental_load.py --since ... --reconcile-from data/samples/full_load/questions.jsonl --out ...
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import ROOT, batch_folder, load_config, to_epoch, utc_now_iso, write_batch  # noqa: E402
from so_api import QuotaExhausted, StackExchangeClient  # noqa: E402

STATE_FILE = os.path.join(ROOT, "data", "state", "watermark.json")
OVERLAP_SECONDS = 3600


def read_watermark():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)["last_activity_epoch"]
    return None


def save_watermark(epoch):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump({"last_activity_epoch": epoch, "updated_at_utc": utc_now_iso()}, f, indent=2)


def reconcile(client, ids):
    """Re-request known ids (100 per call).

    Returns (tombstones, snapshots):
      tombstones: ids the API no longer returns -> deleted
      snapshots:  current score / views / answers for ids still alive. Votes and
                  views do NOT move last_activity_date, so this is the only way
                  the incremental feed sees them change.
    """
    ids = list(ids)
    now = int(time.time())
    alive, snapshots = set(), []
    for i in range(0, len(ids), 100):
        chunk = ";".join(str(x) for x in ids[i:i + 100])
        for q in client.paginate(f"/questions/{chunk}", pagesize=100, filter="default"):
            alive.add(q["question_id"])
            snapshots.append({"question_id": q["question_id"], "snapshot_at": now, "score": q["score"],
                              "view_count": q["view_count"], "answer_count": q["answer_count"],
                              "is_answered": q["is_answered"], "closed_date": q.get("closed_date")})
    tombstones = [{"question_id": qid, "is_deleted": True, "detected_at": now} for qid in ids if qid not in alive]
    return tombstones, snapshots


def main():
    cfg = load_config()
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", help="UTC ISO timestamp; default = stored watermark minus overlap")
    dest = ap.add_mutually_exclusive_group(required=True)
    dest.add_argument("--out", help="explicit output folder")
    dest.add_argument("--landing-root", help="write to <root>/incremental/incr_<UTC timestamp>/ (pipeline layout)")
    ap.add_argument("--tags", nargs="*", default=cfg["tags"])
    ap.add_argument("--reconcile-from", help="JSONL of previously loaded questions to check for deletions")
    ap.add_argument("--no-state", action="store_true", help="do not advance the watermark (for samples/tests)")
    args = ap.parse_args()
    if args.landing_root:
        args.out = batch_folder(args.landing_root, "incremental")

    if args.since:
        since = to_epoch(args.since)
    else:
        wm = read_watermark()
        if wm is None:
            sys.exit("No watermark yet: pass --since or run the full load first.")
        since = wm - OVERLAP_SECONDS

    client = StackExchangeClient(cfg["site"], cfg["min_quota_reserve"])
    changed, answers, deletions, snapshots = {}, [], [], []
    try:
        for tag in args.tags:
            for q in client.paginate("/questions", tagged=tag, sort="activity", min=since, order="asc",
                                     pagesize=cfg["page_size"], filter=cfg["question_filter"]):
                changed[q["question_id"]] = q
        print(f"[incr] changed questions: {len(changed)} (requests={client.requests_made}, quota={client.quota_remaining})")

        with_answers = [qid for qid, q in changed.items() if q.get("answer_count", 0) > 0]
        answers = list(client.answers_for(with_answers, cfg["answer_filter"]))
        print(f"[incr] answers on changed questions: {len(answers)}")

        if args.reconcile_from:
            with open(args.reconcile_from) as f:
                known = [json.loads(line)["question_id"] for line in f]
            deletions, snapshots = reconcile(client, known)
            print(f"[incr] reconciled {len(known)} ids -> {len(deletions)} deleted, {len(snapshots)} snapshots")
    except QuotaExhausted as exc:
        print(f"[incr] STOPPED EARLY: {exc}")

    new_count = sum(1 for q in changed.values() if q["creation_date"] >= since)
    max_activity = max([q["last_activity_date"] for q in changed.values()] or [since])
    manifest = write_batch(
        args.out,
        {"questions": sorted(changed.values(), key=lambda q: q["last_activity_date"]),
         "answers": answers, "deleted_questions": deletions, "question_snapshots": snapshots},
        {"load_type": "incremental", "site": cfg["site"], "since_epoch": since, "tags": args.tags,
         "questions_new": new_count, "questions_updated": len(changed) - new_count,
         "next_watermark_epoch": max_activity,
         "api_requests": client.requests_made, "quota_remaining": client.quota_remaining},
    )
    if not args.no_state:
        save_watermark(max_activity)
    print(manifest["files"])


if __name__ == "__main__":
    main()
