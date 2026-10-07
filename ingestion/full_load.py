"""FULL LOAD: historical baseline of questions (+ their answers) for the configured tags.

Pulls one calendar month at a time per tag, so the job is resumable and each
request window stays small. Questions carrying several tracked tags are
de-duplicated on question_id.

Examples
  # Phase 1 sample (3 months)
  python ingestion/full_load.py --from 2025-04-01 --to 2025-07-01 --out data/samples/full_load
  # Real baseline (run on Databricks / with an API key)
  python ingestion/full_load.py --from 2018-01-01 --to 2026-09-01 --landing-root /Volumes/workspace/so_raw/landing
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import batch_folder, load_config, month_windows, write_batch  # noqa: E402
from so_api import QuotaExhausted, StackExchangeClient  # noqa: E402


def main():
    cfg = load_config()
    ap = argparse.ArgumentParser()
    ap.add_argument("--from", dest="start", default=cfg["history_start_date"])
    ap.add_argument("--to", dest="end", required=True)
    dest = ap.add_mutually_exclusive_group(required=True)
    dest.add_argument("--out", help="explicit output folder")
    dest.add_argument("--landing-root", help="write to <root>/full/full_<from>_<to>/ (pipeline layout)")
    ap.add_argument("--tags", nargs="*", default=cfg["tags"])
    ap.add_argument("--no-answers", action="store_true")
    args = ap.parse_args()
    if args.landing_root:
        args.out = batch_folder(args.landing_root, "full", args.start, args.end)

    client = StackExchangeClient(cfg["site"], cfg["min_quota_reserve"])
    questions = {}
    per_tag = {}
    try:
        for tag in args.tags:
            before = len(questions)
            for frm, to, label in month_windows(args.start, args.end):
                for q in client.paginate("/questions", tagged=tag, fromdate=frm, todate=to,
                                         sort="creation", order="asc",
                                         pagesize=cfg["page_size"], filter=cfg["question_filter"]):
                    questions[q["question_id"]] = q
            per_tag[tag] = len(questions) - before
            print(f"[full] {tag:<32} +{per_tag[tag]:>6} new ids   (requests={client.requests_made}, quota={client.quota_remaining})")

        answers = []
        if not args.no_answers:
            answered = [qid for qid, q in questions.items() if q.get("answer_count", 0) > 0]
            answers = list(client.answers_for(answered, cfg["answer_filter"]))
            print(f"[full] answers fetched: {len(answers)}")
    except QuotaExhausted as exc:
        print(f"[full] STOPPED EARLY: {exc}")

    manifest = write_batch(
        args.out,
        {"questions": sorted(questions.values(), key=lambda q: q["creation_date"]), "answers": answers},
        {"load_type": "full", "site": cfg["site"], "window_start": args.start, "window_end_exclusive": args.end,
         "tags": args.tags, "new_ids_per_tag_in_order": per_tag,
         "api_requests": client.requests_made, "quota_remaining": client.quota_remaining},
    )
    print(manifest["files"])


if __name__ == "__main__":
    main()
