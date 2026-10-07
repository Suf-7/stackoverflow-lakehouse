"""Build a deliberately broken landing batch to demonstrate schema-drift handling."""
import json
import os

DRIFT_BATCH_ID = "incr_20261008T000000Z"


def make_drift_batch(landing_root, base_questions_file, batch_id=DRIFT_BATCH_ID,
                     extracted_at="2026-10-08T00:00:00Z"):
    """Write <landing_root>/incremental/<batch_id>/questions.jsonl with 5 lines:

    1. valid record + NEW top-level column `ai_assisted` + NEW nested field `owner.badge_counts`
    2. TYPE CHANGE: `score` arrives as the string "five" instead of an integer
    3. MALFORMED: truncated JSON
    4. MISSING PRIMARY KEY: no `question_id`
    5. valid record
    Expected: 2 rows land in Bronze (ai_assisted evolves the table), 3 rows go to quarantine.
    """
    with open(base_questions_file) as f:
        base = [json.loads(line) for line in f if line.strip()][:3]
    if len(base) < 3:
        raise ValueError("need at least 3 questions in the base file")
    new_col = dict(base[0], question_id=990000001, ai_assisted=True)
    new_col["owner"] = dict(new_col.get("owner") or {}, badge_counts={"gold": 1, "silver": 4})
    type_change = dict(base[1], question_id=990000002, score="five")
    no_key = {k: v for k, v in base[2].items() if k != "question_id"}
    valid = dict(base[2], question_id=990000004)
    lines = [json.dumps(new_col), json.dumps(type_change), '{"question_id": 990000003, "title": ',
             json.dumps(no_key), json.dumps(valid)]
    folder = os.path.join(landing_root, "incremental", batch_id)
    os.makedirs(folder, exist_ok=True)
    with open(os.path.join(folder, "questions.jsonl"), "w") as f:
        f.write("\n".join(lines) + "\n")
    with open(os.path.join(folder, "_manifest.json"), "w") as f:
        json.dump({"load_type": "incremental", "extracted_at_utc": extracted_at,
                   "note": "synthetic schema-drift demo batch"}, f, indent=2)
    return folder
