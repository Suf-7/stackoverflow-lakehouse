"""Profile the sample payloads: schema, PII exposure, overlap and size stats.

Output: data/samples/profile_report.json (used in the Phase 1 proposal).
"""
import collections
import json
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
S = os.path.join(ROOT, "data", "samples")
CFG = json.load(open(os.path.join(ROOT, "config", "pipeline_config.json")))

PATTERNS = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "ipv4": re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"),
    "credential_like": re.compile(r"(?i)\b(password|passwd|pwd|secret|api[_-]?key|token)\s*[=:]\s*['\"]?[^\s'\"<]{4,}"),
    "aws_access_key": re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    "jdbc_or_conn_string": re.compile(r"(?i)\b(jdbc:|mongodb(\+srv)?://|postgres(ql)?://|mysql://|Server=.+;.*Password=)"),
}


def load(path):
    p = os.path.join(S, path)
    if not os.path.exists(p) or os.path.getsize(p) == 0:
        return []
    return [json.loads(l) for l in open(p)]


def key_paths(rows):
    counts = collections.Counter()
    for r in rows:
        for k, v in r.items():
            counts[k] += 1
            if isinstance(v, dict):
                for k2 in v:
                    counts[f"{k}.{k2}"] += 1
    return {k: round(c / len(rows), 3) for k, c in sorted(counts.items())}


def scan_bodies(rows):
    hits = collections.Counter()
    docs_with_hit = collections.Counter()
    for r in rows:
        text = (r.get("title") or "") + "\n" + (r.get("body") or "")
        for name, rx in PATTERNS.items():
            n = len(rx.findall(text))
            if n:
                hits[name] += n
                docs_with_hit[name] += 1
    return {name: {"matches": hits[name], "posts_affected": docs_with_hit[name],
                   "pct_posts": round(100 * docs_with_hit[name] / max(len(rows), 1), 2)} for name in PATTERNS}


def main():
    tracked = set(CFG["tags"])
    report = {}
    for load_type in ("full_load", "incremental"):
        qs, ans = load(f"{load_type}/questions.jsonl"), load(f"{load_type}/answers.jsonl")
        tags_per_q = [len(tracked & set(q["tags"])) for q in qs]
        owners = [r.get("owner", {}) for r in qs + ans]
        report[load_type] = {
            "questions": len(qs),
            "answers": len(ans),
            "answers_per_question": round(len(ans) / max(len(qs), 1), 3),
            "avg_question_bytes": round(sum(len(json.dumps(q)) for q in qs) / max(len(qs), 1)),
            "avg_answer_bytes": round(sum(len(json.dumps(a)) for a in ans) / max(len(ans), 1)),
            "avg_tracked_tags_per_question": round(sum(tags_per_q) / max(len(qs), 1), 3),
            "pct_questions_answered": round(100 * sum(q["is_answered"] for q in qs) / max(len(qs), 1), 1),
            "pct_questions_closed": round(100 * sum("closed_date" in q for q in qs) / max(len(qs), 1), 1),
            "pct_questions_with_accepted_answer": round(100 * sum("accepted_answer_id" in q for q in qs) / max(len(qs), 1), 1),
            "owner_user_types": dict(collections.Counter(o.get("user_type", "missing") for o in owners)),
            "distinct_owner_user_ids": len({o["user_id"] for o in owners if "user_id" in o}),
            "question_fields_presence": key_paths(qs),
            "answer_fields_presence": key_paths(ans),
            "free_text_pii_scan_questions": scan_bodies(qs),
            "free_text_pii_scan_answers": scan_bodies(ans),
        }
    full_ids = {q["question_id"] for q in load("full_load/questions.jsonl")}
    inc_ids = {q["question_id"] for q in load("incremental/questions.jsonl")}
    report["incremental_ids_already_in_full_sample"] = len(full_ids & inc_ids)
    out = os.path.join(S, "profile_report.json")
    json.dump(report, open(out, "w"), indent=2)
    for lt in ("full_load", "incremental"):
        r = report[lt]
        print(lt, {k: v for k, v in r.items() if not isinstance(v, dict)})
        print("  owner types:", r["owner_user_types"])
        print("  pii questions:", {k: v["posts_affected"] for k, v in r["free_text_pii_scan_questions"].items()})
        print("  pii answers:  ", {k: v["posts_affected"] for k, v in r["free_text_pii_scan_answers"].items()})
    print("question fields:", list(report["full_load"]["question_fields_presence"].items()))
    print("overlap:", report["incremental_ids_already_in_full_sample"])


if __name__ == "__main__":
    main()
