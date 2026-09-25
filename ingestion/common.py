"""Shared helpers: config loading, date handling, and Bronze landing writer."""
import json
import os
import uuid
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config():
    with open(os.path.join(ROOT, "config", "pipeline_config.json")) as f:
        return json.load(f)


def to_epoch(value):
    """'2025-04-01' or '2025-04-01T12:00:00' (UTC) -> unix seconds."""
    fmt = "%Y-%m-%dT%H:%M:%S" if "T" in value else "%Y-%m-%d"
    return int(datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp())


def utc_now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_windows(start, end):
    """Split [start, end) into calendar-month windows of (from_epoch, to_epoch, label)."""
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    cur = s
    while cur < e:
        nxt = datetime(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
        stop = min(nxt, e)
        yield (int(cur.replace(tzinfo=timezone.utc).timestamp()),
               int(stop.replace(tzinfo=timezone.utc).timestamp()),
               cur.strftime("%Y-%m"))
        cur = stop


def write_batch(out_dir, datasets, manifest):
    """Land one batch in Bronze format: raw API items as JSON Lines + a manifest.

    datasets: {"questions": [...], "answers": [...], ...}
    Every record is written exactly as the API returned it. Ingestion metadata
    lives in the manifest so the raw payload is untouched.
    """
    os.makedirs(out_dir, exist_ok=True)
    manifest = dict(manifest)
    manifest.setdefault("batch_id", str(uuid.uuid4()))
    manifest.setdefault("extracted_at_utc", utc_now_iso())
    manifest["files"] = {}
    for name, rows in datasets.items():
        path = os.path.join(out_dir, f"{name}.jsonl")
        with open(path, "w") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        manifest["files"][f"{name}.jsonl"] = {"records": len(rows), "bytes": os.path.getsize(path)}
    with open(os.path.join(out_dir, "_manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return manifest
