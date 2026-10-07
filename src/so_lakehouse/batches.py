"""Discover landing batches and choose which ones a run should process.

Landing layout (written by ingestion/*.py):

    <landing_root>/full/<batch_id>/{questions,answers}.jsonl + _manifest.json
    <landing_root>/incremental/<batch_id>/{questions,answers,deleted_questions,question_snapshots}.jsonl + _manifest.json

Nothing here assumes "today": every run is driven by parameters
(batch ids, a date range or an explicit folder path).
"""
import json
import os
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import List, Optional

LOAD_TYPES = ("full", "incremental")


@dataclass
class Batch:
    batch_id: str
    load_type: str
    path: str
    extracted_at: Optional[datetime]
    manifest: Optional[dict]
    error: Optional[str] = None

    @property
    def batch_date(self) -> Optional[date]:
        return self.extracted_at.date() if self.extracted_at else None


def _parse_ts(value):
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def read_batch(path, load_type=None):
    path = path.rstrip("/")
    batch_id = os.path.basename(path)
    load_type = load_type or os.path.basename(os.path.dirname(path))
    manifest_path = os.path.join(path, "_manifest.json")
    if not os.path.exists(manifest_path):
        return Batch(batch_id, load_type, path, None, None, error="missing _manifest.json")
    try:
        with open(manifest_path) as f:
            manifest = json.load(f)
        extracted = _parse_ts(manifest.get("extracted_at_utc"))
        if extracted is None:
            raise ValueError("manifest has no extracted_at_utc")
        return Batch(batch_id, manifest.get("load_type", load_type), path, extracted, manifest)
    except Exception as exc:  # corrupt manifest -> reported, not fatal for other batches
        return Batch(batch_id, load_type, path, None, None, error=f"unreadable manifest: {exc}")


def discover_batches(landing_root, load_type="all") -> List[Batch]:
    types = LOAD_TYPES if load_type in (None, "", "all") else (load_type,)
    found = []
    for lt in types:
        folder = os.path.join(landing_root, lt)
        if not os.path.isdir(folder):
            continue
        for name in sorted(os.listdir(folder)):
            full = os.path.join(folder, name)
            if os.path.isdir(full) and not name.startswith((".", "_")):
                found.append(read_batch(full, lt))
    # chronological order: full loads first, then by extraction time
    return sorted(found, key=lambda b: (b.load_type != "full", b.extracted_at or datetime.min.replace(tzinfo=timezone.utc), b.batch_id))


def _as_date(value):
    if value in (None, ""):
        return None
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def parse_list(value):
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).split(",") if v.strip()]


def select_batches(batches, batch_ids=None, start_date=None, end_date=None):
    """Filter by explicit ids and/or an inclusive extraction-date range."""
    ids = set(parse_list(batch_ids))
    start, end = _as_date(start_date), _as_date(end_date)
    chosen = []
    for b in batches:
        if ids and b.batch_id not in ids:
            continue
        if (start or end) and b.batch_date is None:
            continue
        if start and b.batch_date < start:
            continue
        if end and b.batch_date > end:
            continue
        chosen.append(b)
    missing = ids - {b.batch_id for b in batches}
    return chosen, sorted(missing)
