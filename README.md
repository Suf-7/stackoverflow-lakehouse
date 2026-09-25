# Stack Overflow Developer Q&A Lakehouse

An end-to-end Medallion (Bronze, Silver, Gold) pipeline on Databricks that tracks Stack Overflow questions and answers for 18 data-and-analytics technologies from 2018 onward, and a dashboard showing how developer Q&A has changed in the AI era.

- **Phase 1 proposal:** [docs/phase1_proposal.md](docs/phase1_proposal.md)
- **Source:** [Stack Exchange API v2.3](https://api.stackexchange.com/docs). Content is licensed CC BY-SA 4.0 and attributed to Stack Overflow.

## Repository layout

| Path | Purpose |
|---|---|
| `config/pipeline_config.json` | Tracked tags, history start date, API settings |
| `ingestion/` | Standard-library Python ingestion: API client, full load, incremental load, volume estimates |
| `scripts/profile_samples.py` | Schema, PII and size profiling of the samples |
| `data/samples/full_load/` | Full-load sample: questions created Apr to Jun 2025, with answers |
| `data/samples/incremental/` | Incremental sample: 24 hours of changes, deletion check, metric snapshots |
| `data/samples/*.json` | Volume estimates and profiling report |
| `notebooks/` | Databricks notebooks for Bronze, Silver and Gold (Phases 2 and 3) |
| `dashboard/` | Dashboard exports (Phase 3) |

## Running the ingestion

Requires Python 3.9 or newer. No third-party packages.

```bash
# Optional but recommended: free key from https://stackapps.com/apps/oauth/register
# Raises the quota from 300 to 10,000 requests per day.
export SE_API_KEY=your_key

# Volume estimate (count-only requests)
python3 ingestion/estimate_volume.py

# Full load: sample window, or the real 2018+ baseline
python3 ingestion/full_load.py --from 2025-04-01 --to 2025-07-01 --out data/samples/full_load
python3 ingestion/full_load.py --from 2018-01-01 --to 2026-09-01 --out data/landing/full_load

# Incremental: explicit start, or resume from the stored watermark
python3 ingestion/incremental_load.py --since 2026-09-24T00:00:00 --out data/landing/incremental/2026-09-25
python3 ingestion/incremental_load.py --out data/landing/incremental/$(date +%F)

# Incremental plus deletion check and metric snapshots for known ids
python3 ingestion/incremental_load.py --reconcile-from data/landing/full_load/questions.jsonl --out data/landing/incremental/$(date +%F)

# Profile the samples
python3 scripts/profile_samples.py
```

Every batch is written as JSON Lines, with one raw API record per line, plus a `_manifest.json` holding the batch id, extraction time, window, record counts and API usage.

Full-size data goes to `data/landing/` and `data/raw/`. Both are excluded by `.gitignore` and never committed.
