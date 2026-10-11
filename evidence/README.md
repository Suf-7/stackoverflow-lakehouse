# Phase 2 evidence: a full run on Databricks

These notebooks are the output of **one job run on Databricks Free Edition** (serverless compute, Unity Catalog, catalog `workspace`) on 11 October 2026. All 10 tasks finished with status SUCCESS. Each notebook is saved with its outputs exactly as the run produced them; GitHub displays the printed lines and tables directly.

Job run `814281740296490`, run name `so_lakehouse_phase2_evidence`. Each notebook's first cell repeats the run id, task, parameters, start time and result. Email addresses were removed from the outputs.

## Where each rubric item is proven

| Rubric item | Notebook and section |
|---|---|
| Explicit schemas, no `inferSchema` | [t09](t09_audit_and_validation.ipynb) section 1: every raw `StructType` printed as a schema tree, the code that reads files with it, and 0 occurrences of `inferSchema` |
| Data model: names, types, primary keys | [t09](t09_audit_and_validation.ipynb) section 2: `DESCRIBE TABLE` for all 5 Bronze and 7 Silver tables, and their 12 primary keys from Unity Catalog |
| Types cast in Silver | [t09](t09_audit_and_validation.ipynb) section 2: Silver dates are `timestamp`, Bronze dates are epoch `bigint` |
| `load_timestamp` on every row | [t09](t09_audit_and_validation.ipynb) section 3: 0 null `load_timestamp` values in all 12 tables |
| Bronze tracks the source | [t09](t09_audit_and_validation.ipynb) section 3: rows per batch, load type, extraction time and source file |
| MERGE INTO | [t09](t09_audit_and_validation.ipynb) section 4: the exact MERGE statement and the predicates and row counts Delta recorded |
| Re-run creates no duplicates | [t08](t08_backfill_bronze_to_silver.ipynb): every Silver MERGE inserted 0 and updated 0 rows. [t09](t09_audit_and_validation.ipynb) section 5: 0 duplicate keys |
| Backfill with parameters | [t07](t07_backfill_raw_to_bronze.ipynb) and [t08](t08_backfill_bronze_to_silver.ipynb): `mode=backfill`, `start_date=2026-09-25`, `end_date=2026-10-08`. [t09](t09_audit_and_validation.ipynb) section 6 |
| Schema change handled | [t05](t05_schema_drift_demo.ipynb): new column added with `mergeSchema`, 3 bad records quarantined, batch SUCCESS. [t09](t09_audit_and_validation.ipynb) section 7 |
| Log table | The last cell of every task shows its rows of `so_ops.pipeline_execution_logs`. [t09](t09_audit_and_validation.ipynb) section 8 shows the whole table |
| Incremental re-run | [t03](t03_rerun_raw_to_bronze.ipynb) and [t04](t04_rerun_bronze_to_silver.ipynb): nothing new landed, so every step is SKIPPED |

## The 10 tasks, in order

| Task | Notebook | Parameters |
|---|---|---|
| [t00](t00_setup_reset.ipynb) | `00_setup` | `reset=true`: drop and recreate all 15 tables |
| [t01](t01_raw_to_bronze_initial.ipynb) | `01_raw_to_bronze` | `mode=incremental` |
| [t02](t02_bronze_to_silver_initial.ipynb) | `02_bronze_to_silver` | `mode=incremental` |
| [t03](t03_rerun_raw_to_bronze.ipynb) | `01_raw_to_bronze` | `mode=incremental`, run again |
| [t04](t04_rerun_bronze_to_silver.ipynb) | `02_bronze_to_silver` | `mode=incremental`, run again |
| [t05](t05_schema_drift_demo.ipynb) | `04_schema_drift_demo` | `drift_mode=evolve` |
| [t06](t06_bronze_to_silver_after_drift.ipynb) | `02_bronze_to_silver` | `mode=incremental` |
| [t07](t07_backfill_raw_to_bronze.ipynb) | `01_raw_to_bronze` | `mode=backfill`, 2026-09-25 to 2026-10-08 |
| [t08](t08_backfill_bronze_to_silver.ipynb) | `02_bronze_to_silver` | `mode=backfill`, 2026-09-25 to 2026-10-08 |
| [t09](t09_audit_and_validation.ipynb) | `03_audit_and_validation` | none |

## Results from the log table

One row per pipeline run, in run order, summed from `so_ops.pipeline_execution_logs` (67 log rows in total):

| layer | run_mode | success | skipped | failed | rows_read | inserted | updated | deleted_or_replaced | quarantined |
|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | incremental | 16 | 0 | 0 | 24764 | 24764 | 0 | 0 | 0 |
| Bronze-to-Silver | incremental | 9 | 0 | 0 | 90736 | 74352 | 7285 | 0 | 0 |
| Raw-to-Bronze | incremental | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 |
| Bronze-to-Silver | incremental | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 |
| Raw-to-Bronze | backfill | 1 | 0 | 0 | 5 | 2 | 0 | 0 | 3 |
| Bronze-to-Silver | incremental | 9 | 0 | 0 | 14 | 12 | 0 | 0 | 0 |
| Raw-to-Bronze | backfill | 17 | 0 | 0 | 24769 | 24766 | 0 | 24766 | 3 |
| Bronze-to-Silver | backfill | 9 | 0 | 0 | 90750 | 0 | 0 | 0 | 0 |

The last row is the proof of idempotency: the backfill re-merged all 90,750 Bronze rows into Silver and changed nothing. The Bronze backfill before it replaced 24,766 rows of the same batches instead of adding duplicates.

## Silver after the whole run

| Silver table | Rows | Duplicate keys |
|---|---|---|
| silver.questions | 7620 | 0 |
| silver.answers | 9611 | 0 |
| silver.question_tags | 24185 | 0 |
| silver.users | 8332 | 0 |
| silver.question_snapshots | 7385 | 0 |
| silver.post_text_masked | 17231 | 0 |

## How these files were made

`scripts/export_databricks_run.py` exports every task of a Databricks job run and rewrites it as a Jupyter notebook with its outputs:

```bash
python scripts/export_databricks_run.py --run-id 814281740296490 --profile <cli-profile> --out evidence
```
