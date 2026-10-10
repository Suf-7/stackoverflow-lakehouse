# Phase 2 run on Databricks Free Edition (10 October 2026)

The pipeline was run on the team's Databricks Free Edition workspace with **serverless compute** and **Unity Catalog** (catalog `workspace`). One job ran nine notebook tasks in sequence, and all nine finished with status **SUCCESS**. Every table below was queried afterwards from the Unity Catalog tables through the serverless SQL warehouse.

The rendered notebook output of each task is saved next to this file as HTML. Download a file and open it in a browser to see the cells and their output.

| Task | Notebook and parameters | What it shows | Output |
|---|---|---|---|
| 1 | `01_raw_to_bronze`, mode = incremental | First load of all 5 landing batches, 16 files | [t1](t1_raw_to_bronze_initial.html) |
| 2 | `02_bronze_to_silver`, mode = incremental | First Silver load with MERGE INTO | [t2](t2_bronze_to_silver_initial.html) |
| 3 | `01_raw_to_bronze`, mode = incremental | Re-run: every batch SKIPPED | [t3](t3_rerun_raw_to_bronze.html) |
| 4 | `02_bronze_to_silver`, mode = incremental | Re-run: nothing to process | [t4](t4_rerun_bronze_to_silver.html) |
| 5 | `04_schema_drift_demo`, drift_mode = evolve | Drift batch: new column, type change, bad line, missing key | [t5](t5_schema_drift_demo.html) |
| 6 | `02_bronze_to_silver`, mode = incremental | Silver picks up only the new drift batch | [t6](t6_bronze_to_silver_after_drift.html) |
| 7 | `01_raw_to_bronze`, mode = backfill, 2026-09-25 to 2026-10-08 | Reload of all 6 batches | [t7](t7_backfill_raw_to_bronze_all.html) |
| 8 | `02_bronze_to_silver`, mode = backfill, same range | **Idempotency:** re-merge of all batches changes 0 rows | [t8](t8_backfill_bronze_to_silver_all.html) |
| 9 | `03_audit_and_validation` | Logs, registry, drift, duplicate checks, MERGE history | [t9](t9_audit_and_validation.html) |

## 1. Every run in `so_ops.pipeline_execution_logs`

One row per run below; the log itself has one row per file or table processed.

| run_id | layer | run_mode | steps | success | skipped | failed | read | inserted | updated | deleted_or_replaced | quarantined | started | secs |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| raw_to_bronze-20261010T163230Z-77eaff | Raw-to-Bronze | incremental | 16 | 16 | 0 | 0 | 24764 | 24764 | 0 | 0 | 0 | 16:32:38 | 171.5 |
| bronze_to_silver-20261010T163647Z-5ee9e0 | Bronze-to-Silver | incremental | 9 | 9 | 0 | 0 | 90736 | 74352 | 7285 | 0 | 0 | 16:36:49 | 110.1 |
| raw_to_bronze-20261010T163916Z-01d958 | Raw-to-Bronze | incremental | 5 | 0 | 5 | 0 | 0 | 0 | 0 | 0 | 0 | 16:39:18 | 0.0 |
| bronze_to_silver-20261010T163939Z-a1d2d7 | Bronze-to-Silver | incremental | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 16:39:40 | 0.0 |
| raw_to_bronze-20261010T163956Z-d4e00b | Raw-to-Bronze | backfill | 1 | 1 | 0 | 0 | 5 | 2 | 0 | 0 | 3 | 16:39:58 | 24.1 |
| bronze_to_silver-20261010T164044Z-d4fdd8 | Bronze-to-Silver | incremental | 9 | 9 | 0 | 0 | 14 | 12 | 0 | 0 | 0 | 16:40:46 | 75.5 |
| raw_to_bronze-20261010T164235Z-0bc342 | Raw-to-Bronze | backfill | 17 | 17 | 0 | 0 | 24769 | 24766 | 0 | 24766 | 3 | 16:42:37 | 154.9 |
| bronze_to_silver-20261010T164645Z-f95843 | Bronze-to-Silver | backfill | 9 | 9 | 0 | 0 | 90750 | 0 | 0 | 0 | 0 | 16:46:46 | 96.7 |

## 2. Idempotency: the Silver backfill changed nothing

Initial Silver load (task 2):

| entity | status | rows_read | rows_inserted | rows_updated | rows_deleted | rows_quarantined | secs |
|---|---|---|---|---|---|---|---|
| questions | SUCCESS | 7677 | 7618 | 0 | 0 | 0 | 29.2 |
| question_tags | SUCCESS | 24177 | 24177 | 0 | 0 | 0 | 10.5 |
| answers | SUCCESS | 9701 | 9611 | 0 | 0 | 0 | 9.9 |
| users | SUCCESS | 17280 | 8332 | 0 | 0 | 0 | 7.6 |
| question_snapshots | SUCCESS | 7385 | 7385 | 0 | 0 | 0 | 5.3 |
| question_metrics (from snapshots) | SUCCESS | 7285 | 0 | 7282 | 0 | 0 | 10.5 |
| deleted_questions -> questions | SUCCESS | 1 | 0 | 1 | 0 | 0 | 9.0 |
| deleted_questions -> answers | SUCCESS | 1 | 0 | 2 | 0 | 0 | 7.8 |
| post_text_masked | SUCCESS | 17229 | 17229 | 0 | 0 | 0 | 20.2 |

The same Bronze data merged again (task 8). It read 90,750 rows and inserted and updated **0**:

| entity | status | rows_read | rows_inserted | rows_updated | rows_deleted | rows_quarantined | secs |
|---|---|---|---|---|---|---|---|
| questions | SUCCESS | 7679 | 0 | 0 | 0 | 0 | 27.0 |
| question_tags | SUCCESS | 24185 | 0 | 0 | 0 | 0 | 10.0 |
| answers | SUCCESS | 9701 | 0 | 0 | 0 | 0 | 8.7 |
| users | SUCCESS | 17282 | 0 | 0 | 0 | 0 | 7.9 |
| question_snapshots | SUCCESS | 7385 | 0 | 0 | 0 | 0 | 5.4 |
| question_metrics (from snapshots) | SUCCESS | 7285 | 0 | 0 | 0 | 0 | 6.7 |
| deleted_questions -> questions | SUCCESS | 1 | 0 | 0 | 0 | 0 | 5.4 |
| deleted_questions -> answers | SUCCESS | 1 | 0 | 0 | 0 | 0 | 5.5 |
| post_text_masked | SUCCESS | 17231 | 0 | 0 | 0 | 0 | 20.1 |

The Delta transaction log of `silver.questions` confirms it. Versions 8 to 10 are the backfill MERGEs:

| version | operation | inserted | updated | source_rows |
|---|---|---|---|---|
| 1 | MERGE | 7618 | 0 | 7618 |
| 2 | MERGE | 0 | 7282 | 7285 |
| 4 | MERGE | 0 | 1 | 1 |
| 5 | MERGE | 2 | 0 | 2 |
| 6 | MERGE | 0 | 0 | 0 |
| 7 | MERGE | 0 | 0 | 0 |
| 8 | MERGE | 0 | 0 | 7620 |
| 9 | MERGE | 0 | 0 | 7285 |
| 10 | MERGE | 0 | 0 | 1 |

## 3. No duplicate primary keys in any Silver table

| t | dups |
|---|---|
| silver.questions | 0 |
| silver.answers | 0 |
| silver.question_tags | 0 |
| silver.users | 0 |
| silver.question_snapshots | 0 |
| silver.post_text_masked | 0 |

## 4. Schema drift handled without failing the batch

| _batch_id | entity | drift_type | column_path | record_count | action_taken | example_value |
|---|---|---|---|---|---|---|
| incr_20261008T000000Z | questions | malformed_json |  | 1 | quarantined |  |
| incr_20261008T000000Z | questions | missing_primary_key | question_id | 1 | quarantined |  |
| incr_20261008T000000Z | questions | new_column | owner.badge_counts | 1 | rescued | {"gold": 1, "silver": 4} |
| incr_20261008T000000Z | questions | new_column | ai_assisted | 1 | evolved | true |
| incr_20261008T000000Z | questions | type_mismatch | score | 1 | quarantined | expected int, got str: "five" |

The new top-level column `ai_assisted` was added to `so_bronze.questions` through `mergeSchema`. The new nested field went to `_rescued_data`:

| question_id | score | ai_assisted | _rescued_data |
|---|---|---|---|
| 990000001 | 4 | true | {"owner.badge_counts":"{\"gold\": 1, \"silver\": 4}"} |
| 990000004 | 1 |  |  |

## 5. Raw-to-Bronze, one log row per file (task 1)

| batch_id | entity | status | rows_read | rows_inserted | rows_quarantined | secs |
|---|---|---|---|---|---|---|
| full_2025-04-01_2025-07-01 | questions | SUCCESS | 2738 | 2738 | 0 | 39.0 |
| full_2025-04-01_2025-07-01 | answers | SUCCESS | 3384 | 3384 | 0 | 12.7 |
| full_2025-01-01_2025-04-01 | questions | SUCCESS | 4548 | 4548 | 0 | 12.4 |
| full_2025-01-01_2025-04-01 | answers | SUCCESS | 5528 | 5528 | 0 | 11.6 |
| incr_20260925T155859Z | questions | SUCCESS | 35 | 35 | 0 | 9.0 |
| incr_20260925T155859Z | answers | SUCCESS | 59 | 59 | 0 | 8.7 |
| incr_20260925T155859Z | deleted_questions | SUCCESS | 0 | 0 | 0 | 7.1 |
| incr_20260925T155859Z | question_snapshots | SUCCESS | 100 | 100 | 0 | 8.9 |
| incr_20261007T155914Z | questions | SUCCESS | 305 | 305 | 0 | 8.6 |
| incr_20261007T155914Z | answers | SUCCESS | 646 | 646 | 0 | 8.7 |
| incr_20261007T155914Z | deleted_questions | SUCCESS | 1 | 1 | 0 | 8.5 |
| incr_20261007T155914Z | question_snapshots | SUCCESS | 7285 | 7285 | 0 | 9.1 |
| incr_20261007T160033Z | questions | SUCCESS | 51 | 51 | 0 | 7.7 |
| incr_20261007T160033Z | answers | SUCCESS | 84 | 84 | 0 | 7.6 |
| incr_20261007T160033Z | deleted_questions | SUCCESS | 0 | 0 | 0 | 6.5 |
| incr_20261007T160033Z | question_snapshots | SUCCESS | 0 | 0 | 0 | 5.4 |

## 6. Final row counts

| t | n |
|---|---|
| bronze.questions | 7679 |
| bronze.answers | 9701 |
| bronze.deleted_questions | 1 |
| bronze.question_snapshots | 7385 |
| bronze.quarantine_records | 3 |
| silver.questions | 7620 |
| silver.question_tags | 24185 |
| silver.answers | 9611 |
| silver.users | 8332 |
| silver.question_snapshots | 7385 |
| silver.post_text_masked | 17231 |
| silver.dq_quarantine | 0 |
| ops.pipeline_execution_logs | 67 |

`bronze.questions` has 2 more rows than in the first load: the two valid records of the drift batch. The Bronze backfill in task 7 replaced 24,766 rows of the same batches instead of adding duplicates.
