# Phase 2 demo run (local Spark 3.5 + Delta Lake 3.2)

Generated 2026-10-07 16:55 UTC by `scripts/demo_local_run.py` on the five batches in `data/demo_landing`. Every number below comes from the audit log of that run.


## 1. Initial Raw-to-Bronze run (mode = incremental)

Every file of every batch is one log row in `so_ops.pipeline_execution_logs`.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | questions | full_2025-04-01_2025-07-01 | SUCCESS | 2738 | 2738 | 0 | 0 | 0 | 5.4 |  |
| Raw-to-Bronze | answers | full_2025-04-01_2025-07-01 | SUCCESS | 3384 | 3384 | 0 | 0 | 0 | 3.2 |  |
| Raw-to-Bronze | questions | full_2025-01-01_2025-04-01 | SUCCESS | 4548 | 4548 | 0 | 0 | 0 | 3.9 |  |
| Raw-to-Bronze | answers | full_2025-01-01_2025-04-01 | SUCCESS | 5528 | 5528 | 0 | 0 | 0 | 3.1 |  |
| Raw-to-Bronze | questions | incr_20260925T155859Z | SUCCESS | 35 | 35 | 0 | 0 | 0 | 2.9 |  |
| Raw-to-Bronze | answers | incr_20260925T155859Z | SUCCESS | 59 | 59 | 0 | 0 | 0 | 2.5 |  |
| Raw-to-Bronze | deleted_questions | incr_20260925T155859Z | SUCCESS | 0 | 0 | 0 | 0 | 0 | 2.1 |  |
| Raw-to-Bronze | question_snapshots | incr_20260925T155859Z | SUCCESS | 100 | 100 | 0 | 0 | 0 | 2.5 |  |
| Raw-to-Bronze | questions | incr_20261007T155914Z | SUCCESS | 305 | 305 | 0 | 0 | 0 | 2.7 |  |
| Raw-to-Bronze | answers | incr_20261007T155914Z | SUCCESS | 646 | 646 | 0 | 0 | 0 | 3.4 |  |
| Raw-to-Bronze | deleted_questions | incr_20261007T155914Z | SUCCESS | 1 | 1 | 0 | 0 | 0 | 2.2 |  |
| Raw-to-Bronze | question_snapshots | incr_20261007T155914Z | SUCCESS | 7285 | 7285 | 0 | 0 | 0 | 3.9 |  |
| Raw-to-Bronze | questions | incr_20261007T160033Z | SUCCESS | 51 | 51 | 0 | 0 | 0 | 3.2 |  |
| Raw-to-Bronze | answers | incr_20261007T160033Z | SUCCESS | 84 | 84 | 0 | 0 | 0 | 3.1 |  |
| Raw-to-Bronze | deleted_questions | incr_20261007T160033Z | SUCCESS | 0 | 0 | 0 | 0 | 0 | 2.3 |  |
| Raw-to-Bronze | question_snapshots | incr_20261007T160033Z | SUCCESS | 0 | 0 | 0 | 0 | 0 | 2.0 |  |

## 2. Initial Bronze-to-Silver run (mode = incremental)

Each Silver table is one MERGE INTO step, logged with inserted and updated row counts.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Bronze-to-Silver | questions |  | SUCCESS | 7677 | 7618 | 0 | 0 | 0 | 4.2 | 7618 unique questions after de-duplication |
| Bronze-to-Silver | question_tags |  | SUCCESS | 24177 | 24177 | 0 | 0 | 0 | 3.5 |  |
| Bronze-to-Silver | answers |  | SUCCESS | 9701 | 9611 | 0 | 0 | 0 | 3.1 | 9611 unique answers after de-duplication |
| Bronze-to-Silver | users |  | SUCCESS | 17280 | 8332 | 0 | 0 | 0 | 4.0 |  |
| Bronze-to-Silver | question_snapshots |  | SUCCESS | 7385 | 7385 | 0 | 0 | 0 | 1.8 |  |
| Bronze-to-Silver | question_metrics (from snapshots) |  | SUCCESS | 7285 | 0 | 7282 | 0 | 0 | 1.9 |  |
| Bronze-to-Silver | deleted_questions -> questions |  | SUCCESS | 1 | 0 | 1 | 0 | 0 | 2.5 | soft delete (is_deleted = true) |
| Bronze-to-Silver | deleted_questions -> answers |  | SUCCESS | 1 | 0 | 2 | 0 | 0 | 1.6 | soft delete (is_deleted = true) |
| Bronze-to-Silver | post_text_masked |  | SUCCESS | 17229 | 17229 | 0 | 0 | 0 | 8.7 |  |

## 3. Row counts after the initial load

| Table | Rows |
|---|---|
| bronze.questions | 7677 |
| bronze.answers | 9701 |
| bronze.deleted_questions | 1 |
| bronze.question_snapshots | 7385 |
| bronze.quarantine_records | 0 |
| silver.questions | 7618 |
| silver.question_tags | 24177 |
| silver.answers | 9611 |
| silver.users | 8332 |
| silver.question_snapshots | 7385 |
| silver.post_text_masked | 17229 |

## 4. Re-running the incremental pipeline

Nothing new has landed, so both layers log SKIPPED and write nothing.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | * | full_2025-04-01_2025-07-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | full_2025-01-01_2025-04-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20260925T155859Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T155914Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T160033Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Bronze-to-Silver | * |  | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | no matching Bronze batches to process |

## 5. Idempotency proof: force-reprocess ALL batches

Every batch is reloaded into Bronze (replaceWhere replaces the batch's own rows) and merged into Silver again. Bronze row counts stay the same and every Silver MERGE inserts and updates 0 rows. A checksum over every column of every Silver table, including load_timestamp, is unchanged.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Bronze-to-Silver | questions |  | SUCCESS | 7677 | 0 | 0 | 0 | 0 | 7.8 | 7618 unique questions after de-duplication |
| Bronze-to-Silver | question_tags |  | SUCCESS | 24177 | 0 | 0 | 0 | 0 | 5.9 |  |
| Bronze-to-Silver | answers |  | SUCCESS | 9701 | 0 | 0 | 0 | 0 | 5.1 | 9611 unique answers after de-duplication |
| Bronze-to-Silver | users |  | SUCCESS | 17280 | 0 | 0 | 0 | 0 | 7.4 |  |
| Bronze-to-Silver | question_snapshots |  | SUCCESS | 7385 | 0 | 0 | 0 | 0 | 2.7 |  |
| Bronze-to-Silver | question_metrics (from snapshots) |  | SUCCESS | 7285 | 0 | 0 | 0 | 0 | 2.5 |  |
| Bronze-to-Silver | deleted_questions -> questions |  | SUCCESS | 1 | 0 | 0 | 0 | 0 | 1.9 | soft delete (is_deleted = true) |
| Bronze-to-Silver | deleted_questions -> answers |  | SUCCESS | 1 | 0 | 0 | 0 | 0 | 1.9 | soft delete (is_deleted = true) |
| Bronze-to-Silver | post_text_masked |  | SUCCESS | 17229 | 0 | 0 | 0 | 0 | 13.6 |  |

| Table | Rows before | Rows after | Checksum identical |
|---|---|---|---|
| silver.questions | 7618 | 7618 | True |
| silver.question_tags | 24177 | 24177 | True |
| silver.answers | 9611 | 9611 | True |
| silver.users | 8332 | 8332 | True |
| silver.question_snapshots | 7385 | 7385 | True |
| silver.post_text_masked | 17229 | 17229 | True |

| Bronze table | Rows before | Rows after |
|---|---|---|
| bronze.questions | 7677 | 7677 |
| bronze.answers | 9701 | 9701 |
| bronze.deleted_questions | 1 | 1 |
| bronze.question_snapshots | 7385 | 7385 |
| bronze.quarantine_records | 0 | 0 |

## 6. Date-range backfill: start_date = end_date = 2026-09-25

Only the batches extracted on that date are selected: the two Phase 1 samples. Replaying these OLD batches after newer ones updates nothing, because Silver only accepts newer versions.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | questions | full_2025-04-01_2025-07-01 | SUCCESS | 2738 | 2738 | 0 | 2738 | 0 | 4.1 | replaced 2738 rows from a previous load of this batch |
| Raw-to-Bronze | answers | full_2025-04-01_2025-07-01 | SUCCESS | 3384 | 3384 | 0 | 3384 | 0 | 5.5 | replaced 3384 rows from a previous load of this batch |
| Raw-to-Bronze | questions | incr_20260925T155859Z | SUCCESS | 35 | 35 | 0 | 35 | 0 | 4.6 | replaced 35 rows from a previous load of this batch |
| Raw-to-Bronze | answers | incr_20260925T155859Z | SUCCESS | 59 | 59 | 0 | 59 | 0 | 5.3 | replaced 59 rows from a previous load of this batch |
| Raw-to-Bronze | deleted_questions | incr_20260925T155859Z | SUCCESS | 0 | 0 | 0 | 0 | 0 | 2.8 |  |
| Raw-to-Bronze | question_snapshots | incr_20260925T155859Z | SUCCESS | 100 | 100 | 0 | 100 | 0 | 3.9 | replaced 100 rows from a previous load of this batch |

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Bronze-to-Silver | questions |  | SUCCESS | 2773 | 0 | 0 | 0 | 0 | 5.2 | 2773 unique questions after de-duplication |
| Bronze-to-Silver | question_tags |  | SUCCESS | 8670 | 0 | 0 | 0 | 0 | 6.2 |  |
| Bronze-to-Silver | answers |  | SUCCESS | 3443 | 0 | 0 | 0 | 0 | 4.9 | 3443 unique answers after de-duplication |
| Bronze-to-Silver | users |  | SUCCESS | 6175 | 0 | 0 | 0 | 0 | 6.8 |  |
| Bronze-to-Silver | question_snapshots |  | SUCCESS | 100 | 0 | 0 | 0 | 0 | 2.4 |  |
| Bronze-to-Silver | question_metrics (from snapshots) |  | SUCCESS | 100 | 0 | 0 | 0 | 0 | 1.3 |  |
| Bronze-to-Silver | deleted_questions -> questions |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 1.7 | soft delete (is_deleted = true) |
| Bronze-to-Silver | deleted_questions -> answers |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 1.8 | soft delete (is_deleted = true) |
| Bronze-to-Silver | post_text_masked |  | SUCCESS | 6216 | 0 | 0 | 0 | 0 | 4.8 |  |

## 7. Schema drift batch

A synthetic batch with a new column, a new nested field, a type change, a truncated line and a record without its key. The run succeeds: 2 rows load, 3 go to quarantine, and the new column is added to Bronze through mergeSchema.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | * | full_2025-04-01_2025-07-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | full_2025-01-01_2025-04-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20260925T155859Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T155914Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T160033Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | questions | incr_20261008T000000Z | SUCCESS | 5 | 2 | 0 | 0 | 3 | 6.0 | schema evolved: added ai_assisted; rescued unknown fields: owner.badge_counts; quarantined |

| Drift type | Column | Records | Action | Example |
|---|---|---|---|---|
| malformed_json |  | 1 | quarantined |  |
| missing_primary_key | question_id | 1 | quarantined |  |
| new_column | owner.badge_counts | 1 | rescued | {"gold": 1, "silver": 4} |
| new_column | ai_assisted | 1 | evolved | true |
| type_mismatch | score | 1 | quarantined | expected int, got str: "five" |

| Quarantine reason | Error detail |
|---|---|
| malformed_json | line is not a JSON object |
| missing_primary_key | primary key (question_id) is null |
| type_mismatch | [{"path":"score","expected":"int","actual":"str: \"five\""}] |

The Silver run that follows processes only the new batch:

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Bronze-to-Silver | questions |  | SUCCESS | 2 | 2 | 0 | 0 | 0 | 3.5 | 2 unique questions after de-duplication |
| Bronze-to-Silver | question_tags |  | SUCCESS | 7 | 7 | 0 | 0 | 0 | 6.5 |  |
| Bronze-to-Silver | answers |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 3.4 | 0 unique answers after de-duplication |
| Bronze-to-Silver | users |  | SUCCESS | 2 | 0 | 0 | 0 | 0 | 7.3 |  |
| Bronze-to-Silver | question_snapshots |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 1.2 |  |
| Bronze-to-Silver | question_metrics (from snapshots) |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 1.9 |  |
| Bronze-to-Silver | deleted_questions -> questions |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 0.9 | soft delete (is_deleted = true) |
| Bronze-to-Silver | deleted_questions -> answers |  | SUCCESS | 0 | 0 | 0 | 0 | 0 | 0.9 | soft delete (is_deleted = true) |
| Bronze-to-Silver | post_text_masked |  | SUCCESS | 2 | 2 | 0 | 0 | 0 | 4.4 |  |

## 8. A broken batch (no _manifest.json)

The failure is logged with its error message and the run continues with other batches. With fail_on_error = true, the default, the notebook then raises so the job shows as failed.

| Layer | Entity | Batch | Status | Read | Inserted | Updated | Deleted/replaced | Quarantined | Seconds | Note |
|---|---|---|---|---|---|---|---|---|---|---|
| Raw-to-Bronze | * | full_2025-04-01_2025-07-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | full_2025-01-01_2025-04-01 | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261009T000000Z | FAILED | 0 | 0 | 0 | 0 | 0 | 0.0 | ValueError: missing _manifest.json |
| Raw-to-Bronze | * | incr_20260925T155859Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T155914Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261007T160033Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |
| Raw-to-Bronze | * | incr_20261008T000000Z | SKIPPED | 0 | 0 | 0 | 0 | 0 | 0.0 | already loaded; use mode=backfill to reload |

## 9. Audit log totals for this demo

| Layer | Status | Log rows |
|---|---|---|
| Bronze-to-Silver | SKIPPED | 1 |
| Bronze-to-Silver | SUCCESS | 36 |
| Raw-to-Bronze | FAILED | 1 |
| Raw-to-Bronze | SKIPPED | 16 |
| Raw-to-Bronze | SUCCESS | 39 |
