"""End-to-end tests for the Phase 2 requirements. Run: pytest -q tests

Tests run in file order and share one lakehouse (session fixtures), like a
real sequence of pipeline runs.
"""
import json
import os
import re

import pytest
from pyspark.sql import functions as F

from so_lakehouse.bronze import run_raw_to_bronze
from so_lakehouse.ddl import setup
from so_lakehouse.demo import DRIFT_BATCH_ID, make_drift_batch
from so_lakehouse.schemas import RAW_CONTRACTS, TABLES
from so_lakehouse.silver import run_bronze_to_silver

FULL_IDS = ["full_2025-01-01_2025-04-01", "full_2025-04-01_2025-07-01"]
INCR_IDS = ["incr_20260925T155859Z", "incr_20261007T155914Z", "incr_20261007T160033Z"]
SILVER_KEYS = {"silver.questions": ["question_id"], "silver.answers": ["answer_id"],
               "silver.question_tags": ["question_id", "tag"], "silver.users": ["user_key"],
               "silver.question_snapshots": ["question_id", "snapshot_at"],
               "silver.post_text_masked": ["post_type", "post_id"]}


def quiet(*_a, **_k):
    pass


def logs(spark, cfg, run_id):
    return spark.table(cfg.table("ops.pipeline_execution_logs")).where(F.col("run_id") == run_id)


def table_fingerprint(spark, cfg, logical):
    """Order-independent checksum of a whole table (every column, including load_timestamp)."""
    df = spark.table(cfg.table(logical))
    return df.select(F.sum(F.conv(F.substring(F.sha2(F.to_json(F.struct(*df.columns)), 256), 1, 15), 16, 10)
                           .cast("decimal(38,0)")).alias("h"), F.count("*").alias("n")).collect()[0]


def raw_lines(landing, batch_id, entity):
    lt = "full" if batch_id.startswith("full") else "incremental"
    path = os.path.join(landing, lt, batch_id, RAW_CONTRACTS[entity][0])
    if not os.path.exists(path):
        return 0
    with open(path) as f:
        return sum(1 for line in f if line.strip())


# ---------------------------------------------------------------------------------------------- setup
def test_setup_creates_every_table_with_explicit_schema(spark, cfg):
    setup(spark, cfg, reset=True, log=quiet)
    for logical, (_layer, _name, schema, pk, _d) in TABLES.items():
        table = spark.table(cfg.table(logical))
        assert [f.name for f in table.schema.fields] == [f.name for f in schema.fields], logical
        assert "load_timestamp" in table.columns, f"{logical} lacks load_timestamp"
        for k in pk:  # primary key columns are NOT NULL
            assert not table.schema[k].nullable, f"{logical}.{k} should be NOT NULL"


def test_no_schema_inference_in_source():
    src = os.path.join(os.path.dirname(os.path.dirname(__file__)), "src", "so_lakehouse")
    code = "".join(open(os.path.join(src, f)).read() for f in os.listdir(src) if f.endswith(".py"))
    assert "inferSchema" not in code and "samplingRatio" not in code


# ---------------------------------------------------------------------------------- full + incremental
def test_raw_to_bronze_full_and_incremental(spark, cfg, landing):
    audit = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    assert not audit.failures
    for bid in FULL_IDS + INCR_IDS:
        for entity in RAW_CONTRACTS:
            expected = raw_lines(landing, bid, entity)
            got = spark.table(cfg.table(f"bronze.{entity}")).where(F.col("_batch_id") == bid).count()
            quarantined = (spark.table(cfg.table("bronze.quarantine_records"))
                           .where((F.col("_batch_id") == bid) & (F.col("entity") == entity)).count())
            assert got + quarantined == expected, (bid, entity, got, quarantined, expected)
    # one log row per processed file, each with every audit metric populated
    entries = logs(spark, cfg, audit.run_id).collect()
    files = sum(1 for bid in FULL_IDS + INCR_IDS for e in RAW_CONTRACTS
                if os.path.exists(os.path.join(landing, "full" if bid.startswith("full") else "incremental", bid,
                                               RAW_CONTRACTS[e][0])))
    assert len(entries) == files
    for e in entries:
        assert e.layer == "Raw-to-Bronze" and e.status == "SUCCESS"
        assert e.start_time <= e.end_time and e.source_parameter and e.rows_inserted is not None


def test_bronze_to_silver_initial(spark, cfg):
    audit = run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet)
    assert not audit.failures
    q = spark.table(cfg.table("silver.questions"))
    distinct_ids = spark.table(cfg.table("bronze.questions")).select("question_id").distinct().count()
    dq_failed = (spark.table(cfg.table("silver.dq_quarantine")).where("entity = 'questions'")
                 .select("record_key").distinct().count())
    assert q.count() + dq_failed == distinct_ids
    assert q.where(F.col("load_timestamp").isNull()).count() == 0
    # types were cast: epoch bigint -> timestamp
    assert dict(q.dtypes)["created_at"] == "timestamp"
    # registry marks every batch as processed
    reg = spark.table(cfg.table("ops.batch_registry")).collect()
    assert {r.batch_id for r in reg} == set(FULL_IDS + INCR_IDS)
    assert all(r.silver_status == "SUCCESS" for r in reg)


def test_silver_primary_keys_are_unique(spark, cfg):
    for logical, keys in SILVER_KEYS.items():
        df = spark.table(cfg.table(logical))
        assert df.count() == df.select(*keys).distinct().count(), f"duplicate keys in {logical}"


def test_pii_is_removed_from_silver(spark, cfg):
    for logical in SILVER_KEYS:
        cols = spark.table(cfg.table(logical)).columns
        assert not {"display_name", "profile_image", "account_id", "user_id", "body"} & set(cols), logical
    keys = [r.owner_user_key for r in spark.table(cfg.table("silver.questions"))
            .where(F.col("owner_user_key").isNotNull()).limit(50).collect()]
    assert keys and all(re.fullmatch(r"[0-9a-f]{64}", k) for k in keys)
    masked = spark.table(cfg.table("silver.post_text_masked"))
    assert masked.where(F.col("body_masked").rlike(r"[A-Za-z0-9._%+-]+@gmail\.com")).count() == 0


def test_deletion_and_snapshots_applied(spark, cfg):
    deleted = spark.table(cfg.table("silver.questions")).where("is_deleted").count()
    tombstones = spark.table(cfg.table("bronze.deleted_questions")).select("question_id").distinct()
    in_silver = tombstones.join(spark.table(cfg.table("silver.questions")), "question_id").count()
    assert deleted == in_silver >= 1
    snap_applied = spark.table(cfg.table("silver.questions")).where("metrics_as_of > source_extracted_at").count()
    assert snap_applied > 0


# ------------------------------------------------------------------------------------------- idempotency
def test_rerun_incremental_is_a_no_op(spark, cfg):
    before = {t: table_fingerprint(spark, cfg, t) for t in SILVER_KEYS}
    a1 = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    assert {r.status for r in a1.records} == {"SKIPPED"}
    a2 = run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet)
    assert {r.status for r in a2.records} == {"SKIPPED"}
    assert {t: table_fingerprint(spark, cfg, t) for t in SILVER_KEYS} == before


def test_full_reprocess_of_same_raw_data_changes_nothing_in_silver(spark, cfg):
    """Force-reload EVERY batch into Bronze and re-MERGE into Silver: Silver must be bit-identical."""
    bronze_before = {e: spark.table(cfg.table(f"bronze.{e}")).count() for e in RAW_CONTRACTS}
    silver_before = {t: table_fingerprint(spark, cfg, t) for t in SILVER_KEYS}

    a1 = run_raw_to_bronze(spark, cfg, mode="backfill", batch_ids=",".join(FULL_IDS + INCR_IDS), echo=quiet)
    assert not a1.failures
    assert {e: spark.table(cfg.table(f"bronze.{e}")).count() for e in RAW_CONTRACTS} == bronze_before
    a2 = run_bronze_to_silver(spark, cfg, mode="backfill", batch_ids=",".join(FULL_IDS + INCR_IDS), echo=quiet)
    assert not a2.failures
    assert sum(r.rows_inserted + r.rows_updated + r.rows_deleted for r in a2.records) == 0
    assert {t: table_fingerprint(spark, cfg, t) for t in SILVER_KEYS} == silver_before


def test_backfill_by_date_range_selects_only_those_batches(spark, cfg):
    # both Phase 1 batches (full sample + incremental sample) were extracted on 2026-09-25
    audit = run_raw_to_bronze(spark, cfg, mode="backfill", start_date="2026-09-25", end_date="2026-09-25",
                              echo=quiet)
    assert {r.batch_id for r in audit.records} == {"full_2025-04-01_2025-07-01", "incr_20260925T155859Z"}
    only_incr = run_raw_to_bronze(spark, cfg, mode="backfill", load_type="incremental", start_date="2026-09-25",
                                  end_date="2026-09-25", echo=quiet)
    assert {r.batch_id for r in only_incr.records} == {"incr_20260925T155859Z"}
    audit2 = run_bronze_to_silver(spark, cfg, mode="backfill", start_date="2026-09-25", end_date="2026-09-25",
                                  echo=quiet)
    assert all("incr_20260925T155859Z" in r.source_parameter and "full_2025-04-01_2025-07-01" in r.source_parameter
               and "incr_20261007" not in r.source_parameter for r in audit2.records)
    # an old batch replayed after newer ones must not overwrite newer data
    assert sum(r.rows_updated for r in audit2.records if r.entity == "questions") == 0


def test_backfill_requires_a_scope(spark, cfg):
    with pytest.raises(ValueError):
        run_raw_to_bronze(spark, cfg, mode="backfill", echo=quiet)
    with pytest.raises(ValueError):
        run_bronze_to_silver(spark, cfg, mode="backfill", echo=quiet)


# ------------------------------------------------------------------------------------------ schema drift
DRIFT_BATCH = DRIFT_BATCH_ID


def test_schema_drift_is_handled_without_crashing(spark, cfg, landing):
    make_drift_batch(landing, os.path.join(landing, "incremental", "incr_20261007T160033Z", "questions.jsonl"))
    audit = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    rec = [r for r in audit.records if r.batch_id == DRIFT_BATCH][0]
    assert rec.status == "SUCCESS"
    assert (rec.rows_read, rec.rows_inserted, rec.rows_quarantined) == (5, 2, 3)

    bronze_q = spark.table(cfg.table("bronze.questions"))
    assert "ai_assisted" in bronze_q.columns                         # evolved via mergeSchema
    row = bronze_q.where("question_id = 990000001").collect()[0]
    assert row.ai_assisted == "true" and "owner.badge_counts" in row._rescued_data

    reasons = {r.reason for r in spark.table(cfg.table("bronze.quarantine_records"))
               .where(F.col("_batch_id") == DRIFT_BATCH).collect()}
    assert reasons == {"type_mismatch", "malformed_json", "missing_primary_key"}
    events = {(r.drift_type, r.column_path, r.action_taken) for r in
              spark.table(cfg.table("ops.schema_drift_events")).where(F.col("_batch_id") == DRIFT_BATCH).collect()}
    assert ("new_column", "ai_assisted", "evolved") in events
    assert ("new_column", "owner.badge_counts", "rescued") in events
    assert ("type_mismatch", "score", "quarantined") in events

    # older batches simply have NULL in the new column; Silver keeps working
    assert run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet).failures == []
    assert spark.table(cfg.table("silver.questions")).where("question_id IN (990000001, 990000004)").count() == 2

    # re-loading the drifted batch is idempotent too
    run_raw_to_bronze(spark, cfg, mode="backfill", batch_ids=DRIFT_BATCH, echo=quiet)
    assert spark.table(cfg.table("bronze.questions")).where(F.col("_batch_id") == DRIFT_BATCH).count() == 2
    assert (spark.table(cfg.table("bronze.quarantine_records"))
            .where(F.col("_batch_id") == DRIFT_BATCH).count()) == 3


# ------------------------------------------------------------------------------------------- failures
def test_broken_batch_is_logged_as_failed_and_others_continue(spark, cfg, landing):
    broken = os.path.join(landing, "incremental", "incr_20261009T000000Z")
    os.makedirs(broken, exist_ok=True)
    open(os.path.join(broken, "questions.jsonl"), "w").close()       # no _manifest.json
    with pytest.raises(RuntimeError):
        run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    failed = (spark.table(cfg.table("ops.pipeline_execution_logs"))
              .where("status = 'FAILED' AND batch_id = 'incr_20261009T000000Z'").collect())
    assert failed and "manifest" in failed[0].message
    audit = run_raw_to_bronze(spark, cfg, mode="incremental", fail_on_error=False, echo=quiet)
    assert any(r.status == "FAILED" for r in audit.records)


def test_every_log_row_has_required_audit_fields(spark, cfg):
    df = spark.table(cfg.table("ops.pipeline_execution_logs"))
    assert df.count() > 0
    required = ["layer", "source_parameter", "start_time", "end_time", "status", "rows_inserted", "rows_updated",
                "load_timestamp"]
    for c in required:
        assert df.where(F.col(c).isNull()).count() == 0, c
    assert {r.layer for r in df.select("layer").distinct().collect()} == {"Raw-to-Bronze", "Bronze-to-Silver"}
