"""Raw-to-Bronze: land raw JSONL files into typed Bronze Delta tables.

Per file:
  1. Read lines as text, then parse with the explicit StructType contract
     (from_json, PERMISSIVE mode). No schema inference anywhere.
  2. Run the contract check (drift.py) on the same lines.
  3. Valid records -> bronze.<entity>, written with replaceWhere on _batch_id,
     so re-running a batch replaces its rows instead of duplicating them.
     New top-level fields evolve the table with mergeSchema (drift_mode=evolve)
     or are kept in _rescued_data (drift_mode=rescue).
  4. Malformed / type-mismatched / key-less records -> bronze.quarantine_records.
     The batch continues.
  5. Drift is recorded in ops.schema_drift_events; every file gets a row in
     ops.pipeline_execution_logs; the batch outcome goes to ops.batch_registry.
"""
import json
import os
import re
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .audit import AuditLogger, utc_now
from .batches import discover_batches, parse_list, read_batch, select_batches
from .delta_utils import merge_sql, run_merge
from .drift import make_contract_check, sanitize_column
from .schemas import BATCH_REGISTRY, BRONZE_METADATA, DRIFT_EVENTS, RAW_CONTRACTS

LAYER = "Raw-to-Bronze"
SAFE_ID = re.compile(r"^[A-Za-z0-9_.:-]+$")
SAFE_KEY = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _sql_str(value):
    if not SAFE_ID.match(value):
        raise ValueError(f"unsafe identifier for SQL predicate: {value!r}")
    return f"'{value}'"


# ============================================================================ public entry point
def run_raw_to_bronze(spark, cfg, mode="incremental", load_type="all", batch_ids=None, start_date=None,
                      end_date=None, landing_path=None, fail_on_error=True, echo=print):
    """Load landing batches into Bronze.

    mode=incremental : every discovered batch that is not yet loaded successfully.
    mode=backfill    : reload the batches selected by batch_ids / start_date..end_date /
                       landing_path, even if they were loaded before (idempotent replace).
    """
    if mode not in ("incremental", "backfill"):
        raise ValueError("mode must be 'incremental' or 'backfill'")
    has_scope = bool(parse_list(batch_ids) or start_date or end_date or landing_path)
    if mode == "backfill" and not has_scope:
        raise ValueError("backfill needs batch_ids, start_date/end_date or landing_path")

    audit = AuditLogger(spark, cfg, "raw_to_bronze", mode, echo=echo)
    if landing_path:
        batches = [read_batch(p) for p in parse_list(landing_path)]
        missing = []
    else:
        batches, missing = select_batches(discover_batches(cfg.landing_root, load_type),
                                          batch_ids, start_date, end_date)
    for bid in missing:
        with audit.step(LAYER, "*", f"batch_id={bid}", "-", batch_id=bid) as rec:
            raise FileNotFoundError(f"batch {bid} not found under {cfg.landing_root}")

    registry = {r["batch_id"]: r for r in spark.table(cfg.table("ops.batch_registry"))
                .select("batch_id", "bronze_status").collect()}
    echo(f"{LAYER}: {len(batches)} batch(es) selected, mode={mode}")

    for batch in batches:
        if batch.error:
            with audit.step(LAYER, "*", batch.path, "-", batch_id=batch.batch_id, load_type=batch.load_type) as rec:
                raise ValueError(batch.error)
            continue
        already = registry.get(batch.batch_id)
        if mode == "incremental" and already is not None and already["bronze_status"] == "SUCCESS":
            with audit.step(LAYER, "*", batch.path, "-", batch_id=batch.batch_id, load_type=batch.load_type) as rec:
                rec.skip("already loaded; use mode=backfill to reload")
            continue

        statuses = []
        for entity, (file_name, _contract, _pk) in RAW_CONTRACTS.items():
            path = os.path.join(batch.path, file_name)
            if not os.path.exists(path):
                continue  # full loads have no deletion/snapshot files
            with audit.step(LAYER, entity, path, cfg.table(f"bronze.{entity}"), batch_id=batch.batch_id,
                            load_type=batch.load_type) as rec:
                load_entity_file(spark, cfg, batch, entity, path, rec, audit.run_id)
            statuses.append(rec.status)
        status = "SUCCESS" if statuses and all(s == "SUCCESS" for s in statuses) else "FAILED"
        upsert_registry_bronze(spark, cfg, batch, status)

    echo(f"{LAYER} summary: {audit.summary()}")
    if fail_on_error and audit.failures:
        raise RuntimeError(f"{len(audit.failures)} Raw-to-Bronze step(s) failed; see "
                           f"{cfg.table('ops.pipeline_execution_logs')} run_id={audit.run_id}")
    return audit


# ============================================================================ one file
def load_entity_file(spark, cfg, batch, entity, path, rec, run_id):
    _file_name, contract, pk = RAW_CONTRACTS[entity]
    target = cfg.table(f"bronze.{entity}")
    bid = batch.batch_id
    _sql_str(bid)

    # 1-2. strict parse + contract check ------------------------------------------------
    parse_schema = StructType(list(contract.fields) + [StructField("_corrupt_record", StringType())])
    check = make_contract_check(contract)
    pk_missing = reduce(lambda a, b: a | b, [F.col(f"rec.{k}").isNull() for k in pk])
    parsed = (spark.read.text(path)
              .select(F.col("value"), F.col("_metadata.file_path").alias("_source_file"))
              .where(F.length(F.trim("value")) > 0)
              .withColumn("_record_hash", F.sha2(F.col("value"), 256))
              .withColumn("rec", F.from_json("value", parse_schema,
                                             {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"}))
              .withColumn("chk", check(F.col("value")))
              .withColumn("_reason",
                          F.when(~F.col("chk.is_json"), F.lit("malformed_json"))
                          .when((F.size("chk.type_errors") > 0) | F.col("rec._corrupt_record").isNotNull(),
                                F.lit("type_mismatch"))
                          .when(pk_missing, F.lit("missing_primary_key"))))

    counts = {r["_reason"]: r["count"] for r in parsed.groupBy("_reason").count().collect()}
    n_good = counts.get(None, 0)
    n_bad = sum(v for k, v in counts.items() if k is not None)
    rec.rows_read = n_good + n_bad

    # 3. drift: which unexpected fields appeared? -----------------------------------------
    new_fields = (parsed.where(F.col("chk.is_json"))
                  .select(F.explode("chk.unexpected").alias("u"))
                  .groupBy(F.col("u.path").alias("path"))
                  .agg(F.count("*").alias("n"), F.first("u.value").alias("example"))
                  .collect())
    table_cols = {f.name: f.dataType for f in spark.table(target).schema.fields}
    contract_names = {f.name for f in contract.fields} | {f.name for f in BRONZE_METADATA}
    evolve_map, drift_rows = {}, []
    for r in new_fields:
        path_ = r["path"]
        is_top = "." not in path_ and "[" not in path_
        colname = sanitize_column(path_) if is_top else None
        can_evolve = (cfg.drift_mode == "evolve" and is_top and SAFE_KEY.match(path_) is not None
                      and colname not in contract_names)
        if can_evolve:
            evolve_map[colname] = path_
        drift_rows.append(("new_column", path_, r["n"], r["example"], "evolved" if can_evolve else "rescued"))
    # columns evolved by earlier batches must be filled for this batch too
    for c in table_cols:
        if c not in contract_names and c not in evolve_map and SAFE_KEY.match(c):
            evolve_map[c] = c

    for r in (parsed.select(F.explode("chk.type_errors").alias("e"))
              .groupBy(F.col("e.path").alias("path"))
              .agg(F.count("*").alias("n"),
                   F.first(F.concat(F.lit("expected "), F.col("e.expected"), F.lit(", got "), F.col("e.actual")))
                   .alias("example")).collect()):
        drift_rows.append(("type_mismatch", r["path"], r["n"], r["example"], "quarantined"))
    for reason in ("malformed_json", "missing_primary_key"):
        if counts.get(reason):
            drift_rows.append((reason, None if reason == "malformed_json" else ",".join(pk),
                               counts[reason], None, "quarantined"))

    # 4. valid records -> Bronze ----------------------------------------------------------
    evolved_keys = list(evolve_map.values())
    remaining = (F.filter("chk.unexpected", lambda u: ~u["path"].isin(evolved_keys)) if evolved_keys
                 else F.col("chk.unexpected"))
    good = parsed.where(F.col("_reason").isNull()).select(
        *[F.col(f"rec.{f.name}").alias(f.name) for f in contract.fields],
        *[F.get_json_object("value", f"$.{key}").alias(colname) for colname, key in evolve_map.items()],
        F.lit(bid).alias("_batch_id"),
        F.lit(batch.load_type).alias("_load_type"),
        F.lit(batch.batch_date).cast("date").alias("_batch_date"),
        F.lit(batch.extracted_at).cast("timestamp").alias("_extracted_at"),
        F.col("_source_file"),
        F.col("_record_hash"),
        F.when(F.size(remaining) > 0, F.to_json(F.map_from_entries(remaining))).alias("_rescued_data"),
        F.current_timestamp().alias("load_timestamp"),
    )
    # align with the table: typed nulls for table columns this file does not carry
    for name, dtype in table_cols.items():
        if name not in good.columns:
            good = good.withColumn(name, F.lit(None).cast(dtype))
    new_cols = [c for c in good.columns if c not in table_cols]
    good = good.select(*list(table_cols), *new_cols)

    replaced = spark.table(target).where(F.col("_batch_id") == bid).count()
    writer = (good.write.format("delta").mode("overwrite")
              .option("replaceWhere", f"_batch_id = {_sql_str(bid)}"))
    if new_cols:
        writer = writer.option("mergeSchema", "true")   # schema evolution for drift
    writer.saveAsTable(target)

    # 5. invalid records -> quarantine (replaced per batch+entity, so reruns stay idempotent)
    bad = (parsed.where(F.col("_reason").isNotNull())
           .select(F.sha2(F.concat_ws("|", F.lit(entity), F.lit(bid), F.col("_source_file"), F.col("_record_hash")),
                          256).alias("quarantine_id"),
                   F.lit(entity).alias("entity"), F.lit(bid).alias("_batch_id"), F.col("_source_file"),
                   F.col("_reason").alias("reason"),
                   F.when(F.col("_reason") == "type_mismatch",
                          F.coalesce(F.when(F.size("chk.type_errors") > 0, F.to_json("chk.type_errors")),
                                     F.lit("Spark could not convert the record to the contract types")))
                   .when(F.col("_reason") == "missing_primary_key", F.lit(f"primary key ({', '.join(pk)}) is null"))
                   .otherwise(F.lit("line is not a JSON object")).alias("error_detail"),
                   F.col("value").alias("raw_record"),
                   F.current_timestamp().alias("load_timestamp"))
           .dropDuplicates(["quarantine_id"]))
    (bad.write.format("delta").mode("overwrite")
     .option("replaceWhere", f"entity = '{entity}' AND _batch_id = {_sql_str(bid)}")
     .saveAsTable(cfg.table("bronze.quarantine_records")))

    record_drift(spark, cfg, run_id, bid, entity, drift_rows)

    rec.rows_inserted = n_good
    rec.rows_deleted = replaced
    rec.rows_quarantined = n_bad
    notes = []
    if replaced:
        notes.append(f"replaced {replaced} rows from a previous load of this batch")
    if new_cols:
        notes.append(f"schema evolved: added {', '.join(new_cols)}")
    rescued = [d[1] for d in drift_rows if d[0] == "new_column" and d[4] == "rescued"]
    if rescued:
        notes.append(f"rescued unknown fields: {', '.join(rescued[:5])}")
    if n_bad:
        notes.append(f"quarantined {n_bad}: " + ", ".join(f"{k}={v}" for k, v in counts.items() if k))
    rec.message = "; ".join(notes) or None


def record_drift(spark, cfg, run_id, bid, entity, drift_rows):
    if not drift_rows:
        return
    import hashlib
    now = utc_now()
    rows = []
    for drift_type, path_, n, example, action in drift_rows:
        key = f"{bid}|{entity}|{drift_type}|{path_}"
        rows.append((hashlib.sha256(key.encode()).hexdigest(), run_id, bid, entity, drift_type, path_, int(n),
                     (example or "")[:300] or None, action, now))
    spark.createDataFrame(rows, DRIFT_EVENTS).createOrReplaceTempView("src_drift_events")
    cols = [f.name for f in DRIFT_EVENTS.fields]
    run_merge(spark, cfg.table("ops.schema_drift_events"), merge_sql(
        cfg.table("ops.schema_drift_events"), "src_drift_events", ["event_id"], cols,
        update_assignments={c: f"s.{c}" for c in cols if c != "event_id"}))


# ============================================================================ batch registry
def upsert_registry_bronze(spark, cfg, batch, status):
    now = utc_now()
    row = (batch.batch_id, batch.load_type, batch.batch_date, batch.extracted_at, batch.path,
           json.dumps(batch.manifest) if batch.manifest else None, status, now, None, None, now)
    spark.createDataFrame([row], BATCH_REGISTRY).createOrReplaceTempView("src_registry")
    cols = [f.name for f in BATCH_REGISTRY.fields]
    upd = {c: f"s.{c}" for c in ("load_type", "batch_date", "extracted_at", "landing_path", "manifest_json",
                                  "bronze_status", "bronze_loaded_at", "load_timestamp")}
    run_merge(spark, cfg.table("ops.batch_registry"),
              merge_sql(cfg.table("ops.batch_registry"), "src_registry", ["batch_id"], cols, upd))
