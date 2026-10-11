# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Audit & validation: Phase 2 evidence
# MAGIC Every section below is computed from the live Unity Catalog tables. They follow the rubric's proof list:
# MAGIC
# MAGIC | # | Proof | Section |
# MAGIC |---|---|---|
# MAGIC | 1 | Explicit schemas, no inference | 1. Raw contracts as StructType, and the code that reads files with them |
# MAGIC | 2 | Data model | 2. `DESCRIBE TABLE` for every Bronze and Silver table, and their primary keys |
# MAGIC | 3 | `load_timestamp` and source tracking | 3. Null checks on every table, and Bronze lineage columns |
# MAGIC | 4 | MERGE INTO | 4. The exact MERGE statement the pipeline runs, and what Delta recorded |
# MAGIC | 5 | Re-run creates no duplicates | 5. Duplicate-key check, and MERGE metrics of the re-run |
# MAGIC | 6 | Backfill | 6. Log rows of the backfill runs |
# MAGIC | 7 | Schema change handled | 7. Drift events, quarantine and the evolved column |
# MAGIC | 8 | Log table | 8. `so_ops.pipeline_execution_logs` and the batch registry |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import inspect

from pyspark.sql import functions as F

from so_lakehouse import bronze as bronze_module
from so_lakehouse.schemas import RAW_CONTRACTS, TABLES
from so_lakehouse.silver import questions_merge_sql

cfg = build_config()
L = cfg.table("ops.pipeline_execution_logs")
BRONZE_SILVER = [k for k, v in TABLES.items() if v[0] in ("bronze", "silver")]

# COMMAND ----------

# MAGIC %md ## 1. Explicit schemas (schema-on-read, no inference)
# MAGIC Each raw file type has a `StructType` contract defined in `src/so_lakehouse/schemas.py` **before** any file is read.

# COMMAND ----------

for entity, (file_name, contract, pk) in RAW_CONTRACTS.items():
    print(f"=== {entity}  (file: {file_name}, primary key: {', '.join(pk)})")
    spark.createDataFrame([], contract).printSchema()

# COMMAND ----------

# MAGIC %md The code that reads every raw file: plain text lines, then `from_json` with the contract in PERMISSIVE mode.

# COMMAND ----------

src = inspect.getsource(bronze_module.load_entity_file)
print(src[src.index("# 1-2."):src.index("counts = ")])

# COMMAND ----------

src_dir = os.path.join(REPO_ROOT, "src", "so_lakehouse")
hits = {f: open(os.path.join(src_dir, f)).read().count("inferSchema")
        for f in sorted(os.listdir(src_dir)) if f.endswith(".py")}
print("Occurrences of 'inferSchema' in the pipeline code:", sum(hits.values()), hits)

# COMMAND ----------

# MAGIC %md ## 2. Data model: `DESCRIBE TABLE` for every Bronze and Silver table

# COMMAND ----------

for logical in BRONZE_SILVER:
    print(f"=== {cfg.table(logical)}   primary key: {', '.join(TABLES[logical][3])}")
    display(spark.sql(f"DESCRIBE TABLE {cfg.table(logical)}"))

# COMMAND ----------

# MAGIC %md Primary keys declared in Unity Catalog (informational constraints created by `00_setup`).

# COMMAND ----------

display(spark.sql(f"""
  SELECT kcu.table_schema, kcu.table_name, tc.constraint_name,
         concat_ws(', ', collect_list(kcu.column_name)) AS primary_key_columns
  FROM {cfg.catalog}.information_schema.table_constraints tc
  JOIN {cfg.catalog}.information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
  WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema IN ('so_bronze', 'so_silver')
  GROUP BY kcu.table_schema, kcu.table_name, tc.constraint_name
  ORDER BY kcu.table_schema, kcu.table_name"""))

# COMMAND ----------

# MAGIC %md ## 3. `load_timestamp` on every row, and Bronze source tracking

# COMMAND ----------

rows = []
for logical in BRONZE_SILVER:
    df = spark.table(cfg.table(logical))
    r = df.agg(F.count("*").alias("n"), F.sum(F.col("load_timestamp").isNull().cast("int")).alias("nulls"),
               F.min("load_timestamp").alias("first"), F.max("load_timestamp").alias("last")).collect()[0]
    rows.append((cfg.table(logical), r["n"], r["nulls"] or 0, str(r["first"]), str(r["last"])))
display(spark.createDataFrame(rows, "table string, rows long, null_load_timestamps long, earliest string, latest string"))

# COMMAND ----------

display(spark.table(cfg.table("bronze.questions"))
        .groupBy("_batch_id", "_load_type", "_batch_date", "_extracted_at", "_source_file")
        .agg(F.count("*").alias("rows"), F.max("load_timestamp").alias("load_timestamp"))
        .orderBy("_extracted_at"))

# COMMAND ----------

# MAGIC %md ## 4. The MERGE INTO statement
# MAGIC This is the exact statement `02_bronze_to_silver` runs for `silver.questions` (built by `questions_merge_sql`).
# MAGIC It updates a row only when the source is **newer and different**, and inserts keys it has never seen.

# COMMAND ----------

print(questions_merge_sql(cfg))

# COMMAND ----------

# MAGIC %md What Delta recorded for each MERGE on `silver.questions`: the predicates and the row counts.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('silver.questions')}")
        .where("operation = 'MERGE'")
        .select("version", "timestamp",
                F.col("operationParameters")["predicate"].alias("on"),
                F.col("operationParameters")["matchedPredicates"].alias("when_matched"),
                F.col("operationMetrics")["numSourceRows"].alias("source_rows"),
                F.col("operationMetrics")["numTargetRowsInserted"].alias("inserted"),
                F.col("operationMetrics")["numTargetRowsUpdated"].alias("updated"))
        .orderBy("version"))

# COMMAND ----------

# MAGIC %md ## 5. Re-running the same input creates no duplicates
# MAGIC Every Silver primary key must be unique, and the backfill that re-merged every batch must have changed 0 rows.

# COMMAND ----------

checks = {"silver.questions": ["question_id"], "silver.answers": ["answer_id"],
          "silver.question_tags": ["question_id", "tag"], "silver.users": ["user_key"],
          "silver.question_snapshots": ["question_id", "snapshot_at"],
          "silver.post_text_masked": ["post_type", "post_id"]}
rows = []
for t, keys in checks.items():
    df = spark.table(cfg.table(t))
    total, distinct = df.count(), df.select(*keys).distinct().count()
    rows.append((t, total, distinct, total - distinct))
display(spark.createDataFrame(rows, "table string, rows long, distinct_keys long, duplicates long"))

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, run_mode, entity, rows_read, rows_inserted, rows_updated, rows_deleted, status
  FROM {L} WHERE layer = 'Bronze-to-Silver' AND status <> 'SKIPPED'
  ORDER BY start_time"""))

# COMMAND ----------

# MAGIC %md ## 6. Backfill runs (mode = backfill, chosen by date range or batch id)

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, layer, entity, batch_id, source_parameter, status, rows_read, rows_inserted, rows_updated,
         rows_deleted AS rows_replaced, start_time
  FROM {L} WHERE run_mode = 'backfill' ORDER BY start_time"""))

# COMMAND ----------

# MAGIC %md ## 7. Schema change handled: drift events, quarantine and the evolved column

# COMMAND ----------

display(spark.table(cfg.table("ops.schema_drift_events"))
        .select("_batch_id", "entity", "drift_type", "column_path", "record_count", "action_taken", "example_value")
        .orderBy("drift_type"))

# COMMAND ----------

display(spark.table(cfg.table("bronze.quarantine_records"))
        .select("entity", "_batch_id", "reason", "error_detail", "raw_record", "load_timestamp"))

# COMMAND ----------

bq = spark.table(cfg.table("bronze.questions"))
evolved = [c for c in bq.columns if c not in {f.name for f in TABLES["bronze.questions"][2].fields}]
print("Columns added to bronze.questions by schema evolution (mergeSchema):", evolved)
display(bq.where("_batch_id = 'incr_20261008T000000Z'").select("question_id", "score", *evolved, "_rescued_data"))

# COMMAND ----------

# MAGIC %md ## 8. The log table: `so_ops.pipeline_execution_logs`
# MAGIC One row per file (Raw-to-Bronze) or target table (Bronze-to-Silver), for full and incremental loads.

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, layer, run_mode, min(start_time) AS started, max(end_time) AS finished, count(*) AS log_rows,
         sum(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success,
         sum(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) AS skipped,
         sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
         sum(rows_inserted) AS inserted, sum(rows_updated) AS updated, sum(rows_quarantined) AS quarantined
  FROM {L} GROUP BY run_id, layer, run_mode ORDER BY started"""))

# COMMAND ----------

display(spark.sql(f"""
  SELECT layer, run_mode, load_type, entity, batch_id, source_parameter, start_time, end_time,
         round(duration_seconds, 1) AS secs, status, rows_read, rows_inserted, rows_updated, rows_deleted,
         rows_quarantined, message
  FROM {L} ORDER BY start_time"""))

# COMMAND ----------

display(spark.table(cfg.table("ops.batch_registry")).drop("manifest_json").orderBy("extracted_at"))
