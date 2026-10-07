# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Audit & validation
# MAGIC Evidence for the Phase 2 requirements: execution logs, batch registry, quarantine, schema drift,
# MAGIC primary-key uniqueness in Silver, and the MERGE history of a Silver table.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pyspark.sql import functions as F

cfg = build_config()
L = cfg.table("ops.pipeline_execution_logs")

# COMMAND ----------

# MAGIC %md ## 1. Latest runs (one row per file or table processed)

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, layer, run_mode, load_type, entity, batch_id, status, rows_read, rows_inserted, rows_updated,
         rows_deleted, rows_quarantined, start_time, end_time, round(duration_seconds, 1) AS secs, message
  FROM {L} ORDER BY start_time DESC LIMIT 200"""))

# COMMAND ----------

# MAGIC %md ## 2. Run summary by layer and status

# COMMAND ----------

display(spark.sql(f"""
  SELECT run_id, layer, run_mode, min(start_time) AS started, max(end_time) AS finished,
         count(*) AS steps, sum(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
         sum(rows_inserted) AS inserted, sum(rows_updated) AS updated, sum(rows_quarantined) AS quarantined
  FROM {L} GROUP BY run_id, layer, run_mode ORDER BY started DESC"""))

# COMMAND ----------

# MAGIC %md ## 3. Batch registry (control table)

# COMMAND ----------

display(spark.table(cfg.table("ops.batch_registry")).drop("manifest_json").orderBy("extracted_at"))

# COMMAND ----------

# MAGIC %md ## 4. Schema drift and quarantine

# COMMAND ----------

display(spark.table(cfg.table("ops.schema_drift_events")).orderBy(F.desc("load_timestamp")))
display(spark.table(cfg.table("bronze.quarantine_records")).select("entity", "_batch_id", "reason", "error_detail",
                                                                    "load_timestamp"))

# COMMAND ----------

# MAGIC %md ## 5. Idempotency check: Silver primary keys must be unique

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

# MAGIC %md ## 6. MERGE history of `silver.questions` (operation metrics per run)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.table('silver.questions')}")
        .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md ## 7. Row counts per layer

# COMMAND ----------

counts = [(t, spark.table(cfg.table(t)).count()) for t in
          ["bronze.questions", "bronze.answers", "bronze.deleted_questions", "bronze.question_snapshots",
           "bronze.quarantine_records", "silver.questions", "silver.question_tags", "silver.answers",
           "silver.users", "silver.question_snapshots", "silver.post_text_masked", "silver.dq_quarantine"]]
display(spark.createDataFrame(counts, "table string, rows long"))
