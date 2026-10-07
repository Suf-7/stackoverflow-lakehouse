# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Raw → Bronze
# MAGIC Loads landing batches (JSONL + `_manifest.json`) into the Bronze Delta tables using **explicit StructType
# MAGIC schemas** (no inference). Invalid records go to `so_bronze.quarantine_records`, schema drift is recorded in
# MAGIC `so_ops.schema_drift_events`, and every file processed writes a row to `so_ops.pipeline_execution_logs`.
# MAGIC
# MAGIC | Widget | Standard incremental run | Backfill |
# MAGIC |---|---|---|
# MAGIC | `mode` | `incremental`: every batch not yet loaded | `backfill`: reload the selected batches |
# MAGIC | `load_type` | `all` | `all`, `full` or `incremental` |
# MAGIC | `batch_ids` | empty | e.g. `incr_20260925T155859Z,full_2025-01-01_2025-04-01` |
# MAGIC | `start_date` / `end_date` | empty | extraction-date range, `YYYY-MM-DD`, inclusive |
# MAGIC | `landing_path` | empty | an explicit batch folder, e.g. `/Volumes/workspace/so_raw/landing/incremental/incr_...` |
# MAGIC | `drift_mode` | `evolve` | `evolve` adds new columns (mergeSchema); `rescue` keeps them in `_rescued_data` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])
dbutils.widgets.dropdown("load_type", "all", ["all", "full", "incremental"])
dbutils.widgets.text("batch_ids", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("landing_path", "")
dbutils.widgets.dropdown("drift_mode", "evolve", ["evolve", "rescue"])

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from so_lakehouse.bronze import run_raw_to_bronze

cfg = build_config()
audit = run_raw_to_bronze(
    spark, cfg,
    mode=widget("mode"),
    load_type=widget("load_type"),
    batch_ids=widget("batch_ids") or None,
    start_date=widget("start_date") or None,
    end_date=widget("end_date") or None,
    landing_path=widget("landing_path") or None,
    fail_on_error=True,
)

# COMMAND ----------

display(spark.table(cfg.table("ops.pipeline_execution_logs"))
        .where(f"run_id = '{audit.run_id}'").orderBy("start_time"))
