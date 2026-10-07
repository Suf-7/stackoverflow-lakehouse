# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze → Silver
# MAGIC Casts epoch integers to timestamps, decodes titles, removes PII (drops names/avatars, hashes user ids with a
# MAGIC salted SHA-256), derives body features, applies data-quality rules and upserts every Silver table with
# MAGIC **`MERGE INTO`**. Re-running the same batches changes nothing (idempotent).
# MAGIC
# MAGIC | Widget | Standard incremental run | Backfill |
# MAGIC |---|---|---|
# MAGIC | `mode` | `incremental`: Bronze batches not yet in Silver | `backfill`: re-process the selected batches |
# MAGIC | `batch_ids` | empty | comma-separated batch ids |
# MAGIC | `start_date` / `end_date` | empty | extraction-date range, `YYYY-MM-DD`, inclusive |
# MAGIC | `pii_salt` | empty (read from secret `so_lakehouse/pii_salt`) | same |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "backfill"])
dbutils.widgets.text("batch_ids", "")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("pii_salt", "")

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from so_lakehouse.silver import run_bronze_to_silver

cfg = build_config(need_salt=True)
audit = run_bronze_to_silver(
    spark, cfg,
    mode=widget("mode"),
    batch_ids=widget("batch_ids") or None,
    start_date=widget("start_date") or None,
    end_date=widget("end_date") or None,
    fail_on_error=True,
)

# COMMAND ----------

display(spark.table(cfg.table("ops.pipeline_execution_logs"))
        .where(f"run_id = '{audit.run_id}'").orderBy("start_time"))
