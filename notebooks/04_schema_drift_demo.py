# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Schema drift demo
# MAGIC Writes a synthetic batch into the landing volume that breaks the contract on purpose, then loads it:
# MAGIC
# MAGIC | Line | Problem | Expected handling |
# MAGIC |---|---|---|
# MAGIC | 1 | new column `ai_assisted`, new nested field `owner.badge_counts` | column added to Bronze via `mergeSchema`; nested field kept in `_rescued_data` |
# MAGIC | 2 | `score` changes type: `"five"` instead of an integer | record quarantined |
# MAGIC | 3 | truncated JSON | record quarantined |
# MAGIC | 4 | no `question_id` | record quarantined |
# MAGIC | 5 | valid | loaded |
# MAGIC
# MAGIC The batch finishes with status SUCCESS: 2 rows loaded, 3 quarantined. Nothing crashes.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("drift_mode", "evolve", ["evolve", "rescue"])

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import glob

from so_lakehouse.bronze import run_raw_to_bronze
from so_lakehouse.demo import DRIFT_BATCH_ID, make_drift_batch

cfg = build_config()
base = sorted(glob.glob(os.path.join(cfg.landing_root, "incremental", "incr_*", "questions.jsonl")))[0]
folder = make_drift_batch(cfg.landing_root, base)
audit = run_raw_to_bronze(spark, cfg, mode="backfill", landing_path=folder, fail_on_error=False)

# COMMAND ----------

display(spark.table(cfg.table("ops.pipeline_execution_logs")).where(f"run_id = '{audit.run_id}'"))
display(spark.table(cfg.table("ops.schema_drift_events")).where(f"_batch_id = '{DRIFT_BATCH_ID}'"))
display(spark.table(cfg.table("bronze.quarantine_records")).where(f"_batch_id = '{DRIFT_BATCH_ID}'")
        .select("reason", "error_detail", "raw_record"))
display(spark.table(cfg.table("bronze.questions")).where(f"_batch_id = '{DRIFT_BATCH_ID}'")
        .select("question_id", "score", "_rescued_data", *([c for c in ["ai_assisted"]
                if c in spark.table(cfg.table("bronze.questions")).columns])))
