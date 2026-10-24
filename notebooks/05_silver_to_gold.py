# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Silver → Gold
# MAGIC Builds the star schema (`dim_date`, `dim_tag`, `dim_user`, `fact_question`, `fact_answer`,
# MAGIC `bridge_question_tag`) and the dashboard aggregates (`agg_tag_month`, `agg_tag_pairs`, `agg_user_cohort`).
# MAGIC
# MAGIC Deleted questions are excluded. Each table is refreshed with `MERGE INTO` from the **current** Silver
# MAGIC layer, so this notebook does not take a batch id: run it at the end of every incremental load and Gold
# MAGIC follows Silver. A second run on the same Silver data changes nothing. `OPTIMIZE` then compacts the
# MAGIC fact and aggregate tables (liquid clustering on Databricks).
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `catalog` | Unity Catalog. Free Edition uses `workspace`. |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from so_lakehouse.gold import run_silver_to_gold

cfg = build_config()
audit = run_silver_to_gold(spark, cfg, fail_on_error=True)

# COMMAND ----------

display(spark.table(cfg.table("ops.pipeline_execution_logs"))
        .where(f"run_id = '{audit.run_id}'").orderBy("start_time"))

# COMMAND ----------

for logical in ("gold.fact_question", "gold.fact_answer", "gold.dim_tag", "gold.agg_tag_month"):
    print(logical, spark.table(cfg.table(logical)).count())
