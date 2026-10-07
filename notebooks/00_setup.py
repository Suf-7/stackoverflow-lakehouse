# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the Unity Catalog schemas (`so_raw`, `so_bronze`, `so_silver`, `so_ops`), the landing volume and
# MAGIC every Delta table with its explicit schema, comments and primary key. Safe to run repeatedly.
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `catalog` | Unity Catalog to use. Free Edition's default catalog is `workspace`. |
# MAGIC | `reset` | `true` drops and recreates every table. Use only to start over. |
# MAGIC | `seed_demo_landing` | `true` copies the demo batches in `data/demo_landing` from the repo into the volume. |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("reset", "false", ["false", "true"])
dbutils.widgets.dropdown("seed_demo_landing", "true", ["true", "false"])

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import shutil

from so_lakehouse.ddl import setup

cfg = build_config()
setup(spark, cfg, reset=widget("reset") == "true")

# COMMAND ----------

def copy_tree_plain(src, dst):
    """Copy files only (no permission/timestamp copying, which UC volumes do not support)."""
    copied = 0
    for folder, _dirs, files in os.walk(src):
        target = os.path.join(dst, os.path.relpath(folder, src))
        os.makedirs(target, exist_ok=True)
        for name in files:
            shutil.copyfile(os.path.join(folder, name), os.path.join(target, name))
            copied += 1
    return copied


if widget("seed_demo_landing") == "true":
    n = copy_tree_plain(os.path.join(REPO_ROOT, "data", "demo_landing"), cfg.landing_root)
    print(f"copied {n} files into {cfg.landing_root}")

for lt in ("full", "incremental"):
    folder = os.path.join(cfg.landing_root, lt)
    print(lt, sorted(os.listdir(folder)) if os.path.isdir(folder) else "(none)")

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {cfg.schema_name('silver')}"))
