# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Ingest from the Stack Exchange API into the landing volume (optional)
# MAGIC Runs the repo's ingestion scripts directly against the volume. Needs outbound internet access from the
# MAGIC workspace. If your workspace blocks it, run the same scripts on a laptop and upload the batch folder to
# MAGIC the volume (README, "Getting data into the volume").
# MAGIC
# MAGIC | Widget | Meaning |
# MAGIC |---|---|
# MAGIC | `load_type` | `incremental` (changes since `since`) or `full` (questions created in `from_date`..`to_date`) |
# MAGIC | `since` | incremental start, UTC `YYYY-MM-DDTHH:MM:SS` |
# MAGIC | `from_date` / `to_date` | full-load window, `YYYY-MM-DD` |

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.dropdown("load_type", "incremental", ["incremental", "full"])
dbutils.widgets.text("since", "")
dbutils.widgets.text("from_date", "")
dbutils.widgets.text("to_date", "")

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import subprocess

cfg = build_config()
env = dict(os.environ)
try:
    env["SE_API_KEY"] = dbutils.secrets.get("so_lakehouse", "se_api_key")
except Exception:
    print("No API key secret: limited to 300 requests/day.")

if widget("load_type") == "full":
    cmd = [sys.executable, "ingestion/full_load.py", "--from", widget("from_date"), "--to", widget("to_date"),
           "--landing-root", cfg.landing_root]
else:
    cmd = [sys.executable, "ingestion/incremental_load.py", "--since", widget("since"),
           "--landing-root", cfg.landing_root, "--no-state"]
print(" ".join(cmd))
result = subprocess.run(cmd, cwd=REPO_ROOT, env=env, capture_output=True, text=True)
print(result.stdout[-4000:], result.stderr[-4000:])
result.check_returncode()
