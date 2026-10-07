# Databricks notebook source
# MAGIC %md
# MAGIC # _bootstrap (helper, run by the other notebooks with `%run ./_bootstrap`)
# MAGIC Puts the repo's `src/` folder on the Python path and builds the pipeline config from widgets.

# COMMAND ----------

import os
import sys


def _find_repo_root():
    here = os.getcwd()  # in a Databricks Git folder this is the notebook's folder
    for candidate in (here, os.path.dirname(here)):
        if os.path.isdir(os.path.join(candidate, "src", "so_lakehouse")):
            return candidate
    nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()  # noqa: F821
    return os.path.dirname(os.path.dirname("/Workspace" + nb))


REPO_ROOT = _find_repo_root()
if os.path.join(REPO_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(REPO_ROOT, "src"))

from so_lakehouse.config import PipelineConfig  # noqa: E402


def widget(name, default=""):
    try:
        value = dbutils.widgets.get(name)  # noqa: F821
    except Exception:
        value = default
    return value.strip() if isinstance(value, str) else value


def get_pii_salt():
    """Secret scope first, then the optional pii_salt widget (dev only)."""
    try:
        return dbutils.secrets.get("so_lakehouse", "pii_salt")  # noqa: F821
    except Exception:
        salt = widget("pii_salt")
        if not salt:
            raise RuntimeError("Create the secret so_lakehouse/pii_salt (see README) or fill the pii_salt widget.")
        print("WARNING: using pii_salt from widget; use a secret scope for anything real.")
        return salt


def build_config(need_salt=False):
    cfg = PipelineConfig.for_databricks(catalog=widget("catalog", "workspace") or "workspace",
                                        drift_mode=widget("drift_mode") or None)
    if need_salt:
        cfg.pii_salt = get_pii_salt()
    return cfg


print(f"repo root: {REPO_ROOT}")
