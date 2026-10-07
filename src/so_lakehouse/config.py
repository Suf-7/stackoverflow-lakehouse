"""Runtime configuration shared by every notebook, job and test.

The same code runs in two places:
  * Databricks (Free Edition): Unity Catalog three-level names
    `<catalog>.<schema>.<table>` and raw files in a UC Volume.
  * Local Spark for development and tests: two-level names `<schema>.<table>`
    in a local Hive metastore, raw files in a local folder.

Pass catalog=None for local runs.
"""
import json
import os
from dataclasses import dataclass, field
from typing import List, Optional

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CONFIG_PATH = os.path.join(REPO_ROOT, "config", "pipeline_config.json")


def _file_config():
    with open(CONFIG_PATH) as f:
        return json.load(f)


@dataclass
class PipelineConfig:
    catalog: Optional[str]
    landing_root: str
    pii_salt: Optional[str] = None      # resolved lazily: only Silver needs it
    bronze_schema: str = "so_bronze"
    silver_schema: str = "so_silver"
    ops_schema: str = "so_ops"
    raw_schema: str = "so_raw"
    landing_volume: str = "landing"
    drift_mode: str = "evolve"          # evolve: add new columns (mergeSchema); rescue: keep them in _rescued_data
    tracked_tags: List[str] = field(default_factory=list)

    # ---------------------------------------------------------------- naming
    def schema_name(self, layer):
        schema = {"bronze": self.bronze_schema, "silver": self.silver_schema,
                  "ops": self.ops_schema, "raw": self.raw_schema}[layer]
        return f"{self.catalog}.{schema}" if self.catalog else schema

    def table(self, logical_name):
        """'silver.questions' -> 'workspace.so_silver.questions' (or 'so_silver.questions' locally)."""
        layer, name = logical_name.split(".", 1)
        return f"{self.schema_name(layer)}.{name}"

    def salt(self):
        if not self.pii_salt:
            self.pii_salt = resolve_pii_salt()
        return self.pii_salt

    # -------------------------------------------------------------- builders
    @classmethod
    def for_databricks(cls, catalog=None, landing_root=None, pii_salt=None, drift_mode=None):
        lh = _file_config()["lakehouse"]
        catalog = catalog or lh["catalog"]
        landing_root = landing_root or f"/Volumes/{catalog}/{lh['raw_schema']}/{lh['landing_volume']}"
        return cls(catalog=catalog, landing_root=landing_root,
                   pii_salt=pii_salt or None,
                   bronze_schema=lh["bronze_schema"], silver_schema=lh["silver_schema"],
                   ops_schema=lh["ops_schema"], raw_schema=lh["raw_schema"],
                   landing_volume=lh["landing_volume"], drift_mode=drift_mode or lh["drift_mode"],
                   tracked_tags=_file_config()["tags"])

    @classmethod
    def for_local(cls, landing_root, pii_salt=None, drift_mode=None, schema_prefix=""):
        lh = _file_config()["lakehouse"]
        return cls(catalog=None, landing_root=os.path.abspath(landing_root),
                   pii_salt=pii_salt or os.environ.get("SO_PII_SALT", "local-dev-salt-not-secret"),
                   bronze_schema=schema_prefix + lh["bronze_schema"],
                   silver_schema=schema_prefix + lh["silver_schema"],
                   ops_schema=schema_prefix + lh["ops_schema"], raw_schema=schema_prefix + lh["raw_schema"],
                   drift_mode=drift_mode or lh["drift_mode"], tracked_tags=_file_config()["tags"])


def resolve_pii_salt():
    """Salt for pseudonymising user ids.

    Order: Databricks secret scope -> SO_PII_SALT environment variable.
    The salt must stay the same forever, otherwise user keys change and
    Silver users split into duplicates.
    """
    lh = _file_config()["lakehouse"]
    try:
        from pyspark.dbutils import DBUtils  # noqa: F401  (only exists on Databricks)
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils.secrets.get(lh["pii_salt_secret_scope"], lh["pii_salt_secret_key"])
    except Exception:
        pass
    salt = os.environ.get("SO_PII_SALT")
    if not salt:
        raise RuntimeError(
            "No PII salt found. Create Databricks secret "
            f"{lh['pii_salt_secret_scope']}/{lh['pii_salt_secret_key']}, set SO_PII_SALT, "
            "or pass pii_salt explicitly (see README, Databricks setup).")
    return salt
