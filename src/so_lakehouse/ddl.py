"""Create schemas, the landing volume and every Delta table from schemas.TABLES."""
from .delta_utils import table_exists
from .schemas import TABLES


def _escape(text):
    return text.replace("\\", "\\\\").replace("'", "\\'")


def create_table_sql(full_name, schema, primary_key, description, layer):
    cols = []
    for f in schema.fields:
        not_null = " NOT NULL" if (f.name in primary_key or not f.nullable) else ""
        comment = _escape(f.metadata.get("comment", ""))
        cols.append(f"  `{f.name}` {f.dataType.simpleString()}{not_null} COMMENT '{comment}'")
    return (f"CREATE TABLE IF NOT EXISTS {full_name} (\n" + ",\n".join(cols) + "\n)\nUSING DELTA\n"
            f"COMMENT '{_escape(description)}'\n"
            f"TBLPROPERTIES ('layer' = '{layer}', 'primary_key' = '{','.join(primary_key)}')")


def setup(spark, cfg, reset=False, log=print):
    """Idempotent: safe to run any number of times. reset=True drops all tables first."""
    for layer in ("raw", "bronze", "silver", "ops"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema_name(layer)}")
    if cfg.catalog:   # Unity Catalog volume for raw files (Databricks only)
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema_name('raw')}.{cfg.landing_volume}")

    for logical, (layer, _name, schema, pk, description) in TABLES.items():
        full = cfg.table(logical)
        if reset and table_exists(spark, full):
            spark.sql(f"DROP TABLE {full}")
            log(f"dropped {full}")
        existed = table_exists(spark, full)
        spark.sql(create_table_sql(full, schema, pk, description, layer))
        if not existed:
            ensure_primary_key(spark, cfg, logical, log=log)
        log(f"{'exists ' if existed else 'created'} {full}")


def ensure_primary_key(spark, cfg, logical, log=print):
    """Declare the informational PRIMARY KEY (a Unity Catalog feature) if the table lacks it.

    Called at setup and again after any write that evolves a table's schema,
    because a mergeSchema write can drop Unity Catalog constraints.
    Returns True when a constraint was added.
    """
    if not cfg.catalog:
        return False
    _layer, name, _schema, pk, _d = TABLES[logical]
    schema_part = cfg.schema_name(_layer).split(".")[-1]
    present = spark.sql(
        f"SELECT 1 FROM {cfg.catalog}.information_schema.table_constraints "
        f"WHERE table_schema = '{schema_part}' AND table_name = '{name}' AND constraint_type = 'PRIMARY KEY'"
    ).count()
    if present:
        return False
    try:
        spark.sql(f"ALTER TABLE {cfg.table(logical)} ADD CONSTRAINT pk_{logical.replace('.', '_')} "
                  f"PRIMARY KEY ({', '.join(pk)})")
        return True
    except Exception as exc:  # not fatal: NOT NULL is already enforced on the key columns
        log(f"note: primary key constraint skipped on {cfg.table(logical)}: {str(exc)[:120]}")
        return False
