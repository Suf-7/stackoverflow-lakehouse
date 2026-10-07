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
        if cfg.catalog and not existed:
            # Informational primary keys are a Unity Catalog feature.
            cname = f"pk_{logical.replace('.', '_')}"
            try:
                spark.sql(f"ALTER TABLE {full} ADD CONSTRAINT {cname} PRIMARY KEY ({', '.join(pk)})")
            except Exception as exc:  # not fatal: NOT NULL is already enforced
                log(f"note: primary key constraint skipped on {full}: {str(exc)[:120]}")
        log(f"{'exists ' if existed else 'created'} {full}")
