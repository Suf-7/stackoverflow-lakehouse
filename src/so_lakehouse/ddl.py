"""Create schemas, the landing volume and every Delta table from schemas.TABLES."""
from .delta_utils import table_exists
from .schemas import GOLD_FOREIGN_KEYS, TABLES


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
    for layer in ("raw", "bronze", "silver", "gold", "ops"):
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
    ensure_foreign_keys(spark, cfg, log=log)


def ensure_foreign_keys(spark, cfg, log=print):
    """Declare the Gold star-schema relationships as informational FOREIGN KEY constraints.

    BI tools that read Unity Catalog constraints (Publish to Power BI) build the
    model relationships from these. Unity Catalog only, like primary keys.
    """
    if not cfg.catalog:
        return 0
    added = 0
    for child, column, parent, parent_column in GOLD_FOREIGN_KEYS:
        ensure_primary_key(spark, cfg, parent, log=log)
        _layer, child_name, _s, _pk, _d = TABLES[child]
        name = f"fk_{child_name}_{column}"
        schema_part = cfg.schema_name("gold").split(".")[-1]
        present = spark.sql(
            f"SELECT 1 FROM {cfg.catalog}.information_schema.table_constraints "
            f"WHERE table_schema = '{schema_part}' AND table_name = '{child_name}' "
            f"AND constraint_name = '{name}'").count()
        if present:
            continue
        try:
            spark.sql(f"ALTER TABLE {cfg.table(child)} ADD CONSTRAINT {name} "
                      f"FOREIGN KEY ({column}) REFERENCES {cfg.table(parent)} ({parent_column})")
            added += 1
            log(f"foreign key {name}: {child}.{column} -> {parent}.{parent_column}")
        except Exception as exc:  # informational only; a missing constraint does not change the data
            log(f"note: foreign key {name} skipped: {str(exc)[:160]}")
    return added


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
