"""Small helpers around Delta Lake that work on Databricks serverless and local Spark.

Only SQL and DataFrame APIs are used (no SparkContext / RDD), because
Databricks serverless runs on Spark Connect where those do not exist.
"""


def table_exists(spark, name):
    try:
        return spark.catalog.tableExists(name)
    except Exception:
        try:
            spark.sql(f"DESCRIBE TABLE {name}")
            return True
        except Exception:
            return False


def latest_version(spark, table):
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]["version"]


def run_merge(spark, table, merge_sql):
    """Execute a MERGE INTO statement and return its row metrics.

    Metrics come from the Delta transaction log (DESCRIBE HISTORY), which is
    the same on Databricks and open-source Delta.
    """
    before = latest_version(spark, table)
    spark.sql(merge_sql).collect()
    last = spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").collect()[0]
    if last["version"] == before:           # nothing committed
        return {"inserted": 0, "updated": 0, "deleted": 0, "source_rows": 0}
    m = last["operationMetrics"] or {}

    def num(key):
        try:
            return int(m.get(key, 0) or 0)
        except (TypeError, ValueError):
            return 0
    return {"inserted": num("numTargetRowsInserted"), "updated": num("numTargetRowsUpdated"),
            "deleted": num("numTargetRowsDeleted"), "source_rows": num("numSourceRows")}


def merge_sql(target, source_view, keys, insert_cols, update_assignments=None, matched_condition=None,
              extra_clauses=""):
    """Build a MERGE INTO statement.

    update_assignments: dict column -> SQL expression (None = no UPDATE clause)
    """
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    parts = [f"MERGE INTO {target} AS t", f"USING {source_view} AS s", f"ON {on}"]
    if update_assignments:
        cond = f" AND ({matched_condition})" if matched_condition else ""
        sets = ",\n    ".join(f"t.{c} = {expr}" for c, expr in update_assignments.items())
        parts.append(f"WHEN MATCHED{cond} THEN UPDATE SET\n    {sets}")
    if extra_clauses:
        parts.append(extra_clauses)
    if insert_cols:
        cols = ", ".join(insert_cols)
        vals = ", ".join(f"s.{c}" for c in insert_cols)
        parts.append(f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})")
    return "\n".join(parts)
