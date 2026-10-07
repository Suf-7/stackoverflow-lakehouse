"""Replay the Phase 2 scenario on local Spark and write docs/phase2_demo_results.md.

Steps: setup -> initial incremental run -> re-run (no-op) -> full reprocess (backfill)
-> date-range backfill -> schema-drift batch -> broken batch. Each step's audit rows
are captured so the learning guide can show real numbers.

    PYTHONPATH=src python scripts/demo_local_run.py
"""
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "src"))

from pyspark.sql import functions as F  # noqa: E402

from so_lakehouse.bronze import run_raw_to_bronze  # noqa: E402
from so_lakehouse.config import PipelineConfig  # noqa: E402
from so_lakehouse.ddl import setup  # noqa: E402
from so_lakehouse.demo import make_drift_batch  # noqa: E402
from so_lakehouse.silver import run_bronze_to_silver  # noqa: E402
from so_lakehouse.spark_session import local_spark  # noqa: E402

SILVER = ["silver.questions", "silver.question_tags", "silver.answers", "silver.users",
          "silver.question_snapshots", "silver.post_text_masked"]
BRONZE = ["bronze.questions", "bronze.answers", "bronze.deleted_questions", "bronze.question_snapshots",
          "bronze.quarantine_records"]
out = []


def md_table(headers, rows):
    lines = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    for r in rows:
        lines.append("| " + " | ".join("" if v is None else str(v) for v in r) + " |")
    return "\n".join(lines)


def audit_table(audit):
    rows = []
    for r in audit.records:
        rows.append((r.layer, r.entity, r.batch_id or "", r.status, r.rows_read, r.rows_inserted, r.rows_updated,
                     r.rows_deleted, r.rows_quarantined, f"{(r.end_time - r.start_time).total_seconds():.1f}",
                     (r.message or "")[:90]))
    return md_table(["Layer", "Entity", "Batch", "Status", "Read", "Inserted", "Updated", "Deleted/replaced",
                     "Quarantined", "Seconds", "Note"], rows)


def counts(spark, cfg, tables):
    return {t: spark.table(cfg.table(t)).count() for t in tables}


def fingerprint(spark, cfg, t):
    df = spark.table(cfg.table(t))
    return df.select(F.sum(F.conv(F.substring(F.sha2(F.to_json(F.struct(*df.columns)), 256), 1, 15), 16, 10)
                           .cast("decimal(38,0)"))).collect()[0][0]


def section(title, text=""):
    out.append(f"\n## {title}\n")
    if text:
        out.append(text + "\n")


def main():
    work = tempfile.mkdtemp(prefix="so_demo_")
    landing = os.path.join(work, "landing")
    shutil.copytree(os.path.join(ROOT, "data", "demo_landing"), landing)
    spark = local_spark("so-lakehouse-demo", work_dir=os.path.join(work, "spark"))
    cfg = PipelineConfig.for_local(landing, pii_salt="demo-salt")
    quiet = lambda *a, **k: None  # noqa: E731

    out.append("# Phase 2 demo run (local Spark 3.5 + Delta Lake 3.2)\n")
    out.append(f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC by `scripts/demo_local_run.py` on the five "
               "batches in `data/demo_landing`. Every number below comes from the audit log of that run.\n")

    setup(spark, cfg, reset=True, log=quiet)

    a = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    section("1. Initial Raw-to-Bronze run (mode = incremental)",
            "Every file of every batch is one log row in `so_ops.pipeline_execution_logs`.")
    out.append(audit_table(a))
    b = run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet)
    section("2. Initial Bronze-to-Silver run (mode = incremental)",
            "Each Silver table is one MERGE INTO step, logged with inserted and updated row counts.")
    out.append(audit_table(b))
    section("3. Row counts after the initial load")
    c_b, c_s = counts(spark, cfg, BRONZE), counts(spark, cfg, SILVER)
    out.append(md_table(["Table", "Rows"], list(c_b.items()) + list(c_s.items())))

    fp_before = {t: fingerprint(spark, cfg, t) for t in SILVER}
    a2 = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    b2 = run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet)
    section("4. Re-running the incremental pipeline",
            "Nothing new has landed, so both layers log SKIPPED and write nothing.")
    out.append(audit_table(a2) + "\n\n" + audit_table(b2))

    all_ids = ",".join(r["batch_id"] for r in spark.table(cfg.table("ops.batch_registry")).collect())
    a3 = run_raw_to_bronze(spark, cfg, mode="backfill", batch_ids=all_ids, echo=quiet)
    b3 = run_bronze_to_silver(spark, cfg, mode="backfill", batch_ids=all_ids, echo=quiet)
    fp_after = {t: fingerprint(spark, cfg, t) for t in SILVER}
    section("5. Idempotency proof: force-reprocess ALL batches",
            "Every batch is reloaded into Bronze (replaceWhere replaces the batch's own rows) and merged into "
            "Silver again. Bronze row counts stay the same and every Silver MERGE inserts and updates 0 rows. "
            "A checksum over every column of every Silver table, including load_timestamp, is unchanged.")
    out.append(audit_table(b3))
    out.append("\n" + md_table(["Table", "Rows before", "Rows after", "Checksum identical"],
                               [(t, c_s[t], spark.table(cfg.table(t)).count(), fp_before[t] == fp_after[t])
                                for t in SILVER]))
    out.append("\n" + md_table(["Bronze table", "Rows before", "Rows after"],
                               [(t, c_b[t], spark.table(cfg.table(t)).count()) for t in BRONZE]))

    a4 = run_raw_to_bronze(spark, cfg, mode="backfill", start_date="2026-09-25", end_date="2026-09-25", echo=quiet)
    b4 = run_bronze_to_silver(spark, cfg, mode="backfill", start_date="2026-09-25", end_date="2026-09-25",
                              echo=quiet)
    section("6. Date-range backfill: start_date = end_date = 2026-09-25",
            "Only the batches extracted on that date are selected: the two Phase 1 samples. Replaying these OLD "
            "batches after newer ones updates nothing, because Silver only accepts newer versions.")
    out.append(audit_table(a4) + "\n\n" + audit_table(b4))

    base = os.path.join(landing, "incremental", "incr_20261007T160033Z", "questions.jsonl")
    make_drift_batch(landing, base)
    a5 = run_raw_to_bronze(spark, cfg, mode="incremental", echo=quiet)
    section("7. Schema drift batch",
            "A synthetic batch with a new column, a new nested field, a type change, a truncated line and a "
            "record without its key. The run succeeds: 2 rows load, 3 go to quarantine, and the new column "
            "is added to Bronze through mergeSchema.")
    out.append(audit_table(a5))
    ev = spark.table(cfg.table("ops.schema_drift_events")).where("_batch_id = 'incr_20261008T000000Z'")
    out.append("\n" + md_table(["Drift type", "Column", "Records", "Action", "Example"],
                               [(r.drift_type, r.column_path, r.record_count, r.action_taken,
                                 (r.example_value or "")[:60]) for r in ev.orderBy("drift_type").collect()]))
    qr = spark.table(cfg.table("bronze.quarantine_records")).where("_batch_id = 'incr_20261008T000000Z'")
    out.append("\n" + md_table(["Quarantine reason", "Error detail"],
                               [(r.reason, (r.error_detail or "")[:110]) for r in qr.orderBy("reason").collect()]))
    b5 = run_bronze_to_silver(spark, cfg, mode="incremental", echo=quiet)
    out.append("\nThe Silver run that follows processes only the new batch:\n\n" + audit_table(b5))

    broken = os.path.join(landing, "incremental", "incr_20261009T000000Z")
    os.makedirs(broken, exist_ok=True)
    open(os.path.join(broken, "questions.jsonl"), "w").close()
    a6 = run_raw_to_bronze(spark, cfg, mode="incremental", fail_on_error=False, echo=quiet)
    section("8. A broken batch (no _manifest.json)",
            "The failure is logged with its error message and the run continues with other batches. "
            "With fail_on_error = true, the default, the notebook then raises so the job shows as failed.")
    out.append(audit_table(a6))

    logs = spark.table(cfg.table("ops.pipeline_execution_logs"))
    section("9. Audit log totals for this demo")
    out.append(md_table(["Layer", "Status", "Log rows"],
                        [(r.layer, r.status, r["count"]) for r in
                         logs.groupBy("layer", "status").count().orderBy("layer", "status").collect()]))

    with open(os.path.join(ROOT, "docs", "phase2_demo_results.md"), "w") as f:
        f.write("\n".join(out) + "\n")
    print("wrote docs/phase2_demo_results.md")
    spark.stop()
    shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
