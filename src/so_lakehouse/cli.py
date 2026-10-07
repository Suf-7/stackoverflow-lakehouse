"""Command-line runner (local development, CI, or a Databricks Python-script task).

Examples
  # one-time setup (add --reset to drop and recreate all tables)
  python -m so_lakehouse.cli setup --landing-root data/demo_landing

  # standard incremental run: whatever is new in landing
  python -m so_lakehouse.cli raw-to-bronze   --mode incremental
  python -m so_lakehouse.cli bronze-to-silver --mode incremental

  # backfill: reprocess a date range or specific batches
  python -m so_lakehouse.cli raw-to-bronze   --mode backfill --start-date 2026-09-25 --end-date 2026-09-30
  python -m so_lakehouse.cli bronze-to-silver --mode backfill --batch-ids full_2025-01-01_2025-04-01

  # both layers in one go
  python -m so_lakehouse.cli all --mode incremental
"""
import argparse
import sys

from .bronze import run_raw_to_bronze
from .config import PipelineConfig
from .ddl import setup
from .silver import run_bronze_to_silver


def build_parser():
    p = argparse.ArgumentParser(prog="so_lakehouse")
    p.add_argument("command", choices=["setup", "raw-to-bronze", "bronze-to-silver", "all"])
    p.add_argument("--target", choices=["local", "databricks"], default="local")
    p.add_argument("--catalog", help="Unity Catalog name (databricks target)")
    p.add_argument("--landing-root", default="data/demo_landing", help="folder with full/ and incremental/")
    p.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    p.add_argument("--load-type", choices=["all", "full", "incremental"], default="all")
    p.add_argument("--batch-ids", help="comma-separated batch ids (folder names)")
    p.add_argument("--start-date", help="YYYY-MM-DD, inclusive, on batch extraction date")
    p.add_argument("--end-date", help="YYYY-MM-DD, inclusive")
    p.add_argument("--landing-path", help="explicit batch folder(s), comma-separated (Raw-to-Bronze)")
    p.add_argument("--drift-mode", choices=["evolve", "rescue"])
    p.add_argument("--reset", action="store_true", help="setup: drop and recreate every table")
    p.add_argument("--no-fail", action="store_true", help="exit 0 even if a step failed (it is still logged)")
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.target == "databricks":
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        cfg = PipelineConfig.for_databricks(catalog=args.catalog, drift_mode=args.drift_mode)
    else:
        from .spark_session import local_spark
        spark = local_spark()
        cfg = PipelineConfig.for_local(args.landing_root, drift_mode=args.drift_mode)

    fail = not args.no_fail
    if args.command == "setup":
        setup(spark, cfg, reset=args.reset)
    if args.command in ("raw-to-bronze", "all"):
        run_raw_to_bronze(spark, cfg, mode=args.mode, load_type=args.load_type, batch_ids=args.batch_ids,
                          start_date=args.start_date, end_date=args.end_date, landing_path=args.landing_path,
                          fail_on_error=fail)
    if args.command in ("bronze-to-silver", "all"):
        silver_ids = args.batch_ids
        run_bronze_to_silver(spark, cfg, mode=args.mode, batch_ids=silver_ids, start_date=args.start_date,
                             end_date=args.end_date, fail_on_error=fail)
    return 0


if __name__ == "__main__":
    sys.exit(main())
