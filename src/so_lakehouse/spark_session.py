"""Spark session for LOCAL development and tests (Delta Lake + persistent metastore).

On Databricks you never call this: notebooks already have `spark`.
Requires Java 17 and `pip install -r requirements-dev.txt`.
"""
import os

from .config import REPO_ROOT


def local_spark(app_name="so-lakehouse-local", work_dir=None):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    work_dir = os.path.abspath(work_dir or os.path.join(REPO_ROOT, ".local"))
    os.makedirs(work_dir, exist_ok=True)
    builder = (SparkSession.builder.appName(app_name).master("local[4]")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", os.path.join(work_dir, "spark-warehouse"))
               .config("javax.jdo.option.ConnectionURL",
                       f"jdbc:derby:;databaseName={os.path.join(work_dir, 'metastore_db')};create=true")
               .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={work_dir}")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "8")
               .config("spark.ui.enabled", "false")
               .config("spark.ui.showConsoleProgress", "false")
               .enableHiveSupport())
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
