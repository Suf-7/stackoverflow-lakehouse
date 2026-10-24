"""Silver-to-Gold: build the star schema and the dashboard aggregates.

Gold is the current analytical state, not another copy of Silver history.
Every run reads the whole Silver layer, drops deleted questions and their
answers, and MERGEs the result. A row is updated only when its business
columns changed (`record_hash`). A row that is no longer in Silver is
deleted (`WHEN NOT MATCHED BY SOURCE`), so a question deleted since the
last run disappears from Gold. Running it again on unchanged Silver
inserts 0, updates 0 and deletes 0.

The job runs this step after Bronze-to-Silver, so a new incremental batch
that lands in Silver is refreshed into Gold in the same pipeline.
"""
from datetime import date

from pyspark.sql import Window
from pyspark.sql import functions as F

from .audit import AuditLogger
from .delta_utils import merge_sql, run_merge
from .schemas import TABLES
from .silver import conform, record_hash

LAYER = "Silver-to-Gold"
CHATGPT_LAUNCH = date(2022, 11, 30)

# The 18 collected tags, grouped the way the Phase 1 dashboard describes them.
TAG_CATEGORIES = {
    "pandas": "DataFrame and query engines",
    "numpy": "DataFrame and query engines",
    "python-polars": "DataFrame and query engines",
    "duckdb": "DataFrame and query engines",
    "apache-spark": "Big data",
    "pyspark": "Big data",
    "scikit-learn": "Machine learning",
    "tensorflow": "Machine learning",
    "pytorch": "Machine learning",
    "matplotlib": "Visualisation and BI",
    "powerbi": "Visualisation and BI",
    "tableau-api": "Visualisation and BI",
    "airflow": "Data platforms and orchestration",
    "dbt": "Data platforms and orchestration",
    "databricks": "Data platforms and orchestration",
    "snowflake-cloud-data-platform": "Data platforms and orchestration",
    "sql": "Languages",
    "r": "Languages",
}

# Tables compacted after each refresh. Date and tag are the columns dashboards filter on.
OPTIMIZE_KEYS = {
    "gold.fact_question": ["created_date_key"],
    "gold.fact_answer": ["created_date_key"],
    "gold.bridge_question_tag": ["tag"],
    "gold.agg_tag_month": ["month_start"],
}


def _hours(start_col, end_col):
    """Hours between two timestamps. Null when either side is null."""
    return ((F.unix_timestamp(F.col(end_col)) - F.unix_timestamp(F.col(start_col))) / F.lit(3600.0)).cast("double")


def _date_key(column):
    return F.date_format(F.col(column), "yyyyMMdd").cast("int")


def _reputation_band(column="reputation"):
    c = F.col(column)
    return (F.when(c.isNull(), "unknown")
            .when(c < 100, "under 100")
            .when(c < 1000, "100 to 999")
            .when(c < 10000, "1,000 to 9,999")
            .otherwise("10,000 or more"))


def _hash_and_stamp(df, logical):
    """Add record_hash over the business columns, then load_timestamp.

    load_timestamp is not part of the hash, so a rerun that changes nothing
    does not rewrite the row just to bump the clock.
    """
    names = [f.name for f in TABLES[logical][2].fields if f.name not in ("record_hash", "load_timestamp")]
    return (df.withColumn("record_hash", record_hash(names))
            .withColumn("load_timestamp", F.current_timestamp()))


def _upsert(spark, cfg, df, logical, rec, rows_read, message):
    """MERGE the full current result into one Gold table, deleting rows that fell out."""
    schema = TABLES[logical][2]
    keys = TABLES[logical][3]
    ready = conform(_hash_and_stamp(df, logical), schema)
    ready.createOrReplaceTempView("src_gold")
    cols = [f.name for f in schema.fields]
    upd = {c: f"s.{c}" for c in cols if c not in keys}
    sql = merge_sql(cfg.table(logical), "src_gold", keys, cols, upd,
                    matched_condition="NOT (s.record_hash <=> t.record_hash)")
    sql += "\nWHEN NOT MATCHED BY SOURCE THEN DELETE"
    metrics = run_merge(spark, cfg.table(logical), sql)
    rec.rows_read = rows_read
    rec.rows_inserted = metrics["inserted"]
    rec.rows_updated = metrics["updated"]
    rec.rows_deleted = metrics["deleted"]
    rec.message = message
    return metrics


def _alive(spark, cfg):
    """Silver rows Gold is allowed to publish: deleted questions are removed, and so are their answers."""
    questions = (spark.table(cfg.table("silver.questions"))
                 .where(F.col("created_at").isNotNull() & ~F.coalesce(F.col("is_deleted"), F.lit(False))))
    answers = (spark.table(cfg.table("silver.answers"))
               .where(~F.coalesce(F.col("is_deleted"), F.lit(False)))
               .join(questions.select("question_id"), "question_id", "left_semi"))
    tags = (spark.table(cfg.table("silver.question_tags"))
            .join(questions.select("question_id"), "question_id", "left_semi"))
    return questions, answers, tags


def _question_base(questions, answers):
    """One row per question with the response-time metrics the facts and aggregates share."""
    first = answers.groupBy("question_id").agg(F.min("created_at").alias("first_answer_at"))
    accepted = (answers.where(F.col("is_accepted"))
                .groupBy("question_id").agg(F.min("created_at").alias("accepted_at")))
    return (questions.join(first, "question_id", "left").join(accepted, "question_id", "left")
            .withColumn("hours_to_first_answer", _hours("created_at", "first_answer_at"))
            .withColumn("hours_to_accepted_answer", _hours("created_at", "accepted_at"))
            .withColumn("has_any_answer", F.col("first_answer_at").isNotNull())
            .withColumn("has_accepted_answer", F.col("accepted_at").isNotNull())
            .withColumn("answered_within_24h",
                        F.col("hours_to_first_answer").isNotNull() & (F.col("hours_to_first_answer") <= 24))
            .withColumn("created_date_key", _date_key("created_at"))
            .withColumn("month_start", F.trunc("created_at", "month")))


def _answers_base(questions, answers):
    order = Window.partitionBy("question_id").orderBy(F.col("created_at").asc_nulls_last(), F.col("answer_id").asc())
    return (answers.join(questions.select("question_id", F.col("created_at").alias("question_created_at")),
                         "question_id")
            .withColumn("is_first_answer", F.row_number().over(order) == 1)
            .withColumn("hours_after_question", _hours("question_created_at", "created_at"))
            .withColumn("created_date_key", _date_key("created_at")))


# ============================================================================ dimensions
def step_dim_date(spark, cfg, questions, answers, rec):
    bounds = (questions.select(F.to_date("created_at").alias("d"))
              .union(answers.select(F.to_date("created_at")))
              .agg(F.min("d").alias("lo"), F.max("d").alias("hi"))
              .collect()[0])
    start = min(bounds["lo"] or CHATGPT_LAUNCH, date(2022, 11, 1))
    end = max(bounds["hi"] or CHATGPT_LAUNCH, CHATGPT_LAUNCH)
    days = spark.sql(
        f"SELECT explode(sequence(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS full_date")
    dim = (days
           .withColumn("date_key", _date_key("full_date"))
           .withColumn("year", F.year("full_date"))
           .withColumn("quarter", F.quarter("full_date"))
           .withColumn("month", F.month("full_date"))
           .withColumn("month_name", F.date_format("full_date", "MMMM"))
           .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
           .withColumn("week_of_year", F.weekofyear("full_date"))
           .withColumn("day_of_month", F.dayofmonth("full_date"))
           .withColumn("day_of_week", F.dayofweek("full_date"))
           .withColumn("day_name", F.date_format("full_date", "EEEE"))
           .withColumn("is_after_chatgpt", F.col("full_date") >= F.lit(CHATGPT_LAUNCH)))
    _upsert(spark, cfg, dim, "gold.dim_date", rec, dim.count(),
            f"calendar {start.isoformat()} to {end.isoformat()}")


def step_dim_tag(spark, cfg, tags, rec):
    observed = tags.select("tag").distinct()
    tracked = spark.createDataFrame([(t,) for t in cfg.tracked_tags], "tag string")
    categories = spark.createDataFrame(list(TAG_CATEGORIES.items()), "tag string, category string")
    dim = (observed.union(tracked).distinct()
           .join(categories, "tag", "left")
           .withColumn("category", F.coalesce(F.col("category"), F.lit("Other")))
           .withColumn("is_tracked", F.col("tag").isin(list(cfg.tracked_tags))))
    _upsert(spark, cfg, dim, "gold.dim_tag", rec, observed.count(),
            f"{len(cfg.tracked_tags)} tracked tags, other tags categorised as Other")


def step_dim_user(spark, cfg, questions, answers, rec):
    posts = (questions.select(F.col("owner_user_key").alias("user_key"), "created_at")
             .union(answers.select(F.col("owner_user_key").alias("user_key"), "created_at"))
             .where(F.col("user_key").isNotNull()))
    first_post = posts.groupBy("user_key").agg(F.min("created_at").alias("first_post_at"))
    users = spark.table(cfg.table("silver.users"))
    dim = (users.join(first_post, "user_key", "left")
           .withColumn("reputation_band", _reputation_band())
           .withColumn("cohort_month", F.trunc(F.coalesce(F.col("first_post_at"), F.col("first_seen_at")), "month"))
           .select("user_key", "user_type", "reputation", "reputation_band", "accept_rate", "first_seen_at",
                   "cohort_month"))
    _upsert(spark, cfg, dim, "gold.dim_user", rec, users.count(), "reputation band and cohort month derived")


# ============================================================================ facts and bridge
def step_fact_question(spark, cfg, base, rec):
    fact = base.select(
        "question_id", "created_date_key", "owner_user_key", "title", "score", "view_count", "answer_count",
        "has_any_answer", "has_accepted_answer", "is_closed", "closed_reason_code",
        "hours_to_first_answer", "hours_to_accepted_answer", "answered_within_24h")
    deleted = spark.table(cfg.table("silver.questions")).where(F.coalesce(F.col("is_deleted"), F.lit(False))).count()
    _upsert(spark, cfg, fact, "gold.fact_question", rec, base.count() + deleted,
            f"aggregated {base.count()} current questions; excluded {deleted} deleted")


def step_fact_answer(spark, cfg, answers, rec):
    fact = answers.select("answer_id", "question_id", "created_date_key", "owner_user_key", "score",
                          "is_accepted", "is_first_answer", "hours_after_question")
    _upsert(spark, cfg, fact, "gold.fact_answer", rec, answers.count(),
            f"aggregated {answers.count()} current answers")


def step_bridge(spark, cfg, tags, rec):
    tracked = list(cfg.tracked_tags)
    bridge = (tags.select("question_id", "tag")
              .dropDuplicates(["question_id", "tag"])
              .withColumn("is_tracked", F.col("tag").isin(tracked)))
    _upsert(spark, cfg, bridge, "gold.bridge_question_tag", rec, tags.count(),
            f"aggregated {bridge.count()} question-tag pairs")


# ============================================================================ dashboard aggregates
def step_agg_tag_month(spark, cfg, base, answers, tags, rec):
    tracked = tags.where(F.col("tag").isin(list(cfg.tracked_tags))).select("question_id", "tag").dropDuplicates()
    questions = base.join(tracked, "question_id")
    month_size = (questions.select("month_start", "question_id").distinct()
                  .groupBy("month_start").agg(F.count("*").alias("tracked_questions")))
    by_tag = questions.groupBy("tag", "month_start").agg(
        F.countDistinct("question_id").alias("questions"),
        F.sum((~F.col("has_any_answer")).cast("int")).alias("unanswered"),
        F.round(F.expr("percentile_approx(hours_to_first_answer, 0.5)"), 4).alias("median_hours_to_first_answer"),
        F.round(F.expr("percentile_approx(hours_to_first_answer, 0.9)"), 4).alias("p90_hours_to_first_answer"),
        F.sum(F.col("is_closed").cast("int")).alias("closed"),
        F.sum(F.col("answered_within_24h").cast("int")).alias("within_24h"),
    )
    # Answers belonging to questions created that month, so the count sits next to questions posted.
    answer_counts = (answers
                     .join(tracked.select("question_id", "tag"), "question_id")
                     .join(base.select("question_id", "month_start"), "question_id")
                     .groupBy("tag", "month_start").agg(F.count("*").alias("answers")))
    categories = spark.createDataFrame(list(TAG_CATEGORIES.items()), "tag string, category string")
    agg = (by_tag.join(answer_counts, ["tag", "month_start"], "left")
           .join(month_size, "month_start")
           .join(categories, "tag", "left")
           .withColumn("answers", F.coalesce(F.col("answers"), F.lit(0)))
           .withColumn("year_month", F.date_format("month_start", "yyyy-MM"))
           .withColumn("share_of_tracked_questions",
                       (F.col("questions") / F.col("tracked_questions") * 100).cast("double"))
           .withColumn("pct_unanswered", (F.col("unanswered") / F.col("questions") * 100).cast("double"))
           .withColumn("pct_closed", (F.col("closed") / F.col("questions") * 100).cast("double"))
           .withColumn("pct_answered_within_24h", (F.col("within_24h") / F.col("questions") * 100).cast("double"))
           .select("tag", "month_start", "year_month", "category", "questions", "answers",
                   "share_of_tracked_questions", "pct_unanswered", "median_hours_to_first_answer",
                   "p90_hours_to_first_answer", "pct_closed", "pct_answered_within_24h"))
    _upsert(spark, cfg, agg, "gold.agg_tag_month", rec, questions.count(),
            f"aggregated {questions.count()} question-tag rows into {agg.count()} tag-months")


def step_agg_tag_pairs(spark, cfg, tags, rec):
    tracked = (tags.where(F.col("tag").isin(list(cfg.tracked_tags)))
               .select("question_id", "tag").dropDuplicates(["question_id", "tag"]))
    left = tracked.select("question_id", F.col("tag").alias("tag_a"))
    right = tracked.select("question_id", F.col("tag").alias("tag_b"))
    pairs = (left.join(right, "question_id").where(F.col("tag_a") < F.col("tag_b"))
             .groupBy("tag_a", "tag_b").agg(F.countDistinct("question_id").alias("question_count")))
    categories = spark.createDataFrame(list(TAG_CATEGORIES.items()), "tag string, category string")
    agg = (pairs
           .join(categories.select(F.col("tag").alias("tag_a"), F.col("category").alias("category_a")), "tag_a")
           .join(categories.select(F.col("tag").alias("tag_b"), F.col("category").alias("category_b")), "tag_b"))
    _upsert(spark, cfg, agg, "gold.agg_tag_pairs", rec, tracked.count(),
            f"aggregated {agg.count()} tracked tag pairs")


def step_agg_user_cohort(spark, cfg, questions, answers, rec):
    posts = (questions.select(F.col("owner_user_key").alias("user_key"), "created_at")
             .unionByName(answers.select(F.col("owner_user_key").alias("user_key"), "created_at"))
             .where(F.col("user_key").isNotNull())
             .withColumn("activity_month", F.trunc("created_at", "month")))
    users = posts.groupBy("user_key").agg(F.min("activity_month").alias("cohort_month"))
    activity = posts.select("user_key", "activity_month").distinct().join(users, "user_key")
    active = (activity
              .withColumn("months_since", F.months_between("activity_month", "cohort_month").cast("int"))
              .where(F.col("months_since") >= 0)
              .groupBy("cohort_month", "months_since").agg(F.countDistinct("user_key").alias("users_active")))
    sizes = users.groupBy("cohort_month").agg(F.count("*").alias("users_in_cohort"))
    latest = posts.agg(F.max("activity_month").alias("latest")).collect()[0]["latest"]
    if latest is None:
        grid = spark.createDataFrame([], "cohort_month date, months_since int, users_in_cohort long, users_active long")
    else:
        grid = (sizes
                .withColumn("max_since", F.months_between(F.lit(latest), F.col("cohort_month")).cast("int"))
                .withColumn("months_since", F.explode(F.sequence(F.lit(0), F.greatest(F.col("max_since"), F.lit(0)))))
                .select("cohort_month", "months_since", "users_in_cohort"))
    agg = (grid.join(active, ["cohort_month", "months_since"], "left")
           .withColumn("users_active", F.coalesce(F.col("users_active"), F.lit(0)))
           .withColumn("share_still_active",
                       (F.col("users_active") / F.col("users_in_cohort")).cast("double")))
    _upsert(spark, cfg, agg, "gold.agg_user_cohort", rec, users.count(),
            f"aggregated {users.count()} users into {agg.count()} cohort cells")


# ============================================================================ compaction
def step_optimize(spark, cfg, rec):
    """Compact the tables the dashboard scans.

    On Databricks the tables are liquid-clustered on the filter column.
    Local Delta has no liquid clustering, so the same columns are used as a ZORDER.
    """
    done = []
    for logical, keys in OPTIMIZE_KEYS.items():
        table = cfg.table(logical)
        cols = ", ".join(keys)
        if cfg.catalog:
            spark.sql(f"ALTER TABLE {table} CLUSTER BY ({cols})")
            spark.sql(f"OPTIMIZE {table}")
            done.append(f"{logical} CLUSTER BY ({cols})")
        else:
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({cols})")
            done.append(f"{logical} ZORDER BY ({cols})")
    rec.rows_read = 0
    rec.message = "OPTIMIZE " + "; ".join(done)


# ============================================================================ public entry point
def run_silver_to_gold(spark, cfg, fail_on_error=True, optimize=True, echo=print):
    """Refresh every Gold table from the current Silver layer.

    There is no batch-id scope: a fact row is the current version of a question,
    so the step always rebuilds from Silver and lets MERGE apply the difference.
    """
    audit = AuditLogger(spark, cfg, "silver_to_gold", "refresh", echo=echo)
    questions, answers, tags = _alive(spark, cfg)
    base = _question_base(questions, answers)
    answer_rows = _answers_base(questions, answers)
    # Do not cache these frames. Databricks serverless rejects cache() and unpersist().

    steps = [
        ("dim_date", "gold.dim_date", lambda rec: step_dim_date(spark, cfg, questions, answers, rec)),
        ("dim_tag", "gold.dim_tag", lambda rec: step_dim_tag(spark, cfg, tags, rec)),
        ("dim_user", "gold.dim_user", lambda rec: step_dim_user(spark, cfg, questions, answers, rec)),
        ("fact_question", "gold.fact_question", lambda rec: step_fact_question(spark, cfg, base, rec)),
        ("fact_answer", "gold.fact_answer", lambda rec: step_fact_answer(spark, cfg, answer_rows, rec)),
        ("bridge_question_tag", "gold.bridge_question_tag", lambda rec: step_bridge(spark, cfg, tags, rec)),
        ("agg_tag_month", "gold.agg_tag_month",
         lambda rec: step_agg_tag_month(spark, cfg, base, answer_rows, tags, rec)),
        ("agg_tag_pairs", "gold.agg_tag_pairs", lambda rec: step_agg_tag_pairs(spark, cfg, tags, rec)),
        ("agg_user_cohort", "gold.agg_user_cohort",
         lambda rec: step_agg_user_cohort(spark, cfg, questions, answers, rec)),
    ]
    if optimize:
        steps.append(("optimize", "*", lambda rec: step_optimize(spark, cfg, rec)))

    for entity, target, fn in steps:
        target_table = cfg.table(target) if target != "*" else "-"
        with audit.step(LAYER, entity, "current Silver layer", target_table) as rec:
            fn(rec)

    echo(f"{LAYER} summary: {audit.summary()}")
    if fail_on_error and audit.failures:
        raise RuntimeError(f"{len(audit.failures)} Silver-to-Gold step(s) failed; see "
                           f"{cfg.table('ops.pipeline_execution_logs')} run_id={audit.run_id}")
    return audit
