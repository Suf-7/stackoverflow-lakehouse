"""Bronze-to-Silver: cleanse, cast, de-duplicate, pseudonymise and MERGE.

Idempotency rule used by every MERGE below
------------------------------------------
Each Silver row remembers the version it came from (source_extracted_at) and a
hash of its business columns (record_hash). A source row only UPDATES the
target when it is NEWER and DIFFERENT:

    WHEN MATCHED AND s.source_extracted_at > t.source_extracted_at
                 AND s.record_hash <> t.record_hash THEN UPDATE ...
    WHEN NOT MATCHED THEN INSERT ...

So re-running the same Bronze batches (same versions, same hashes) inserts 0
and updates 0 rows, and replaying an OLD batch after a newer one cannot
overwrite newer data. Silver keys are unique by construction because the
source is de-duplicated to one row per key before every MERGE.
"""
import html

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from .audit import AuditLogger, utc_now
from .batches import parse_list
from .delta_utils import merge_sql, run_merge
from .schemas import (BATCH_REGISTRY, SILVER_ANSWERS, SILVER_DQ_QUARANTINE, SILVER_POST_TEXT, SILVER_QUESTION_TAGS,
                      SILVER_QUESTIONS, SILVER_SNAPSHOTS, SILVER_USERS)

LAYER = "Bronze-to-Silver"

# PII masking patterns (Java regex) for the restricted text table
MASKS = [
    (r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", "[EMAIL]"),
    (r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}(?:25[0-5]|2[0-4]\d|1?\d?\d)\b", "[IP]"),
    (r"(?i)\b(password|passwd|pwd|secret|api[_-]?key|token)(\s*[=:]\s*['\"]?)[^\s'\"<&]{4,}", "$1$2[SECRET]"),
    (r"(?i)\b(jdbc:[a-z0-9]+://|mongodb(?:\+srv)?://|postgres(?:ql)?://|mysql://)[^\s'\"<]+", "$1[CONNECTION]"),
]
MASK_TOKEN = r"\[(EMAIL|IP|SECRET|CONNECTION)\]"


# ============================================================================ helpers
def _html_unescape():
    return F.udf(lambda s: html.unescape(s) if s is not None else None, StringType())


def ts(col_name):
    """Unix epoch seconds (bigint) -> TIMESTAMP. The main type cast into Silver."""
    return F.timestamp_seconds(F.col(col_name).cast("bigint"))


def occurrences(col, regex):
    return (F.size(F.split(F.coalesce(col, F.lit("")), regex)) - 1).cast("int")


def user_key(cfg, user_id_col):
    """Salted SHA-256 of the numeric user id: stable pseudonym, not reversible without the salt."""
    return F.when(user_id_col.isNotNull(),
                  F.sha2(F.concat_ws(":", F.lit(cfg.salt()), user_id_col.cast("string")), 256))


def latest_per_key(df, keys, order_cols):
    """Keep one row per key: the newest extraction (MERGE needs unique source keys)."""
    w = Window.partitionBy(*keys).orderBy(*order_cols)
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def conform(df, schema):
    """Explicit cast of every column to the Silver contract type, in contract order."""
    return df.select(*[F.col(f.name).cast(f.dataType).alias(f.name) for f in schema.fields])


def record_hash(cols):
    return F.sha2(F.to_json(F.struct(*[F.col(c) for c in cols])), 256)


def bronze(spark, cfg, entity, batch_ids):
    return spark.table(cfg.table(f"bronze.{entity}")).where(F.col("_batch_id").isin(batch_ids))


def split_dq(df, rules):
    """Return (valid_df, failed_df_with_rule). rules: list of (name, condition that must hold)."""
    failed_rule = F.coalesce(*[F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules])
    tagged = df.withColumn("_failed_rule", failed_rule)
    return tagged.where(F.col("_failed_rule").isNull()).drop("_failed_rule"), tagged.where(
        F.col("_failed_rule").isNotNull())


def quarantine_dq(spark, cfg, failed, entity, key_col):
    payload_cols = [c for c in failed.columns if c != "_failed_rule"]
    src = failed.select(
        F.sha2(F.concat_ws("|", F.lit(entity), F.col(key_col).cast("string"), F.col("_failed_rule"),
                           F.col("source_batch_id")), 256).alias("dq_id"),
        F.lit(entity).alias("entity"), F.col(key_col).cast("string").alias("record_key"),
        F.col("_failed_rule").alias("rule"), F.col("source_batch_id"),
        F.to_json(F.struct(*payload_cols)).alias("payload"), F.current_timestamp().alias("load_timestamp"),
    ).dropDuplicates(["dq_id"])
    n = src.count()
    if n:
        src.createOrReplaceTempView("src_dq")
        cols = [f.name for f in SILVER_DQ_QUARANTINE.fields]
        run_merge(spark, cfg.table("silver.dq_quarantine"),
                  merge_sql(cfg.table("silver.dq_quarantine"), "src_dq", ["dq_id"], cols))
    return n


# ============================================================================ entity transforms
QUESTION_BUSINESS_COLS = [f.name for f in SILVER_QUESTIONS.fields if f.name not in (
    "is_deleted", "deleted_detected_at", "metrics_as_of", "source_batch_id", "source_extracted_at",
    "record_hash", "first_loaded_at", "load_timestamp")]
ANSWER_BUSINESS_COLS = [f.name for f in SILVER_ANSWERS.fields if f.name not in (
    "is_deleted", "deleted_detected_at", "source_batch_id", "source_extracted_at", "record_hash",
    "first_loaded_at", "load_timestamp")]


def transform_questions(raw, cfg):
    unescape = _html_unescape()
    tracked = F.array(*[F.lit(t) for t in cfg.tracked_tags]) if cfg.tracked_tags else F.array().cast("array<string>")
    df = raw.select(
        F.col("question_id").cast("bigint").alias("question_id"),
        unescape(F.col("title")).alias("title"),
        ts("creation_date").alias("created_at"),
        ts("last_activity_date").alias("last_activity_at"),
        ts("last_edit_date").alias("last_edit_at"),
        ts("closed_date").alias("closed_at"),
        F.col("closed_reason"),
        F.regexp_replace(F.lower(F.regexp_replace(F.trim("closed_reason"), r"[^A-Za-z0-9]+", "_")), r"^_+|_+$", "")
        .alias("closed_reason_code"),
        F.col("closed_date").isNotNull().alias("is_closed"),
        F.col("score").cast("int").alias("score"),
        F.col("view_count").cast("int").alias("view_count"),
        F.col("answer_count").cast("int").alias("answer_count"),
        F.col("is_answered").cast("boolean").alias("is_answered_api"),
        (F.coalesce(F.col("answer_count"), F.lit(0)) > 0).alias("has_any_answer"),
        F.col("accepted_answer_id").cast("bigint").alias("accepted_answer_id"),
        F.col("accepted_answer_id").isNotNull().alias("has_accepted_answer"),
        F.col("bounty_amount").cast("int").alias("bounty_amount"),
        F.coalesce(F.col("tags"), F.array().cast("array<string>")).alias("tags"),
        F.array_intersect(F.coalesce(F.col("tags"), F.array().cast("array<string>")), tracked).alias("tracked_tags"),
        user_key(cfg, F.col("owner.user_id")).alias("owner_user_key"),           # PII: hashed
        F.col("owner.user_type").alias("owner_user_type"),
        F.col("owner.reputation").cast("int").alias("owner_reputation"),
        # PII: owner.display_name, owner.profile_image, owner.link, owner.account_id are dropped here
        F.length("body").cast("int").alias("body_length"),
        occurrences(F.col("body"), "<pre").alias("code_block_count"),
        occurrences(F.col("body"), "<code").alias("inline_code_count"),
        occurrences(F.col("body"), "<a ").alias("link_count"),
        occurrences(F.col("body"), "<img").alias("image_count"),
        F.col("migrated_from").isNotNull().alias("is_migrated"),
        F.col("content_license"),
        F.col("link").alias("question_url"),
        F.col("_batch_id").alias("source_batch_id"),
        F.col("_extracted_at").alias("source_extracted_at"),
    ).withColumn("title_length", F.length("title").cast("int"))
    df = (df.withColumn("is_deleted", F.lit(False))
          .withColumn("deleted_detected_at", F.lit(None).cast("timestamp"))
          .withColumn("metrics_as_of", F.col("source_extracted_at"))
          .withColumn("record_hash", record_hash(QUESTION_BUSINESS_COLS))
          .withColumn("first_loaded_at", F.current_timestamp())
          .withColumn("load_timestamp", F.current_timestamp()))
    return conform(df, SILVER_QUESTIONS)


def transform_answers(raw, cfg):
    df = raw.select(
        F.col("answer_id").cast("bigint").alias("answer_id"),
        F.col("question_id").cast("bigint").alias("question_id"),
        ts("creation_date").alias("created_at"),
        ts("last_activity_date").alias("last_activity_at"),
        ts("last_edit_date").alias("last_edit_at"),
        F.col("score").cast("int").alias("score"),
        F.col("is_accepted").cast("boolean").alias("is_accepted"),
        user_key(cfg, F.col("owner.user_id")).alias("owner_user_key"),
        F.col("owner.user_type").alias("owner_user_type"),
        F.col("owner.reputation").cast("int").alias("owner_reputation"),
        F.length("body").cast("int").alias("body_length"),
        occurrences(F.col("body"), "<pre").alias("code_block_count"),
        occurrences(F.col("body"), "<code").alias("inline_code_count"),
        occurrences(F.col("body"), "<a ").alias("link_count"),
        occurrences(F.col("body"), "<img").alias("image_count"),
        F.col("content_license"),
        F.col("_batch_id").alias("source_batch_id"),
        F.col("_extracted_at").alias("source_extracted_at"),
    )
    df = (df.withColumn("is_deleted", F.lit(False))
          .withColumn("deleted_detected_at", F.lit(None).cast("timestamp"))
          .withColumn("record_hash", record_hash(ANSWER_BUSINESS_COLS))
          .withColumn("first_loaded_at", F.current_timestamp())
          .withColumn("load_timestamp", F.current_timestamp()))
    return conform(df, SILVER_ANSWERS)


def question_dq_rules():
    """Data-quality rules for questions: (rule name, condition that must hold)."""
    return [
        ("created_at_not_null", F.col("created_at").isNotNull()),
        ("activity_not_before_creation", F.col("last_activity_at").isNull() | (F.col("last_activity_at") >= F.col("created_at"))),
        ("title_not_blank", F.length(F.trim(F.coalesce(F.col("title"), F.lit("")))) > 0),
    ]


def answer_dq_rules():
    """Data-quality rules for answers."""
    return [
        ("question_id_not_null", F.col("question_id").isNotNull()),
        ("created_at_not_null", F.col("created_at").isNotNull()),
        ("activity_not_before_creation", F.col("last_activity_at").isNull() | (F.col("last_activity_at") >= F.col("created_at"))),
    ]


# ============================================================================ steps
def step_questions(spark, cfg, ids, rec):
    raw = bronze(spark, cfg, "questions", ids)
    rec.rows_read = raw.count()
    latest = latest_per_key(raw, ["question_id"], [F.col("_extracted_at").desc(), F.col("last_activity_date").desc(),
                                                   F.col("_record_hash")])
    valid, failed = split_dq(transform_questions(latest, cfg), question_dq_rules())
    rec.rows_quarantined = quarantine_dq(spark, cfg, failed, "questions", "question_id")
    valid.createOrReplaceTempView("src_questions")

    metrics = ["score", "view_count", "answer_count", "is_answered_api", "has_any_answer"]
    keep = {"question_id", "first_loaded_at", "is_deleted", "deleted_detected_at", "metrics_as_of"} | set(metrics)
    cols = [f.name for f in SILVER_QUESTIONS.fields]
    upd = {c: f"s.{c}" for c in cols if c not in keep}
    for c in metrics:  # a newer snapshot may already hold fresher metrics
        upd[c] = f"CASE WHEN s.source_extracted_at >= t.metrics_as_of THEN s.{c} ELSE t.{c} END"
    upd["metrics_as_of"] = "GREATEST(s.source_extracted_at, t.metrics_as_of)"
    undelete = "t.is_deleted AND s.source_extracted_at > t.deleted_detected_at"
    upd["is_deleted"] = f"CASE WHEN {undelete} THEN false ELSE t.is_deleted END"
    upd["deleted_detected_at"] = f"CASE WHEN {undelete} THEN NULL ELSE t.deleted_detected_at END"
    m = run_merge(spark, cfg.table("silver.questions"), merge_sql(
        cfg.table("silver.questions"), "src_questions", ["question_id"], cols, upd,
        matched_condition="s.source_extracted_at > t.source_extracted_at AND s.record_hash <> t.record_hash"))
    rec.rows_inserted, rec.rows_updated = m["inserted"], m["updated"]
    rec.message = f"{latest.count()} unique questions after de-duplication"


def step_question_tags(spark, cfg, ids, rec):
    """Rebuild tag pairs only for questions whose CURRENT Silver version came from these batches."""
    unescape_free = latest_per_key(bronze(spark, cfg, "questions", ids), ["question_id"],
                                   [F.col("_extracted_at").desc(), F.col("last_activity_date").desc(),
                                    F.col("_record_hash")])
    src_q = unescape_free.select("question_id", "tags", F.col("_batch_id").alias("source_batch_id"),
                                 F.col("_extracted_at").alias("source_extracted_at"))
    current = spark.table(cfg.table("silver.questions")).select("question_id", "source_batch_id",
                                                                 "source_extracted_at")
    winners = src_q.join(current, ["question_id", "source_batch_id", "source_extracted_at"], "inner")
    tracked = F.array(*[F.lit(t) for t in cfg.tracked_tags])
    new_pairs = (winners.select("question_id", F.explode(F.coalesce("tags", F.array().cast("array<string>")))
                                .alias("tag"), "source_batch_id")
                 .dropDuplicates(["question_id", "tag"])
                 .withColumn("is_tracked_tag", F.array_contains(tracked, F.col("tag"))))
    existing = (spark.table(cfg.table("silver.question_tags"))
                .join(winners.select("question_id").distinct(), "question_id"))
    stale = (existing.join(new_pairs.select("question_id", "tag"), ["question_id", "tag"], "left_anti")
             .select("question_id", "tag", "is_tracked_tag", "source_batch_id"))
    src = (new_pairs.select("question_id", "tag", "is_tracked_tag", "source_batch_id")
           .withColumn("_action", F.lit("upsert"))
           .unionByName(stale.withColumn("_action", F.lit("delete")))
           .withColumn("load_timestamp", F.current_timestamp()))
    rec.rows_read = src.count()
    src.createOrReplaceTempView("src_question_tags")
    cols = [f.name for f in SILVER_QUESTION_TAGS.fields]
    t = cfg.table("silver.question_tags")
    sql = (f"MERGE INTO {t} AS t\nUSING src_question_tags AS s\n"
           f"ON t.question_id = s.question_id AND t.tag = s.tag\n"
           f"WHEN MATCHED AND s._action = 'delete' THEN DELETE\n"
           f"WHEN MATCHED AND s._action = 'upsert' AND t.is_tracked_tag <> s.is_tracked_tag THEN UPDATE SET "
           f"t.is_tracked_tag = s.is_tracked_tag, t.load_timestamp = s.load_timestamp\n"
           f"WHEN NOT MATCHED AND s._action = 'upsert' THEN INSERT ({', '.join(cols)}) "
           f"VALUES ({', '.join('s.' + c for c in cols)})")
    m = run_merge(spark, t, sql)
    rec.rows_inserted, rec.rows_updated, rec.rows_deleted = m["inserted"], m["updated"], m["deleted"]


def step_answers(spark, cfg, ids, rec):
    raw = bronze(spark, cfg, "answers", ids)
    rec.rows_read = raw.count()
    latest = latest_per_key(raw, ["answer_id"], [F.col("_extracted_at").desc(), F.col("last_activity_date").desc(),
                                                 F.col("_record_hash")])
    valid, failed = split_dq(transform_answers(latest, cfg), answer_dq_rules())
    rec.rows_quarantined = quarantine_dq(spark, cfg, failed, "answers", "answer_id")
    valid.createOrReplaceTempView("src_answers")
    cols = [f.name for f in SILVER_ANSWERS.fields]
    keep = {"answer_id", "first_loaded_at", "is_deleted", "deleted_detected_at"}
    upd = {c: f"s.{c}" for c in cols if c not in keep}
    undelete = "t.is_deleted AND s.source_extracted_at > t.deleted_detected_at"
    upd["is_deleted"] = f"CASE WHEN {undelete} THEN false ELSE t.is_deleted END"
    upd["deleted_detected_at"] = f"CASE WHEN {undelete} THEN NULL ELSE t.deleted_detected_at END"
    m = run_merge(spark, cfg.table("silver.answers"), merge_sql(
        cfg.table("silver.answers"), "src_answers", ["answer_id"], cols, upd,
        matched_condition="s.source_extracted_at > t.source_extracted_at AND s.record_hash <> t.record_hash"))
    rec.rows_inserted, rec.rows_updated = m["inserted"], m["updated"]
    rec.message = f"{latest.count()} unique answers after de-duplication"


def step_users(spark, cfg, ids, rec):
    def owners(entity):
        return bronze(spark, cfg, entity, ids).select(
            F.col("owner.user_id").alias("user_id"), F.col("owner.user_type").alias("user_type"),
            F.col("owner.reputation").cast("int").alias("reputation"),
            F.col("owner.accept_rate").cast("int").alias("accept_rate"),
            ts("creation_date").alias("post_created_at"), F.col("_extracted_at"))
    all_owners = (owners("questions").unionByName(owners("answers"))
                  .where(F.col("user_id").isNotNull())
                  .withColumn("user_key", user_key(cfg, F.col("user_id"))))
    rec.rows_read = all_owners.count()
    newest = latest_per_key(all_owners, ["user_key"], [F.col("_extracted_at").desc(), F.col("reputation").desc_nulls_last()])
    span = all_owners.groupBy("user_key").agg(F.min("post_created_at").alias("first_seen_at"),
                                              F.max("post_created_at").alias("last_seen_at"))
    src = conform(newest.join(span, "user_key")
                  .withColumn("source_extracted_at", F.col("_extracted_at"))
                  .withColumn("first_loaded_at", F.current_timestamp())
                  .withColumn("load_timestamp", F.current_timestamp()), SILVER_USERS)
    src.createOrReplaceTempView("src_users")
    newer = "s.source_extracted_at > t.source_extracted_at"
    changed = " OR ".join(f"NOT (s.{c} <=> t.{c})" for c in ("user_type", "reputation", "accept_rate"))
    upd = {c: f"CASE WHEN {newer} THEN s.{c} ELSE t.{c} END" for c in ("user_type", "reputation", "accept_rate")}
    upd.update({"source_extracted_at": "GREATEST(s.source_extracted_at, t.source_extracted_at)",
                "first_seen_at": "LEAST(s.first_seen_at, t.first_seen_at)",
                "last_seen_at": "GREATEST(s.last_seen_at, t.last_seen_at)",
                "load_timestamp": "s.load_timestamp"})
    cols = [f.name for f in SILVER_USERS.fields]
    m = run_merge(spark, cfg.table("silver.users"), merge_sql(
        cfg.table("silver.users"), "src_users", ["user_key"], cols, upd,
        matched_condition=f"({newer} AND ({changed})) OR s.first_seen_at < t.first_seen_at "
                          f"OR s.last_seen_at > t.last_seen_at"))
    rec.rows_inserted, rec.rows_updated = m["inserted"], m["updated"]


def step_snapshots(spark, cfg, ids, rec):
    raw = bronze(spark, cfg, "question_snapshots", ids)
    rec.rows_read = raw.count()
    src = conform(latest_per_key(raw, ["question_id", "snapshot_at"], [F.col("_extracted_at").desc()]).select(
        "question_id", ts("snapshot_at").alias("snapshot_at"), F.to_date(ts("snapshot_at")).alias("snapshot_date"),
        "score", "view_count", "answer_count", F.col("is_answered").alias("is_answered_api"),
        ts("closed_date").alias("closed_at"), F.col("_batch_id").alias("source_batch_id"),
        F.current_timestamp().alias("load_timestamp")), SILVER_SNAPSHOTS)
    src.createOrReplaceTempView("src_snapshots")
    cols = [f.name for f in SILVER_SNAPSHOTS.fields]
    m = run_merge(spark, cfg.table("silver.question_snapshots"), merge_sql(
        cfg.table("silver.question_snapshots"), "src_snapshots", ["question_id", "snapshot_at"], cols))
    rec.rows_inserted = m["inserted"]


def step_apply_snapshots(spark, cfg, ids, rec):
    """Votes and views do not move last_activity_date, so fresher metrics arrive via snapshots."""
    snaps = spark.table(cfg.table("silver.question_snapshots")).where(F.col("source_batch_id").isin(ids))
    latest = latest_per_key(snaps, ["question_id"], [F.col("snapshot_at").desc()])
    rec.rows_read = latest.count()
    latest.createOrReplaceTempView("src_latest_snapshots")
    upd = {"score": "s.score", "view_count": "s.view_count", "answer_count": "s.answer_count",
           "is_answered_api": "s.is_answered_api", "has_any_answer": "COALESCE(s.answer_count, 0) > 0",
           "metrics_as_of": "s.snapshot_at", "load_timestamp": "current_timestamp()"}
    m = run_merge(spark, cfg.table("silver.questions"), merge_sql(
        cfg.table("silver.questions"), "src_latest_snapshots", ["question_id"], None, upd,
        matched_condition="s.snapshot_at > t.metrics_as_of"))
    rec.rows_updated = m["updated"]


def step_deletions(spark, cfg, ids, rec, target_logical):
    raw = bronze(spark, cfg, "deleted_questions", ids)
    rec.rows_read = raw.count()
    tomb = (raw.groupBy("question_id").agg(F.min("detected_at").alias("detected_at"))
            .select("question_id", ts("detected_at").alias("deleted_detected_at")))
    tomb.createOrReplaceTempView("src_deletions")
    upd = {"is_deleted": "true", "deleted_detected_at": "s.deleted_detected_at",
           "load_timestamp": "current_timestamp()"}
    cond = "NOT t.is_deleted AND t.source_extracted_at < s.deleted_detected_at"
    m = run_merge(spark, cfg.table(target_logical), merge_sql(
        cfg.table(target_logical), "src_deletions", ["question_id"], None, upd, matched_condition=cond))
    rec.rows_updated = m["updated"]
    rec.message = "soft delete (is_deleted = true)"


def _mask(col):
    for pattern, repl in MASKS:
        col = F.regexp_replace(col, pattern, repl)
    return col


def step_post_text(spark, cfg, ids, rec):
    unescape = _html_unescape()
    q = latest_per_key(bronze(spark, cfg, "questions", ids), ["question_id"],
                       [F.col("_extracted_at").desc(), F.col("_record_hash")]).select(
        F.lit("question").alias("post_type"), F.col("question_id").alias("post_id"),
        _mask(unescape(F.col("title"))).alias("title_masked"), _mask(F.col("body")).alias("body_masked"),
        F.col("_extracted_at").alias("source_extracted_at"))
    a = latest_per_key(bronze(spark, cfg, "answers", ids), ["answer_id"],
                       [F.col("_extracted_at").desc(), F.col("_record_hash")]).select(
        F.lit("answer").alias("post_type"), F.col("answer_id").alias("post_id"),
        F.lit(None).cast("string").alias("title_masked"), _mask(F.col("body")).alias("body_masked"),
        F.col("_extracted_at").alias("source_extracted_at"))
    src = conform(q.unionByName(a)
                  .withColumn("masked_token_count", occurrences(F.col("title_masked"), MASK_TOKEN)
                              + occurrences(F.col("body_masked"), MASK_TOKEN))
                  .withColumn("record_hash", record_hash(["post_type", "post_id", "title_masked", "body_masked"]))
                  .withColumn("load_timestamp", F.current_timestamp()), SILVER_POST_TEXT)
    rec.rows_read = src.count()
    src.createOrReplaceTempView("src_post_text")
    cols = [f.name for f in SILVER_POST_TEXT.fields]
    m = run_merge(spark, cfg.table("silver.post_text_masked"), merge_sql(
        cfg.table("silver.post_text_masked"), "src_post_text", ["post_type", "post_id"], cols,
        {c: f"s.{c}" for c in cols if c not in ("post_type", "post_id")},
        matched_condition="s.source_extracted_at > t.source_extracted_at AND s.record_hash <> t.record_hash"))
    rec.rows_inserted, rec.rows_updated = m["inserted"], m["updated"]


STEPS = [
    ("questions", "silver.questions", step_questions),
    ("question_tags", "silver.question_tags", step_question_tags),
    ("answers", "silver.answers", step_answers),
    ("users", "silver.users", step_users),
    ("question_snapshots", "silver.question_snapshots", step_snapshots),
    ("question_metrics (from snapshots)", "silver.questions", step_apply_snapshots),
    ("deleted_questions -> questions", "silver.questions",
     lambda s, c, i, r: step_deletions(s, c, i, r, "silver.questions")),
    ("deleted_questions -> answers", "silver.answers",
     lambda s, c, i, r: step_deletions(s, c, i, r, "silver.answers")),
    ("post_text_masked", "silver.post_text_masked", step_post_text),
]


# ============================================================================ public entry point
def run_bronze_to_silver(spark, cfg, mode="incremental", batch_ids=None, start_date=None, end_date=None,
                         fail_on_error=True, echo=print):
    """mode=incremental: Bronze batches not yet processed into Silver.
    mode=backfill   : batches chosen by batch_ids and/or start_date..end_date (extraction date),
                      re-processed even if done before. Safe because every MERGE is idempotent."""
    if mode not in ("incremental", "backfill"):
        raise ValueError("mode must be 'incremental' or 'backfill'")
    ids_param = parse_list(batch_ids)
    if mode == "backfill" and not (ids_param or start_date or end_date):
        raise ValueError("backfill needs batch_ids and/or start_date/end_date")

    audit = AuditLogger(spark, cfg, "bronze_to_silver", mode, echo=echo)
    reg = spark.table(cfg.table("ops.batch_registry")).where("bronze_status = 'SUCCESS'")
    if mode == "incremental":
        reg = reg.where("silver_processed_at IS NULL OR silver_processed_at < bronze_loaded_at "
                        "OR silver_status <> 'SUCCESS'")
        param = "unprocessed batches"
    else:
        if ids_param:
            reg = reg.where(F.col("batch_id").isin(ids_param))
        if start_date:
            reg = reg.where(F.col("batch_date") >= F.lit(start_date).cast("date"))
        if end_date:
            reg = reg.where(F.col("batch_date") <= F.lit(end_date).cast("date"))
        param = f"batch_ids={','.join(ids_param) or '*'}; dates={start_date or '-'}..{end_date or '-'}"
    rows = reg.select("batch_id", "load_type").orderBy("extracted_at").collect()
    ids = [r["batch_id"] for r in rows]

    if not ids:
        with audit.step(LAYER, "*", param, "-") as rec:
            rec.skip("no matching Bronze batches to process")
        return audit

    types = {r["load_type"] for r in rows}
    load_type = types.pop() if len(types) == 1 else "mixed"
    echo(f"{LAYER}: processing {len(ids)} batch(es): {', '.join(ids)}")
    source_parameter = f"{param} -> [{', '.join(ids)}]"
    for entity, target, fn in STEPS:
        with audit.step(LAYER, entity, source_parameter, cfg.table(target), load_type=load_type) as rec:
            fn(spark, cfg, ids, rec)

    update_registry_silver(spark, cfg, ids, "FAILED" if audit.failures else "SUCCESS")
    echo(f"{LAYER} summary: {audit.summary()}")
    if fail_on_error and audit.failures:
        raise RuntimeError(f"{len(audit.failures)} Bronze-to-Silver step(s) failed; see "
                           f"{cfg.table('ops.pipeline_execution_logs')} run_id={audit.run_id}")
    return audit


def update_registry_silver(spark, cfg, ids, status):
    now = utc_now()
    rows = [(i, None, None, None, None, None, None, None, status, now, now) for i in ids]
    spark.createDataFrame(rows, BATCH_REGISTRY).createOrReplaceTempView("src_registry_silver")
    run_merge(spark, cfg.table("ops.batch_registry"), merge_sql(
        cfg.table("ops.batch_registry"), "src_registry_silver", ["batch_id"], None,
        {"silver_status": "s.silver_status", "silver_processed_at": "s.silver_processed_at",
         "load_timestamp": "s.load_timestamp"}))
