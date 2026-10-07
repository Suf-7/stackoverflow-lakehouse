"""Explicit schema contracts for every layer.

Nothing in this project uses Spark schema inference. Every raw file is read
against one of the RAW_CONTRACTS below, and every Delta table is created from
the TABLES registry with explicit types, comments and primary keys.

The same objects generate the data dictionary in README.md
(scripts/generate_data_dictionary.py), so code and documentation cannot drift.
"""
from collections import OrderedDict

from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType,
                               StringType, StructField, StructType, TimestampType)


def col(name, dtype, comment, nullable=True):
    """StructField with a human-readable comment stored in its metadata."""
    return StructField(name, dtype, nullable, metadata={"comment": comment})


# =============================================================================
# 1. RAW CONTRACTS: the shape we expect from the Stack Exchange API files.
#    Dates arrive as Unix epoch seconds (bigint). Rarely used nested objects
#    (collectives, migration site details) are declared as STRING so Spark
#    keeps their raw JSON text instead of us modelling 40 sub-fields.
# =============================================================================
OWNER = StructType([
    col("account_id", LongType(), "Network-wide Stack Exchange account id (PII, pseudonymous)"),
    col("user_id", LongType(), "Stack Overflow user id (PII, pseudonymous)"),
    col("user_type", StringType(), "registered, unregistered, moderator, team_admin or does_not_exist"),
    col("reputation", IntegerType(), "Reputation at extraction time"),
    col("accept_rate", IntegerType(), "Percent of the user's questions with an accepted answer"),
    col("display_name", StringType(), "Public display name (PII, direct identifier)"),
    col("profile_image", StringType(), "Avatar URL, may point to a personal photo (PII)"),
    col("link", StringType(), "Profile URL containing id and name (PII)"),
])

MIGRATED_FROM = StructType([
    col("question_id", LongType(), "Question id on the site it was migrated from"),
    col("on_date", LongType(), "Migration time, epoch seconds"),
    col("other_site", StringType(), "Raw JSON of the source site description"),
])

QUESTION_RAW = StructType([
    col("question_id", LongType(), "Question id (natural key)"),
    col("title", StringType(), "Title, HTML-entity encoded"),
    col("body", StringType(), "Body HTML (contains free text and possible PII)"),
    col("tags", ArrayType(StringType()), "All tags on the question"),
    col("owner", OWNER, "Author snapshot"),
    col("is_answered", BooleanType(), "API flag: has an upvoted or accepted answer"),
    col("view_count", IntegerType(), "Lifetime views at extraction time"),
    col("answer_count", IntegerType(), "Number of answers at extraction time"),
    col("score", IntegerType(), "Upvotes minus downvotes at extraction time"),
    col("accepted_answer_id", LongType(), "Accepted answer id, if any"),
    col("creation_date", LongType(), "Creation time, epoch seconds"),
    col("last_activity_date", LongType(), "Last activity (edit, answer, closure), epoch seconds"),
    col("last_edit_date", LongType(), "Last edit, epoch seconds"),
    col("closed_date", LongType(), "Closure time, epoch seconds"),
    col("closed_reason", StringType(), "Closure reason text"),
    col("protected_date", LongType(), "Protection time, epoch seconds"),
    col("locked_date", LongType(), "Lock time, epoch seconds"),
    col("community_owned_date", LongType(), "Community wiki time, epoch seconds"),
    col("bounty_amount", IntegerType(), "Open bounty in reputation points"),
    col("bounty_closes_date", LongType(), "Bounty expiry, epoch seconds"),
    col("content_license", StringType(), "Content licence, e.g. CC BY-SA 4.0"),
    col("link", StringType(), "Public question URL"),
    col("migrated_from", MIGRATED_FROM, "Migration details, if migrated"),
    col("posted_by_collectives", StringType(), "Raw JSON array of collectives"),
])

ANSWER_RAW = StructType([
    col("answer_id", LongType(), "Answer id (natural key)"),
    col("question_id", LongType(), "Parent question id"),
    col("body", StringType(), "Body HTML (contains free text and possible PII)"),
    col("owner", OWNER, "Author snapshot"),
    col("is_accepted", BooleanType(), "Whether this is the accepted answer"),
    col("score", IntegerType(), "Score at extraction time"),
    col("creation_date", LongType(), "Creation time, epoch seconds"),
    col("last_activity_date", LongType(), "Last activity, epoch seconds"),
    col("last_edit_date", LongType(), "Last edit, epoch seconds"),
    col("locked_date", LongType(), "Lock time, epoch seconds"),
    col("community_owned_date", LongType(), "Community wiki time, epoch seconds"),
    col("content_license", StringType(), "Content licence"),
    col("posted_by_collectives", StringType(), "Raw JSON array of collectives"),
    col("recommendations", StringType(), "Raw JSON array of collective recommendations"),
])

DELETED_RAW = StructType([
    col("question_id", LongType(), "Id that the API no longer returns"),
    col("is_deleted", BooleanType(), "Always true for a tombstone"),
    col("detected_at", LongType(), "When the reconciliation noticed the deletion, epoch seconds"),
])

SNAPSHOT_RAW = StructType([
    col("question_id", LongType(), "Question id"),
    col("snapshot_at", LongType(), "Snapshot time, epoch seconds"),
    col("score", IntegerType(), "Score at snapshot time"),
    col("view_count", IntegerType(), "Views at snapshot time"),
    col("answer_count", IntegerType(), "Answers at snapshot time"),
    col("is_answered", BooleanType(), "API answered flag at snapshot time"),
    col("closed_date", LongType(), "Closure time if closed, epoch seconds"),
])

# entity name -> (raw file name, contract, primary key used to reject records)
RAW_CONTRACTS = OrderedDict([
    ("questions", ("questions.jsonl", QUESTION_RAW, ["question_id"])),
    ("answers", ("answers.jsonl", ANSWER_RAW, ["answer_id"])),
    ("deleted_questions", ("deleted_questions.jsonl", DELETED_RAW, ["question_id"])),
    ("question_snapshots", ("question_snapshots.jsonl", SNAPSHOT_RAW, ["question_id", "snapshot_at"])),
])

# =============================================================================
# 2. BRONZE: raw contract columns + ingestion metadata. One row per record
#    per batch, so history is kept and a batch can be replayed.
# =============================================================================
BRONZE_METADATA = [
    col("_batch_id", StringType(), "Landing folder name, e.g. incr_20261007T155914Z", nullable=False),
    col("_load_type", StringType(), "full or incremental"),
    col("_batch_date", DateType(), "UTC date of extraction; used for date-range backfills"),
    col("_extracted_at", TimestampType(), "When the batch was pulled from the API (from _manifest.json)"),
    col("_source_file", StringType(), "Full path of the raw file the record came from"),
    col("_record_hash", StringType(), "SHA-256 of the raw JSON line"),
    col("_rescued_data", StringType(), "JSON of fields not in the contract (schema drift), else null"),
    col("load_timestamp", TimestampType(), "When this record was written to Bronze"),
]


def bronze_schema(entity):
    _, contract, _ = RAW_CONTRACTS[entity]
    return StructType(list(contract.fields) + BRONZE_METADATA)


QUARANTINE = StructType([
    col("quarantine_id", StringType(), "SHA-256 of entity, batch, file and raw line", nullable=False),
    col("entity", StringType(), "questions, answers, deleted_questions or question_snapshots"),
    col("_batch_id", StringType(), "Batch the record came from"),
    col("_source_file", StringType(), "Raw file path"),
    col("reason", StringType(), "malformed_json, type_mismatch or missing_primary_key"),
    col("error_detail", StringType(), "Which fields failed and why"),
    col("raw_record", StringType(), "The untouched raw line, for replay after a fix"),
    col("load_timestamp", TimestampType(), "When the record was quarantined"),
])

# =============================================================================
# 3. SILVER: cleansed, typed, de-duplicated, PII-safe. One row per entity.
# =============================================================================
SILVER_QUESTIONS = StructType([
    col("question_id", LongType(), "Question id", nullable=False),
    col("title", StringType(), "Title with HTML entities decoded"),
    col("created_at", TimestampType(), "Creation time, UTC"),
    col("last_activity_at", TimestampType(), "Last activity time, UTC"),
    col("last_edit_at", TimestampType(), "Last edit time, UTC"),
    col("closed_at", TimestampType(), "Closure time, UTC"),
    col("closed_reason", StringType(), "Closure reason as published"),
    col("closed_reason_code", StringType(), "Normalised reason, e.g. needs_details_or_clarity"),
    col("is_closed", BooleanType(), "closed_at is not null"),
    col("score", IntegerType(), "Latest known score"),
    col("view_count", IntegerType(), "Latest known lifetime views"),
    col("answer_count", IntegerType(), "Latest known answer count"),
    col("is_answered_api", BooleanType(), "API flag: has an upvoted or accepted answer"),
    col("has_any_answer", BooleanType(), "answer_count > 0"),
    col("accepted_answer_id", LongType(), "Accepted answer id"),
    col("has_accepted_answer", BooleanType(), "accepted_answer_id is not null"),
    col("bounty_amount", IntegerType(), "Open bounty, reputation points"),
    col("tags", ArrayType(StringType()), "All tags"),
    col("tracked_tags", ArrayType(StringType()), "Tags that are in the project's tracked list"),
    col("owner_user_key", StringType(), "Salted SHA-256 of owner user_id (pseudonymised)"),
    col("owner_user_type", StringType(), "Owner account type"),
    col("owner_reputation", IntegerType(), "Owner reputation at extraction"),
    col("body_length", IntegerType(), "Characters in body HTML"),
    col("code_block_count", IntegerType(), "Number of <pre> code blocks"),
    col("inline_code_count", IntegerType(), "Number of <code> elements"),
    col("link_count", IntegerType(), "Number of <a> links"),
    col("image_count", IntegerType(), "Number of <img> elements"),
    col("title_length", IntegerType(), "Characters in decoded title"),
    col("is_migrated", BooleanType(), "Question was migrated from another site"),
    col("content_license", StringType(), "Content licence"),
    col("question_url", StringType(), "Public question URL"),
    col("is_deleted", BooleanType(), "Deleted on Stack Overflow (soft delete)"),
    col("deleted_detected_at", TimestampType(), "When the deletion was detected"),
    col("metrics_as_of", TimestampType(), "Time the score/view/answer metrics were observed"),
    col("source_batch_id", StringType(), "Bronze batch that supplied the current version"),
    col("source_extracted_at", TimestampType(), "Extraction time of the current version (version column)"),
    col("record_hash", StringType(), "SHA-256 of business columns; detects real changes"),
    col("first_loaded_at", TimestampType(), "When the row was first inserted into Silver"),
    col("load_timestamp", TimestampType(), "When the row was last inserted or updated in Silver"),
])

SILVER_QUESTION_TAGS = StructType([
    col("question_id", LongType(), "Question id", nullable=False),
    col("tag", StringType(), "Tag name", nullable=False),
    col("is_tracked_tag", BooleanType(), "Tag is in the tracked list"),
    col("source_batch_id", StringType(), "Batch that supplied this tag set"),
    col("load_timestamp", TimestampType(), "When the pair was inserted into Silver"),
])

SILVER_ANSWERS = StructType([
    col("answer_id", LongType(), "Answer id", nullable=False),
    col("question_id", LongType(), "Parent question id"),
    col("created_at", TimestampType(), "Creation time, UTC"),
    col("last_activity_at", TimestampType(), "Last activity time, UTC"),
    col("last_edit_at", TimestampType(), "Last edit time, UTC"),
    col("score", IntegerType(), "Latest known score"),
    col("is_accepted", BooleanType(), "Accepted answer flag"),
    col("owner_user_key", StringType(), "Salted SHA-256 of owner user_id"),
    col("owner_user_type", StringType(), "Owner account type"),
    col("owner_reputation", IntegerType(), "Owner reputation at extraction"),
    col("body_length", IntegerType(), "Characters in body HTML"),
    col("code_block_count", IntegerType(), "Number of <pre> code blocks"),
    col("inline_code_count", IntegerType(), "Number of <code> elements"),
    col("link_count", IntegerType(), "Number of <a> links"),
    col("image_count", IntegerType(), "Number of <img> elements"),
    col("content_license", StringType(), "Content licence"),
    col("is_deleted", BooleanType(), "Parent question deleted on Stack Overflow"),
    col("deleted_detected_at", TimestampType(), "When the deletion was detected"),
    col("source_batch_id", StringType(), "Bronze batch of the current version"),
    col("source_extracted_at", TimestampType(), "Extraction time of the current version"),
    col("record_hash", StringType(), "SHA-256 of business columns"),
    col("first_loaded_at", TimestampType(), "When first inserted into Silver"),
    col("load_timestamp", TimestampType(), "When last inserted or updated in Silver"),
])

SILVER_USERS = StructType([
    col("user_key", StringType(), "Salted SHA-256 of user_id", nullable=False),
    col("user_type", StringType(), "Latest account type"),
    col("reputation", IntegerType(), "Latest known reputation"),
    col("accept_rate", IntegerType(), "Latest known accept rate"),
    col("first_seen_at", TimestampType(), "Earliest post time seen for this user"),
    col("last_seen_at", TimestampType(), "Latest post time seen for this user"),
    col("source_extracted_at", TimestampType(), "Extraction time of the latest profile values"),
    col("first_loaded_at", TimestampType(), "When first inserted into Silver"),
    col("load_timestamp", TimestampType(), "When last inserted or updated in Silver"),
])

SILVER_SNAPSHOTS = StructType([
    col("question_id", LongType(), "Question id", nullable=False),
    col("snapshot_at", TimestampType(), "Observation time, UTC", nullable=False),
    col("snapshot_date", DateType(), "Observation date, UTC"),
    col("score", IntegerType(), "Score at snapshot"),
    col("view_count", IntegerType(), "Views at snapshot"),
    col("answer_count", IntegerType(), "Answers at snapshot"),
    col("is_answered_api", BooleanType(), "API answered flag at snapshot"),
    col("closed_at", TimestampType(), "Closure time if closed"),
    col("source_batch_id", StringType(), "Batch the snapshot came from"),
    col("load_timestamp", TimestampType(), "When inserted into Silver"),
])

SILVER_POST_TEXT = StructType([
    col("post_type", StringType(), "question or answer", nullable=False),
    col("post_id", LongType(), "question_id or answer_id", nullable=False),
    col("title_masked", StringType(), "Decoded title with PII masked (questions only)"),
    col("body_masked", StringType(), "Body HTML with emails, IPs, secrets and connection strings masked"),
    col("masked_token_count", IntegerType(), "How many values were masked"),
    col("source_extracted_at", TimestampType(), "Extraction time of the current version"),
    col("record_hash", StringType(), "SHA-256 of the masked text"),
    col("load_timestamp", TimestampType(), "When last inserted or updated in Silver"),
])

SILVER_DQ_QUARANTINE = StructType([
    col("dq_id", StringType(), "SHA-256 of entity, key, rule and batch", nullable=False),
    col("entity", StringType(), "questions or answers"),
    col("record_key", StringType(), "Natural key of the failing record"),
    col("rule", StringType(), "Data-quality rule that failed"),
    col("source_batch_id", StringType(), "Batch of the failing record"),
    col("payload", StringType(), "JSON of the record as it would have entered Silver"),
    col("load_timestamp", TimestampType(), "When the record was quarantined"),
])

# =============================================================================
# 4. OPS: audit and control tables
# =============================================================================
EXECUTION_LOGS = StructType([
    col("log_id", StringType(), "Unique id of this log entry", nullable=False),
    col("run_id", StringType(), "Id shared by all entries of one pipeline invocation"),
    col("pipeline_name", StringType(), "raw_to_bronze or bronze_to_silver"),
    col("layer", StringType(), "Raw-to-Bronze or Bronze-to-Silver"),
    col("run_mode", StringType(), "incremental or backfill"),
    col("load_type", StringType(), "full, incremental or mixed"),
    col("entity", StringType(), "Entity or step processed"),
    col("batch_id", StringType(), "Batch processed (Raw-to-Bronze) or null"),
    col("source_parameter", StringType(), "File path, batch list or date range processed"),
    col("target_table", StringType(), "Table written"),
    col("start_time", TimestampType(), "Step start, UTC"),
    col("end_time", TimestampType(), "Step end, UTC"),
    col("duration_seconds", DoubleType(), "end_time minus start_time"),
    col("status", StringType(), "SUCCESS, FAILED or SKIPPED"),
    col("rows_read", LongType(), "Records read from the source"),
    col("rows_inserted", LongType(), "Rows inserted into the target"),
    col("rows_updated", LongType(), "Rows updated in the target"),
    col("rows_deleted", LongType(), "Rows deleted or replaced in the target"),
    col("rows_quarantined", LongType(), "Records sent to quarantine"),
    col("message", StringType(), "Error message or note"),
    col("load_timestamp", TimestampType(), "When this log entry was written"),
])

BATCH_REGISTRY = StructType([
    col("batch_id", StringType(), "Landing folder name", nullable=False),
    col("load_type", StringType(), "full or incremental"),
    col("batch_date", DateType(), "UTC extraction date"),
    col("extracted_at", TimestampType(), "Extraction time from the manifest"),
    col("landing_path", StringType(), "Folder the batch was read from"),
    col("manifest_json", StringType(), "Copy of _manifest.json"),
    col("bronze_status", StringType(), "Outcome of the last Raw-to-Bronze run"),
    col("bronze_loaded_at", TimestampType(), "When Bronze last loaded this batch"),
    col("silver_status", StringType(), "Outcome of the last Bronze-to-Silver run"),
    col("silver_processed_at", TimestampType(), "When Silver last processed this batch"),
    col("load_timestamp", TimestampType(), "When this registry row was last written"),
])

DRIFT_EVENTS = StructType([
    col("event_id", StringType(), "SHA-256 of batch, entity, drift type and column", nullable=False),
    col("run_id", StringType(), "Pipeline run that saw the drift"),
    col("_batch_id", StringType(), "Batch containing the drift"),
    col("entity", StringType(), "Entity file"),
    col("drift_type", StringType(), "new_column, type_mismatch, malformed_json or missing_primary_key"),
    col("column_path", StringType(), "Affected column, dotted for nested fields"),
    col("record_count", LongType(), "Records affected in the batch"),
    col("example_value", StringType(), "One example value, truncated"),
    col("action_taken", StringType(), "evolved, rescued or quarantined"),
    col("load_timestamp", TimestampType(), "When the event was recorded"),
])

# =============================================================================
# 5. Registry: logical name -> (layer, table, schema, primary key, description)
# =============================================================================
TABLES = OrderedDict([
    ("bronze.questions", ("bronze", "questions", bronze_schema("questions"), ["question_id", "_batch_id"],
                          "Raw questions exactly as received, one row per question per batch")),
    ("bronze.answers", ("bronze", "answers", bronze_schema("answers"), ["answer_id", "_batch_id"],
                        "Raw answers exactly as received, one row per answer per batch")),
    ("bronze.deleted_questions", ("bronze", "deleted_questions", bronze_schema("deleted_questions"),
                                  ["question_id", "_batch_id"], "Deletion tombstones from reconciliation")),
    ("bronze.question_snapshots", ("bronze", "question_snapshots", bronze_schema("question_snapshots"),
                                   ["question_id", "snapshot_at", "_batch_id"],
                                   "Score and view snapshots from reconciliation")),
    ("bronze.quarantine_records", ("bronze", "quarantine_records", QUARANTINE, ["quarantine_id"],
                                   "Raw records that broke the contract, kept for replay")),
    ("silver.questions", ("silver", "questions", SILVER_QUESTIONS, ["question_id"],
                          "Current cleansed version of each question")),
    ("silver.question_tags", ("silver", "question_tags", SILVER_QUESTION_TAGS, ["question_id", "tag"],
                              "Question-to-tag bridge, one row per pair")),
    ("silver.answers", ("silver", "answers", SILVER_ANSWERS, ["answer_id"],
                        "Current cleansed version of each answer")),
    ("silver.users", ("silver", "users", SILVER_USERS, ["user_key"],
                      "Pseudonymous users built from post owners")),
    ("silver.question_snapshots", ("silver", "question_snapshots", SILVER_SNAPSHOTS,
                                   ["question_id", "snapshot_at"], "Metric history per question")),
    ("silver.post_text_masked", ("silver", "post_text_masked", SILVER_POST_TEXT, ["post_type", "post_id"],
                                 "Restricted: masked titles and bodies for text analysis")),
    ("silver.dq_quarantine", ("silver", "dq_quarantine", SILVER_DQ_QUARANTINE, ["dq_id"],
                              "Records that failed Silver data-quality rules")),
    ("ops.pipeline_execution_logs", ("ops", "pipeline_execution_logs", EXECUTION_LOGS, ["log_id"],
                                     "One row per file or table processed by any run")),
    ("ops.batch_registry", ("ops", "batch_registry", BATCH_REGISTRY, ["batch_id"],
                            "Control table: which batches are loaded into Bronze and Silver")),
    ("ops.schema_drift_events", ("ops", "schema_drift_events", DRIFT_EVENTS, ["event_id"],
                                 "Schema drift seen in raw files and the action taken")),
])
