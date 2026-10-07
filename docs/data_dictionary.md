# Data dictionary

Generated from `src/so_lakehouse/schemas.py` by `scripts/generate_data_dictionary.py`. Do not edit by hand.

### Bronze layer (`so_bronze`)

#### `bronze.questions`

Raw questions exactly as received, one row per question per batch. **Primary key:** `question_id, _batch_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Question id (natural key) |
| `title` | `string` |  | yes | Title, HTML-entity encoded |
| `body` | `string` |  | yes | Body HTML (contains free text and possible PII) |
| `tags` | `array<string>` |  | yes | All tags on the question |
| `owner` | `struct` |  | yes | Author snapshot |
| `owner.account_id` | `bigint` |  | yes | Network-wide Stack Exchange account id (PII, pseudonymous) |
| `owner.user_id` | `bigint` |  | yes | Stack Overflow user id (PII, pseudonymous) |
| `owner.user_type` | `string` |  | yes | registered, unregistered, moderator, team_admin or does_not_exist |
| `owner.reputation` | `int` |  | yes | Reputation at extraction time |
| `owner.accept_rate` | `int` |  | yes | Percent of the user's questions with an accepted answer |
| `owner.display_name` | `string` |  | yes | Public display name (PII, direct identifier) |
| `owner.profile_image` | `string` |  | yes | Avatar URL, may point to a personal photo (PII) |
| `owner.link` | `string` |  | yes | Profile URL containing id and name (PII) |
| `is_answered` | `boolean` |  | yes | API flag: has an upvoted or accepted answer |
| `view_count` | `int` |  | yes | Lifetime views at extraction time |
| `answer_count` | `int` |  | yes | Number of answers at extraction time |
| `score` | `int` |  | yes | Upvotes minus downvotes at extraction time |
| `accepted_answer_id` | `bigint` |  | yes | Accepted answer id, if any |
| `creation_date` | `bigint` |  | yes | Creation time, epoch seconds |
| `last_activity_date` | `bigint` |  | yes | Last activity (edit, answer, closure), epoch seconds |
| `last_edit_date` | `bigint` |  | yes | Last edit, epoch seconds |
| `closed_date` | `bigint` |  | yes | Closure time, epoch seconds |
| `closed_reason` | `string` |  | yes | Closure reason text |
| `protected_date` | `bigint` |  | yes | Protection time, epoch seconds |
| `locked_date` | `bigint` |  | yes | Lock time, epoch seconds |
| `community_owned_date` | `bigint` |  | yes | Community wiki time, epoch seconds |
| `bounty_amount` | `int` |  | yes | Open bounty in reputation points |
| `bounty_closes_date` | `bigint` |  | yes | Bounty expiry, epoch seconds |
| `content_license` | `string` |  | yes | Content licence, e.g. CC BY-SA 4.0 |
| `link` | `string` |  | yes | Public question URL |
| `migrated_from` | `struct` |  | yes | Migration details, if migrated |
| `migrated_from.question_id` | `bigint` |  | yes | Question id on the site it was migrated from |
| `migrated_from.on_date` | `bigint` |  | yes | Migration time, epoch seconds |
| `migrated_from.other_site` | `string` |  | yes | Raw JSON of the source site description |
| `posted_by_collectives` | `string` |  | yes | Raw JSON array of collectives |
| `_batch_id` | `string` | PK | no | Landing folder name, e.g. incr_20261007T155914Z |
| `_load_type` | `string` |  | yes | full or incremental |
| `_batch_date` | `date` |  | yes | UTC date of extraction; used for date-range backfills |
| `_extracted_at` | `timestamp` |  | yes | When the batch was pulled from the API (from _manifest.json) |
| `_source_file` | `string` |  | yes | Full path of the raw file the record came from |
| `_record_hash` | `string` |  | yes | SHA-256 of the raw JSON line |
| `_rescued_data` | `string` |  | yes | JSON of fields not in the contract (schema drift), else null |
| `load_timestamp` | `timestamp` |  | yes | When this record was written to Bronze |

#### `bronze.answers`

Raw answers exactly as received, one row per answer per batch. **Primary key:** `answer_id, _batch_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `answer_id` | `bigint` | PK | no | Answer id (natural key) |
| `question_id` | `bigint` |  | yes | Parent question id |
| `body` | `string` |  | yes | Body HTML (contains free text and possible PII) |
| `owner` | `struct` |  | yes | Author snapshot |
| `owner.account_id` | `bigint` |  | yes | Network-wide Stack Exchange account id (PII, pseudonymous) |
| `owner.user_id` | `bigint` |  | yes | Stack Overflow user id (PII, pseudonymous) |
| `owner.user_type` | `string` |  | yes | registered, unregistered, moderator, team_admin or does_not_exist |
| `owner.reputation` | `int` |  | yes | Reputation at extraction time |
| `owner.accept_rate` | `int` |  | yes | Percent of the user's questions with an accepted answer |
| `owner.display_name` | `string` |  | yes | Public display name (PII, direct identifier) |
| `owner.profile_image` | `string` |  | yes | Avatar URL, may point to a personal photo (PII) |
| `owner.link` | `string` |  | yes | Profile URL containing id and name (PII) |
| `is_accepted` | `boolean` |  | yes | Whether this is the accepted answer |
| `score` | `int` |  | yes | Score at extraction time |
| `creation_date` | `bigint` |  | yes | Creation time, epoch seconds |
| `last_activity_date` | `bigint` |  | yes | Last activity, epoch seconds |
| `last_edit_date` | `bigint` |  | yes | Last edit, epoch seconds |
| `locked_date` | `bigint` |  | yes | Lock time, epoch seconds |
| `community_owned_date` | `bigint` |  | yes | Community wiki time, epoch seconds |
| `content_license` | `string` |  | yes | Content licence |
| `posted_by_collectives` | `string` |  | yes | Raw JSON array of collectives |
| `recommendations` | `string` |  | yes | Raw JSON array of collective recommendations |
| `_batch_id` | `string` | PK | no | Landing folder name, e.g. incr_20261007T155914Z |
| `_load_type` | `string` |  | yes | full or incremental |
| `_batch_date` | `date` |  | yes | UTC date of extraction; used for date-range backfills |
| `_extracted_at` | `timestamp` |  | yes | When the batch was pulled from the API (from _manifest.json) |
| `_source_file` | `string` |  | yes | Full path of the raw file the record came from |
| `_record_hash` | `string` |  | yes | SHA-256 of the raw JSON line |
| `_rescued_data` | `string` |  | yes | JSON of fields not in the contract (schema drift), else null |
| `load_timestamp` | `timestamp` |  | yes | When this record was written to Bronze |

#### `bronze.deleted_questions`

Deletion tombstones from reconciliation. **Primary key:** `question_id, _batch_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Id that the API no longer returns |
| `is_deleted` | `boolean` |  | yes | Always true for a tombstone |
| `detected_at` | `bigint` |  | yes | When the reconciliation noticed the deletion, epoch seconds |
| `_batch_id` | `string` | PK | no | Landing folder name, e.g. incr_20261007T155914Z |
| `_load_type` | `string` |  | yes | full or incremental |
| `_batch_date` | `date` |  | yes | UTC date of extraction; used for date-range backfills |
| `_extracted_at` | `timestamp` |  | yes | When the batch was pulled from the API (from _manifest.json) |
| `_source_file` | `string` |  | yes | Full path of the raw file the record came from |
| `_record_hash` | `string` |  | yes | SHA-256 of the raw JSON line |
| `_rescued_data` | `string` |  | yes | JSON of fields not in the contract (schema drift), else null |
| `load_timestamp` | `timestamp` |  | yes | When this record was written to Bronze |

#### `bronze.question_snapshots`

Score and view snapshots from reconciliation. **Primary key:** `question_id, snapshot_at, _batch_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Question id |
| `snapshot_at` | `bigint` | PK | no | Snapshot time, epoch seconds |
| `score` | `int` |  | yes | Score at snapshot time |
| `view_count` | `int` |  | yes | Views at snapshot time |
| `answer_count` | `int` |  | yes | Answers at snapshot time |
| `is_answered` | `boolean` |  | yes | API answered flag at snapshot time |
| `closed_date` | `bigint` |  | yes | Closure time if closed, epoch seconds |
| `_batch_id` | `string` | PK | no | Landing folder name, e.g. incr_20261007T155914Z |
| `_load_type` | `string` |  | yes | full or incremental |
| `_batch_date` | `date` |  | yes | UTC date of extraction; used for date-range backfills |
| `_extracted_at` | `timestamp` |  | yes | When the batch was pulled from the API (from _manifest.json) |
| `_source_file` | `string` |  | yes | Full path of the raw file the record came from |
| `_record_hash` | `string` |  | yes | SHA-256 of the raw JSON line |
| `_rescued_data` | `string` |  | yes | JSON of fields not in the contract (schema drift), else null |
| `load_timestamp` | `timestamp` |  | yes | When this record was written to Bronze |

#### `bronze.quarantine_records`

Raw records that broke the contract, kept for replay. **Primary key:** `quarantine_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `quarantine_id` | `string` | PK | no | SHA-256 of entity, batch, file and raw line |
| `entity` | `string` |  | yes | questions, answers, deleted_questions or question_snapshots |
| `_batch_id` | `string` |  | yes | Batch the record came from |
| `_source_file` | `string` |  | yes | Raw file path |
| `reason` | `string` |  | yes | malformed_json, type_mismatch or missing_primary_key |
| `error_detail` | `string` |  | yes | Which fields failed and why |
| `raw_record` | `string` |  | yes | The untouched raw line, for replay after a fix |
| `load_timestamp` | `timestamp` |  | yes | When the record was quarantined |

### Silver layer (`so_silver`)

#### `silver.questions`

Current cleansed version of each question. **Primary key:** `question_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Question id |
| `title` | `string` |  | yes | Title with HTML entities decoded |
| `created_at` | `timestamp` |  | yes | Creation time, UTC |
| `last_activity_at` | `timestamp` |  | yes | Last activity time, UTC |
| `last_edit_at` | `timestamp` |  | yes | Last edit time, UTC |
| `closed_at` | `timestamp` |  | yes | Closure time, UTC |
| `closed_reason` | `string` |  | yes | Closure reason as published |
| `closed_reason_code` | `string` |  | yes | Normalised reason, e.g. needs_details_or_clarity |
| `is_closed` | `boolean` |  | yes | closed_at is not null |
| `score` | `int` |  | yes | Latest known score |
| `view_count` | `int` |  | yes | Latest known lifetime views |
| `answer_count` | `int` |  | yes | Latest known answer count |
| `is_answered_api` | `boolean` |  | yes | API flag: has an upvoted or accepted answer |
| `has_any_answer` | `boolean` |  | yes | answer_count > 0 |
| `accepted_answer_id` | `bigint` |  | yes | Accepted answer id |
| `has_accepted_answer` | `boolean` |  | yes | accepted_answer_id is not null |
| `bounty_amount` | `int` |  | yes | Open bounty, reputation points |
| `tags` | `array<string>` |  | yes | All tags |
| `tracked_tags` | `array<string>` |  | yes | Tags that are in the project's tracked list |
| `owner_user_key` | `string` |  | yes | Salted SHA-256 of owner user_id (pseudonymised) |
| `owner_user_type` | `string` |  | yes | Owner account type |
| `owner_reputation` | `int` |  | yes | Owner reputation at extraction |
| `body_length` | `int` |  | yes | Characters in body HTML |
| `code_block_count` | `int` |  | yes | Number of <pre> code blocks |
| `inline_code_count` | `int` |  | yes | Number of <code> elements |
| `link_count` | `int` |  | yes | Number of <a> links |
| `image_count` | `int` |  | yes | Number of <img> elements |
| `title_length` | `int` |  | yes | Characters in decoded title |
| `is_migrated` | `boolean` |  | yes | Question was migrated from another site |
| `content_license` | `string` |  | yes | Content licence |
| `question_url` | `string` |  | yes | Public question URL |
| `is_deleted` | `boolean` |  | yes | Deleted on Stack Overflow (soft delete) |
| `deleted_detected_at` | `timestamp` |  | yes | When the deletion was detected |
| `metrics_as_of` | `timestamp` |  | yes | Time the score/view/answer metrics were observed |
| `source_batch_id` | `string` |  | yes | Bronze batch that supplied the current version |
| `source_extracted_at` | `timestamp` |  | yes | Extraction time of the current version (version column) |
| `record_hash` | `string` |  | yes | SHA-256 of business columns; detects real changes |
| `first_loaded_at` | `timestamp` |  | yes | When the row was first inserted into Silver |
| `load_timestamp` | `timestamp` |  | yes | When the row was last inserted or updated in Silver |

#### `silver.question_tags`

Question-to-tag bridge, one row per pair. **Primary key:** `question_id, tag`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Question id |
| `tag` | `string` | PK | no | Tag name |
| `is_tracked_tag` | `boolean` |  | yes | Tag is in the tracked list |
| `source_batch_id` | `string` |  | yes | Batch that supplied this tag set |
| `load_timestamp` | `timestamp` |  | yes | When the pair was inserted into Silver |

#### `silver.answers`

Current cleansed version of each answer. **Primary key:** `answer_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `answer_id` | `bigint` | PK | no | Answer id |
| `question_id` | `bigint` |  | yes | Parent question id |
| `created_at` | `timestamp` |  | yes | Creation time, UTC |
| `last_activity_at` | `timestamp` |  | yes | Last activity time, UTC |
| `last_edit_at` | `timestamp` |  | yes | Last edit time, UTC |
| `score` | `int` |  | yes | Latest known score |
| `is_accepted` | `boolean` |  | yes | Accepted answer flag |
| `owner_user_key` | `string` |  | yes | Salted SHA-256 of owner user_id |
| `owner_user_type` | `string` |  | yes | Owner account type |
| `owner_reputation` | `int` |  | yes | Owner reputation at extraction |
| `body_length` | `int` |  | yes | Characters in body HTML |
| `code_block_count` | `int` |  | yes | Number of <pre> code blocks |
| `inline_code_count` | `int` |  | yes | Number of <code> elements |
| `link_count` | `int` |  | yes | Number of <a> links |
| `image_count` | `int` |  | yes | Number of <img> elements |
| `content_license` | `string` |  | yes | Content licence |
| `is_deleted` | `boolean` |  | yes | Parent question deleted on Stack Overflow |
| `deleted_detected_at` | `timestamp` |  | yes | When the deletion was detected |
| `source_batch_id` | `string` |  | yes | Bronze batch of the current version |
| `source_extracted_at` | `timestamp` |  | yes | Extraction time of the current version |
| `record_hash` | `string` |  | yes | SHA-256 of business columns |
| `first_loaded_at` | `timestamp` |  | yes | When first inserted into Silver |
| `load_timestamp` | `timestamp` |  | yes | When last inserted or updated in Silver |

#### `silver.users`

Pseudonymous users built from post owners. **Primary key:** `user_key`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `user_key` | `string` | PK | no | Salted SHA-256 of user_id |
| `user_type` | `string` |  | yes | Latest account type |
| `reputation` | `int` |  | yes | Latest known reputation |
| `accept_rate` | `int` |  | yes | Latest known accept rate |
| `first_seen_at` | `timestamp` |  | yes | Earliest post time seen for this user |
| `last_seen_at` | `timestamp` |  | yes | Latest post time seen for this user |
| `source_extracted_at` | `timestamp` |  | yes | Extraction time of the latest profile values |
| `first_loaded_at` | `timestamp` |  | yes | When first inserted into Silver |
| `load_timestamp` | `timestamp` |  | yes | When last inserted or updated in Silver |

#### `silver.question_snapshots`

Metric history per question. **Primary key:** `question_id, snapshot_at`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `question_id` | `bigint` | PK | no | Question id |
| `snapshot_at` | `timestamp` | PK | no | Observation time, UTC |
| `snapshot_date` | `date` |  | yes | Observation date, UTC |
| `score` | `int` |  | yes | Score at snapshot |
| `view_count` | `int` |  | yes | Views at snapshot |
| `answer_count` | `int` |  | yes | Answers at snapshot |
| `is_answered_api` | `boolean` |  | yes | API answered flag at snapshot |
| `closed_at` | `timestamp` |  | yes | Closure time if closed |
| `source_batch_id` | `string` |  | yes | Batch the snapshot came from |
| `load_timestamp` | `timestamp` |  | yes | When inserted into Silver |

#### `silver.post_text_masked`

Restricted: masked titles and bodies for text analysis. **Primary key:** `post_type, post_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `post_type` | `string` | PK | no | question or answer |
| `post_id` | `bigint` | PK | no | question_id or answer_id |
| `title_masked` | `string` |  | yes | Decoded title with PII masked (questions only) |
| `body_masked` | `string` |  | yes | Body HTML with emails, IPs, secrets and connection strings masked |
| `masked_token_count` | `int` |  | yes | How many values were masked |
| `source_extracted_at` | `timestamp` |  | yes | Extraction time of the current version |
| `record_hash` | `string` |  | yes | SHA-256 of the masked text |
| `load_timestamp` | `timestamp` |  | yes | When last inserted or updated in Silver |

#### `silver.dq_quarantine`

Records that failed Silver data-quality rules. **Primary key:** `dq_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `dq_id` | `string` | PK | no | SHA-256 of entity, key, rule and batch |
| `entity` | `string` |  | yes | questions or answers |
| `record_key` | `string` |  | yes | Natural key of the failing record |
| `rule` | `string` |  | yes | Data-quality rule that failed |
| `source_batch_id` | `string` |  | yes | Batch of the failing record |
| `payload` | `string` |  | yes | JSON of the record as it would have entered Silver |
| `load_timestamp` | `timestamp` |  | yes | When the record was quarantined |

### Operational tables (`so_ops`)

#### `ops.pipeline_execution_logs`

One row per file or table processed by any run. **Primary key:** `log_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `log_id` | `string` | PK | no | Unique id of this log entry |
| `run_id` | `string` |  | yes | Id shared by all entries of one pipeline invocation |
| `pipeline_name` | `string` |  | yes | raw_to_bronze or bronze_to_silver |
| `layer` | `string` |  | yes | Raw-to-Bronze or Bronze-to-Silver |
| `run_mode` | `string` |  | yes | incremental or backfill |
| `load_type` | `string` |  | yes | full, incremental or mixed |
| `entity` | `string` |  | yes | Entity or step processed |
| `batch_id` | `string` |  | yes | Batch processed (Raw-to-Bronze) or null |
| `source_parameter` | `string` |  | yes | File path, batch list or date range processed |
| `target_table` | `string` |  | yes | Table written |
| `start_time` | `timestamp` |  | yes | Step start, UTC |
| `end_time` | `timestamp` |  | yes | Step end, UTC |
| `duration_seconds` | `double` |  | yes | end_time minus start_time |
| `status` | `string` |  | yes | SUCCESS, FAILED or SKIPPED |
| `rows_read` | `bigint` |  | yes | Records read from the source |
| `rows_inserted` | `bigint` |  | yes | Rows inserted into the target |
| `rows_updated` | `bigint` |  | yes | Rows updated in the target |
| `rows_deleted` | `bigint` |  | yes | Rows deleted or replaced in the target |
| `rows_quarantined` | `bigint` |  | yes | Records sent to quarantine |
| `message` | `string` |  | yes | Error message or note |
| `load_timestamp` | `timestamp` |  | yes | When this log entry was written |

#### `ops.batch_registry`

Control table: which batches are loaded into Bronze and Silver. **Primary key:** `batch_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `batch_id` | `string` | PK | no | Landing folder name |
| `load_type` | `string` |  | yes | full or incremental |
| `batch_date` | `date` |  | yes | UTC extraction date |
| `extracted_at` | `timestamp` |  | yes | Extraction time from the manifest |
| `landing_path` | `string` |  | yes | Folder the batch was read from |
| `manifest_json` | `string` |  | yes | Copy of _manifest.json |
| `bronze_status` | `string` |  | yes | Outcome of the last Raw-to-Bronze run |
| `bronze_loaded_at` | `timestamp` |  | yes | When Bronze last loaded this batch |
| `silver_status` | `string` |  | yes | Outcome of the last Bronze-to-Silver run |
| `silver_processed_at` | `timestamp` |  | yes | When Silver last processed this batch |
| `load_timestamp` | `timestamp` |  | yes | When this registry row was last written |

#### `ops.schema_drift_events`

Schema drift seen in raw files and the action taken. **Primary key:** `event_id`

| Column | Type | Key | Nullable | Description |
|---|---|---|---|---|
| `event_id` | `string` | PK | no | SHA-256 of batch, entity, drift type and column |
| `run_id` | `string` |  | yes | Pipeline run that saw the drift |
| `_batch_id` | `string` |  | yes | Batch containing the drift |
| `entity` | `string` |  | yes | Entity file |
| `drift_type` | `string` |  | yes | new_column, type_mismatch, malformed_json or missing_primary_key |
| `column_path` | `string` |  | yes | Affected column, dotted for nested fields |
| `record_count` | `bigint` |  | yes | Records affected in the batch |
| `example_value` | `string` |  | yes | One example value, truncated |
| `action_taken` | `string` |  | yes | evolved, rescued or quarantined |
| `load_timestamp` | `timestamp` |  | yes | When the event was recorded |
