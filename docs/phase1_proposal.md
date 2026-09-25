# Phase 1 Proposal: Stack Overflow Developer Q&A Lakehouse

**Course:** [Course name and code]
**Team:** [Student 1 name, ID] · [Student 2 name, ID]
**Repository:** [GitHub repository URL]
**Date:** 25 September 2026

---

## 1. Domain and Source Identification

### 1.1 Project concept

Stack Overflow is the largest public question-and-answer site for software developers. It is also a record of which technologies developers struggle with, how quickly the community helps them, and how that behaviour is changing. Since the release of ChatGPT on 30 November 2022, public Q&A activity has fallen sharply. Our own count of surviving questions per year, taken from the API on 25 September 2026, shows the scale:

| Year | Questions on Stack Overflow (not deleted) |
|---|---|
| 2018 | 1,875,801 |
| 2019 | 1,755,709 |
| 2020 | 1,855,032 |
| 2021 | 1,534,281 |
| 2022 | 1,335,301 |
| 2023 | 787,772 |
| 2024 | 398,430 |
| 2025 | 109,754 |
| 2026 (to 25 Sep) | 19,539 |

We will build an automated lakehouse pipeline that tracks questions and answers for **18 data-and-analytics technology tags** from 1 January 2018 onward. Starting in 2018 gives a five-year baseline before AI coding assistants became widely available.

**Tracked tags:** `pandas`, `numpy`, `python-polars`, `duckdb`, `apache-spark`, `pyspark`, `databricks`, `scikit-learn`, `tensorflow`, `pytorch`, `matplotlib`, `powerbi`, `tableau-api`, `airflow`, `dbt`, `snowflake-cloud-data-platform`, `sql`, `r`.

The tag list is stored in `config/pipeline_config.json` and can be changed without code changes. Tag names were validated against the API. For example, the main Polars tag is `python-polars`, and `apache-airflow` is a synonym of `airflow`.

### 1.2 Data source

- **Source:** Stack Exchange API v2.3, site `stackoverflow`. Documentation: https://api.stackexchange.com/docs
- **Endpoints used:**

| Purpose | Endpoint | Key parameters |
|---|---|---|
| Full load of questions | `GET /2.3/questions` | `tagged`, `fromdate`, `todate`, `sort=creation`, `filter=withbody` |
| Incremental questions | `GET /2.3/questions` | `tagged`, `sort=activity`, `min=<watermark>` |
| Answers for questions | `GET /2.3/questions/{ids}/answers` | up to 100 ids per call, `filter=withbody` |
| Deletion check and metric snapshot | `GET /2.3/questions/{ids}` | up to 100 ids per call |
| Volume estimation | `GET /2.3/questions` | `filter=total` returns only a count |

- **Access:** No authentication is needed for public data. An unkeyed client gets 300 requests per day per IP. A free registered app key from stackapps.com raises this to 10,000 requests per day. We will register a key before the full load.
- **Licence:** Content is published under CC BY-SA 4.0. The dashboard will credit Stack Overflow and link to the source.

### 1.3 Ingestion pattern

**Full load.** The full-load job pulls all questions for each tag from 2018-01-01 up to the load date, one calendar month per request window. Month windows keep each paging chain short and make the job resumable. Questions that carry several tracked tags are de-duplicated on `question_id`. Answers are then fetched for every question that has at least one answer.

**Incremental load.** The incremental job runs daily and captures four kinds of change:

| Change type | How it is captured |
|---|---|
| New questions and answers | `sort=activity&min=<watermark>` returns every question created since the watermark |
| Updated questions | The same call returns questions whose `last_activity_date` moved, for example because of a new answer, an edit or a closure |
| Deleted questions | The API never returns deleted posts. A reconciliation step re-requests known recent ids, and ids missing from the response become deletion records |
| Score and view changes | Votes and views do not change `last_activity_date`. The reconciliation response carries current score and view counts, which we store as snapshots |

The watermark is the highest `last_activity_date` seen in the previous run, minus a one-hour overlap. Duplicates caused by the overlap are removed by the Silver `MERGE`. The reconciliation step was tested by including a non-existent question id, which the script correctly reported as deleted.

---

## 2. Data Samples and Volume

### 2.1 Sample files in the repository

All samples were pulled live from the API on 25 September 2026 with the scripts in `ingestion/`.

| Sample | Folder | Contents | Records | Size |
|---|---|---|---|---|
| Full load | `data/samples/full_load/` | Questions created 1 Apr to 30 Jun 2025, all 18 tags, with bodies | 2,738 questions | 8.9 MB |
| | | Answers to those questions | 3,384 answers | 7.7 MB |
| Incremental | `data/samples/incremental/` | Questions with any activity in the 24 hours before extraction | 35 questions: 2 new, 33 updated | 90 KB |
| | | Answers on those questions | 59 answers | 90 KB |
| | | Deletion check of all 2,738 full-load ids | 0 deletions | 0 KB |
| | | Metric snapshots for 100 full-load ids | 100 rows | 15 KB |

Each batch folder has a `_manifest.json` recording the batch id, extraction time, request window, tags, record counts, bytes and API requests used. Files are JSON Lines, one raw API record per line, exactly as returned.

The deletion check found no deletions because it ran minutes after the full-load sample. In production it runs weekly against the previous 12 months of questions, which is when Stack Overflow's automatic clean-up removes abandoned questions.

### 2.2 Volume and frequency estimation

Estimates combine exact counts from the API's `total` filter with average record sizes measured on the sample. The raw numbers are in `data/samples/volume_estimates.json` and `data/samples/profile_report.json`.

**Full load (2018-01-01 to load date)**

| Measure | Value | Basis |
|---|---|---|
| Question-tag matches | 1,164,064 | Sum of exact per-tag counts |
| Unique questions | ~1.07 million | Divided by 1.087 tracked tags per question, measured on sample |
| Answers | ~1.3 to 1.6 million | 1.24 answers per question in the 2025 sample; older questions usually have more |
| Raw JSON size | ~6.5 to 7 GB | 3.3 KB per question and 2.3 KB per answer, measured |
| Compressed landing size | ~1.8 GB | Gzip ratio of 3.6 to 3.9 measured on sample |
| Delta size after Bronze | ~1.5 to 2 GB | Parquet compression expected to be similar to gzip |
| API requests | ~30,000 | About 3 days with a free key |

**Incremental loads**

| Frequency | Records | Size |
|---|---|---|
| Daily | ~35 changed questions and ~60 answers, measured | ~180 KB |
| Weekly reconciliation | a few thousand recent question ids | ~1 MB of snapshots, a few dozen requests |
| Monthly total | ~1,000 questions and ~1,800 answers | ~6 MB |

The full load is large enough to justify Spark: more than 2 million records, several gigabytes of semi-structured JSON with HTML bodies, and a many-to-many tag relationship. The incremental volume is small. That is an honest finding about Stack Overflow today, and it keeps daily runs cheap. The incremental loads still exercise the harder engineering: upserts, deletions and slowly changing metrics.

---

## 3. Security and Compliance

### 3.1 PII identified in the samples

All content is public, but it still contains personal data. We profiled every field and scanned all question and answer bodies with regular expressions (`scripts/profile_samples.py`).

**Structured fields**

| Field | Classification | Finding |
|---|---|---|
| `owner.display_name` | Direct identifier | Often a real full name |
| `owner.profile_image` | Direct identifier | URL can point to a personal Facebook or Google photo |
| `owner.link` | Direct identifier | Profile URL containing user id and name |
| `owner.user_id`, `owner.account_id` | Pseudonymous identifier | Links activity across posts and across Stack Exchange sites |
| `owner.reputation`, `owner.user_type`, `owner.accept_rate` | Low-risk attribute | Not identifying alone |
| `question_id`, `link`, `tags`, dates, counts | Not personal | Public content metadata |

**Free-text fields: title and body**

| Pattern | Question posts affected | Answer posts affected | Notes |
|---|---|---|---|
| Email address | 19 | 15 | About half are placeholders such as example.com; others look real |
| IPv4 address | 29 | 15 | 60 of 242 matches look like public addresses |
| Credential-like string such as `password=...` | 19 | 10 | Users sometimes paste real secrets by mistake |
| Database connection string | 12 | 4 | JDBC, Postgres and similar URLs |
| AWS access key | 0 | 0 | |

The data contains no financial or payment details and no sensitive categories such as health or religion.

### 3.2 Handling strategy

1. **Bronze keeps the raw payload** for replay and audit. Access is limited to the two team members through Unity Catalog permissions. Bronze tables are never exposed to the dashboard.
2. **Dropped before Silver:** `owner.display_name`, `owner.profile_image` and `owner.link`.
3. **Pseudonymised before Silver:** `owner.user_id` and `owner.account_id` become a salted SHA-256 `user_key`. The salt is kept in a Databricks secret scope, not in code or GitHub. This keeps the ability to count distinct users and build retention cohorts without storing the real id.
4. **Free text:** the main Silver tables do not store bodies. We derive features in Silver instead: body length, number of code blocks, links and images. If a text analysis needs the body, a restricted Silver table stores a masked copy with emails, IPs, credential values and connection strings replaced by tokens such as `[EMAIL]`.
5. **Deletions propagate.** When a question is deleted on Stack Overflow, the reconciliation step marks it deleted in Silver, and Gold excludes it. This respects authors who remove their content.
6. **GitHub hygiene:** only the samples required for this submission are committed. Full-size data folders are listed in `.gitignore`. API keys come from environment variables or secrets and are never committed.

---

## 4. High-Level Medallion Data Modeling

### 4.1 Bronze: raw

Batches land as JSON Lines files in a Unity Catalog volume. Auto Loader ingests them append-only into `bronze.questions_raw`, `bronze.answers_raw`, `bronze.deleted_questions_raw` and `bronze.question_snapshots_raw`. Each row gets ingestion metadata: source file, batch id and ingestion time. A rescued-data column captures fields that do not match the schema. The API adds optional fields such as `migrated_from`, `protected_date` and `posted_by_collectives` on a small share of records, so schema drift is real.

### 4.2 Bronze to Silver: cleaning steps

| Step | Why it is needed, from the samples |
|---|---|
| Cast Unix epoch integers to UTC timestamps | All dates arrive as integers such as `1789743096` |
| Decode HTML entities in titles | 245 of 2,738 titles contain entities like `&#39;` or `&quot;` |
| De-duplicate on `question_id` and `answer_id` | A question appears once per tracked tag, and overlapping incremental windows repeat records |
| Keep the latest version of each record | Upsert with `MERGE`, updating only when the incoming `last_activity_date` is newer |
| Handle missing owners | 39 sampled posts belong to deleted users with no `user_id` |
| Apply PII rules | Drop, hash and mask as described in section 3.2 |
| Extract features from HTML bodies | Code block count, body length, link count, image count |
| Separate answered from accepted | `is_answered` is true for 66% of sampled questions, but 83.5% have at least one answer. The API only counts upvoted or accepted answers |
| Normalise close reasons | Values such as "Duplicate" and "Needs details or clarity" |
| Apply soft deletes | Set `is_deleted` from reconciliation tombstones |
| Run data-quality checks | Non-null keys, creation before last activity, every answer has a parent question |

### 4.3 Silver data model

| Table | Grain | Main columns |
|---|---|---|
| `silver.questions` | One row per question, latest version | question_id, created_at, last_activity_at, closed_at, closed_reason, title, score, view_count, answer_count, accepted_answer_id, owner_user_key, owner_reputation, body_length, code_block_count, is_deleted |
| `silver.question_tags` | One row per question and tag | question_id, tag, is_tracked_tag |
| `silver.answers` | One row per answer, latest version | answer_id, question_id, created_at, score, is_accepted, owner_user_key, body_length, code_block_count, is_deleted |
| `silver.users` | One row per pseudonymous user | user_key, user_type, latest_reputation, first_seen_at |
| `silver.question_snapshots` | One row per question per snapshot date | question_id, snapshot_date, score, view_count, answer_count |

### 4.4 Gold: analyst-facing model

**Star schema**

| Table | Type | Grain | Purpose |
|---|---|---|---|
| `gold.fact_question` | Fact | Question | Score, views, answer count, hours to first answer, hours to accepted answer, closed flag |
| `gold.fact_answer` | Fact | Answer | Score, accepted flag, hours after the question, first-answer flag |
| `gold.bridge_question_tag` | Bridge | Question and tag | Resolves the many-to-many relationship between questions and tags |
| `gold.dim_tag` | Dimension | Tag | Tag name plus a technology category |
| `gold.dim_date` | Dimension | Day | Calendar attributes plus a "before or after ChatGPT" flag |
| `gold.dim_user` | Dimension | Pseudonymous user | Reputation band and first-post cohort month |

Technology categories for `dim_tag` are: DataFrame and query engines, Big data, Machine learning, Visualisation and BI, Data platforms and orchestration, and Languages.

**Aggregate tables for the dashboard**

| Table | Grain | Metrics |
|---|---|---|
| `gold.agg_tag_month` | Tag and month | Questions, answers, share of all tracked questions, % unanswered, median and 90th-percentile hours to first answer, % closed |
| `gold.agg_tag_pairs` | Pair of tags | Number of questions that use both tags |
| `gold.agg_user_cohort` | Cohort month and months since first post | Share of users still active |

---

## 5. Business Intelligence and Dashboards

**Tool:** Databricks AI/BI Dashboards. They are included in the Free Edition, read Gold tables directly without exports, and run in the browser.

**Business questions**

1. How much has developer Q&A activity declined since late 2022, and which technologies were hit hardest?
2. Where do developers wait longest for help, and which technologies are under-served by the community?
3. Which technologies are gaining or losing share of developer attention?

**Planned visuals**

1. **AI-era trend line.** Monthly questions per technology category from 2018, with a marker on 30 November 2022. A companion bar chart ranks tags by percentage change between 2022 and 2025.
2. **Response-time comparison.** Median and 90th-percentile hours to first answer by tag, plus % unanswered. The sample already shows a clear gap:

| Tag, Q2 2025 sample | Questions | Median hours to first answer | Never answered |
|---|---|---|---|
| pandas | 285 | 0.9 | 7.7% |
| sql | 540 | 1.3 | 9.3% |
| r | 883 | 1.5 | 15.2% |
| powerbi | 159 | 10.6 | 27.7% |
| pyspark | 131 | 18.9 | 26.0% |
| apache-spark | 139 | 29.1 | 32.4% |

3. **Technology momentum quadrant.** Each tag is plotted by question volume against year-over-year change in share. This separates growing, mature, declining and niche tools.

**Headline KPI tiles:** questions this month compared with the same month in 2022, % answered within 24 hours, and median hours to first answer.

**Stretch goals:** a tag co-occurrence heatmap, a user retention cohort chart, and a Spark MLlib model that predicts whether a question gets answered from its features.

---

## 6. Engineering Setup and FinOps

### 6.1 Version control

The GitHub repository contains the ingestion code, configuration, samples and this proposal:

```
config/pipeline_config.json    tags, history start date, API settings
ingestion/so_api.py            API client: compression, backoff, quota, paging
ingestion/full_load.py         historical baseline, month by month
ingestion/incremental_load.py  watermark-based changes, deletions, snapshots
ingestion/estimate_volume.py   count-only volume estimates
scripts/profile_samples.py     schema, PII and size profiling
data/samples/                  full-load and incremental samples with manifests
notebooks/bronze|silver|gold/  Phase 2 and 3 Databricks notebooks
docs/phase1_proposal.md        this document
```

### 6.2 Infrastructure

- **Compute and storage:** Databricks Free Edition with serverless compute, Unity Catalog, Delta Lake and Lakeflow Jobs for scheduling.
- **Ingestion runtime:** the scripts use only the Python standard library, so they run unchanged in a Databricks notebook or on a laptop. In week 1 of Phase 2 we will confirm that Free Edition compute can reach `api.stackexchange.com`. If outbound access is restricted, ingestion will run as a scheduled GitHub Actions workflow that uploads each batch to a Unity Catalog volume with the Databricks CLI.

### 6.3 Cost and quota management

| Risk | Control |
|---|---|
| Exhausting the daily compute allowance | Develop and test on the 17 MB repository samples. Run the full load once. Schedule a single daily incremental job, and use no always-on compute |
| Storage growth | Land raw files gzip-compressed, about 1.8 GB. Keep only Delta tables after Bronze is verified. Run `OPTIMIZE` once after the full load |
| API quota | Use a free key for 10,000 requests per day. The client honours server backoff and stops when its quota reserve is reached. The full load resumes by month window, spread over about 3 days |
| Wasted reruns | Every batch has a manifest and batch id, so a failed run can be replayed from Bronze instead of calling the API again |
| Large files in GitHub | Only samples are committed. Raw and landing folders are in `.gitignore` |

### 6.4 Tentative plan for Phases 2 and 3

| Phase | Due | Planned scope |
|---|---|---|
| 2 | 10 Oct 2026 | Full load into Bronze, Silver cleaning with PII rules, daily incremental job with `MERGE` and deletions |
| 3 | 24 Oct 2026 | Gold star schema and aggregates, dashboard, data-quality report |

We will adjust this plan when the Phase 2 and 3 requirements are published.
