# Phase 3 Follow Guide: Power BI on the Gold layer

Work through this top to bottom. At the end you have one Power BI report, connected to the Databricks SQL warehouse, with a star-schema model and five pages: the three Phase 1 business questions, a tag-pair and cohort page, and a pipeline audit page.

**Project:** Stack Overflow Developer Q&A Lakehouse · **Team:** Muhammad Sufyan (24L-2601), Muaaz Fahad (24L-2563)

This machine is a Mac. Power BI Desktop does not run on macOS. You have two ways through:

| Path | Where you work | What you submit | When to use it |
|---|---|---|---|
| A. Publish from Databricks | The browser, on this Mac | A link to the report in app.powerbi.com | You can start a Microsoft Fabric trial, or you already have Power BI Premium |
| B. Power BI Desktop | A Windows PC or a lab machine | A `.pbix` file | The publish button is missing, the university blocks the consent prompt, or you want the file the rubric names |

Path B is the one that always works. Path A is the one you can finish without leaving this Mac. Both join back together at section 5, where the model and the pages are the same.

The rubric also allows Tableau. This guide uses Power BI.

---

## 1. What the report has to contain

| Requirement | Where it is in this guide |
|---|---|
| Connected to Databricks | Section 3 or 4. The data is read from the SQL warehouse, not from a CSV export |
| Star schema, one-to-many relationships | Section 5. Eight relationships are already declared in Unity Catalog |
| Answers the three Phase 1 questions | Pages 1, 2 and 3 |
| Audit and error visuals | Page 5, from `so_ops` |
| At least three visuals | Five pages, well past three |

The three questions, from the Phase 1 proposal:

1. How much has Q&A activity declined since late 2022, and which technologies were hit hardest?
2. Where do developers wait longest for help, and which technologies are under-served?
3. Which technologies are gaining or losing share of attention?

### Read this before you build the trend line

The warehouse does **not** yet hold a full history from 2018. The two full loads cover questions created from January through June 2025. Older questions appear only when an incremental load happened to see recent activity on them. Of about 7,600 questions in `fact_question`, 7,286 were created in 2025. Every year before that has a few dozen rows at most, and 2023 has six.

A line from 2008 to 2026 would show a spike in 2025. That spike is the load window, not a real change in Stack Overflow, and it is not evidence about ChatGPT. The Phase 1 proposal has the real yearly totals, taken from the API. This dashboard shows the months that were fully loaded.

Every business page is filtered to `2025-01` through `2025-06`. If you are asked about the decline since 2022, say that, and point at `dim_date.is_after_chatgpt`, which is ready for the day a full history is loaded.

---

## 2. Values you will paste

| Field | Value |
|---|---|
| Workspace | `https://dbc-f67bb652-2597.cloud.databricks.com` |
| Server hostname | `dbc-f67bb652-2597.cloud.databricks.com` |
| HTTP path | `/sql/1.0/warehouses/44ea9d68f2743938` |
| SQL warehouse | Serverless Starter Warehouse |
| Catalog | `workspace` |
| Gold schema | `so_gold` |
| Audit schema | `so_ops` |

This workspace is Databricks on AWS. In Power BI, prefer the connector named **Databricks**. If the only connector you see is **Azure Databricks**, use that. The hostname and HTTP path are the same. Sign-in with a Microsoft work account will not work against this warehouse. Use a personal access token.

### 2.1 Start the warehouse

1. In Databricks, open **SQL Warehouses**.
2. Open **Serverless Starter Warehouse**.
3. If the state is not Running, click **Start** and wait until it is.

The warehouse sleeps when idle. If a later step times out, come back here and start it again.

### 2.2 Create a personal access token

The token is a password. Do not paste it into this guide, into git, or into a screenshot.

1. Click your username in the top right, then **Settings**.
2. Open **Developer**.
3. Next to **Access tokens**, click **Manage**, then **Generate new token**.
4. Name it `powerbi`. Set the lifetime to 30 days.
5. If a scope list appears, choose the **BI tools** scope.
6. Click **Generate**, copy the token into a password manager or a local note, then click **Done**. You cannot see it again.

### 2.3 Tables you will load

From `workspace.so_gold`, all nine:

`dim_date`, `dim_tag`, `dim_user`, `fact_question`, `fact_answer`, `bridge_question_tag`, `agg_tag_month`, `agg_tag_pairs`, `agg_user_cohort`.

From `workspace.so_ops`, these three:

`pipeline_execution_logs`, `batch_registry`, `schema_drift_events`.

Do not load `so_bronze` or `so_silver`. Bronze still holds the raw owner fields. The dashboard is supposed to read Gold.

After the load, the counts should be in this neighbourhood. A later pipeline run changes the exact numbers. Zero means the wrong catalog or an empty table.

| Table | Rows at the last Gold refresh |
|---|---|
| `fact_question` | 7,619 |
| `fact_answer` | 9,609 |
| `bridge_question_tag` | 24,182 |
| `dim_date` | 6,565 |
| `dim_tag` | 2,811 |
| `dim_user` | 8,332 |
| `agg_tag_month` | 357 |
| `agg_tag_pairs` | 62 |
| `agg_user_cohort` | 17,131 |

---

## 3. Path A — publish from the browser

Skip this section if you are on a Windows PC. Do section 4 instead.

Publishing needs a Power BI workspace on Premium, Premium Per User, or a Fabric capacity. A free Power BI licence is not enough.

1. Open [app.powerbi.com](https://app.powerbi.com) and sign in with your university email. A personal Gmail address is rejected.
2. Create a workspace named `stackoverflow-lakehouse`.
3. If Power BI tells you the workspace has no capacity, start the free Fabric trial from the same site and attach this workspace to that capacity. The trial is 60 days. If the button is missing, or a prompt says the university admin must approve it, stop this path and use section 4.
4. In the workspace, open **Workspace settings**, then the semantic-model section, and turn on **Users can edit data models in the Power BI service**. You need this to add the two date relationships and the measures. Save.

Back in Databricks:

5. Click **Catalog**. In the warehouse dropdown at the top left, choose **Serverless Starter Warehouse**.
6. Open catalog `workspace`, then schema `so_gold`. Click the schema name itself, not a single table.
7. On the upper right, click **Use with BI tools**.
8. Click **Publish to Power BI workspace**.
9. Click **Connect to Microsoft Entra ID** and accept the permissions (`Content.Create`, `Dataset.ReadWrite.All`, `Workspace.Read.All`). If you get **Need admin approval**, the university has blocked the Databricks publishing app. Stop and use section 4.
10. Workspace: `stackoverflow-lakehouse`.
11. Dataset mode: **Import**. The Gold tables are small. Import keeps the report usable in a viva if the SQL warehouse is asleep. DirectQuery is the live alternative, and it waits on the warehouse at every click.
12. Authentication: **PAT**. Paste the token from section 2.2 when asked. OAuth on this AWS workspace is easier to get wrong.
13. Dataset name: **Publish as a new data set**, named `stackoverflow_gold`.
14. Publish, and wait until Power BI opens the new semantic model.

You should see the nine Gold tables. The eight foreign keys already declared in Unity Catalog become relationships. Section 5 tells you the two that still have to be drawn, and the one whose cross-filter you change.

Then add the audit tables into the same model:

15. In Databricks, open schema `workspace.so_ops`.
16. **Use with BI tools**, then **Publish to Power BI workspace**.
17. Same workspace, same authentication, **Import**.
18. Dataset name: **Use an existing data set**, and pick `stackoverflow_gold`.
19. Publish. The three `so_ops` tables are added. Existing Gold tables are not replaced.

Continue at section 5. In Power BI, open the semantic model and choose **Create report**, then **Open data model** when a step talks about the Model view.

---

## 4. Path B — Power BI Desktop on Windows

1. Install Power BI Desktop from the Microsoft Store, or use the copy already on the lab PC. You do not need a paid licence to build and save a `.pbix`.
2. Before connecting, go to **File > Options and settings > Options > Data Load** (under Current File). Uncheck **Autodetect new relationships after data is loaded**. You will draw the relationships yourself, so you can explain each one.
3. **Get data**. Search for `Databricks`. Click the connector **Databricks**. Use **Azure Databricks** only if **Databricks** is not in the list. Click **Connect**.
4. Server hostname: `dbc-f67bb652-2597.cloud.databricks.com`
5. HTTP path: `/sql/1.0/warehouses/44ea9d68f2743938`
6. Data connectivity mode: **Import**. Click **OK**.
7. Authentication: **Personal Access Token**. Paste the token. Click **Connect**.
8. In the Navigator, expand catalog `workspace`, then `so_gold`. Tick all nine tables from section 2.3. Then expand `so_ops` and tick the three audit tables. Do not tick anything else.
9. Click the arrow next to **Load** and choose **Load**. Transform Data is for shaping files. Gold is already shaped.
10. Wait for the load to finish. In the Data pane, confirm you have 12 tables and that `fact_question` has about 7,619 rows. Click the table, then **Table tools > the row count** at the bottom left.

Save now, so a crash does not cost you the connection:

11. **File > Save as**. Name it `stackoverflow_gold.pbix`. Put it in the repo at `dashboards/stackoverflow_gold.pbix` when you are back on your own machine.

Optional shortcut, same result: in Databricks, **Marketplace > Partner Connect > Power BI > Download connection file**, open the file on the Windows PC, and paste the token. You still select the 12 tables yourself.

---

## 5. Build the model

Open the **Model** view. On the service this is **Open data model**. On Desktop it is the third icon on the left.

Delete any relationship Power BI added that is not in the list below. Click the line, then press Delete. Extra lines, especially from `agg_tag_pairs`, make a category slicer filter the wrong charts.

### 5.1 Relationships that should already exist

After Path A these eight come from the Unity Catalog foreign keys. After Path B you drag them. Drag the column in the first table onto the column in the second.

| From | Column | To | Column |
|---|---|---|---|
| `fact_question` | `created_date_key` | `dim_date` | `date_key` |
| `fact_question` | `owner_user_key` | `dim_user` | `user_key` |
| `fact_answer` | `question_id` | `fact_question` | `question_id` |
| `fact_answer` | `created_date_key` | `dim_date` | `date_key` |
| `fact_answer` | `owner_user_key` | `dim_user` | `user_key` |
| `bridge_question_tag` | `question_id` | `fact_question` | `question_id` |
| `bridge_question_tag` | `tag` | `dim_tag` | `tag` |
| `agg_tag_month` | `tag` | `dim_tag` | `tag` |

For each one, the properties must be:

- Cardinality: **Many to one (\*:1)**. The fact or bridge is the many side. The dimension is the one side.
- Cross filter direction: **Single**. Filters flow from the dimension down to the fact.
- Make this relationship active: **on**.
- Assume referential integrity: **off**. Some questions have a null owner, because the Stack Overflow account was deleted. Leaving this off is what makes those rows legal.

### 5.2 The one relationship you set to Both

`bridge_question_tag` sits between `dim_tag` and `fact_question`. With Single on both hops, a tag slicer filters the bridge and then stops. It never reaches the questions, so the Page 2 bars would be identical for every tag.

1. Click the line from `bridge_question_tag.question_id` to `fact_question.question_id`.
2. Cross filter direction: **Both**.
3. Leave the line from `bridge_question_tag.tag` to `dim_tag.tag` as **Single**.
4. Leave every other relationship as **Single**. Setting two bridges to Both is what creates ambiguous paths.

### 5.3 Two relationships you add by hand

A Unity Catalog foreign key can only point at a primary key. `dim_date.full_date` is not the primary key (`date_key` is), so these two were never declared. Draw them.

| From | Column | To | Column |
|---|---|---|---|
| `agg_tag_month` | `month_start` | `dim_date` | `full_date` |
| `agg_user_cohort` | `cohort_month` | `dim_date` | `full_date` |

Same properties: many to one, cross filter Single, active, referential integrity off.

### 5.4 The line that may be dotted

`fact_answer` can reach `dim_date` two ways: directly, on the day the answer was posted, and through `fact_question`, on the day the question was asked. Power BI allows one active path. It may draw the direct answer-date line as a dotted, inactive relationship.

Leave it dotted. These pages filter by the day the question was asked. Clicking **Make this relationship active** would switch the charts onto the answer date, and the months would move.

The same thing can happen between `fact_answer` and `dim_user`. Leave that dotted too. No page on this report charts answers by the answerer's reputation.

### 5.5 Hide the columns that are not for a chart

In the Data pane, right-click each of these and choose **Hide in report view**. Hiding does not break a relationship.

- `record_hash` and `load_timestamp` on every Gold table. They exist so a Gold rerun can skip unchanged rows.
- `user_key` on `dim_user`, `fact_question` and `fact_answer`. It is a salted hash, not a person. The report never shows a display name, a profile link, or a real user id. That was the Phase 1 PII decision.

`dim_user.reputation_band` is the column you would plot if someone asks for a user breakdown. The raw reputation number stays available, but the band is the one that reads well on an axis.

---

## 6. Measures and one column

Measures are the numbers the visuals use. A measure respects the slicers. A raw column of medians does not: Power BI would sum six monthly medians and show nonsense.

Select `fact_question` in the Data pane first, so each measure is stored on that table. Then **New measure** and paste one formula. Repeat.

```
Questions =
COUNTROWS ( fact_question )
```

```
Median hours to first answer =
MEDIAN ( fact_question[hours_to_first_answer] )
```

```
Pct unanswered =
DIVIDE (
    CALCULATE (
        COUNTROWS ( fact_question ),
        fact_question[has_any_answer] = FALSE ()
    ),
    COUNTROWS ( fact_question )
)
```

```
Pct answered within 24h =
DIVIDE (
    CALCULATE (
        COUNTROWS ( fact_question ),
        fact_question[answered_within_24h] = TRUE ()
    ),
    COUNTROWS ( fact_question )
)
```

Select `Pct unanswered` and `Pct answered within 24h`, and set the format to **Percentage**. Select `Median hours to first answer` and set the format to a number with one decimal place.

`MEDIAN` skips blank `hours_to_first_answer`. Unanswered questions stay out of the median, which is the same rule Gold uses, because a median cannot include "never". They do count in `Pct unanswered`.

Then a label for the pair chart. Right-click `agg_tag_pairs`, choose **New column**, and paste:

```
Pair =
agg_tag_pairs[tag_a] & " + " & agg_tag_pairs[tag_b]
```

If a table name in your model has a prefix, use the name you see in the Data pane inside the formula.

---

## 7. Page 1 — Did activity fall, and where?

Rename the page to `Activity`. Right-click the page tab.

This page uses `agg_tag_month`, which already holds one row per tracked tag per month. `category` is stored on that table, and `dim_tag` filters it through the relationship.

### Filter the page to the loaded window

1. Open the **Filters** pane.
2. Drag `agg_tag_month.year_month` into **Filters on this page**.
3. Filter type: **Advanced filter**.
4. Show items when the value **is greater than or equal to** `2025-01`.
5. **And**, **is less than or equal to** `2025-06`.
6. Click **Apply filter**.

`year_month` is text in the form `YYYY-MM`, so alphabetical order is chronological order. Do not put `month_name` on an axis. April would sort next to August.

### Slicer

1. Insert a **Slicer**.
2. Put `dim_tag.category` on its Field.
3. In the slicer format, set the style to **Dropdown**.

Clicking a category should change the line chart. If every category still draws the same line, the relationship from `agg_tag_month.tag` to `dim_tag.tag` is missing. Go back to section 5.

### Line chart

1. Insert a **Line chart**.
2. X-axis: `agg_tag_month.year_month`.
3. Y-axis: `agg_tag_month.questions`. Confirm the aggregation is **Sum**.
4. Legend: `agg_tag_month.category`.

You should see six months and six lines, one per technology category. Languages (`r` and `sql`) sit well above the others. Data platforms and machine learning sit lower.

Add a **Text box** under the chart and paste this, so the page does not get read as a ChatGPT finding:

```
Fully loaded window: questions created January to June 2025.
Years before 2025 are not a census in this warehouse, so they
are filtered out. A question with two tags is counted in each
tag. Summing by category can count one question twice.
```

### What not to put on this page

Do not add a card that sums `agg_tag_month.questions` and call it "questions". That sum double-counts a question that carries two tracked tags. The distinct count is the `Questions` measure, and it lives on Page 2, where the filter is on `dim_date` and reaches `fact_question`.

---

## 8. Page 2 — Who waits longest?

New page. Name it `Response time`.

This page uses `fact_question` and the measures, not the monthly medians. Averaging `median_hours_to_first_answer` across six months is not the median of the half-year.

### Filter

1. Drag `dim_date.year` into **Filters on this page**. Basic filter. Tick **2025** only.
2. Drag `dim_date.month` into **Filters on this page**. Tick **1, 2, 3, 4, 5, 6**.

Both filters apply together, so this is January to June 2025, not every January in the calendar. `dim_date` reaches the questions through `created_date_key`.

### Cards

Insert three **Card** visuals:

| Card field | What it answers |
|---|---|
| `Questions` | How many distinct questions are in the window |
| `Median hours to first answer` | Typical wait, ignoring questions that were never answered |
| `Pct unanswered` | Share of questions with no surviving answer |

The question card should be in the thousands, not in the tens. If it shows 7,619, the page filter is not applied and you are counting every year.

### Bar charts

1. Insert a **Bar chart** (horizontal bars, so the tag names fit).
2. Y-axis: `dim_tag.tag`.
3. X-axis: `Median hours to first answer`.
4. Open **Filters on this visual**. Drag in `dim_tag.is_tracked`. Basic filter, tick **True**. Otherwise the long tail of untracked tags that rode in on the same questions will bury the 18 tags this project collects.
5. Click the **...** on the visual, **Sort axis**, and sort by **Median hours to first answer** descending.
6. Still in the visual filters, click `dim_tag.tag`, change the filter type to **Top N**, show **10**, by value `Median hours to first answer`.

Copy that visual. On the copy, replace the X-axis with `Pct unanswered`, and change the Top N and the sort to that measure.

What you should see, roughly, on the loaded data:

| Fast, and usually answered | Slow, or often unanswered |
|---|---|
| `pandas`, `sql`, `r` | `tensorflow`, `airflow`, `apache-spark`, `dbt` |

`tableau-api` is a small tag. A high unanswered rate there is a handful of questions, not a trend. Say that if it tops the chart. Hover a bar: the tooltip shows the measure value for that tag only, which is the check that the bridge filter is working. If every bar shows the same number, section 5.2 was skipped.

### A table, so you can point at a real question

1. Insert a **Table**.
2. Columns, in this order: `dim_tag.tag`, `fact_question.title`, `fact_question.score`, `Median hours to first answer`, `fact_question.has_any_answer`.
3. Add the same visual filter: `dim_tag.is_tracked` is True.
4. Sort by score descending.

Titles are public posts. This is the drill-through. There is still no display name on the table. That is deliberate.

Add a short text box:

```
Median hours ignores questions that were never answered.
Pct unanswered is where "never" is counted. Deleted questions
are already excluded in Gold.
```

---

## 9. Page 3 — Who is gaining or losing share?

New page. Name it `Share`.

Share is already computed in Gold as `share_of_tracked_questions`: this tag's questions, divided by the distinct tracked questions that month, times 100. A question with two tracked tags counts in each tag and only once in the denominator, so the shares of one month can add up to more than 100. That is the defined metric, not a bug. Do not "fix" it with a pie chart. A pie that sums past 100 is wrong, and Power BI will draw it anyway.

### Filter

Same as Page 1. Drag `agg_tag_month.year_month` to **Filters on this page**, advanced filter, `2025-01` through `2025-06`.

### Slicer and line

1. Slicer on `dim_tag.category`, dropdown style.
2. Line chart.
3. X-axis: `agg_tag_month.year_month`.
4. Y-axis: `agg_tag_month.share_of_tracked_questions`.
5. Legend: `agg_tag_month.tag`.
6. Because each point is one row (a tag and a month), Sum and Average show the same number. Leave the default.

Pick one category in the slicer before you read the chart. All 18 tags at once is not readable. With **Languages** selected you should see `r` and `sql` as the two large shares. With **Big data** you should see `apache-spark` and `pyspark` moving together, which fits: they are usually the same questions tagged twice.

Y-axis title: click the axis title and set it to `Share of tracked questions (%)`. The column is already a percent. Do not also set the format to Percentage, or Power BI will multiply by 100 again and show 1,200%.

Text box:

```
Share is this tag's questions divided by the distinct tracked
questions that month. Two tags on one question count in both
shares, so a month can add up to more than 100%.
```

---

## 10. Page 4 — Tag pairs and cohorts

New page. Name it `Pairs and cohorts`. These two tables have no shared slicer. Do not draw a relationship from `agg_tag_pairs` to `dim_tag`. A pair has two tags, and Power BI can only keep one of those relationships active, which silently drops half the pairs.

### Top pairs

1. Bar chart.
2. Y-axis: `agg_tag_pairs.Pair` (the column from section 6).
3. X-axis: `agg_tag_pairs.question_count`, aggregation **Sum**.
4. Visual filter on `Pair`: **Top N**, 10, by `question_count`.
5. Sort the axis by `question_count` descending.

You should see pairs such as `pandas` with `numpy`, and `apache-spark` with `pyspark`, near the top. `tag_a` is alphabetically before `tag_b`, so each pair appears once. There is no date on this table. It covers every non-deleted question currently in Gold, which in practice is dominated by the 2025 window.

### Cohort heatmap

1. Insert a **Matrix**.
2. Rows: `agg_user_cohort.cohort_month`.
3. Columns: `agg_user_cohort.months_since`.
4. Values: `agg_user_cohort.share_still_active`.
5. Page or visual filter: `months_since` is less than or equal to 6. Six months fits on a slide. Month 0 is the month of the user's first post and is always 1.
6. Filter `cohort_month` to 2025, the same way as the other pages: on or after 1 January 2025, and on or before 30 June 2025. Older cohorts exist, but they are users who appeared on a recently active old question, not a complete user history.
7. Format the value as **Percentage**.
8. Select the matrix. In the format pane open **Cell elements**, turn **Background color** on, click **fx**. Format style: **Gradient**. What field: `share_still_active`. Summarization: **Average**. Apply.

Reading it: each row is people whose first post in this data fell in that month. Each column is how many months later. The colour fading to the right is people who did not post again. Say "in this loaded data", not "of all Stack Overflow users".

---

## 11. Page 5 — Audit and errors

New page. Name it `Audit`. Nothing on this page uses a Gold table. If you took Path A and the `so_ops` tables are missing, do steps 15 to 19 in section 3, then come back.

These tables are the pipeline's own record. `pipeline_execution_logs` has one row per step. `batch_registry` has one row per landing batch. `schema_drift_events` has one row per new column, type change, or bad line.

### Runs and failures

1. Card. Field: `pipeline_execution_logs.run_id`. Click the dropdown on the field and set the aggregation to **Count (Distinct)**. Rename the card to `Pipeline runs`.
2. Card. Field: `pipeline_execution_logs.rows_quarantined`, aggregation **Sum**. Rename it to `Rows quarantined`.
3. Stacked bar chart. Y-axis: `pipeline_execution_logs.layer`. Legend: `pipeline_execution_logs.status`. X-axis: `log_id`, aggregation **Count**.

You want a bar for `Raw-to-Bronze`, `Bronze-to-Silver`, and `Silver-to-Gold`, almost all of it `SUCCESS`. A `FAILED` segment is the error visual. A `SKIPPED` segment is a step that had nothing to do, which is a normal outcome for an incremental run, not a failure.

### The log table

Insert a **Table**. Columns:

`start_time`, `layer`, `entity`, `status`, `duration_seconds`, `rows_read`, `rows_inserted`, `rows_updated`, `rows_deleted`, `message`.

Sort by `start_time` descending. Widen `message`. That column is where a failed step writes the reason, and where the Gold step records how many questions it aggregated and how many deleted questions it excluded.

In the visual filters, you can tick `status` = `FAILED` during a demo and the table becomes the error list. Leave it unticked in the saved report so the page shows the whole history first.

### Drift

1. Bar chart. Y-axis: `schema_drift_events.drift_type`. X-axis: `event_id`, aggregation **Count**.
2. A small table beside it: `entity`, `column_path`, `action_taken`, `record_count`.

The drift types you can be asked about are `new_column`, `type_mismatch`, `malformed_json`, and `missing_primary_key`. `new_column` is evolved into the table. The other three are quarantined or rescued, and the rows still land in `so_bronze.quarantine_records`, which is intentionally not on this report.

### Batches

One more table, from `batch_registry`:

`batch_id`, `load_type`, `batch_date`, `bronze_status`, `silver_status`.

Sort by `batch_date` descending. Both status columns should be `SUCCESS` for the batches you loaded. This is the visual that shows full loads and incremental loads are both registered.

---

## 12. Save, and what you hand in

1. On every page, glance at the filter pane. The 2025 window should still be applied. Slicers should be cleared, so the report opens on all categories.
2. **File > Save**. Desktop writes `stackoverflow_gold.pbix`.
3. Put that file in the repo as `dashboards/stackoverflow_gold.pbix`.
4. On Path A there is no file. Copy the report link from the browser. That link is the submission the rubric allows in place of a `.pbix`.
5. Take one screenshot per page while the warehouse numbers are on screen. The Phase 3 report needs them.

Refresh, when Gold has been rebuilt and you want the report to match: Desktop, **Home > Refresh**. The service, open the semantic model, **Refresh now**. Refresh fails when the SQL warehouse is stopped. Start it (section 2.1) and try again.

Data credit, in a text box on Page 1 if you have room: Stack Overflow content is used under CC BY-SA 4.0.

---

## 13. When a step fails

| What you see | What it means | What you do |
|---|---|---|
| Publish says a Premium or Fabric capacity is required | The Power BI workspace is on a free licence | Start the Fabric trial, or move to section 4 |
| **Need admin approval** | The university has to consent to the Databricks publishing app | Do not wait on that. Use section 4 on a lab PC |
| Connector times out | The SQL warehouse is asleep or cold | Section 2.1, then retry |
| Navigator does not show `so_gold` | Wrong hostname, or the token has no access | Recheck section 2. The catalog is `workspace` |
| A tag slicer does nothing to the bars on Page 2 | Bridge cross-filter is still Single | Section 5.2 |
| Every tag shows the same median | Same cause | Section 5.2 |
| Median hours in the hundreds of thousands | The visual is summing the monthly median column | Replace the field with the measure from section 6 |
| Share axis shows values like 1,200 | Percentage format applied on top of a column that is already a percent | Set the format back to a decimal number |
| Line chart peaks in 2025 and is near zero before that | The page filter was not applied | Section 1. Filter to `2025-01` through `2025-06` |
| `fact_answer` date line is dotted | Power BI dropped the second path to `dim_date` | Leave it dotted. Section 5.4 |
| Cards show blank | The measure name does not match the table name | Open the measure and use the table name from the Data pane |
| Audit page is empty | `so_ops` was not loaded | Section 3 steps 15 to 19, or Get data again on Desktop and tick the three tables |

---

## 14. What to say if you are asked to walk through it

Keep the report in this order.

1. **Model view.** "Gold is a star. `fact_question` and `fact_answer` sit in the middle. `dim_date`, `dim_tag` and `dim_user` are the one side. The bridge is how a question has many tags. Eight of these relationships are foreign keys in Unity Catalog. I set the bridge to filter both ways so a tag slicer reaches the questions. The dotted line is the answer's own date. I left it off so the pages stay on the day the question was asked."
2. **Activity.** "This is January to June 2025, which is what we fully loaded. Languages dominate. I am not using this chart to claim a post-ChatGPT decline. The calendar dimension already has the 30 November 2022 flag for when the history is loaded. The yearly decline is in the Phase 1 proposal, counted from the API."
3. **Response time.** "pandas, sql and r are answered in about an hour. Spark, Airflow and TensorFlow wait much longer, and a larger share are never answered. The median ignores the never-answered questions. The other card counts them."
4. **Share.** "This is each tag's share of tracked questions that month. Shares can pass 100% because one question can carry two of our tags, and the denominator counts that question once."
5. **Audit.** "Every Bronze, Silver and Gold step writes a row: status, duration, and rows read, inserted, updated and deleted. Quarantine and drift have their own visuals. Failed steps would show as their own colour and in the message column."
