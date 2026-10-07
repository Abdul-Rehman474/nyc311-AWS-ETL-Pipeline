# Architecture: NYC 311 ETL Pipeline — Phase 1 (Land to S3)

**Owner:** Abdul Rehman
**Status:** Draft — for review before implementation
**Last updated:** 2026-08-03
**Companion docs:** `DataHealthReport.md` (measured data findings), `decisionlog.md` (why each decision was made)

## 1. Purpose

This document designs the concrete technical architecture for Phase 1 of the NYC 311 ETL project, as scoped for Phase 1: land the historical dataset and an automated incremental feed into AWS S3. It covers what AWS resources get created, how the incremental Lambda works, how the historical data gets uploaded, and how to deploy and operate the whole thing.

Nothing in this document is implemented yet — it's the design to review before the actual `template.yaml`, `app.py`, `backfill_historical.py`, and `README.md` files get written.

## 2. Tooling Decisions

Two tools were available for Infrastructure as Code — **AWS SAM CLI** and **Terraform** — but neither is installed on this machine; only AWS CLI v2 is. Rather than requiring a new install, this design uses:

- **Plain CloudFormation**, deployed with the AWS CLI's built-in `aws cloudformation deploy` command — no new tools needed.
- **`urllib.request`** (Python's built-in HTTP client) instead of the third-party `requests` library for the Lambda's API calls. Combined with `boto3` (which is pre-installed in every AWS Lambda Python runtime), this means the Lambda function has **zero third-party dependencies** — its entire source can be embedded directly inside the CloudFormation template (`Code.ZipFile`), with no separate zip/package/upload step.
- **No Socrata app token for now** — the Lambda calls the NYC Open Data API anonymously. This is a one-line environment variable change to add later if throttling ever becomes an issue; it doesn't affect anything else in this design.

**Prerequisite, not part of this design**: valid AWS credentials must be configured (`aws configure`) before any deployment or testing happens.

## 3. The Backlog Problem

The historical data ends 2025-03-31. Today is 2026-08-03. That's **about 16 months of "incremental" data that doesn't exist yet** — this is not a small daily top-up, it's a large one-time catch-up before the pipeline reaches a true "one day at a time" steady state.

A single Lambda invocation cannot safely pull that whole gap in one shot — millions of records would risk exceeding Lambda's execution timeout and memory limits. Instead:

- Each Lambda invocation is capped at a fixed number of API pages (`MAX_PAGES_PER_RUN`, default 1 page × 50,000 records = up to 50,000 records per run — see the memory note below for why this is 1, not more).
- The Lambda always resumes safely from its last saved watermark, whether that's from an automatic daily run or a manual catch-up run.
- **Practical implication**: right after first deploying this pipeline, the backlog is cleared by manually invoking the Lambda several times in a row (a loop of `aws lambda invoke` calls). Once a run returns fewer records than a full page batch, the backlog is cleared and the daily schedule maintains steady state from then on.

This is called out explicitly here so it's an expected step, not a surprise during testing.

**Update, post-implementation:** this is exactly what happened. Starting from `MAX_PAGES_PER_RUN=5` at `MemorySize: 512`, the first real catch-up invocation hit `Runtime.OutOfMemory` (parsed NYC 311 records take up far more memory as Python dicts than their raw JSON size suggests). The fix — detailed in `decisionlog.md` — was to drop `MAX_PAGES_PER_RUN` to 1 (50,000 records/run) and raise `MemorySize` to 1024 (confirmed safe: ~438MB actually used per run, comfortable headroom). Roughly 104 invocations later, the catch-up finished, landing the full backlog from 2025-03-31 through 2026-08-02 in S3.

**Second update, post-catch-up:** those 104 catch-up batch files were subsequently moved out of `raw/311/incremental/` into `raw/311/historical/api_backfill_2025-04-01_to_2026-08-02/` — see `decisionlog.md` #18 for why. In short: by nature (a fixed, static, never-growing-again range) that data belongs with historical, not with the live incremental feed, even though it happened to be fetched via the incremental Lambda's mechanism. `raw/311/incremental/` now holds only genuine future daily runs, starting fresh from 2026-08-03 onward.

## 4. Architecture Diagram

```mermaid
flowchart TD
    subgraph Local["Local machine (one-time)"]
        CSV["Data/NYC_Data_From_jan1st_2025_to_mar31st_2025.csv"]
        Backfill["scripts/backfill_historical.py"]
        CSV --> Backfill
    end

    subgraph AWS["AWS Account"]
        EB["EventBridge Rule\ncron(0 6 * * ? *) — daily 6am UTC"]
        Lambda["Lambda: incremental_lambda/app.py\ntimeout 300s, 512MB\nurllib + boto3 only"]
        SSM["SSM Parameter\n/nyc311-etl/incremental/watermark"]
        S3["S3 Bucket (raw zone)"]
        Socrata["NYC Open Data / Socrata API\ndataset erm2-nwe9"]

        EB -->|triggers daily| Lambda
        Lambda -->|1. read watermark| SSM
        Lambda -->|2. GET with $where updated_date > watermark| Socrata
        Socrata -->|3. paginated JSON results| Lambda
        Lambda -->|4. write JSONL batch| S3
        Lambda -->|5. advance watermark\n(only after successful write)| SSM
    end

    Backfill -->|one-time upload| S3

    S3 --- Raw["raw/311/historical/... (incl. api_backfill_2025-04-01_to_2026-08-02/)\nraw/311/incremental/year=/month=/day=/ (2026-08-03 onward only)"]
```

**Note on the diagram:** it shows the steady-state design. In practice, the first ~104 Lambda invocations were a manual catch-up for the historical→live gap, and their output was afterward moved into `raw/311/historical/api_backfill_2025-04-01_to_2026-08-02/` rather than left in `raw/311/incremental/` — see the update above and `decisionlog.md` #18.

## 5. AWS Resources (`infra/template.yaml`, CloudFormation)

| Resource | Type | Purpose |
|---|---|---|
| `RawDataBucket` | `AWS::S3::Bucket` | Raw landing zone. No explicit name — CloudFormation auto-generates a globally-unique one. |
| `WatermarkParameter` | `AWS::SSM::Parameter` | String parameter (`/nyc311-etl/incremental/watermark`), seeded with the historical cutoff `2025-03-31T23:59:59.000`. Tracks incremental progress. |
| `IncrementalLambdaRole` | `AWS::IAM::Role` | Trust policy for `lambda.amazonaws.com`. `AWSLambdaBasicExecutionRole` managed policy (CloudWatch Logs) + an inline policy scoped to `s3:PutObject` on `arn:aws:s3:::<bucket>/raw/311/incremental/*` and `ssm:GetParameter`/`ssm:PutParameter` on the watermark parameter's ARN only. Nothing broader. |
| `IncrementalLambdaFunction` | `AWS::Lambda::Function` | `Runtime: python3.12`, `Handler: index.handler` (CloudFormation always names inline `ZipFile` source `index.py` internally, regardless of the local filename — see `decisionlog.md`), inline `Code.ZipFile` (no dependencies to bundle), `Timeout: 300`, `MemorySize: 1024` (raised from an initial 512 after an observed OOM). Env vars: `BUCKET_NAME`, `WATERMARK_PARAM_NAME`, `SOCRATA_DATASET_ID=erm2-nwe9`, `MAX_PAGES_PER_RUN=1` (reduced from an initial 5 for the same reason). |
| `IncrementalScheduleRule` | `AWS::Events::Rule` | `ScheduleExpression: cron(0 6 * * ? *)` — daily at 6am UTC. Target: the Lambda. |
| `LambdaInvokePermission` | `AWS::Lambda::Permission` | Allows `events.amazonaws.com` to invoke the Lambda, scoped to this rule's ARN only. |
| Output: `BucketName` | Stack output | So the backfill script and manual checks can look up the bucket name without hardcoding it. |

## 6. Incremental Lambda Logic (`src/incremental_lambda/app.py`)

Pseudocode of the handler:

```python
def handler(event, context):
    watermark = ssm.get_parameter(Name=WATERMARK_PARAM)["Parameter"]["Value"]
    all_records = []
    max_created = watermark

    for page in range(MAX_PAGES_PER_RUN):
        query = {
            "$where": f"created_date > '{watermark}'",
            "$order": "created_date ASC",
            "$limit": 50000,
            "$offset": page * 50000,
        }
        rows = fetch_json(SOCRATA_URL, query)   # urllib.request
        if not rows:
            break
        all_records.extend(rows)
        max_created = rows[-1].get("created_date", max_created)
        if len(rows) < 50000:
            break

    if all_records:
        key = f"raw/311/incremental/year={Y}/month={M}/day={D}/batch_{HHMMSS}.json"
        body = "\n".join(json.dumps(r) for r in all_records)   # JSON Lines
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode())
        ssm.put_parameter(Name=WATERMARK_PARAM, Value=max_created,
                           Type="String", Overwrite=True)

    return {"records_pulled": len(all_records), "watermark_advanced_to": max_created}
```

**Note on the watermark field:** the design originally assumed an `updated_date` field (to catch records modified after creation, not just new ones). Testing against the live API found no such column exists in this dataset's actual schema — see `decisionlog.md` for the full finding. `created_date` is used instead, which means incremental pulls catch **new** complaints filed since the watermark, not existing complaints that were later updated (e.g. a status change with no matching new row). This is a narrower but simpler and more standard definition of "incremental," and is what's actually implemented.

Key design points:
- **Output format is JSON Lines** (one record per line), not a single giant JSON array — avoids holding one huge in-memory blob, and is the more common raw-zone format for tools like Athena/Glue to read later.
- **Partitioned by ingestion date** (the date the Lambda ran), under `raw/311/incremental/year=/month=/day=/` — simple and deterministic for a scheduled batch job. Partitioning by each record's own date is a possible future refinement, not needed for Phase 1.
- **Watermark only advances after a successful S3 write** — if the Lambda fails partway (network error, timeout), the next run (scheduled or manual) simply retries from the same point. No data is silently skipped.
- **Bounded work per invocation** (`MAX_PAGES_PER_RUN`) — makes both the "16-month backlog catch-up" and the "small daily top-up" cases go through the exact same code path, just naturally taking more or fewer manual invocations to fully catch up.

## 7. Historical Backfill (`scripts/backfill_historical.py`)

A short, one-time-use `boto3` script (run locally, not deployed):

- Uploads `Data/NYC_Data_From_jan1st_2025_to_mar31st_2025.csv` (the canonical merged historical file) to `s3://<bucket>/raw/311/historical/NYC_Data_From_jan1st_2025_to_mar31st_2025.csv`.
- Takes the bucket name as a command-line argument (obtained from the CloudFormation stack output).
- Uses `s3.upload_file()`, which handles multipart upload automatically for the 620MB file — no extra logic needed for the file size.
- The file is uploaded as-is, unmodified — per the decision not to reconcile the historical/incremental schema mismatch at landing time.

## 8. Deploy & Operate (to be detailed fully in `README.md`)

1. Configure AWS credentials: `aws configure` (prerequisite — must point at the account you want to deploy into).
2. Deploy the stack:
   `aws cloudformation deploy --template-file infra/template.yaml --stack-name nyc311-etl --capabilities CAPABILITY_IAM`
3. Get the bucket name:
   `aws cloudformation describe-stacks --stack-name nyc311-etl --query "Stacks[0].Outputs"`
4. Run the historical backfill:
   `python scripts/backfill_historical.py <bucket-name>`
5. Catch up the backlog: repeatedly run
   `aws lambda invoke --function-name <fn-name> out.json && cat out.json`
   until `records_pulled` comes back lower than a full batch (250,000) — meaning the ~16-month gap is cleared.
6. From then on, the daily EventBridge schedule keeps it current with no manual steps.
7. Verify anytime with `aws s3 ls s3://<bucket-name>/raw/311/ --recursive`.

## 9. What This Design Deliberately Defers

Consistent with the Phase 1 scope, this architecture does **not** include:
- Any data cleaning, type coercion, or schema reconciliation between historical and incremental data.
- A data catalog (Glue), query layer (Athena), or warehouse (Redshift).
- Automated data-quality checks or alerting.
- Retry/backoff logic beyond "the watermark doesn't advance on failure, so re-running is safe."
- A Socrata app token (deferred until/if throttling is actually observed).

---

# Phase 2: Transform & Query Layer

**Status:** ✅ Complete — built, deployed, and verified (6,366,286 curated rows; Glue Data Quality score 1.0)
**Context:** Phase 2 tooling was decided in `decisionlog.md` #20: **AWS Glue (Python Shell job)** for transform, **Amazon Athena + Glue Data Catalog** for querying, **AWS Glue Data Quality** for automated checks. Live ingestion was paused the same day (`IncrementalScheduleRule` set to `DISABLED`) specifically so Phase 2 could be built against a fixed, non-growing snapshot without accruing further cost — the freeze point is watermark `2026-09-07T01:51:00.000`.

## 10. Goal

Turn the raw, messy data already sitting in S3 (the original historical CSV, the API-sourced historical backfill, and the incremental daily files) into **one clean, deduplicated, typed, queryable table** — addressing the concrete issues already measured in `DataHealthReport.md` (comma-formatted numbers, text dates, placeholder nulls, the historical/incremental schema mismatch, the structural `location` field mismatch, and cross-source duplicate `unique_key`s).

## 11. New Tooling Decisions (this phase)

- **A separate CloudFormation stack** (`infra/phase2_transform.yaml`, stack name `nyc311-etl-phase2`) rather than adding these resources to the Phase 1 stack. Keeps the already-working ingestion pipeline isolated from any risk while Phase 2 is iterated on — a mistake in a Glue Job definition can't accidentally break the Lambda/EventBridge stack.
- **Output format: Parquet**, not JSON/CSV. Parquet is columnar and compressed — the standard format for anything Athena queries, since Athena bills by bytes scanned; a compressed columnar format is both cheaper and faster to query than the raw JSON/CSV.
- **New S3 zone: `curated/311/`** — sits alongside `raw/311/` in the same bucket. Raw stays untouched (bronze-zone principle, unchanged from Phase 1); curated holds only the cleaned, unified output.
- **Glue Python Shell's `analytics` library set** (`--library-set analytics` job parameter) — gives `pandas` and `pyarrow` pre-installed without managing our own dependency packaging, consistent with Phase 1's preference for avoiding unnecessary packaging complexity.

## 12. Data Catalog Structure

One Glue **Database** (`nyc311_db`) containing:

| Table | Source | Format | Schema |
|---|---|---|---|
| `historical_csv_raw` | `raw/311/historical/*.csv` (the original manual export) | CSV | 41 human-readable columns (`"Problem (formerly Complaint Type)"`, etc.) |
| `historical_api_backfill_raw` | `raw/311/historical/api_backfill_2025-04-01_to_2026-08-02/*.json` | JSON Lines | 42 snake_case columns (API schema) |
| `incremental_raw` | `raw/311/incremental/year=*/month=*/day=*/*.json` | JSON Lines, Hive-partitioned | Same 42 snake_case columns as above |
| `nyc311_curated` | `curated/311/` (written by the transform job, not crawled) | Parquet | One unified schema — see §13 |

The two API-sourced raw tables (`historical_api_backfill_raw`, `incremental_raw`) are kept as **separate** Glue tables rather than one, because `incremental_raw` is Hive-partitioned by ingestion date and the backfill isn't — mixing them under one crawler risks Glue misinterpreting the partition structure. The transform job reads both and unions them in code instead, which is simpler than fighting the crawler over it.

Three **Crawlers** (one per raw table above) populate the catalog by scanning S3 and inferring schema — no manual schema definition needed for the raw side.

## 13. The Unified Curated Schema

Built from the union of both source schemas, using the API's snake_case naming as the standard (since that's what the ongoing incremental feed will always use going forward). Historical CSV columns are renamed per `CSV_COLUMN_MAP` in `src/glue_transform/clean_and_unify.py`, verified against what live API testing actually confirmed (`decisionlog.md` #16):
- `Additional Details` (historical-only, no API equivalent) → kept as a nullable `additional_details` column, populated only for historical-CSV-sourced rows.
- `descriptor_2` (API-only, not in the historical CSV's 41 columns) → kept as a nullable column, populated only for API-sourced rows.
- `police_precinct` and `council_district` — both confirmed present in the live API (an earlier, unverified appendix note had flagged `police_precinct` as "may not be standard" — it is).
- `location` — **dropped entirely**, not parsed. Both sources already carry plain `latitude`/`longitude` columns natively, so the structured `location` field (a string in the CSV, a nested struct in the API) is pure redundancy — simpler to just drop it than parse two different structural forms of the same information. (Implemented this way rather than the parse-both-forms approach originally sketched here — see `decisionlog.md` #22's script design.)
- A `source` column is added (`historical_csv` / `historical_api_backfill` / `incremental`) so it's always possible to trace which raw table a curated row came from.

Type coercions applied: comma-stripped numeric coordinates → proper floats; text dates (both the CSV's `MM/DD/YYYY HH:MM:SS AM/PM` and the API's ISO-ish floating timestamp) → one real timestamp type; placeholder strings (`"Unspecified"`, `"Unspecified <BOROUGH>"`) → true nulls, not left as text.

## 14. Deduplication Strategy

Deduplicate globally on `unique_key`. Since the three raw sources cover non-overlapping `created_date`/date ranges by design (historical: through Mar 2025; backfill: Apr 2025–Aug 2026; incremental: Sep 2026 onward), true duplicates across sources should be rare — but the already-documented ~44,000-row historical discrepancy (`DataHealthReport.md` §5) proves the underlying dataset does shift between pulls, so duplicates **will** occur. Resolution: **keep the version from the most-recently-ingested source** when a `unique_key` appears more than once (priority order: `incremental` > `historical_api_backfill` > `historical_csv`) — the more recently a pull happened, the more likely it reflects the record's current, corrected state.

## 15. Data Quality Rules (AWS Glue Data Quality, DQDL) — as actually implemented

Defined as an `AWS::Glue::DataQualityRuleset` CloudFormation resource (`nyc311-curated-ruleset`, in `infra/phase2_transform.yaml`) targeting `nyc311_curated`, and run via `aws glue start-data-quality-ruleset-evaluation-run` (a Spark-based evaluation, unlike the Python Shell transform job itself):

```
Rules = [
  RowCount > 6000000,
  ColumnCount = 47,
  IsUnique "unique_key",
  Completeness "unique_key" > 0.99,
  Completeness "created_date" > 0.99,
  Completeness "borough" > 0.85
]
```

This differs from the original draft sketched here before implementation (which assumed `created_date` was still a string, and guessed `RowCount > 800000` before the real count was known) — rewritten once real, measured values existed: **6,366,286** true row count, **47** true column count (45 regular columns + 2 Hive partition columns, `year`/`month` — the partition keys count too, which wasn't obvious until a first evaluation run actually failed on it; see `decisionlog.md` #25).

**Two real problems surfaced and fixed while getting this working** (full detail in `decisionlog.md` #24-25):
1. The first evaluation run failed outright on a Spark/Parquet error — the transform job's many independently-written Parquet chunks had inconsistent per-chunk column encoding (a column empty in one chunk could be encoded differently than the same column with real values in another). Fixed by enforcing one explicit `pyarrow.schema()` on every chunk write, rather than letting each chunk infer its own.
2. Once evaluation actually ran, `ColumnCount = 45` failed — the true logical count is 47, because Glue Data Quality's Spark reader counts Hive partition columns as part of the table's columns, not just what's physically in the Parquet files.

Final result: **Score 1.0 — all 6 rules pass.**

## 16. Athena

A single `AWS::Athena::WorkGroup` resource, with query results written to `s3://<bucket>/athena-results/`. No cluster, no server — Athena queries the Glue Catalog tables directly. Once `nyc311_curated` exists, queries like "complaints per borough per month" or "average time-to-close by complaint type" become plain SQL against a normal table, for the first time in this project.

## 17. What Phase 2 Still Defers

Consistent with keeping scope deliberate rather than open-ended:
- No dashboard/visualization layer (QuickSight or otherwise) — Athena SQL access is the extent of "query layer" for now.
- No automated re-run schedule for the transform job — it's run manually against the frozen snapshot for now, consistent with ingestion also being paused.
- No handling for what happens when ingestion is eventually re-enabled and new incremental data needs to be merged into the curated table incrementally (today's design assumes one full transform pass over the whole raw zone each run, not an incremental upsert) — worth revisiting if/when the pause is lifted.

---

# Phase 3: Gold Layer

**Status:** ✅ Complete — built and verified (`decisionlog.md` #26)

## 18. Design

Gold tables are pre-aggregated, report-ready summaries built from `nyc311_curated` (silver) with **Athena CTAS** (`CREATE TABLE AS SELECT`) — the cloud equivalent of a Postgres materialized view. Each table:
- is defined by one SQL file in `src/gold/` (the file name is the table name),
- is written as Parquet to `s3://<bucket>/gold/311/<table>/`,
- is registered in the same `nyc311_db` Glue database, queryable from Athena like any other table.

`scripts/build_gold.py` rebuilds all of them: for each SQL file it drops the table, clears its S3 folder, and re-runs the CTAS. Re-running is always safe.

| Table | Answers |
|---|---|
| `gold_complaints_by_borough_month` | How many complaints per borough per month? |
| `gold_complaint_types_by_month` | Which complaint types dominate each month (with rank)? |
| `gold_resolution_time_by_agency` | How long do agencies take to close each complaint type (avg + median hours)? |
| `gold_status_backlog_by_borough` | How many complaints are open / in progress / closed per borough? |

The Athena workgroup has `EnforceWorkGroupConfiguration: false`, which is what allows CTAS to write to `gold/311/` rather than the workgroup's default results folder.

## 19. Deliberately Deferred

- No dashboard (QuickSight etc.) — it would add a real monthly cost.
- No automatic gold rebuild — run `build_gold.py` by hand after a transform run, consistent with ingestion being paused.

