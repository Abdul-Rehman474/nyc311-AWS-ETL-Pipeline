# Decision Log: NYC 311 ETL Pipeline

**Owner:** Abdul Rehman
**Last updated:** 2026-08-03

This is a running record of *what was decided and why* for this project — not a description of the current design (that's `Architecture.md`), but the reasoning trail behind it. Each entry: the decision, why it was made, what was considered and rejected instead, and its current status.

---

### 1. Incremental data source
**Decision:** Pull the same NYC 311 dataset (Socrata `erm2-nwe9`) going forward from the historical cutoff, via its API.
**Why:** Keeps historical and incremental data as one consistent, naturally messy source, rather than mixing datasets.
**Rejected:** Pairing the historical NYC 311 data with a different dataset (e.g. NYPD complaints) to simulate multi-source ingestion.
**Status:** Decided.

### 2. What "noisy" means for this project
**Decision:** Rely on the data's own natural real-world messiness — nulls, inconsistent formats, duplicates, late-arriving records.
**Why:** The project requirement was realistic messiness for learning purposes, and NYC 311 already provides plenty of it (confirmed directly — see `DataHealthReport.md`).
**Rejected:** Deliberately injecting synthetic corruption on top of the natural noise.
**Status:** Decided.

### 3. Incremental automation approach
**Decision:** Fully automated pipeline using Lambda + EventBridge.
**Why:** Serverless, free-tier friendly, and the right level of complexity for a first AWS automation project.
**Rejected:** Step Functions + Lambda (more moving parts than needed here); Managed Airflow / MWAA (too costly and heavy for this stage).
**Status:** Decided.

### 4. Infrastructure as Code tool
**Decision:** Plain CloudFormation, deployed via the AWS CLI that's already installed.
**Why:** Neither AWS SAM CLI nor Terraform was installed on this machine when the architecture was designed; CloudFormation needed no new install and the AWS CLI could deploy it directly.
**Rejected:** AWS SAM CLI (nicer Lambda-focused DX, but requires installing the `sam` tool); Terraform (cross-cloud, widely used in industry, but introduces its own state file and language).
**Status:** Decided, but revisit if the project grows beyond a few resources — SAM's `sam build`/`sam deploy` would pay off more once there are actual dependencies to package.

### 5. Lambda HTTP client
**Decision:** Python's built-in `urllib.request`, not the third-party `requests` library.
**Why:** Combined with `boto3` (pre-installed in the Lambda runtime), this means the Lambda has zero third-party dependencies — its code can be embedded directly in the CloudFormation template with no separate packaging step.
**Rejected:** `requests` — nicer API, but would require bundling dependencies (Lambda layer or manual pip install + zip).
**Status:** Decided.

### 6. Socrata app token
**Decision:** Skip registering one for now; call the API anonymously.
**Why:** A once-daily pull of a small batch of records is unlikely to hit the anonymous rate limit.
**Rejected:** Nothing rejected — this is deferred, not decided against. Adding a token later is a one-line environment variable change, no redesign needed.
**Status:** Deferred — revisit if throttling errors are ever observed.

### 7. Canonical historical data file
**Decision:** Use the merged `NYC_Data_From_jan1st_2025_to_mar31st_2025.csv` as the sole historical source for the backfill.
**Why:** Direct inspection found the three separate monthly export files sum to 832,316 rows, while the merged file covering the identical Jan–Mar range has 876,414 — about 44,000 more. This reflects that the Socrata dataset is a live, continuously-updated view, not a frozen snapshot (see `DataHealthReport.md`). The merged file is the more complete/recent pull.
**Rejected:** Using the three monthly files as three separate partitions; loading both versions side by side to preserve the discrepancy for later investigation.
**Status:** Decided.

### 8. Schema mismatch between historical and incremental data
**Decision:** Don't reconcile at landing time. Historical (UI-export column names) and incremental (Socrata API snake_case field names) data are written to S3 as-is, in separate prefixes. A column-mapping table was kept as a reference for whenever the transform phase was scoped (it became `CSV_COLUMN_MAP` in the Glue job).
**Why:** Matches standard raw/bronze-zone practice (preserve source fidelity, don't guess a canonical schema before the next phase is even planned) and matches the Phase 1 scope — land the data first, decide what comes next afterward.
**Rejected:** Normalizing both sources into one schema now, before knowing what the transform phase actually requires.
**Status:** Decided for Phase 1; explicitly revisit at the transform phase.

### 9. Handling the ~16-month incremental backlog
**Decision:** Cap each Lambda invocation at `MAX_PAGES_PER_RUN` (default 5 pages × 50,000 records); catch up the gap between the historical cutoff (2025-03-31) and today by manually re-invoking the Lambda several times after first deploying it.
**Why:** A single invocation can't safely pull ~16 months of backlog (potentially millions of records) without risking a timeout or memory limit. Capping keeps every run — whether catching up or running steady-state — on the exact same code path.
**Rejected:** A separate one-off "catch-up mode" code path distinct from the daily incremental logic.
**Status:** Decided.

### 10. Incremental output format
**Decision:** JSON Lines (one record per line) instead of a single JSON array per batch.
**Why:** Avoids holding one large in-memory blob when serializing; also the more common raw-zone format for tools like Athena/Glue to read later.
**Rejected:** A single `json.dumps()` array per batch file.
**Status:** Decided.

### 11. Incremental partitioning scheme
**Decision:** Partition incremental S3 objects by the date the Lambda ran (`year=/month=/day=`), not by each record's own event date.
**Why:** Simple and deterministic for a scheduled batch job — no need to fan out writes across multiple date partitions per run.
**Rejected:** Partitioning by each record's `created_date`/`updated_date`.
**Status:** Decided for Phase 1; noted as a possible future refinement.

### 12. S3 bucket naming
**Decision:** No explicit bucket name in the CloudFormation template — let CloudFormation auto-generate a globally-unique name.
**Why:** S3 bucket names are globally unique across all AWS accounts; auto-generation avoids naming collisions entirely.
**Rejected:** A hand-picked name (e.g. `nyc311-etl-raw`), which risks colliding with a bucket someone else already created.
**Status:** Decided.

### 13. IAM policy scope
**Decision:** Least-privilege inline policies — S3 write scoped to one prefix, SSM get/put scoped to one parameter ARN — plus the standard `AWSLambdaBasicExecutionRole` managed policy for CloudWatch Logs.
**Why:** The Lambda only ever needs to touch one S3 prefix and one SSM parameter; broader access isn't justified.
**Rejected:** A broad managed policy like full S3 or SSM access.
**Status:** Decided.

### 14. Incremental schedule
**Decision:** Daily, at 6am UTC, via an EventBridge cron rule.
**Why:** Matches the daily "incremental" cadence; a fixed time of day is easier to reason about than a rolling interval.
**Rejected:** `rate(1 day)` (equivalent, but doesn't pin a specific time of day).
**Status:** Decided.

### 15. Inline Lambda code requires `Handler: index.handler`, not `app.handler`
**Decision:** When CloudFormation's `Code.ZipFile` inline property is used (no S3-uploaded zip), the handler must be `index.handler`.
**Why:** Discovered during first deploy — CloudFormation always saves inline `ZipFile` source as a file literally named `index.py` internally, regardless of what the source is conceptually called. The original design assumed the handler could reference `app.handler` to match the local `src/incremental_lambda/app.py` filename; the first invocation failed with `Unable to import module 'app': No module named 'app'` until corrected.
**Rejected:** N/A — this is a platform constraint, not a preference.
**Status:** Decided (and fixed). `app.py` remains the local, editable source-of-truth filename; only the CloudFormation `Handler` property and the fact that AWS internally calls it `index.py` differ.

### 16. Watermark field: `created_date`, not `updated_date`
**Decision:** The incremental Lambda filters and orders on `created_date`, not `updated_date`.
**Why:** Testing the live Socrata API directly (`curl` against `erm2-nwe9`) returned `query.soql.no-such-column: updated_date` — the actual field list for this dataset has no general "last modified" column, only `created_date`, `closed_date`, and `resolution_action_updated_date` (which only fires on resolution actions, not general edits). The original plan had already hedged with "`updated_date` (or `created_date`)," so this fallback was anticipated as a possibility, just not confirmed until live testing.
**Rejected:** Using `resolution_action_updated_date` as a general "last modified" proxy — rejected because it's only populated for records that received a resolution action, not a reliable general-purpose watermark field.
**Status:** Decided. Practical implication: incremental pulls now catch **new** complaints filed since the watermark, not existing complaints that were later edited/updated with no new row. Narrower than the original "catch anything touched" intent, but simpler, always-available, and matches how most append-style incremental feeds work in practice.

### 17. Lambda memory (1024MB) and batch size (`MAX_PAGES_PER_RUN=1`)
**Decision:** Raised `MemorySize` from 512 to 1024 and reduced `MAX_PAGES_PER_RUN` from 5 to 1 (50,000 records/invocation instead of up to 250,000).
**Why:** The first real catch-up invocation at the original settings (512MB, 5 pages) failed with `Runtime.OutOfMemory` after 66 seconds. Parsed NYC 311 records take up considerably more memory as Python dicts (nested `location` object, ~40 string fields per record) than their raw JSON size suggests — holding 250,000 of them plus the JSON-Lines-serialized copy in memory at once exceeded 512MB by a wide margin. At the corrected settings, a full 50,000-record run measured `Max Memory Used: 438 MB` out of 1024MB — safe headroom confirmed empirically, not just estimated.
**Rejected:** Streaming/chunked S3 writes (e.g. multipart upload per sub-batch) instead of buffering the whole page in memory — would have been more memory-robust at scale, but added real implementation complexity for a fix that a simple resource-size adjustment already solved.
**Status:** Decided. Practical implication: the ~16-month backlog required roughly 104 invocations (50,000 records each, ~104 × ~20s ≈ 35 minutes total) run in scripted background batches rather than the ~15-20 originally estimated at the larger batch size — noted here since the original plan estimated a smaller number of catch-up runs.

### 18. Catch-up backlog data moved from `incremental/` to `historical/`
**Decision:** After the ~104-invocation catch-up finished, its output was moved from `raw/311/incremental/year=2026/month=08/day=03/` to `raw/311/historical/api_backfill_2025-04-01_to_2026-08-02/` (an S3 same-bucket move: server-side copy + delete per object, ~104 objects/7.3GB, verified byte-for-byte afterward — 104 objects and 7,314,504,466 bytes matched exactly at the new location, and the old prefix was confirmed empty).
**Why:** The historical-vs-incremental split is about the *nature* of the data (a fixed, closed, never-growing-again range vs. a live ongoing feed), not about which mechanism fetched it. This catch-up data is static by nature — exactly like the original CSV — even though it happened to be fetched via the incremental Lambda's watermark/pagination logic rather than a manual export. Leaving it under `incremental/` would permanently muddle that prefix's meaning: anyone inspecting it later would see it dominated by a one-time bulk catch-up (~7.3GB across 104 files) rather than small genuine daily deltas, and might wrongly assume the daily increments are that large.
**Rejected:** (a) Leaving it in place under `incremental/` — rejected because it misrepresents what "incremental" means going forward. (b) A new top-level sibling prefix (e.g. `raw/311/incremental_backfill/`) — rejected in favor of nesting under `historical/`, since the data's nature (fixed past snapshot) already fits that category cleanly, and a third top-level category would add a concept to explain without adding real clarity.
**Status:** Decided and applied. `raw/311/incremental/` now contains only genuine daily runs starting 2026-08-03 onward. `raw/311/historical/` now contains two parts: the original manual CSV export (Jan-Mar 2025) and this API-sourced backfill (Apr 2025-Aug 2026) — both static, both closed, neither will grow further.

### 19. Days with no incremental file are not data loss
**Decision:** Treat calendar days that have no file under `raw/311/incremental/` as a harmless labeling effect, not missing data. No re-fetch needed.
**Why:** Several expected days had no file of their own (for example Aug 12, 14, 18, 21, 24 and 26). Rather than assume, the actual `created_date` values inside every file were checked: each file starts within seconds of where the previous one ended, so coverage is continuous. Those days' scheduled runs most likely failed silently (the exact cause was not determined); the next successful run fetched everything after the unchanged watermark, and because normal daily volume (~10-11k records) is far below the 50,000-record page cap, the whole backlog fit in one run — visible as roughly double-size files (for example 21,911 records vs. ~10k on a normal day).
**Rejected:** Re-pulling the "missing" days — nothing was missing, and doing so would have created duplicates.
**Status:** Decided. Related design property: the watermark advances only after a successful S3 write, so a failed run can never skip data.
### 20. Phase 2 tooling choices, and pausing live ingestion
**Decision:** Three things decided together: (a) transform tool = **AWS Glue, Python Shell job** (not full PySpark — data volume doesn't need it); (b) query layer = **Athena** over a **Glue Data Catalog**; (c) data-quality checks = **AWS Glue Data Quality** (declarative DQDL rules), not custom Python assertions or manual-only spot-checks. Additionally, the daily **`IncrementalScheduleRule` EventBridge rule was disabled** (`State: DISABLED` in `template.yaml`, redeployed) — the Lambda and all other resources still exist, just no longer triggered on a schedule.
**Why:** (a)/(b)/(c) are the standard, serverless "lake house" pattern — cheapest, no cluster/server to provision, and directly builds on Glue's own Data Catalog rather than a separate custom validation layer. On pausing ingestion: keeping ongoing AWS cost near zero is an explicit goal, so the project can be shown without live billing risk. Rather than let the pipeline keep growing S3 storage and invoking Lambda daily while Phase 2 is being built, ingestion is frozen now at a known point, and Phase 2 work proceeds against that fixed snapshot.
**Freeze point:** watermark `2026-09-07T01:51:00.000` as of 2026-09-09 — this is the boundary of what Phase 2's transform/query work covers. `raw/311/incremental/` will not receive new files until the rule is re-enabled (if ever).
**Rejected:** (a) Redshift over Athena — rejected as a bigger commitment (a cluster/serverless endpoint to manage) not justified at this data volume or for this goal. (b) Leaving the schedule running while building Phase 2 — rejected given the explicit cost concern; nothing is gained by letting it keep running mid-build. (c) Fully tearing down all resources now — rejected for now, since the data and infrastructure are still needed to build and demo Phase 2; a fuller wind-down remains a decision for later.
**Status:** Decided and applied. EventBridge rule confirmed `DISABLED` via `aws events describe-rule`. Phase 2 build proceeds against the frozen snapshot.

### 21. Phase 2 architecture specifics
**Decision:** Several concrete design choices made while writing `Architecture.md`'s Phase 2 section: (a) a separate CloudFormation stack (`nyc311-etl-phase2`) rather than extending the Phase 1 stack; (b) Parquet as the curated output format, in a new `curated/311/` S3 zone; (c) three separate raw Glue tables (`historical_csv_raw`, `historical_api_backfill_raw`, `incremental_raw`) rather than merging the two API-sourced ones into one crawler target; (d) cross-source dedup on `unique_key`, keeping whichever source ingested most recently (`incremental` > `historical_api_backfill` > `historical_csv`); (e) DQDL rules for Glue Data Quality drawn directly from already-measured findings in `DataHealthReport.md`, not invented thresholds.
**Why:** (a) isolates Phase 2 iteration risk from the working Phase 1 pipeline. (b) Parquet is the standard columnar format for Athena — cheaper and faster to query than raw JSON/CSV, since Athena bills by bytes scanned. (c) `incremental_raw` is Hive-partitioned by ingestion date and the backfill isn't; mixing them under one crawler risks Glue misinterpreting partition structure, so the union happens in the transform code instead. (d) the three sources cover non-overlapping date ranges by design, but the already-documented ~44K-row historical discrepancy proves the source data does shift between pulls, so cross-source duplicates are expected, not hypothetical. (e) grounding DQ rules in already-measured data (e.g., the confirmed zero-duplicate-`unique_key` finding) makes them meaningful regression checks rather than guesses.
**Rejected:** Merging the two API-sourced raw tables into a single crawler target — rejected due to the partition-structure mismatch described above.
**Status:** Decided. Documented in `Architecture.md` §10-17. Implementation follows.

### 22. Transform job rewritten for streaming/chunked processing after an OOM kill
**Decision:** Rewrote `clean_and_unify.py` from a "load all three sources fully into memory, concat, dedup, write" design to a streaming one: process one JSONL file (or one 50,000-row CSV chunk) at a time, dedup using a lightweight in-memory `set()` of already-seen `unique_key` strings (not full records), and write each cleaned chunk straight to curated Parquet before moving to the next.
**Why:** The first real run failed with exit code 137 (SIGKILL — an out-of-memory kill) after ~188 seconds, before any output was written. Root cause: the original design accumulated all ~5.1M+ API-sourced records into one Python list before doing anything else. This is the exact same per-record memory cost already measured in `decisionlog.md` #17 (~438MB per 50,000 parsed records in the Lambda) — scaled to ~5.1M records, that's roughly **~44GB**, far past what a single machine (or a Python Shell job, capped at 16GB/1 DPU — there is no larger option for this job type, unlike Spark) can hold. This should have been anticipated from #17 directly; it wasn't, and cost one failed run to catch.
**Rejected:** Bumping `MaxCapacity` higher — not possible; Glue Python Shell jobs have a hard ceiling of 1 DPU (16GB). Switching to a Spark ETL job type instead — rejected as a bigger change than needed; a streaming rewrite solves the actual problem without abandoning the "Python Shell, no Spark" decision from #20.
**Known tradeoff accepted:** this produces many small Parquet files in `curated/311/` (one or more per chunk — roughly 100-150 files total) rather than a few large, well-compacted ones. Not ideal for Athena query performance at scale, but acceptable for this data volume; a compaction pass is a reasonable future improvement, not required now.
**Status:** Decided and applied. Re-running against the same frozen snapshot.

### 23. AWS Glue Data Quality ruleset design
**Decision:** Defined `nyc311-curated-ruleset` (an `AWS::Glue::DataQualityRuleset` CloudFormation resource, keeping it as infrastructure-as-code like everything else) targeting `nyc311_curated`, with six rules grounded in actually-measured values rather than guesses: `RowCount > 6000000` and `ColumnCount = 45` (both confirmed via direct query against the real deployed table — true counts are 6,366,286 rows and 45 columns), `IsUnique "unique_key"` and `Completeness "unique_key" > 0.99` (regression checks on the dedup logic from #20/#24), `Completeness "created_date" > 0.99`, and `Completeness "borough" > 0.85`.
**Why:** `Architecture.md` §15's original draft rules assumed `created_date` was still a string (a regex-based format check) — no longer valid once it's a real `timestamp` column, and its `RowCount > 800000` threshold was a rough guess made before the real row count was known. Rewrote using rule types with well-established, unambiguous DQDL syntax rather than risking less-certain clauses (e.g. categorical `in [...] with threshold` checks), preferring a ruleset that reliably runs over a marginally more elaborate one that might not.
**Rejected:** Keeping the original speculative thresholds from `Architecture.md` §15 — rejected once real measured values were available; a regression check is only meaningful if grounded in truth, not a guess made before the data existed.
**Status:** Decided and deployed. Running it immediately surfaced a real, separate problem — see #24.

### 24. Explicit Parquet schema required after Glue Data Quality's Spark reader choked on inferred per-chunk types
**Decision:** Added an explicit `pyarrow.schema()` (`CURATED_SCHEMA`, matching `phase2_transform.yaml`'s `CuratedTable` column types exactly) and write every chunk against it via `pa.Table.from_pandas(..., schema=CURATED_SCHEMA)` + `pq.write_table`, instead of letting each chunk's `.to_parquet()` call infer its own column encodings independently.
**Why:** The first evaluation run of #23's ruleset failed with `java.lang.UnsupportedOperationException: ...PlainValuesDictionary$PlainBinaryDictionary`, a Spark/Parquet error. Root cause: since the transform writes ~150+ independent Parquet files chunk-by-chunk (per #22's streaming design), and `pandas.to_parquet()` infers each column's Arrow type from whatever data is actually present *in that specific chunk*, a column that's always empty in one source (e.g. `descriptor_2`, which only the API-sourced data populates) could get encoded differently across chunks. Athena's query engine tolerated the inconsistency; Glue Data Quality's underlying Spark engine did not when reading all files together as one logical table.
**Rejected:** Compacting/rewriting the Parquet files after the fact as a separate cleanup step — rejected as more moving parts than just writing correctly the first time with a fixed schema.
**Status:** Decided and applied. Curated output cleared and the transform job re-run from scratch against the same frozen snapshot to regenerate it with consistent encoding throughout.

### 25. `ColumnCount` includes Hive partition columns, not just physical Parquet columns
**Decision:** Corrected the ruleset's `ColumnCount = 45` rule to `ColumnCount = 47`.
**Why:** Re-running #23's ruleset (after #24's schema fix) produced a real, clean result: 5 of 6 rules passed, and `ColumnCount = 45` failed with `Dataset has 47.0 columns`. The `nyc311_curated` table has 45 columns in its `StorageDescriptor` (the actual Parquet file contents) plus 2 Hive partition keys (`year`, `month`, defined separately as `PartitionKeys` — derived from the S3 folder path, not stored in the files themselves). Spark's logical view of the table for DQ evaluation counts both together: 45 + 2 = 47. This wasn't obvious beforehand — it's a real, useful thing learned from running the check, not something to have guessed correctly in advance.
**Rejected:** N/A — this is a factual correction once the true count was confirmed by evaluation, not a judgment call.
**Status:** Confirmed. Re-run scored **1.0 — all 6 rules pass**, including the corrected `ColumnCount = 47`. Phase 2 is now fully complete.

### 26. Phase 3 (gold layer) built with Athena CTAS
**Decision:** Built the gold layer as four Athena `CREATE TABLE AS SELECT` tables (Parquet, under `gold/311/<table>/`), each defined by one SQL file in `src/gold/` and rebuilt by `scripts/build_gold.py` (drop table → clear its S3 folder → CTAS). No dashboard. The tables: `gold_complaints_by_borough_month`, `gold_complaint_types_by_month` (with a per-month rank), `gold_resolution_time_by_agency` (avg/median hours to close), `gold_status_backlog_by_borough`. Also set the Athena workgroup's `EnforceWorkGroupConfiguration: false`.
**Why:** CTAS is pure SQL — the closest cloud equivalent to Postgres materialized views, matching existing skills — and needs no new AWS service or ongoing cost. A dashboard (e.g. QuickSight) would add a real monthly charge, deferred until wanted. The workgroup change was forced by a real error: CloudFormation enables workgroup enforcement by default, which forbids CTAS from choosing its own `external_location`.
**Verification:** all three count-based gold tables sum to exactly 6,366,286 — matching silver, so nothing lost or double-counted. Results are realistic (HEAT/HOT WATER tops January 2026; NYPD closes in ~2.8h avg vs. DOB ~870h). One data finding: 246 curated rows have a null `status`, kept as-is.
**Rejected:** A Glue job for aggregation — more machinery than plain SQL needs at this size. Defining gold tables as CloudFormation `AWS::Glue::Table` resources — CTAS creates the table and its data together; the SQL files plus the runner are the reproducible source of truth instead.
**Status:** Decided, built, and verified.
