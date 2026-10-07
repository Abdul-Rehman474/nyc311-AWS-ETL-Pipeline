# Runbook: deploy and operate the pipeline

A self-directed ETL project: lands NYC 311 Service Requests data (historical + a live daily incremental feed) into an S3 raw zone on AWS. See `Docs/Architecture.md` for the technical design and `Docs/decisionlog.md` for why things ended up the way they did — this file is just the practical "how do I run this" reference.

## Prerequisites

- **AWS CLI v2** installed and on your PATH (`aws --version`).
- **Python 3.12+** installed (`python --version`).
- **AWS credentials configured** for the account you want to deploy into:
  ```
  aws configure
  ```
  You'll need an IAM user's Access Key ID + Secret Access Key (Console → IAM → Users → your user → Security credentials → Create access key → CLI use case), region `us-east-1`, output format `json`. Verify with:
  ```
  aws sts get-caller-identity
  ```
  This should return your account ID with no error before doing anything else.
- The historical CSV must exist locally at `Data/NYC_Data_From_jan1st_2025_to_mar31st_2025.csv` (not included in this repository; it is a public NYC Open Data export of dataset `erm2-nwe9`).

## Deploy the infrastructure

```
aws cloudformation deploy --template-file infra/template.yaml --stack-name nyc311-etl --capabilities CAPABILITY_IAM
```

This creates (or updates) everything in one shot: the S3 bucket, the incremental Lambda (code embedded directly in the template — see `Docs/Architecture.md` §5), the daily EventBridge schedule, the IAM role, and the SSM watermark parameter (seeded to `2025-03-31T23:59:59.000`).

Confirm it succeeded and get the bucket/function names:
```
aws cloudformation describe-stacks --stack-name nyc311-etl --query "Stacks[0].Outputs"
```

## Historical backfill (one-time)

```
python scripts/backfill_historical.py <bucket-name>
```

Uploads the historical CSV as-is to `raw/311/historical/`. Verify:
```
aws s3 ls s3://<bucket-name>/raw/311/historical/
```

## Clearing an incremental backlog (only needed after a gap since the historical cutoff)

If it's been a while since `2025-03-31` (or since you last ran this), the incremental Lambda needs to catch up before daily runs are meaningful. Each invocation only pulls up to 50,000 records (`MAX_PAGES_PER_RUN=1` — see `Docs/decisionlog.md` #17 for why it's capped this low), so a large gap needs repeated manual invocations:

```
aws lambda invoke --function-name <function-name> --cli-read-timeout 320 out.json && cat out.json
```

Repeat until the response's `records_pulled` drops below 50,000 — that means it's caught up to the present. For a large gap (months), script this as a loop rather than invoking by hand each time; see `Docs/decisionlog.md` #17 for how long the original 16-month catch-up took (~104 invocations, ~35 minutes, run in background batches).

Check progress anytime:
```
aws ssm get-parameter --name /nyc311-etl/incremental/watermark --query "Parameter.Value" --output text
```

## Verifying it's actually running unattended

The EventBridge rule fires daily at **6:00 AM UTC** with zero manual involvement needed after that. To confirm it's actually working on its own (don't just assume):

```
aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name Invocations \
  --dimensions Name=FunctionName,Value=<function-name> \
  --start-time <a few days ago> --end-time <now> --period 86400 --statistics Sum
```

You should see roughly one invocation per day. Also spot-check S3 for new files landing under `raw/311/incremental/year=/month=/day=/` without you doing anything.

**A known, harmless quirk**: some days won't have their own file. This isn't data loss — a day's run occasionally fails silently, and because normal daily volume (~10-15k records) is well under the 50,000-record cap, the *next* successful run simply absorbs the previous day's backlog into one larger file. You can tell this happened by file size (a "catch-up" file is roughly 2x a normal day's size). See `Docs/decisionlog.md` #19 for the full investigation that confirmed this is safe.

## Phase 2: transform and query layer

1. Upload the Glue job script (the job reads it from S3):
   ```
   aws s3 cp src/glue_transform/clean_and_unify.py s3://<bucket-name>/glue-scripts/clean_and_unify.py
   ```
2. Deploy the second stack:
   ```
   aws cloudformation deploy --template-file infra/phase2_transform.yaml --stack-name nyc311-etl-phase2 --capabilities CAPABILITY_IAM --parameter-overrides RawBucketName=<bucket-name>
   ```
3. Run the transform job (about 25 minutes for ~6.4M rows). It streams the raw data in chunks, cleans and deduplicates it, and writes Parquet to `curated/311/`:
   ```
   aws glue start-job-run --job-name nyc311-transform-job
   aws glue get-job-run --job-name nyc311-transform-job --run-id <JobRunId>
   ```
4. Load the table's partitions by running this in Athena (database `nyc311_db`, workgroup `nyc311-workgroup`):
   ```
   MSCK REPAIR TABLE nyc311_curated
   ```
5. Run the data-quality ruleset (the role ARN is in the stack outputs):
   ```
   aws glue start-data-quality-ruleset-evaluation-run --data-source '{"GlueTable":{"DatabaseName":"nyc311_db","TableName":"nyc311_curated"}}' --role <GlueServiceRoleArn> --ruleset-names nyc311-curated-ruleset
   ```

Optionally run the three crawlers (`nyc311-historical-csv-crawler`, `nyc311-historical-api-backfill-crawler`, `nyc311-incremental-crawler`) to register the raw tables in the Glue Data Catalog for ad-hoc Athena queries on raw data.

## Scope notes

- The daily ingestion schedule is **disabled** in `infra/template.yaml` (`State: DISABLED`) to keep ongoing cost near zero. Set it to `ENABLED` and redeploy to resume daily ingestion.
- There is no dashboard layer; the gold tables are queried with Athena SQL.

## Rebuilding the gold layer (Phase 3)

```
python scripts/build_gold.py <bucket-name>
```

Drops and recreates the four `gold_*` Athena tables from `src/gold/*.sql`. Safe to re-run.
