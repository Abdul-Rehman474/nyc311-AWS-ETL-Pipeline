"""Rebuild the gold layer: one Athena CTAS table per SQL file in src/gold/.

Usage: python scripts/build_gold.py <bucket-name>
"""

import pathlib
import sys
import time

import boto3

DATABASE = "nyc311_db"
WORKGROUP = "nyc311-workgroup"
GOLD_PREFIX = "gold/311/"
SQL_DIR = pathlib.Path(__file__).parent.parent / "src" / "gold"

athena = boto3.client("athena")
s3 = boto3.client("s3")


def run_query(sql):
    qid = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        WorkGroup=WORKGROUP,
    )["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"{status['State']}: {status.get('StateChangeReason')}\n{sql}")


def clear_prefix(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})


def build_table(bucket, sql_file):
    table = sql_file.stem
    location = f"s3://{bucket}/{GOLD_PREFIX}{table}/"
    print(f"Building {table} ...")
    run_query(f"DROP TABLE IF EXISTS {table}")
    clear_prefix(bucket, f"{GOLD_PREFIX}{table}/")
    select_sql = sql_file.read_text(encoding="utf-8").strip().rstrip(";")
    run_query(
        f"CREATE TABLE {table} "
        f"WITH (format = 'PARQUET', external_location = '{location}') AS\n{select_sql}"
    )
    print(f"  done -> {location}")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python build_gold.py <bucket-name>")
    bucket = sys.argv[1]
    for sql_file in sorted(SQL_DIR.glob("*.sql")):
        build_table(bucket, sql_file)


if __name__ == "__main__":
    main()
