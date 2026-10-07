import datetime
import json
import os
import urllib.parse
import urllib.request

import boto3

BUCKET_NAME = os.environ["BUCKET_NAME"]
WATERMARK_PARAM_NAME = os.environ["WATERMARK_PARAM_NAME"]
SOCRATA_DATASET_ID = os.environ["SOCRATA_DATASET_ID"]
MAX_PAGES_PER_RUN = int(os.environ.get("MAX_PAGES_PER_RUN", "5"))

PAGE_SIZE = 50000
SOCRATA_URL = f"https://data.cityofnewyork.us/resource/{SOCRATA_DATASET_ID}.json"

ssm = boto3.client("ssm")
s3 = boto3.client("s3")


def fetch_page(watermark, offset):
    query = {
        "$where": f"created_date > '{watermark}'",
        "$order": "created_date ASC",
        "$limit": PAGE_SIZE,
        "$offset": offset,
    }
    url = f"{SOCRATA_URL}?{urllib.parse.urlencode(query)}"
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.loads(response.read())


def handler(event, context):
    watermark = ssm.get_parameter(Name=WATERMARK_PARAM_NAME)["Parameter"]["Value"]
    max_created = watermark
    all_records = []
    pages_fetched = 0

    for page in range(MAX_PAGES_PER_RUN):
        rows = fetch_page(watermark, page * PAGE_SIZE)
        pages_fetched += 1
        if not rows:
            break
        all_records.extend(rows)
        last_created = rows[-1].get("created_date")
        if last_created:
            max_created = last_created
        if len(rows) < PAGE_SIZE:
            break

    if all_records:
        run_ts = datetime.datetime.now(datetime.UTC)
        key = (
            f"raw/311/incremental/year={run_ts:%Y}/month={run_ts:%m}/day={run_ts:%d}/"
            f"batch_{run_ts:%H%M%S}.json"
        )
        body = "\n".join(json.dumps(record) for record in all_records)
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=body.encode("utf-8"))
        ssm.put_parameter(
            Name=WATERMARK_PARAM_NAME,
            Value=max_created,
            Type="String",
            Overwrite=True,
        )

    return {
        "records_pulled": len(all_records),
        "pages_fetched": pages_fetched,
        "watermark_advanced_to": max_created,
    }
