"""
NYC 311 ETL Phase 2 - transform job (AWS Glue Python Shell).

Reads the three raw sources directly from S3 (bypassing the Glue Catalog's
inferred types on purpose - see Docs/decisionlog.md #23 and Docs/Architecture.md
Sec 13), cleans and unifies them into one schema, deduplicates across sources,
and writes the result as partitioned Parquet to the curated zone.

Memory design (Docs/decisionlog.md #24): Python Shell jobs are capped at 16GB
(MaxCapacity=1 is the ceiling for this job type - there is no bigger option,
unlike Spark). ~9M total records ruled out ever holding the full dataset in
memory (decisionlog #18 already measured ~438MB per 50,000 parsed records in
Lambda - scaled up, the full dataset would need ~80GB). So this processes one
file/chunk at a time (each naturally <=50,000 records, the same chunk size
already proven safe), and implements cross-source dedup with a lightweight
set of already-seen unique_keys (a few hundred MB, not full records) rather
than a global sort-and-drop-duplicates over everything at once.
"""

import io
import json
import sys
import uuid

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ["BUCKET_NAME"])
BUCKET_NAME = args["BUCKET_NAME"]

HISTORICAL_CSV_KEY = "raw/311/historical/NYC_Data_From_jan1st_2025_to_mar31st_2025.csv"
API_BACKFILL_PREFIX = "raw/311/historical/api_backfill_2025-04-01_to_2026-08-02/"
INCREMENTAL_PREFIX = "raw/311/incremental/"
CURATED_PREFIX = "curated/311/"
CSV_CHUNK_SIZE = 50000

s3 = boto3.client("s3")

CSV_COLUMN_MAP = {
    "unique key": "unique_key",
    "created date": "created_date",
    "closed date": "closed_date",
    "agency": "agency",
    "agency name": "agency_name",
    "problem (formerly complaint type)": "complaint_type",
    "problem detail (formerly descriptor)": "descriptor",
    "additional details": "additional_details",
    "location type": "location_type",
    "incident zip": "incident_zip",
    "incident address": "incident_address",
    "street name": "street_name",
    "cross street 1": "cross_street_1",
    "cross street 2": "cross_street_2",
    "intersection street 1": "intersection_street_1",
    "intersection street 2": "intersection_street_2",
    "address type": "address_type",
    "city": "city",
    "landmark": "landmark",
    "facility type": "facility_type",
    "status": "status",
    "due date": "due_date",
    "resolution description": "resolution_description",
    "resolution action updated date": "resolution_action_updated_date",
    "community board": "community_board",
    "council district": "council_district",
    "police precinct": "police_precinct",
    "bbl": "bbl",
    "borough": "borough",
    "x coordinate (state plane)": "x_coordinate_state_plane",
    "y coordinate (state plane)": "y_coordinate_state_plane",
    "open data channel type": "open_data_channel_type",
    "park facility name": "park_facility_name",
    "park borough": "park_borough",
    "vehicle type": "vehicle_type",
    "taxi company borough": "taxi_company_borough",
    "taxi pick up location": "taxi_pick_up_location",
    "bridge highway name": "bridge_highway_name",
    "bridge highway direction": "bridge_highway_direction",
    "road ramp": "road_ramp",
    "bridge highway segment": "bridge_highway_segment",
    "latitude": "latitude",
    "longitude": "longitude",
    # "location" is intentionally dropped - both sources carry plain
    # latitude/longitude columns already, so the structured POINT/struct
    # form is pure redundancy. See Docs/Architecture.md Sec 13 update.
}

UNIFIED_COLUMNS = [
    "unique_key", "created_date", "closed_date", "agency", "agency_name",
    "complaint_type", "descriptor", "descriptor_2", "additional_details",
    "location_type", "incident_zip", "incident_address", "street_name",
    "cross_street_1", "cross_street_2", "intersection_street_1", "intersection_street_2",
    "address_type", "city", "landmark", "facility_type", "status", "due_date",
    "resolution_description", "resolution_action_updated_date", "community_board",
    "council_district", "police_precinct", "bbl", "borough",
    "x_coordinate_state_plane", "y_coordinate_state_plane", "open_data_channel_type",
    "park_facility_name", "park_borough", "vehicle_type", "taxi_company_borough",
    "taxi_pick_up_location", "bridge_highway_name", "bridge_highway_direction",
    "road_ramp", "bridge_highway_segment", "latitude", "longitude", "source",
]

NUMERIC_COLUMNS = ["latitude", "longitude", "x_coordinate_state_plane", "y_coordinate_state_plane"]
DATE_COLUMNS = ["created_date", "closed_date", "due_date", "resolution_action_updated_date"]

# Explicit schema, matching infra/phase2_transform.yaml's CuratedTable column
# types exactly. Every chunk is written against THIS schema, not whatever
# pyarrow would infer per-chunk - inferred per-chunk types caused a real
# cross-file Parquet encoding mismatch that broke Glue Data Quality's Spark
# reader (Athena tolerated it, Spark didn't). See Docs/decisionlog.md #25.
CURATED_SCHEMA = pa.schema([
    (col, pa.timestamp("us")) if col in DATE_COLUMNS
    else (col, pa.float64()) if col in NUMERIC_COLUMNS
    else (col, pa.string())
    for col in UNIFIED_COLUMNS
])

# Processed in this order so the lightweight seen_keys set correctly
# implements "keep the most-recently-ingested source" per Architecture.md Sec 14.
SOURCE_ORDER = ["incremental", "historical_api_backfill", "historical_csv"]

seen_keys = set()
total_written = 0
total_seen = 0


def list_s3_keys(prefix, suffix):
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                keys.append(obj["Key"])
    return keys


def normalize_placeholders(df):
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].where(
                ~df[col].astype(str).str.fullmatch(r"Unspecified(\s+\S+)?", na=False),
                other=pd.NA,
            )
    return df


def coerce_types(df):
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", "", regex=False), errors="coerce"
            )
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def align_to_unified_schema(df):
    for col in UNIFIED_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df[UNIFIED_COLUMNS]


def process_chunk(df, source_name):
    """Dedup against already-seen keys, clean, and write this chunk to curated Parquet."""
    global total_written, total_seen

    df["source"] = source_name
    df = align_to_unified_schema(df)
    total_seen += len(df)

    df = df[~df["unique_key"].isin(seen_keys)]
    df = df.drop_duplicates(subset="unique_key", keep="first")
    seen_keys.update(df["unique_key"].tolist())

    if df.empty:
        return

    df = normalize_placeholders(df)
    df = coerce_types(df)
    write_chunk_partitioned(df)
    total_written += len(df)


def write_chunk_partitioned(df):
    df = df.copy()
    df["_year"] = df["created_date"].dt.year
    df["_month"] = df["created_date"].dt.month
    for (year, month), group in df.groupby(["_year", "_month"], dropna=True):
        buf = io.BytesIO()
        table = pa.Table.from_pandas(
            group.drop(columns=["_year", "_month"]), schema=CURATED_SCHEMA, preserve_index=False
        )
        pq.write_table(table, buf)
        key = (
            f"{CURATED_PREFIX}year={int(year)}/month={int(month):02d}/"
            f"part-{uuid.uuid4().hex[:12]}.parquet"
        )
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=buf.getvalue())


def process_historical_csv():
    print("Processing historical CSV (chunked)...")
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=HISTORICAL_CSV_KEY)
    text_stream = io.TextIOWrapper(obj["Body"], encoding="utf-8-sig")
    chunk_num = 0
    for chunk in pd.read_csv(text_stream, dtype=str, chunksize=CSV_CHUNK_SIZE):
        chunk.columns = [c.strip().lower() for c in chunk.columns]
        chunk = chunk.rename(columns=CSV_COLUMN_MAP)
        chunk = chunk.drop(columns=["location"], errors="ignore")
        process_chunk(chunk, "historical_csv")
        chunk_num += 1
        print(f"  chunk {chunk_num} done (total_seen={total_seen:,}, total_written={total_written:,})")


def process_jsonl_source(prefix, source_name):
    print(f"Processing {source_name} (file by file)...")
    keys = list_s3_keys(prefix, ".json")
    print(f"  {len(keys)} files")
    for i, key in enumerate(keys, 1):
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=key)
        records = []
        for line in obj["Body"].read().decode("utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            rec.pop("location", None)
            records.append(rec)
        if records:
            chunk = pd.DataFrame.from_records(records)
            process_chunk(chunk, source_name)
        if i % 20 == 0 or i == len(keys):
            print(f"  {i}/{len(keys)} files done (total_seen={total_seen:,}, total_written={total_written:,})")


def main():
    process_jsonl_source(INCREMENTAL_PREFIX, "incremental")
    process_jsonl_source(API_BACKFILL_PREFIX, "historical_api_backfill")
    process_historical_csv()
    print(f"Done. total_seen={total_seen:,}, total_written={total_written:,} "
          f"(deduped={total_seen - total_written:,}) to s3://{BUCKET_NAME}/{CURATED_PREFIX}")


if __name__ == "__main__":
    main()
