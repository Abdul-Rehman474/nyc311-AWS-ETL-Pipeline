import pathlib
import sys

import boto3

HISTORICAL_FILE = "NYC_Data_From_jan1st_2025_to_mar31st_2025.csv"
S3_KEY = f"raw/311/historical/{HISTORICAL_FILE}"


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python backfill_historical.py <bucket-name>")
    bucket = sys.argv[1]

    file_path = pathlib.Path(__file__).parent.parent / "Data" / HISTORICAL_FILE
    if not file_path.exists():
        raise SystemExit(f"Historical file not found: {file_path}")

    s3 = boto3.client("s3")
    print(f"Uploading {file_path} ({file_path.stat().st_size:,} bytes) to s3://{bucket}/{S3_KEY} ...")
    s3.upload_file(str(file_path), bucket, S3_KEY)
    print("Done.")


if __name__ == "__main__":
    main()
