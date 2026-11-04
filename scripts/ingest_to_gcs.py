"""
Sync reference tables: PostgreSQL -> Snappy Parquet -> GCS.

For each table:
    1. Extract  : SELECT all rows from PostgreSQL
    2. Validate : cast to a fixed PyArrow schema (fails on schema drift),
                  check row count and primary key
    3. Load     : write Snappy Parquet to gs://<bucket>/raw/reference/<table>/<table>.parquet
                  (same path overwritten every run => idempotent, BigQuery external tables point here)

Usage:
    uv run python scripts/ingest_to_gcs.py
    uv run python scripts/ingest_to_gcs.py --local   # write to data/reference/ instead of GCS
"""

import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from db_connection import pg_cursor

timestamp = pa.timestamp("us", tz="UTC")

# Expected schema per table (must match docker/postgres/init_tables.sql). First column = primary key.
SCHEMAS = {
    "customers": pa.schema([
        ("customer_id", pa.string()),
        ("full_name", pa.string()),
        ("email", pa.string()),
        ("phone", pa.string()),
        ("home_country", pa.string()),
        ("home_city", pa.string()),
        ("risk_profile", pa.string()),
        ("created_at", timestamp),
        ("updated_at", timestamp),
    ]),
    "cards": pa.schema([
        ("card_id", pa.string()),
        ("customer_id", pa.string()),
        ("card_number_masked", pa.string()),
        ("card_brand", pa.string()),
        ("card_type", pa.string()),
        ("credit_limit", pa.decimal128(12, 2)),  # -> BigQuery NUMERIC
        ("is_active", pa.bool_()),
        ("expiry_date", pa.date32()),
        ("issued_at", timestamp),
    ]),
    "merchants": pa.schema([
        ("merchant_id", pa.string()),
        ("merchant_name", pa.string()),
        ("mcc_code", pa.string()),
        ("merchant_category", pa.string()),
        ("country", pa.string()),
        ("city", pa.string()),
        ("is_high_risk_mcc", pa.bool_()),
        ("created_at", timestamp),
    ]),
}


def extract(cur, table: str) -> pa.Table:
    schema = SCHEMAS[table]
    cur.execute(f"SELECT {', '.join(schema.names)} FROM {table}")
    rows = [dict(zip(schema.names, row)) for row in cur.fetchall()]
    return pa.Table.from_pylist(rows, schema=schema)  # raises if a value doesn't fit the schema


def validate(cur, table: str, data: pa.Table) -> None:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    source_count = cur.fetchone()[0]
    if data.num_rows != source_count:
        raise ValueError(f"{table}: extracted {data.num_rows} rows, source has {source_count}")

    pk = data.column(0)
    if pk.null_count > 0 or len(pc.unique(pk)) != data.num_rows:
        raise ValueError(f"{table}: primary key '{data.column_names[0]}' has nulls or duplicates")


def main() -> None:
    local_mode = "--local" in sys.argv
    if not local_mode:
        from google.cloud import storage

        bucket = storage.Client().bucket(os.environ["GCS_BUCKET_NAME"])

    with pg_cursor() as cur:
        for table in SCHEMAS:
            data = extract(cur, table)
            validate(cur, table, data)

            path = f"raw/reference/{table}/{table}.parquet"
            if local_mode:
                out = Path("data/reference") / table / f"{table}.parquet"
                out.parent.mkdir(parents=True, exist_ok=True)
                pq.write_table(data, out, compression="snappy")
                target = out
            else:
                buf = pa.BufferOutputStream()
                pq.write_table(data, buf, compression="snappy")
                bucket.blob(path).upload_from_string(
                buf.getvalue().to_pybytes(),
                content_type="application/octet-stream")
                target = f"gs://{bucket.name}/{path}"

            print(f"{table:<10} {data.num_rows:>6} rows -> {target}")


if __name__ == "__main__":
    main()
