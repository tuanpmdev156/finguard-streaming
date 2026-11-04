"""Shared PostgreSQL helpers for the scripts in this folder."""

import os
from contextlib import contextmanager

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

load_dotenv()


@contextmanager
def pg_cursor():
    """Yield a cursor inside ONE transaction: commit on success, rollback on any error."""
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=os.getenv("PG_PORT", "5432"),
        user=os.getenv("PG_USER", "finguard"),
        password=os.environ["PG_PASSWORD"],
        dbname=os.getenv("PG_DB", "finguard_oltp"),
    )
    try:
        with conn, conn.cursor() as cur:
            yield cur
    finally:
        conn.close()


def insert_rows(cur, table: str, rows: list[dict]) -> None:
    """Bulk insert a list of dicts (dict keys = column names)."""
    if not rows:
        return
    cols = list(rows[0])
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
    execute_values(cur, sql, [tuple(row[c] for c in cols) for row in rows])

