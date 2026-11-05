"""
PySpark Structured Streaming Fraud Processor
Reads from Redpanda ('fin_transactions'), applies Watermarking & Sliding Window
for Velocity Fraud detection, auto-freezes cards, and writes Snappy Parquet to GCS.

Usage:
    uv run python spark/streaming_consumer.py
    uv run python spark/streaming_consumer.py --local
"""

import io
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window
from pyspark.version import __version__ as SPARK_VERSION

load_dotenv()

# 1. Environment & Runtime Configurations
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "fin_transactions")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "")
LOCAL_MODE = "--local" in sys.argv

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "finguard")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_DB = os.getenv("PG_DB", "finguard_oltp")

CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "data/checkpoints").rstrip("/")
LOCAL_OUTPUT_DIR = Path("data/streaming_transactions")

# 2. Strict JSON Schema matching scripts/transaction_producer.py
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("card_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("merchant_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("transaction_channel", StringType(), True),
    StructField("transaction_country", StringType(), True),
    StructField("transaction_city", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("transaction_timestamp", StringType(), False),
    StructField("injected_scenario", StringType(), True),
])

# 3. Transform PyArrow Schema for Parquet
PARQUET_SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("card_id", pa.string()),
    ("customer_id", pa.string()),
    ("merchant_id", pa.string()),
    ("amount", pa.float64()),
    ("currency", pa.string()),
    ("transaction_channel", pa.string()),
    ("transaction_country", pa.string()),
    ("transaction_city", pa.string()),
    ("ip_address", pa.string()),
    ("transaction_timestamp", pa.timestamp("us", tz="UTC")),
    ("injected_scenario", pa.string()),
    ("batch_card_tx_count", pa.int64()),
    ("stream_fraud_flag", pa.bool_()),
    ("stream_fraud_reason", pa.string()),
    ("recommended_action", pa.string()),
    ("ingested_at", pa.timestamp("us", tz="UTC")),
])


def create_spark_session() -> SparkSession:
    """Initialize SparkSession with matching spark-sql-kafka connector."""
    scala_ver = "2.13" if int(SPARK_VERSION.split(".")[0]) >= 4 else "2.12"
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala_ver}:{SPARK_VERSION}"

    return (
        SparkSession.builder.appName("FinGuard-Streaming-Fraud-Processor")
        .master("local[*]")
        .config("spark.jars.packages", kafka_pkg)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def auto_freeze_cards(card_ids: set[str]) -> set[str]:
    """Closed-Loop Remediation: Immediately deactivate compromised cards in PostgreSQL OLTP."""
    if not card_ids:
        return set()
    try:
        conn = psycopg2.connect(
            host=PG_HOST,
            port=PG_PORT,
            user=PG_USER,
            password=PG_PASSWORD,
            dbname=PG_DB,
            connect_timeout=5,
        )
        try:
            with conn, conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE cards
                    SET is_active = FALSE
                    WHERE card_id = ANY(%s)
                    RETURNING card_id
                    """,
                    (list(card_ids),),
                )
                frozen_ids = {row[0] for row in cur.fetchall()}
                if frozen_ids:
                    short_ids = ", ".join(cid[:8] + "..." for cid in frozen_ids)
                    print(f"🔒 [AUTO-FREEZE] Locked {len(frozen_ids)} card(s) in PostgreSQL OLTP: {short_ids}")
                return frozen_ids
        finally:
            conn.close()
    except Exception as exc:
        print(f"[WARN] Closed-loop auto-freeze failed: {exc}")
        return set()


def write_parquet_sink(df_rows: list[dict], batch_id: int) -> str:
    """Convert transaction records to Parquet and write to GCS or Local."""
    arrow_table = pa.Table.from_pylist(df_rows, schema=PARQUET_SCHEMA)
    ts_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"part_batch_{batch_id:05d}_{ts_str}.parquet"

    # Write to local directory for testing/debugging
    if LOCAL_MODE:
        LOCAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = LOCAL_OUTPUT_DIR / filename
        pq.write_table(arrow_table, out_path, compression="snappy")
        return str(out_path)

    # Write to GCS using google-cloud-storage
    from google.cloud import storage

    bucket = storage.Client().bucket(GCS_BUCKET_NAME)
    blob_path = f"raw/streaming_transactions/{filename}"
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="snappy")
    bucket.blob(blob_path).upload_from_string(
        buf.getvalue(), content_type="application/octet-stream"
    )
    return f"gs://{GCS_BUCKET_NAME}/{blob_path}"


def process_transactions_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Micro-batch processor: flag stream fraud rules, assign decision tiers, auto-freeze cards, and write Parquet."""
    if batch_df.isEmpty():
        return
    enriched_df = (
        batch_df.withColumn("batch_card_tx_count", F.count("transaction_id").over(Window.partitionBy("card_id")))
        .withColumn(
            "stream_fraud_reason",
            F.when(F.col("amount") <= 0, F.lit("CORRUPT_AMOUNT"))
            .when(F.col("batch_card_tx_count") >= 4, F.lit("VELOCITY_ATTACK"))
            .when(F.col("amount") > 5000.0, F.lit("HIGH_AMOUNT_SPIKE"))
            .when(
                F.col("injected_scenario").isin("GEO_MISMATCH", "COMPROMISED_CARD"),
                F.col("injected_scenario"),
            )
            .otherwise(F.lit("CLEAN")),
        )
        .withColumn(
            "recommended_action",
            F.when(F.col("stream_fraud_reason") == "VELOCITY_ATTACK", F.lit("AUTO_FREEZE_CARD"))
            .when(F.col("stream_fraud_reason") == "HIGH_AMOUNT_SPIKE", F.lit("HOLD_FOR_STEP_UP_AUTH"))
            .when(
                F.col("stream_fraud_reason").isin("GEO_MISMATCH", "COMPROMISED_CARD"),
                F.lit("DECLINE_AND_CREATE_CASE"),
            )
            .when(F.col("stream_fraud_reason") == "CORRUPT_AMOUNT", F.lit("QUARANTINE_RECORD"))
            .otherwise(F.lit("APPROVE")),
        )
        .withColumn(
            "stream_fraud_flag",
            F.col("stream_fraud_reason").isin(
                "VELOCITY_ATTACK", "HIGH_AMOUNT_SPIKE", "GEO_MISMATCH", "COMPROMISED_CARD"
            ),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    rows = [row.asDict() for row in enriched_df.collect()]
    target_uri = write_parquet_sink(rows, batch_id)

    critical_events = [r for r in rows if r["stream_fraud_flag"]]
    cards_to_freeze = {
        r["card_id"] for r in critical_events if r["recommended_action"] == "AUTO_FREEZE_CARD"
    }
    frozen_cards = auto_freeze_cards(cards_to_freeze)

    print(
        f"[Batch {batch_id:>4}] Processed {len(rows):>4} tx | "
        f"Critical Fraud: {len(critical_events):>3} | "
        f"Auto-Frozen Cards: {len(frozen_cards):>2} | Sink: {target_uri}"
    )


def process_velocity_window_batch(window_df: DataFrame, batch_id: int) -> None:
    """Stateful sliding window processor: detect cross-batch Velocity Attacks & auto-freeze cards."""
    if window_df.isEmpty():
        return

    alerts = window_df.collect()
    cards_to_freeze = {row["card_id"] for row in alerts}
    frozen_cards = auto_freeze_cards(cards_to_freeze)

    for row in alerts:
        w_start = row["window"]["start"].strftime("%H:%M:%S")
        w_end = row["window"]["end"].strftime("%H:%M:%S")
        freeze_status = "🔒 FROZEN" if row["card_id"] in frozen_cards else "⏳ PENDING_FREEZE"
        msg = (
            f"[CRITICAL] Window `[{w_start} - {w_end}]` | "
            f"Card: `{row['card_id'][:8]}...` ({freeze_status}) | "
            f"Tx Count: `{row['tx_count']}` | Exposure: `${row['total_amount']:,.2f}` | "
            f"Action: `AUTO_FREEZE_CARD`"
        )
        print(f"🚨 [VELOCITY WINDOW ALERT] {msg}")


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 1. Read streaming events from Redpanda (Kafka API)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parse JSON payload & cast transaction_timestamp to TimestampType
    parsed_stream = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_payload")
        .select(F.from_json(F.col("json_payload"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
        .filter(F.col("transaction_id").isNotNull())
        .withColumn("transaction_timestamp", F.to_timestamp("transaction_timestamp"))
    )

    # 3. Stream 1: Stateful Watermarking (2m) & Sliding Window (2m window, 30s slide)
    velocity_window_stream = (
        parsed_stream.withWatermark("transaction_timestamp", "2 minutes")
        .groupBy(
            F.window(F.col("transaction_timestamp"), "2 minutes", "30 seconds"),
            F.col("card_id"),
        )
        .agg(
            F.count("transaction_id").alias("tx_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
        )
        .filter(F.col("tx_count") >= 4)
    )

    velocity_query = (
        velocity_window_stream.writeStream.outputMode("update")
        .foreachBatch(process_velocity_window_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/velocity_window")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # 4. Stream 2: Transaction Enrichment & Snappy Parquet Sink (GCS / Local)
    sink_query = (
        parsed_stream.writeStream.outputMode("append")
        .foreachBatch(process_transactions_batch)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/parquet_sink")
        .trigger(processingTime="30 seconds")
        .start()
    )

    mode_str = "LOCAL (data/streaming_transactions)" if LOCAL_MODE else f"GCS (gs://{GCS_BUCKET_NAME})"
    print(f"FinGuard PySpark Streaming started! Sink mode: {mode_str}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

