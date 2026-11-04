"""Airflow DAG: finguard_orchestrator

Hybrid Orchestration Pipeline for FinGuard Streaming Fraud Detection:
0. Simulate OLTP Reference Data Changes (new customers/cards, blocked cards, credit limit updates).
1. Sync Reference Data (PostgreSQL -> PyArrow Schema Validation -> GCS Snappy Parquet).
2. Verify PySpark Structured Streaming Parquet files exist on GCS.
3. Provision/Refresh BigQuery External Tables in 'finguard_raw'.
4. Execute Raw Data Quality Audit Gates (row counts & primary key null checks).
5. Run dbt transformations (staging views, quarantine routing, production marts) & dbt tests.
6. Trigger GitHub Actions workflow_dispatch to update Evidence.dev BI Dashboard.
"""

from __future__ import annotations

import os
import sys
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG  # type: ignore[import-not-found]
from airflow.operators.bash import BashOperator  # type: ignore[import-not-found]
from airflow.operators.python import PythonOperator  # type: ignore[import-not-found]
from airflow.providers.google.cloud.operators.bigquery import (  # type: ignore[import-not-found]
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)

# ---------------------------------------------------------------------------
# 1. Pipeline Configuration & Constants
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "fraud-risk-analytics-131026")
GCS_BUCKET = os.environ.get("GCS_BUCKET_NAME", "finguard-streaming-291026-bucket")
BQ_DATASET_RAW = os.environ.get("BQ_DATASET_RAW", "finguard_raw")
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080").rstrip("/")
AIRFLOW_DAG_SCHEDULE = os.environ.get("AIRFLOW_DAG_SCHEDULE", "@hourly")

AIRFLOW_HOME = Path(os.environ.get("AIRFLOW_HOME", "/opt/airflow"))
SCRIPTS_DIR = AIRFLOW_HOME / "scripts"
DBT_DIR = AIRFLOW_HOME / "dbt"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

REFERENCE_TABLES = {
    "customers": "customer_id",
    "cards": "card_id",
    "merchants": "merchant_id",
}

# Set task_id to simulate task failure for resilience & Slack alert testing (Phase 3.2)
SIMULATE_FAIL_TASK = os.environ.get("SIMULATE_FAIL_TASK", "None")


# ---------------------------------------------------------------------------
# 2. Failure Callback & Task Callables
# ---------------------------------------------------------------------------
def slack_alert_on_failure(context) -> None:
    """Send rich Slack notification when a task exhausts all retries."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("[WARN] SLACK_WEBHOOK_URL is not configured. Skipping Slack alert.")
        return

    ti = context.get("task_instance")
    dag_id = ti.dag_id
    task_id = ti.task_id
    logical_date = context.get("logical_date").strftime("%Y-%m-%d %H:%M:%S UTC")

    encoded_run_id = urllib.parse.quote(ti.run_id, safe="")
    log_url = (
        f"{AIRFLOW_BASE_URL}/dags/{dag_id}/grid"
        f"?tab=logs&dag_run_id={encoded_run_id}&task_id={task_id}"
    )
    exception = context.get("exception", "No exception message provided")
    error_summary = str(exception)[:350]

    slack_msg = {
        "text": f":rotating_light: FinGuard Pipeline Alert: Task `{task_id}` FAILED in DAG `{dag_id}`",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚨 FinGuard Airflow Pipeline Failure",
                    "emoji": True,
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n`{dag_id}`"},
                    {"type": "mrkdwn", "text": f"*Task:*\n`{task_id}`"},
                    {"type": "mrkdwn", "text": f"*Execution Time:*\n{logical_date}"},
                    {"type": "mrkdwn", "text": "*Severity:*\n*P1 - High (Retries Exhausted)*"},
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Root Cause / Error Message:*\n```{error_summary}```",
                },
            },
            {
                "type": "actions",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "🔍 Inspect Task Log"},
                        "url": log_url,
                        "style": "danger",
                    }
                ],
            },
        ],
    }

    try:
        response = requests.post(webhook_url, json=slack_msg, timeout=10)
        response.raise_for_status()
        print(f"[INFO] Slack failure alert sent for task={task_id}")
    except Exception as exc:
        print(f"[ERROR] Failed to send Slack alert: {exc}")


def simulate_reference_changes(**context) -> None:
    """Step 0: Simulate day-to-day OLTP reference data changes before syncing to GCS."""
    if SIMULATE_FAIL_TASK == "simulate_reference_changes":
        raise RuntimeError("SIMULATED ERROR at simulate_reference_changes")

    from scripts.postgres_incremental_load import main as run_incremental_load

    run_incremental_load()


def sync_reference_data_to_gcs(**context) -> None:
    """Step 1: Extract reference tables from PostgreSQL, validate via PyArrow, upload to GCS."""
    if SIMULATE_FAIL_TASK == "sync_reference_to_gcs":
        raise RuntimeError("SIMULATED ERROR at sync_reference_to_gcs")

    from scripts.ingest_to_gcs import main as run_ingest_to_gcs

    # Ensure --local flag is not passed when running inside Airflow
    sys.argv = [sys.argv[0]]
    run_ingest_to_gcs()


def verify_streaming_parquet_on_gcs(**context) -> None:
    """Step 2: Verify PySpark Streaming has written Snappy Parquet batches to GCS."""
    if SIMULATE_FAIL_TASK == "verify_streaming_parquet_on_gcs":
        raise RuntimeError("SIMULATED ERROR at verify_streaming_parquet_on_gcs")

    from google.cloud import storage

    prefix = "raw/streaming_transactions/"
    client = storage.Client(project=GCP_PROJECT_ID)
    blobs = [
        b
        for b in client.list_blobs(GCS_BUCKET, prefix=prefix)
        if b.name.endswith(".parquet") and b.size > 0
    ]

    if not blobs:
        raise FileNotFoundError(
            f"No non-empty Parquet files found in gs://{GCS_BUCKET}/{prefix}. "
            "Ensure PySpark Structured Streaming consumer (spark/streaming_consumer.py) is running."
        )

    total_kb = sum(b.size for b in blobs) / 1024.0
    print(
        f"[INFO] Verified {len(blobs)} streaming Parquet file(s) "
        f"in gs://{GCS_BUCKET}/{prefix} (Total: {total_kb:.2f} KB)"
    )


def trigger_evidence_deploy(**context) -> None:
    """Step 6: Trigger GitHub Actions workflow_dispatch to rebuild and deploy Evidence.dev."""
    if SIMULATE_FAIL_TASK == "trigger_evidence_deploy":
        raise RuntimeError("SIMULATED ERROR at trigger_evidence_deploy")

    github_token = os.environ.get("GITHUB_TOKEN")
    github_repo = os.environ.get("GITHUB_REPO")
    workflow_file = os.environ.get("GITHUB_WORKFLOW_FILE", "deploy-evidence.yml")
    branch = os.environ.get("GITHUB_BRANCH", "main")

    if not github_token or not github_repo:
        print(
            "[WARN] GITHUB_TOKEN or GITHUB_REPO is not set. "
            "Skipping GitHub Actions workflow_dispatch trigger."
        )
        return

    url = f"https://api.github.com/repos/{github_repo}/actions/workflows/{workflow_file}/dispatches"
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {github_token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    response = requests.post(url, headers=headers, json={"ref": branch}, timeout=15)
    if response.status_code != 204:
        raise RuntimeError(
            f"Failed to trigger GitHub Actions (HTTP {response.status_code}): {response.text}"
        )
    print(f"[INFO] Triggered '{workflow_file}' on {github_repo} (ref={branch}).")


# ---------------------------------------------------------------------------
# 3. DAG Definition & Task Graph
# ---------------------------------------------------------------------------
default_args = {
    "owner": "finguard-streaming",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": slack_alert_on_failure,
}

with DAG(
    dag_id="finguard_orchestrator",
    default_args=default_args,
    description="Hybrid ELT Orchestrator: Postgres Reference + PySpark Streaming GCS -> BigQuery -> dbt -> Evidence",
    schedule_interval=AIRFLOW_DAG_SCHEDULE,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    # 0. Simulate OLTP Reference Data Changes (PostgreSQL incremental updates)
    task_simulate_reference = PythonOperator(
        task_id="simulate_reference_changes",
        python_callable=simulate_reference_changes,
    )

    # 1. Sync Reference Tables (PostgreSQL -> GCS Parquet)
    task_sync_reference = PythonOperator(
        task_id="sync_reference_to_gcs",
        python_callable=sync_reference_data_to_gcs,
    )

    # 2. Check Streaming Parquet on GCS
    task_verify_streaming_gcs = PythonOperator(
        task_id="verify_streaming_parquet_on_gcs",
        python_callable=verify_streaming_parquet_on_gcs,
    )

    # 3A. Create/Refresh BigQuery External Tables for Reference Data
    create_ref_ext_tasks = []
    for table_name in REFERENCE_TABLES:
        sql_ext_ref = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_{table_name}`
        OPTIONS (
          format = 'PARQUET',
          uris = ['gs://{GCS_BUCKET}/raw/reference/{table_name}/{table_name}.parquet']
        );
        """
        task_ext = BigQueryInsertJobOperator(
            task_id=f"create_bq_ext_{table_name}",
            configuration={"query": {"query": sql_ext_ref, "useLegacySql": False}},
        )
        create_ref_ext_tasks.append(task_ext)

    # 3B. Create/Refresh BigQuery External Table for Streaming Transactions
    sql_ext_streaming = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_streaming_transactions`
    OPTIONS (
      format = 'PARQUET',
      uris = ['gs://{GCS_BUCKET}/raw/streaming_transactions/*.parquet']
    );
    """
    task_create_ext_streaming = BigQueryInsertJobOperator(
        task_id="create_bq_ext_streaming_transactions",
        configuration={"query": {"query": sql_ext_streaming, "useLegacySql": False}},
    )

    # 4A. Data Quality Audit Gate: Reference Tables (row count > 0 & PK not null)
    task_audit_reference = BigQueryCheckOperator(
        task_id="audit_raw_reference_quality",
        sql=f"""
        SELECT
          (SELECT COUNT(1) > 0 AND COUNTIF(customer_id IS NULL) = 0 FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_customers`)
          AND (SELECT COUNT(1) > 0 AND COUNTIF(card_id IS NULL) = 0 FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_cards`)
          AND (SELECT COUNT(1) > 0 AND COUNTIF(merchant_id IS NULL) = 0 FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_merchants`)
        """,
        use_legacy_sql=False,
    )

    # 4B. Data Quality Audit Gate: Streaming Transactions (row count > 0 & mandatory keys not null)
    task_audit_streaming = BigQueryCheckOperator(
        task_id="audit_raw_streaming_quality",
        sql=f"""
        SELECT
          COUNT(1) > 0
          AND COUNTIF(transaction_id IS NULL) = 0
          AND COUNTIF(card_id IS NULL) = 0
          AND COUNTIF(customer_id IS NULL) = 0
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.ext_streaming_transactions`
        """,
        use_legacy_sql=False,
    )

    # 5. dbt Transformations & Automated Tests
    task_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
    )

    # 6. Trigger CI/CD Evidence.dev Deployment
    task_trigger_evidence = PythonOperator(
        task_id="trigger_evidence_deploy",
        python_callable=trigger_evidence_deploy,
    )

    # -----------------------------------------------------------------------
    # Task Dependencies Graph
    # -----------------------------------------------------------------------
    task_simulate_reference >> task_sync_reference >> create_ref_ext_tasks >> task_audit_reference
    task_verify_streaming_gcs >> task_create_ext_streaming >> task_audit_streaming
    [task_audit_reference, task_audit_streaming] >> task_dbt_run >> task_dbt_test >> task_trigger_evidence

