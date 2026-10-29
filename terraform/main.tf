# 1. GCS Bucket — FinGuard Data Lake (raw/reference/, raw/streaming_transactions/, checkpoints/)
resource "google_storage_bucket" "data_lake" {
  name                        = var.gcs_bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30 # Auto-delete objects older than 30 days (cost control)
    }
    action {
      type = "Delete"
    }
  }
}

# 2. BigQuery Datasets — Medallion Architecture (Raw -> Staging -> Production)
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = var.bq_dataset_raw
  friendly_name              = "FinGuard Raw Data"
  description                = "Raw layer — External tables pointing to GCS Parquet (Reference & Streaming)"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = var.bq_dataset_staging
  friendly_name              = "FinGuard Staging Data"
  description                = "Staging layer — dbt views (stg_*) & Quarantine routing"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "production" {
  dataset_id                 = var.bq_dataset_production
  friendly_name              = "FinGuard Production Data"
  description                = "Production marts — Partitioned & Clustered Fraud Tables"
  location                   = var.region
  delete_contents_on_destroy = true
}

# 3. Reference Existing Pipeline Service Account (credentials/pipeline-sa-key.json)
data "google_service_account" "pipeline_sa" {
  account_id = var.service_account_id
  project    = var.project_id
}

# IAM: GCS Storage Admin — read/write/manage bucket & external table files
resource "google_project_iam_member" "pipeline_sa_storage" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}

# IAM: BigQuery Admin — manage external tables, datasets, dbt models, and jobs
resource "google_project_iam_member" "pipeline_sa_bigquery" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}
