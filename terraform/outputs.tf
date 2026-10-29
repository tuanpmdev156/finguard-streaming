output "gcs_bucket_url" {
  description = "GCS Data Lake bucket URL"
  value       = "gs://${google_storage_bucket.data_lake.name}"
}

output "service_account_email" {
  description = "Pipeline Service Account email"
  value       = data.google_service_account.pipeline_sa.email
}

output "sa_key_path" {
  description = "Path to Service Account JSON key file"
  value       = "${path.module}/../credentials/pipeline-sa-key.json"
}

output "bq_dataset_raw" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw.dataset_id
}

output "bq_dataset_staging" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging.dataset_id
}

output "bq_dataset_production" {
  description = "BigQuery production dataset ID"
  value       = google_bigquery_dataset.production.dataset_id
}
