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

output "vm_name" {
  description = "GCE VM instance name"
  value       = google_compute_instance.finguard_vm.name
}

output "vm_zone" {
  description = "GCE VM zone"
  value       = google_compute_instance.finguard_vm.zone
}

output "vm_ssh_command" {
  description = "Command to SSH into the VM via IAP tunnel"
  value       = "gcloud compute ssh ${google_compute_instance.finguard_vm.name} --project=${var.project_id} --zone=${google_compute_instance.finguard_vm.zone} --tunnel-through-iap"
}

output "vm_ui_tunnel_command" {
  description = "Command to port-forward Airflow (8080) and Redpanda Console (8081) via IAP tunnel"
  value       = "gcloud compute ssh ${google_compute_instance.finguard_vm.name} --project=${var.project_id} --zone=${google_compute_instance.finguard_vm.zone} --tunnel-through-iap -- -L 8080:localhost:8080 -L 8081:localhost:8081"
}

