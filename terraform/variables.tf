variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region for resources"
  type        = string
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "GCS bucket name for FinGuard Data Lake & Spark Checkpoints"
  type        = string
}

variable "bq_dataset_raw" {
  description = "BigQuery dataset for raw data"
  type        = string
  default     = "finguard_raw"
}

variable "bq_dataset_staging" {
  description = "BigQuery dataset for staging (dbt views & quarantine)"
  type        = string
  default     = "finguard_staging"
}

variable "bq_dataset_production" {
  description = "BigQuery dataset for production marts"
  type        = string
  default     = "finguard_production"
}

variable "service_account_id" {
  description = "Existing Service Account ID for pipeline automation"
  type        = string
  default     = "pipeline-sa"
}

variable "gcp_api_services" {
  description = "GCP APIs required for VM + Cloud SQL deployment"
  type        = list(string)
  default = [
    "compute.googleapis.com",       # GCE VM, firewall
    "iap.googleapis.com",           # IAP tunnel SSH
    "secretmanager.googleapis.com", # Save Postgres password
    "sqladmin.googleapis.com",      # Cloud SQL + Auth Proxy
  ]
}

# --- Phase 1: Compute (GCE VM) Variables ---

variable "zone" {
  description = "GCP zone for GCE VM"
  type        = string
  default     = "us-central1-a"
}

variable "vm_name" {
  description = "Name of the GCE VM instance"
  type        = string
  default     = "finguard-vm"
}

variable "vm_machine_type" {
  description = "Machine type for GCE VM (minimum 16GB RAM for Spark + Airflow + Redpanda)"
  type        = string
  default     = "e2-standard-4"
}

variable "vm_disk_size_gb" {
  description = "Boot disk size in GB"
  type        = number
  default     = 50
}

variable "vm_use_spot" {
  description = "Use Spot VM to reduce compute cost by ~60-70%"
  type        = bool
  default     = false
}

variable "git_repo_url" {
  description = "Git repository URL cloned on VM startup"
  type        = string
  default     = "https://github.com/tuanpmdev156/finguard-streaming.git"
}

variable "git_repo_branch" {
  description = "Git branch to checkout on VM startup"
  type        = string
  default     = "main"
}

variable "vm_app_dir" {
  description = "Directory on VM where repository is cloned"
  type        = string
  default     = "/opt/finguard-streaming"
}

variable "vm_app_user" {
  description = "Linux user created on VM to own the repo and run Docker"
  type        = string
  default     = "finguard"
}

# --- Phase 2: Cloud SQL (PostgreSQL 16 OLTP) Variables ---

variable "cloudsql_instance_name" {
  description = "Name of the Cloud SQL PostgreSQL instance"
  type        = string
  default     = "finguard-pg"
}

variable "cloudsql_tier" {
  description = "Machine tier for Cloud SQL (db-f1-micro is cheapest shared-core for demo)"
  type        = string
  default     = "db-f1-micro"
}

variable "cloudsql_disk_type" {
  description = "Disk type for Cloud SQL (PD_HDD or PD_SSD)"
  type        = string
  default     = "PD_HDD"
}

variable "cloudsql_disk_size_gb" {
  description = "Disk size in GB for Cloud SQL"
  type        = number
  default     = 10
}

variable "cloudsql_db_name" {
  description = "PostgreSQL OLTP database name"
  type        = string
  default     = "finguard_oltp"
}

variable "cloudsql_user" {
  description = "PostgreSQL application user"
  type        = string
  default     = "finguard"
}

variable "pg_password_secret_id" {
  description = "Secret Manager secret ID storing the Cloud SQL password"
  type        = string
  default     = "finguard-pg-password"
}

