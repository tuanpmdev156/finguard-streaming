# 0. Enable required GCP APIs
resource "google_project_service" "apis" {
  for_each                   = toset(var.gcp_api_services)
  project                    = var.project_id
  service                    = each.value
  disable_on_destroy         = false # destroy does not disable API (avoid breaking other resources/ next apply)
  disable_dependent_services = false
}


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

# IAM: Cloud Logging Writer — allow VM containers/agents to write logs
resource "google_project_iam_member" "pipeline_sa_logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}

# IAM: Cloud Monitoring Metric Writer — allow VM to publish metrics
resource "google_project_iam_member" "pipeline_sa_monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}

# IAM: Secret Manager Secret Accessor — allow VM to fetch DB password & secrets
resource "google_project_iam_member" "pipeline_sa_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}

# IAM: Cloud SQL Client — allow Cloud SQL Auth Proxy on VM to connect to Cloud SQL via IAM
resource "google_project_iam_member" "pipeline_sa_cloudsql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${data.google_service_account.pipeline_sa.email}"
}

# 4. Firewall: Allow SSH only via Google Cloud IAP (35.235.240.0/20)
resource "google_compute_firewall" "allow_iap_ssh" {
  name        = "finguard-allow-iap-ssh"
  network     = "default"
  description = "Allow SSH (port 22) only from Google Cloud Identity-Aware Proxy (IAP)"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["finguard-vm"]

  depends_on = [google_project_service.apis]
}

# 5. GCE VM Instance (runs Redpanda, Producer, Spark Streaming, Airflow, Cloud SQL Auth Proxy)
resource "google_compute_instance" "finguard_vm" {
  name                      = var.vm_name
  machine_type              = var.vm_machine_type
  zone                      = var.zone
  tags                      = ["finguard-vm"]
  allow_stopping_for_update = true

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = var.vm_disk_size_gb
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = "default"
    # Ephemeral external IP so the VM can pull Docker images, clone GitHub, and reach GCP APIs
    access_config {}
  }

  service_account {
    email  = data.google_service_account.pipeline_sa.email
    scopes = ["cloud-platform"]
  }

  scheduling {
    provisioning_model          = var.vm_use_spot ? "SPOT" : "STANDARD"
    preemptible                 = var.vm_use_spot
    automatic_restart           = var.vm_use_spot ? false : true
    on_host_maintenance         = var.vm_use_spot ? "TERMINATE" : "MIGRATE"
    instance_termination_action = var.vm_use_spot ? "STOP" : null
  }

  metadata_startup_script = templatefile("${path.module}/scripts/vm_startup.sh", {
    repo_url    = var.git_repo_url
    repo_branch = var.git_repo_branch
    app_dir     = var.vm_app_dir
    app_user    = var.vm_app_user
  })

  depends_on = [
    google_project_service.apis,
    google_project_iam_member.pipeline_sa_logging,
    google_project_iam_member.pipeline_sa_monitoring,
    google_project_iam_member.pipeline_sa_secret_accessor,
    google_project_iam_member.pipeline_sa_cloudsql_client,
  ]
}

# 6. Cloud SQL for PostgreSQL 16 (OLTP Reference Database — replaces local finguard-postgres container)
resource "google_sql_database_instance" "finguard_pg" {
  name                = var.cloudsql_instance_name
  database_version    = "POSTGRES_16"
  region              = var.region
  deletion_protection = false # Allow `terraform destroy` after demo

  settings {
    tier              = var.cloudsql_tier
    availability_type = "ZONAL"
    disk_type         = var.cloudsql_disk_type
    disk_size         = var.cloudsql_disk_size_gb
    disk_autoresize   = true

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    ip_configuration {
      # Public IP enabled ONLY for Cloud SQL Auth Proxy (IAM + TLS);
      # no authorized_networks configured so direct connections are blocked.
      ipv4_enabled = true
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_sql_database" "finguard_oltp" {
  name     = var.cloudsql_db_name
  instance = google_sql_database_instance.finguard_pg.name
}

resource "random_password" "pg_password" {
  length  = 24
  special = false # Alphanumeric only to avoid URL/shell escaping issues in .env and JDBC/SQLAlchemy
}

resource "google_sql_user" "finguard" {
  name     = var.cloudsql_user
  instance = google_sql_database_instance.finguard_pg.name
  password = random_password.pg_password.result
}

# 7. Secret Manager — Store Cloud SQL password securely
resource "google_secret_manager_secret" "pg_password" {
  secret_id = var.pg_password_secret_id
  project   = var.project_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "pg_password" {
  secret      = google_secret_manager_secret.pg_password.id
  secret_data = random_password.pg_password.result
}


