# Khai báo provider
provider "google" {
    project = var.project_id
    region = var.region
}

# khai bao GKE cho Datahub
resource "google_container_cluster" "datahub_cluster" {
    name = "datawarehouse"
    location = var.zone

    initial_node_count = 3
    node_config {
      machine_type = "e2-standard-2"
      disk_size_gb = 15
      oauth_scopes = [ "https://www.googleapis.com/auth/cloud-platform" ]
    }
}

# IAM Service Accounts for Datahub and Composer
resource "google_service_account" "datahub_sa" {
    account_id = "datahub"
    display_name = "Datahub Service Account"
  
}

resource "google_service_account" "composer_sa" {
    account_id = "composer"
    display_name = "Composer Service Account"
  
}

# Grant role for service account 
resource "google_project_iam_member" "datahub_roles" {
    for_each = toset([
        "roles/bigquery.dataViewer",
        "roles/bigquery.jobUser",
        "roles/bigquery.metadataViewer",
        "roles/logging.viewer",
        "roles/monitoring.viewer"
        
    ])
    project = var.project_id
    role = each.key
    member = "serviceAccount:${google_service_account.datahub_sa.email}"
}

resource "google_project_iam_member" "composer_roles" {
    for_each = toset([
        "roles/composer.admin",
        "roles/composer.worker",
        "roles/bigquery.admin",
        "roles/bigquery.dataViewer",
        "roles/pubsub.subscriber",
        "roles/storage.objectAdmin",
        "roles/viewer"
    ])
    project = var.project_id
    role = each.key 
    member = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Cloud Composer v2 Environment
resource "google_composer_environment" "composer_env" {
  name   = "datawarehouse-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-3-airflow-2.10.5"

      env_variables = {
        AIRFLOW__CORE__DAGS_FOLDER = "/home/airflow/gcs/dags"
      }
    }

    node_config {
      service_account = google_service_account.composer_sa.email
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"  # Hoặc MEDIUM, LARGE tùy nhu cầu
  }
}

