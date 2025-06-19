# --- PROVIDERS ---
provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
}

provider "google-beta" {
  project     = var.project_id
  region      = var.region
}

data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = "https://${google_container_cluster.primary[0].endpoint}"
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.primary[0].master_auth[0].cluster_ca_certificate)
}

provider "helm" {
  kubernetes {
    config_path = "~/.kube/config"
  }
}

# --- ENABLE REQUIRED APIS ---
resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "container.googleapis.com",
    "composer.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "pubsub.googleapis.com",
    "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com"
  ])
  service = each.value
  project = var.project_id
}

# --- NETWORK ---
resource "google_compute_network" "vpc_network" {
  name                    = "datahub-network-v2"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "vpc_subnet" {
  name          = "datahub-subnet-v2"
  ip_cidr_range = "10.10.0.0/16"
  region        = var.region
  network       = google_compute_network.vpc_network.id
}

# --- GKE CLUSTER ---
resource "google_container_cluster" "primary" {
  count     = var.enable_gke ? 1 : 0
  name      = var.cluster_name
  location  = var.zone

  network    = google_compute_network.vpc_network.name
  subnetwork = google_compute_subnetwork.vpc_subnet.name

  initial_node_count  = 3
  deletion_protection = false

  node_config {
    machine_type = "e2-standard-2"
    disk_size_gb = 15
    oauth_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
  }
}

# --- SECRETS ---
resource "kubernetes_secret" "mysql_secrets" {
  count = var.enable_gke ? 1 : 0
  metadata {
    name = "mysql-secrets"
  }
  data = {
    "mysql-root-password" = base64encode("datahub")
  }
  type = "Opaque"
}

resource "kubernetes_secret" "neo4j_secrets" {
  count = var.enable_gke ? 1 : 0
  metadata {
    name = "neo4j-secrets"
  }
  data = {
    "neo4j-password" = base64encode("datahub")
  }
  type = "Opaque"
}

# --- HELM DATAHUB ---
resource "helm_release" "datahub_prereq" {
  count      = var.enable_gke ? 1 : 0
  name       = "prerequisites"
  repository = "https://helm.datahubproject.io/"
  chart      = "datahub-prerequisites"
  namespace  = "default"
}

resource "helm_release" "datahub" {
  count      = var.enable_gke ? 1 : 0
  name       = "datahub"
  repository = "https://helm.datahubproject.io/"
  chart      = "datahub"
  namespace  = "default"
  depends_on = [helm_release.datahub_prereq]
}

# --- IAM + SERVICE ACCOUNTS ---
resource "google_service_account" "datahub" {
  account_id   = "datahub"
  display_name = "DataHub service account"
}

resource "google_project_iam_member" "datahub_roles" {
  for_each = var.enable_gke ? {
    metadata_viewer   = "roles/bigquery.metadataViewer"
    data_viewer       = "roles/bigquery.dataViewer"
    job_user          = "roles/bigquery.jobUser"
    logging_viewer    = "roles/logging.viewer"
    monitoring_viewer = "roles/monitoring.viewer"
  } : {}
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.datahub.email}"
}

resource "google_service_account_key" "datahub_key" {
  service_account_id = google_service_account.datahub.name
  keepers = {
    request_time = timestamp()
  }
}

resource "local_file" "datahub_credentials" {
  content  = google_service_account_key.datahub_key.private_key
  filename = "${path.module}/datahub-key.json"
}

resource "google_service_account" "composer" {
  account_id   = "composer"
  display_name = "Cloud Composer service account"
}

resource "google_project_iam_member" "composer_roles" {
  for_each = var.enable_composer ? {
    viewer               = "roles/viewer"
    storage_admin        = "roles/storage.objectAdmin"
    pubsub_subscriber    = "roles/pubsub.subscriber"
    bigquery_admin       = "roles/bigquery.admin"
    composer_worker      = "roles/composer.worker"
  } : {}
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# --- COMPOSER v3 ---
resource "google_composer_environment" "composer_env" {
  count    = var.enable_composer ? 1 : 0
  provider = google-beta
  name     = "datahub-composer"
  region   = var.region

  config {
    node_config {
      subnetwork      = google_compute_subnetwork.vpc_subnet.id
      network         = google_compute_network.vpc_network.id
      service_account = google_service_account.composer.email
    }

    software_config {
      image_version = "composer-3-airflow-2.10.5"
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"
  }

  depends_on = [google_project_iam_member.composer_roles]
}

resource "kubernetes_service" "datahub_frontend_lb" {
  metadata {
    name = "datahub-frontend-lb"
    labels = {
      app = "datahub"
    }
  }

  spec {
    selector = {
      app = "datahub-frontend"
    }

    type = "LoadBalancer"

    port {
      port        = 9002
      target_port = 9002
      protocol    = "TCP"
    }
  }
}

resource "google_sql_database_instance" "postgres" {
  name = "postgres"
  database_version = "POSTGRES_17"
  region = var.region

  settings {
    # Second-generation instance tiers are based on the machine
    # type. See argument reference below.
    tier = "db-f1-micro"

    ip_configuration {
      ipv4_enabled = true
      private_network = google_compute_network.vpc_network.id
      }
    backup_configuration {
      enabled = true
      start_time = "00:00"
    }

    location_preference {
      zone = var.zone
    }
  }
  
  deletion_protection = false
  depends_on = [google_project_service.required_apis]

  
}

resource "google_sql_user" "postgres_user" {
  instance = google_sql_database_instance.postgres_instance.name
  name     = "postgres"
  password = "747423"
}

resource "google_sql_database" "datahub_db" {
  name     = "datahubdb"
  instance = google_sql_database_instance.postgres_instance.name
}
