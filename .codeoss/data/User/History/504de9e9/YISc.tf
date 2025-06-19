# --- PROVIDERS ---
provider "google" {
  project     = var.project_id
  region      = var.region
  zone        = var.zone
  credentials = file("datahub-key.json")
}

provider "google-beta" {
  project     = var.project_id
  region      = var.region
  credentials = file("datahub-key.json")
}

data "google_client_config" "default" {}

provider "kubernetes" {
  host                   = google_container_cluster.primary.endpoint
  token                  = data.google_client_config.default.access_token
  cluster_ca_certificate = base64decode(google_container_cluster.primary.master_auth[0].cluster_ca_certificate)
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
    "pubsub.googleapis.com"
  ])
  service = each.key
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
