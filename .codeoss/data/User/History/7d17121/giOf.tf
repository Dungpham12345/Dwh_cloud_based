output "composer_airflow_uri" {
  value = var.enable_composer ? google_composer_environment.composer_env[0].config[0].airflow_uri : null
}

output "datahub_key_json" {
  value     = var.enable_gke ? google_service_account_key.datahub_key.private_key : null
  sensitive = true
}
