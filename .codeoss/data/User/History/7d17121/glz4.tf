output "composer_airflow_uri" {
  value = var.enable_composer ? google_composer_environment.composer_env[0].config[0].airflow_uri : null
}

output "datahub_key_json" {
  value     = var.enable_gke ? google_service_account_key.datahub_key.private_key : null
  sensitive = true
}

output "postgres_instance_public_ip" {
  value = google_sql_database_instance.postgres.public_ip_address
}

output "postgres_instance_private_ip" {
  value = google_sql_database_instance.postgres.private_ip_address
}