output "bucket" {
  value = google_storage_bucket.lake.name
}

output "service_account_email" {
  value = google_service_account.pipeline.email
}

output "raw_table" {
  value = "${var.project_id}.${google_bigquery_dataset.raw.dataset_id}.${google_bigquery_table.recentchange.table_id}"
}

output "env_file_snippet" {
  description = "Paste into .env"
  value       = <<-EOT
    WIKIPULSE_GCP_PROJECT=${var.project_id}
    WIKIPULSE_BUCKET=${google_storage_bucket.lake.name}
    WIKIPULSE_BQ_LOCATION=${var.region}
  EOT
}
