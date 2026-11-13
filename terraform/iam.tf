# Least-privilege identity for Airflow. Spark never talks to GCP (it writes the local lake).
resource "google_service_account" "pipeline" {
  account_id   = "wikipulse-pipeline"
  display_name = "WikiPulse pipeline (Airflow)"
  depends_on   = [google_project_service.apis]
}

locals {
  sa_member = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser" # run load + query jobs
  member  = local.sa_member
}

resource "google_project_iam_member" "bq_metadata_viewer" {
  project = var.project_id
  role    = "roles/bigquery.metadataViewer" # read table metadata (INFORMATION_SCHEMA)
  member  = local.sa_member
}

resource "google_bigquery_dataset_iam_member" "editor" {
  for_each   = { raw = google_bigquery_dataset.raw.dataset_id, analytics = google_bigquery_dataset.analytics.dataset_id }
  dataset_id = each.value
  role       = "roles/bigquery.dataEditor"
  member     = local.sa_member
}

resource "google_storage_bucket_iam_member" "lake_writer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = local.sa_member
}

# The SA key is deliberately NOT created here: keys in Terraform end up in state.
# Create it once with `make sa-key` (gcloud) and keep it out of git.
