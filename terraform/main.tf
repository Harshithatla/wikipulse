locals {
  bucket_name = var.bucket_name != "" ? var.bucket_name : "${var.project_id}-wikipulse-lake"
  labels = {
    app        = "wikipulse"
    managed_by = "terraform"
  }
  apis = [
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
  ]
}

data "google_project" "this" {
  project_id = var.project_id
}

resource "google_project_service" "apis" {
  for_each           = toset(concat(local.apis, var.billing_account_id != "" ? ["billingbudgets.googleapis.com"] : []))
  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}
