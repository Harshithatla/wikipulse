resource "google_storage_bucket" "lake" {
  name                        = local.bucket_name
  location                    = upper(var.region) # single region => Always Free eligible
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  labels                      = local.labels

  # Soft delete keeps deleted objects (and bills them) for 7 days by default.
  # Our objects are reproducible from the local lake, so turn it off.
  soft_delete_policy {
    retention_duration_seconds = 0
  }

  versioning {
    enabled = false
  }

  lifecycle_rule {
    condition {
      age = var.gcs_retention_days
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}
