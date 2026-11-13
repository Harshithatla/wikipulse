variable "project_id" {
  description = "GCP project id (a fresh project is recommended)."
  type        = string
}

variable "region" {
  description = "Must be us-central1, us-east1 or us-west1 for Cloud Storage Always Free. BigQuery uses the same location so loads never cross regions."
  type        = string
  default     = "us-central1"
  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "Only us-central1, us-east1 and us-west1 are covered by the Cloud Storage free tier."
  }
}

variable "bucket_name" {
  description = "Lake bucket name. Defaults to <project_id>-wikipulse-lake."
  type        = string
  default     = ""
}

variable "gcs_retention_days" {
  description = "Delete lake objects after N days (keeps GCS under the 5 GB free tier)."
  type        = number
  default     = 10
}

variable "raw_partition_expiration_days" {
  description = "Hourly raw partitions expire after N days (keeps BigQuery under the 10 GiB free tier)."
  type        = number
  default     = 14
}

variable "analytics_partition_expiration_days" {
  description = "page_hourly grows ~20 MB/day on the default allowlist; 90 days keeps total BigQuery storage ~5 GB."
  type        = number
  default     = 90
}

variable "billing_account_id" {
  description = "Optional billing account id (XXXXXX-XXXXXX-XXXXXX). When set, a $1 budget with alerts is created."
  type        = string
  default     = ""
}

variable "budget_amount_usd" {
  description = "Budget alert threshold. Anything above $0 means something is misconfigured."
  type        = number
  default     = 1
}

variable "deletion_protection" {
  description = "Protect BigQuery tables from terraform destroy. Off by default so the capstone can be torn down cleanly."
  type        = bool
  default     = false
}
