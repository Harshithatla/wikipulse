terraform {
  required_version = ">= 1.6"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 6.0, < 8.0"
    }
  }
  # For a team setup, move state to GCS (also free-tier):
  # backend "gcs" { bucket = "<your-tfstate-bucket>"  prefix = "wikipulse" }
}

provider "google" {
  project               = var.project_id
  region                = var.region
  billing_project       = var.project_id
  user_project_override = true # required for the billing budget API
}
