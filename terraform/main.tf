terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = var.credentials_file
  project     = var.project
  region      = var.region
}


resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.bucket_location
  force_destroy = true
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  project    = var.project
  location   = var.bucket_location
}