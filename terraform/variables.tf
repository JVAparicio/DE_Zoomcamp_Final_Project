locals {
  bucket_name = "prefect-de-course-zoomcamp"
}

variable "project" {
  description = "dtc-de-course-375312"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "europe-west6"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ds_zoomcamp"
}