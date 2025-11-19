# General Project
variable "project_id" {
    type = string
    description = "GCP project ID"
}

variable "project_number" {
    type = string
    description = "GCP project number (for service account emails)"
}

variable "region" {
    type = string
    description = "GCP region for deployment"
    default = "us-west1"
}

# NWS Weather Data
variable "gcs_bucket"{
    type = string
    description = "Name for a GCP bucket to act as a lake"
    default = "historical_nws_observations"
}
variable "create_gcs_bucket"{
    type = string
    description = "Boolean on whether to create a bucket"
    default = true
}

variable "bigquery_dataset_NWS" {
    type = string 
    description = "Bigquery Dataset name for NWS Data warehouse"
    default = "NWS_API"
}
variable "create_bigquery_dataset_NWS" {
    type = bool
    description = "Boolean on whether we create a bigquery warehouse"
    default = true
}
variable "bigquery_table_NWS"{
    type = string
    description = "Bigquery Table name for NWS data"
    default = "bronze_NWS_API_observations"
}
variable "create_bigquery_table_NWS"{
    type = bool
    description = "Whether to create a Bigquery table for NWS data"
    default = true 
}

# yfinance Stock Data
variable "bigquery_dataset_yf" {
    type = string 
    description = "Bigquery Dataset name for yf Data warehouse"
    default = "yfinance_data"
}
variable "bigquery_table_yf"{
    type = string
    description = "Bigquery Table name for yf data (soybeans)"
    default = "soyb_raw"
}

variable "pubsub_topic" {
  type        = string
  description = "Name of Pub/Sub topic for triggering yfinance fetches"
  default     = "yfinance_api_soy"
}

variable "pubsub_subscription" {
  type        = string
  description = "Name of Pub/Sub subscription for yfinance to BigQuery"
  default     = "bronze_yf_api_ingestion"
}

variable "cloud_run_name" {
  type        = string
  description = "Name of Cloud Run service for yfinance ingestion"
  default     = "yf-stocks"
}

variable "cloud_run_source_path" {
  type        = string
  description = "Path to source code directory (relative to terraform root)"
  default     = "../stock_ingestion"
}

variable "cloud_run_container_registry" {
  type        = string
  description = "Container registry"
  default     = "gcr.io"
}


variable "scheduler_name" {
  type        = string
  description = "Name of Cloud Scheduler job"
  default     = "yf-stocks-job"
}

variable "scheduler_cron" {
  type        = string
  description = "Cron schedule for Cloud Scheduler"
  default     = "*/30 * * * *"
}

variable "scheduler_ticker" {
  type        = string
  description = "Ticker symbol to fetch in scheduler message"
  default     = "SOYB"
}

variable "scheduler_service_account" {
  type        = string
  description = "Service account email for Cloud Scheduler to authenticate with Cloud Run"
}

