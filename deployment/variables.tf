# General Project creation
variable "project_id" {
    type = string
    description = "GCP project ID"
}

variable "region" {
    type = string
    description = "GCP region for deployment"
    default = "us-west1"
}
# Big Query NWS
variable "bigquery_dataset_NWS" {
    type = string 
    description = "Bigquery Dataset name for NWS Data warehouse"
    default = "NWS_API"
}
variable "create_bigquery_dataset_NWS" {
    type = string
    description = "Boolean on whether we create a bigquery warehouse"
    default = true
}
variable "bigquery_table_NWS"{
    type = string
    description = "Bigquery Table name for NWS data"
    default = "bronze_NWS_API_observations"
}
variable "create_bigquery_table_NWS"{
    type = string
    description = "Whether to create a Bigquery table for NWS data"
    default = true 
}
# Bigquery Finance
variable "bigquery_dataset_yf" {
    type = string 
    description = "Bigquery Dataset name for yf Data warehouse"
    default = "yfinance_data"
}
variable "create_bigquery_dataset_yf" {
    type = string
    description = "Boolean on whether we create a bigquery warehouse"
    default = true
}
variable "bigquery_table_yf"{
    type = string
    description = "Bigquery Table name for yf data"
    default = "soyb"
}
variable "create_bigquery_table_yf"{
    type = string
    description = "Whether to create a Bigquery table for yf data"
    default = true 
}

# GCS buckets
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