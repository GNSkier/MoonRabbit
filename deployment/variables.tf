variable "projecrt_id" {
    type = string
    description = "GCP project ID"
}

variable "region" {
    type = string
    description = "GCP region for deployment"
}

variable "bigquery_dataset" {
    type = string 
    description = "Bigquery Dataset name for data warehouse"
}
variable "create_bigquery_dataset" {
    type = string
    description = "Boolean on whether we create a bigquery warehouse"
    default = true
}
variable "gcs_bucket"{
    type = string
    description = "Creates a GCP bucket to act as a lake"
}
variable "create_gcs_bucket"{
    type = string
    description = "Boolean on whether to create a bucket"
    default = true
}