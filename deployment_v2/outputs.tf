# General
output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

# NWS Weather Data
output "gcs_bucket" {
  value = var.gcs_bucket
}

output "bigquery_dataset_NWS" {
  value = var.bigquery_dataset_NWS
}

output "bigquery_table_NWS" {
  value = var.bigquery_table_NWS
}

# yfinance Stock Data
output "bigquery_dataset_yf" {
  value = google_bigquery_dataset.yf_dataset.dataset_id
}

output "bigquery_table_yf" {
  value = google_bigquery_table.yf_table.table_id
}

output "pubsub_topic" {
  value = google_pubsub_topic.yf_topic.name
}

output "pubsub_subscription" {
  value = google_pubsub_subscription.yf_to_bq.name
}

output "cloud_run_service" {
  value = google_cloud_run_v2_service.yf_service.name
}

output "scheduler_job" {
  value = google_cloud_scheduler_job.yf_scheduler.name
}
