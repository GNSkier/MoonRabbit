# General Project
project_id     = YOUR_PROJECT_NAME
project_number = YOUR_PROJECT_ID
region         = "us-west1" # Region you wish to deploy in

# NWS Weather Data
gcs_bucket        = "testing_bucket_demo_3" #Name of your default GCP Bucket
create_gcs_bucket = true # Whether to create the bucket

bigquery_dataset_NWS        = "NWS_API" # Name of BigQuery Dataset
create_bigquery_dataset_NWS = true # Whether to create BigQuery Dataset
bigquery_table_NWS          = "bronze_NWS_API_observations" # Name of BigQuery Table
create_bigquery_table_NWS   = true # Whether to create BigQuery Table

# yfinance Stock Data
bigquery_dataset_yf = "yfinance_data" # Name of Bigquery Dataset
bigquery_table_yf   = "soyb_raw" # Name of BigQuery Table

pubsub_topic        = "yfinance_api_soy" # GCP Pub/Sub Topic
pubsub_subscription = "bronze_yf_api_ingestion" # Name of GCP Pub/Sub Subscription Conected to above Topic

cloud_run_name               = "yf-stocks" # Cloud Run service name
cloud_run_source_path        = "../stock_ingestion" # Path to files to deploy in cloudd run
cloud_run_container_registry = "gcr.io"

scheduler_name            = "yf-stocks-job" # Cloud Scheduler job name
scheduler_cron            = "*/30 * * * *" # Cron job for how long to run
scheduler_ticker          = "SOYB" # Name of Scheduler ticker
scheduler_service_account = "service-734228748198@gcp-sa-pubsub.iam.gserviceaccount.com" #Service Account created.
