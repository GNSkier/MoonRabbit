resource "google_bigquery_dataset" "yf_dataset"{
    dataset_id = var.bigquery_dataset_yf
    project = var.project_id
    location = var.region

    # Explicit dependency to ensure APIs are ready FIRST
    depends_on = [time_sleep.wait_for_api_propagation]

    labels = {
        environemnt = "development"
        managed-by = "terraform"
    }
}

resource "google_bigquery_table" "yf_table"{
    table_id = var.bigquery_table_yf
    dataset_id = var.bigquery_dataset_yf
    project = var.project_id

    depends_on = [google_bigquery_dataset.yf_dataset]

    schema = jsonencode ([
        { name = "publish_time", type = "TIMESTAMP", mode = "NULLABLE" },
        { name = "message_id", type = "STRING", mode = "NULLABLE" },
        { name = "attributes", type = "STRING", mode = "NULLABLE" },
        { name = "subscription_name", type = "STRING", mode = "NULLABLE" },
        { name = "ingest_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
        { name = "ticker", type = "STRING", mode = "NULLABLE" },
        { name = "currency", type = "STRING", mode = "NULLABLE" },
        { name = "dayHigh", type = "FLOAT", mode = "NULLABLE" },
        { name = "dayLow", type = "FLOAT", mode = "NULLABLE" },
        { name = "exchange", type = "STRING", mode = "NULLABLE" },
        { name = "fiftyDayAverage", type = "FLOAT", mode = "NULLABLE" },
        { name = "lastPrice", type = "FLOAT", mode = "NULLABLE" },
        { name = "lastVolume", type = "FLOAT", mode = "NULLABLE" },
        { name = "marketCap", type = "FLOAT", mode = "NULLABLE" },
        { name = "open", type = "FLOAT", mode = "NULLABLE" },
        { name = "previousClose", type = "FLOAT", mode = "NULLABLE" },
        { name = "quoteType", type = "STRING", mode = "NULLABLE" },
        { name = "regularMarketPreviousClose", type = "FLOAT", mode = "NULLABLE" },
        { name = "shares", type = "FLOAT", mode = "NULLABLE" },
        { name = "tenDayAverageVolume", type = "FLOAT", mode = "NULLABLE" },
        { name = "threeMonthAverageVolume", type = "FLOAT", mode = "NULLABLE" },
        { name = "timezone", type = "STRING", mode = "NULLABLE" },
        { name = "twoHundredDayAverage", type = "FLOAT", mode = "NULLABLE" },
        { name = "yearChange", type = "FLOAT", mode = "NULLABLE" },
        { name = "yearHigh", type = "FLOAT", mode = "NULLABLE" },
        { name = "yearLow", type = "FLOAT", mode = "NULLABLE" }
    ])
}

resource "google_service_account" "scheduler" {
  account_id   = "yf-scheduler-sa"
  display_name = "Service Account for yfinance Scheduler"
  project      = var.project_id
}

resource "google_project_iam_member" "scheduler_sa_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_pubsub_topic" "yf_topic" {
  name    = var.pubsub_topic
  project = var.project_id

  depends_on = [time_sleep.wait_for_api_propagation]
}

resource "google_bigquery_dataset_iam_member" "pubsub_bq_writer" {
  dataset_id = google_bigquery_dataset.yf_dataset.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:service-${var.project_number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "yf_to_bq" {
  name    = var.pubsub_subscription
  project = var.project_id
  topic   = google_pubsub_topic.yf_topic.name

  depends_on = [
    google_pubsub_topic.yf_topic,
    google_bigquery_table.yf_table,
    google_bigquery_dataset_iam_member.pubsub_bq_writer
  ]

  bigquery_config {
    table               = "${var.project_id}.${var.bigquery_dataset_yf}.${var.bigquery_table_yf}"
    write_metadata      = true
    use_table_schema    = true
  }
}

resource "null_resource" "yf_docker_build" {
  triggers = {
    source_hash = sha256(join("", [
      for f in fileset(var.cloud_run_source_path, "**") : filesha256("${var.cloud_run_source_path}/${f}")
    ]))
  }

  provisioner "local-exec" {
    command = <<-EOT
      cd ${var.cloud_run_source_path} && \
      gcloud builds submit --tag ${var.cloud_run_container_registry}/${var.project_id}/${var.cloud_run_name}:latest --project ${var.project_id}
    EOT
  }

  depends_on = [time_sleep.wait_for_api_propagation]
}

resource "google_cloud_run_v2_service" "yf_service" {
  name     = var.cloud_run_name
  location = var.region
  project  = var.project_id

  depends_on = [
    null_resource.yf_docker_build,
    time_sleep.wait_for_api_propagation
  ]

  template {
    containers {
      image = "${var.cloud_run_container_registry}/${var.project_id}/${var.cloud_run_name}:latest"
      
      ports {
        container_port = 8080
      }
      
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "TOPIC_ID"
        value = var.pubsub_topic
      }
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.yf_service.name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"

  depends_on = [google_cloud_run_v2_service.yf_service]
}

resource "google_cloud_scheduler_job" "yf_scheduler" {
  name        = var.scheduler_name
  description = "Trigger YF ingestion"
  schedule    = var.scheduler_cron
  time_zone   = "UTC"
  region      = var.region
  project     = var.project_id

  depends_on = [
    google_cloud_run_v2_service_iam_member.scheduler_invoker,
    time_sleep.wait_for_api_propagation
  ]

  http_target {
    uri         = "${google_cloud_run_v2_service.yf_service.uri}/"
    http_method = "POST"
    headers = {
      "Content-Type" = "application/json"
    }
    body = base64encode(jsonencode({ ticker = var.scheduler_ticker }))
    
    oidc_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

