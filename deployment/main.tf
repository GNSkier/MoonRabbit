resource "google_project_service" "required_apis"{
  for_each = toset([
    "storage.googleapis.com",                   # Cloud Storage
    "iam.googleapis.com",                       # Identity and Access Management
    "iamcredentials.googleapis.com",            # IAM Service Account Credentials
    "serviceusage.googleapis.com",              # Service Usage (for enabling APIs)
    "pubsub.googleapis.com",                    # Pub/Sub
    "run.googleapis.com",                       # Cloud Run Admin
    "bigquerystorage.googleapis.com",           # BigQuery Storage API
    "cloudscheduler.googleapis.com",            # Cloud Scheduler
    "dataplex.googleapis.com",                  # Cloud Dataplex
    "cloudbuild.googleapis.com",                # Cloud Build
  ])

    project = var.project_id
    service = each.value

    # Keep APIs enabled when destroying resources
    disable_on_destroy = false
}

resource "time_sleep" "wait_for_api_propagation" {
  depends_on = [
    google_project_service.required_apis
  ]

  create_duration = "120s"  # 2 minutes for API propagation
}

# Storage bucket - only create if explicitly requested
resource "google_storage_bucket" "data_lake"{
  count         = var.create_gcs_bucket && var.gcs_bucket != "" ? 1 : 0
  name          = var.gcs_bucket
  location      = var.region
  force_destroy = true
  
  # Explicit dependency to ensure APIs are ready FIRST
  depends_on = [time_sleep.wait_for_api_propagation]
  
  labels = {
    component = "NWS_Historical_Data"
    managed-by = "terraform"
  }
}

resource "google_bigquery_dataset" "NWS_observations"{
    count = var.bigquery_dataset_NWS && var.create_bigquery_dataset_NWS != "" ? 1  :0
    dataset_id = var.bigquery_dataset_NWS
    project = var.project_id
    location = var.region

    # Explicit dependency to ensure APIs are ready FIRST
    depends_on = [time_sleep.wait_for_api_propagation]

    labels = {
        environemnt = "development"
        managed-by = "terraform"
    }
}

resource "google_bigquery_table" "NWS_observations_table"{
    count = var.bigquery_table_NWS && var.create_bigquery_table_NWS != "" ? 1:0
    table_id = var.create_bigquery_table_NWS
    dataset_id = var.bigquery_dataset_NWS
    project = var.project_id

    schema = jsonencode ([
        {
        name = "timestamp"
        type = "TIMESTAMP"
        mode = "NULLABLE"
    },
    {
        name = "stationId"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "stationName"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "lat"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "lon"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "elevation"
        type = "INTEGER"
        mode = "NULLABLE"
    },
    {
        name = "temp_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "temp"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "pressure_pa"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "humidity"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "wind_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "wind_speed"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "precip_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "precip_3hr"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "heat_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "heat_index"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "max_temp_24_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "max_temp_24"
        type = "FLOAT"
        mode = "NULLABLE"
    },
    {
        name = "min_temp_24_unit"
        type = "STRING"
        mode = "NULLABLE"
    },
    {
        name = "min_temp_24"
        type = "FLOAT"
        mode = "NULLABLE"
    }
        ])

    }

resource "google_bigquery_dataset" "yfi"{
    count = var.bigquery_dataset_yf && var.create_bigquery_dataset_yf != "" ? 1  :0
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

resource "google_bigquery_table" "NWS_observations_table"{
    count = var.bigquery_table_NWS && var.create_bigquery_table_NWS != "" ? 1:0
    table_id = var.create_bigquery_table_NWS
    dataset_id = var.bigquery_dataset_NWS
    project = var.project_id

    schema = jsonencode ([
        {
            name = "ingest_timestamp"
            type = "TIMESTAMP"
            mode = "NULLABLE"
        },
        {
            name = "ticker"
            type = "STRING"
            mode = "NULLABLE"
        },
        {
            name = "info"
            type = "JSON"
            mode = "NULLABLE"
        }
        ])
    }