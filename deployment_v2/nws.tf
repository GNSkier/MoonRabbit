resource "google_storage_bucket" "data_lake"{
  count         = var.create_gcs_bucket && var.gcs_bucket != "" ? 1 : 0
  name          = var.gcs_bucket
  location      = var.region
  project       = var.project_id
  force_destroy = true
  
  # Explicit dependency to ensure APIs are ready FIRST
  depends_on = [time_sleep.wait_for_api_propagation]
  
  labels = {
    component = "nws_historical_data"
    managed-by = "terraform"
  }
}

resource "google_bigquery_dataset" "NWS_observations"{
    count = var.create_bigquery_dataset_NWS && var.bigquery_dataset_NWS != "" ? 1 : 0
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
    count = var.create_bigquery_table_NWS && var.bigquery_table_NWS != "" ? 1 : 0
    table_id = var.bigquery_table_NWS
    dataset_id = var.bigquery_dataset_NWS
    project = var.project_id

    depends_on = [google_bigquery_dataset.NWS_observations]

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

