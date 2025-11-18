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
resource "google_storage_bucket" {
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

resource "NWS_Bigquery_table"{
    


}