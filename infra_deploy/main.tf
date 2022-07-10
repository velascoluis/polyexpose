variable "project" {}
variable "region" {}
variable "zone" {}
variable "credentials" {}
variable "dbname_prefix" {}
variable "gkename" {}
variable "dataprocname" {}
variable "bucketname" {}


provider "google-beta" {
  project     = var.project
  region      = var.region
  zone        = var.zone
  credentials = var.credentials
}

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "google_sql_database_instance" "cloud-sql" {
  provider            = google-beta
  name                = format("%s-%s", var.dbname_prefix, random_id.db_name_suffix.hex)
  database_version    = "POSTGRES_9_6"
  deletion_protection = false
  settings {
    tier = "db-f1-micro"
  }
}

output "dbname" {
  value = google_sql_database_instance.cloud-sql.name
}


resource "google_container_cluster" "gke-cluster" {
  provider           = google-beta
  name               = var.gkename
  initial_node_count = 1
  timeouts {
    create = "30m"
    update = "40m"
  }
}

resource "google_dataproc_cluster" "spark-cluster" {
  provider = google-beta
  name     = var.dataprocname
  region   = var.region
  cluster_config {
      gce_cluster_config {
      service_account_scopes = [
        "cloud-platform"
      ]
    }
  
  
    software_config {
      override_properties = {
        "dataproc:dataproc.allow.zero.workers" = "true"
      }
    }
  }
}

resource "google_storage_bucket" "gcs-bucket" {
  provider      = google-beta
  name          = var.bucketname
  location      = "US"
  force_destroy = true
}

