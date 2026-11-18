# Every variable has a default, so `tofu validate` runs offline with no account and no
# tfvars file. The weekly workflow overrides what it must (project, run id, expiry).

variable "project_id" {
  type        = string
  default     = null
  description = "GCP project the run owns. Set with TF_VAR_project_id; never a key file (workload identity). null so validate passes with no account."
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "Region for the network and the instances. One region only: the matrix never spans regions."
}

variable "zone" {
  type        = string
  default     = "us-central1-b"
  description = "Zone the non-HA instances are placed in. The HA failover lane (PR B2) pins its own pair."
}

variable "run_id" {
  type        = string
  default     = "local-validate"
  description = "Identifies one matrix run; it goes in every resource name and in the run-id label. The workflow passes the GitHub run id."
}

variable "expiry_hours" {
  type        = number
  default     = 6
  description = "Hours of headroom over the ~90 min a target takes, written into the expiry label the cleanup job reads. Cost note: this is how long a resource can survive a crashed run."
}

variable "expiry_date" {
  type        = string
  default     = ""
  description = "Explicit yyyy-MM-dd expiry label, overriding expiry_hours. The workflow passes run_end + buffer in UTC."
}

variable "peering_ip_range" {
  type        = string
  default     = "10.70.0.0/16"
  description = "Internal range reserved for the Cloud SQL service peering. It is not a subnet and carries no cost."
}

variable "runner_ip_range" {
  type        = string
  default     = "10.71.0.0/20"
  description = "Private subnet the in-VPC test task runs in (the access model the plan recommends and lv-super accepted). A subnetwork itself is free."
}

variable "mysql_tier" {
  type        = string
  default     = "db-custom1-3840"
  description = "Cloud SQL machine type. Cost note: 1 vCPU / 3.8 GiB is the smallest class that survives the suite (2 GiB is too tight for row-binlog MySQL); on-demand that is about $0.06 per 90 min run. The class is fixed so a run cannot scale itself up."
}

variable "mysql_disk_gb" {
  type        = number
  default     = 20
  description = "Data disk size. Cost note: PD-SSD 20 GiB plus PITR binlogs is the whole storage bill of a run; autoresize is off so nothing grows on its own."
}

variable "availability_type" {
  type        = string
  default     = "ZONAL"
  description = "ZONAL for the suite lane (half the price of REGIONAL). The failover lane sets REGIONAL so it has a standby to promote; that lane is PR B2."
}

variable "test_database" {
  type        = string
  default     = "lvtest"
  description = "The database docker/initdb/01-lazyview.sql creates. The suites make their own lvt_* schemas inside it, so no other schema is created here."
}
