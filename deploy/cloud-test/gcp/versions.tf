# Google Cloud side of the weekly managed-DB matrix (finding M-03: what a release is
# tested against). The suites run against the real managed engines, not only against the
# least-privilege model of docker/initdb/01-lazyview.sql.
#
# Proof without a cloud account (this is what gates the PR):
#   tofu -chdir=deploy/cloud-test/gcp fmt -check
#   TF_PLUGIN_CACHE_DIR=<repo>/tmp/out/lvs-cloud/tofu-plugins tofu -chdir=deploy/cloud-test/gcp init -backend=false
#   TF_PLUGIN_CACHE_DIR=<repo>/tmp/out/lvs-cloud/tofu-plugins tofu -chdir=deploy/cloud-test/gcp validate
#   node --test deploy/cloud-test/gcp/test/
#
# Nothing here is ever deployed by a pull request or a push: the matrix is a weekly
# schedule plus a manual dispatch, and every run destroys what it created.
#
# The provider version is pinned by required_providers below and frozen by the committed
# .terraform.lock.hcl, so `validate` resolves from the cache with no account and no
# download. The OpenTofu CLI version is pinned here and in .terraform-version.

terraform {
  required_version = "= 1.13.9"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.12.0"
    }
  }
}

# The project and the credentials come from the environment: the weekly workflow
# authenticates with workload identity federation, so there is no key file and no
# project id in the repository.
provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  # Neutral naming only: no resource, database or user here is named after an
  # application. The test databases and users are the ones docker/initdb creates.
  name_prefix = "cloud-db-matrix"

  # Everything the cleanup job matches on. `expiry` is a UTC date; a run that dies
  # before its destroy step leaves resources that the separate weekly cleanup job
  # deletes once this date has passed, so a crash never keeps billing.
  # GCP label values allow lowercase letters, digits, "_" and "-", so the date is
  # written without colons.
  expiry_label = var.expiry_date != "" ? var.expiry_date : formatdate("yyyy-MM-dd", timeadd(timestamp(), "${var.expiry_hours}h"))

  common_labels = {
    "managed-by" = "cloud-db-matrix"
    "run-id"     = var.run_id
    "expiry"     = local.expiry_label
  }
}
