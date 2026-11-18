# Private network only. The instances get a private IP and no public IP at all, so the
# suite is reachable only from inside this VPC (the in-VPC test task), never from a
# runner IP allowlist and never over the internet.

resource "google_compute_network" "matrix" {
  name                    = "${local.name_prefix}-vpc"
  auto_create_subnetworks = false
  mtu                     = 1460
}

# The range Cloud SQL's service allocator peers into. It is a reserved internal range,
# not a subnet, so it costs nothing.
resource "google_compute_global_address" "services_peering" {
  name          = "${local.name_prefix}-services-range"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  address       = cidrhost(var.peering_ip_range, 0)
  network       = google_compute_network.matrix.self_link

  labels = local.common_labels
}

resource "google_service_networking_connection" "private_service_access" {
  network                 = google_compute_network.matrix.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.services_peering.name]
}

# Where the test task that drives the suites runs. Private IP only: no external IP is
# ever assigned, so the databases need no inbound firewall rule from outside the VPC.
# The compute network and subnetwork have no labels field, so the cleanup job finds them
# by the name prefix instead (asserted in test/parse.test.mjs).
resource "google_compute_subnetwork" "runner" {
  name          = "${local.name_prefix}-runner"
  region        = var.region
  network       = google_compute_network.matrix.id
  ip_cidr_range = var.runner_ip_range

  private_ip_google_access = true
}
