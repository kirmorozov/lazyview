# The failover lane. The promoted-node scenario itself is not written twice here: it is
# the GTID plan's PR F (tests/failover_source.rs), and the cloud run only promotes a
# standby and re-asserts "resume without rebuild" on the same connection name. What this
# file has to guarantee is the thing that scenario cannot run without — an instance with
# a standby in a second zone that Cloud SQL can promote, for both engine families.
#
# HA here means `availability_type = "REGIONAL"` written literally, not read from
# var.availability_type: an instance the suite promotes is the one place where a default
# that quietly became ZONAL would turn the failover scenario into a reboot test that
# still passes. The structural test asserts the literal.
#
# No location_preference is set. Cloud SQL then chooses the primary and the standby zone
# itself inside var.region; pinning two zones would make the run fail in any region the
# owner picks whose zone names differ, and the scenario does not care which two zones it
# gets. `gcloud sql instances failover` keeps the same private IP, which is why the suite
# resumes on the DSN it already has rather than rediscovering an endpoint.
#
# Cost note: REGIONAL is two copies of the instance, so each entry costs about double the
# ZONAL one (~$0.12 per 90-minute run each, ~$0.24 for the lane) plus a second copy of the
# disk and the WAL retained by PITR. Two entries only: one per family, enough to prove the
# promotion path for both engines. The weekly run that is asked for a subset of targets
# without failover sets enable_failover_lane = false and does not pay for them.

variable "enable_failover_lane" {
  type        = bool
  default     = true
  description = "Create the REGIONAL instances the failover scenario promotes. It has a default so `tofu validate` still runs with no account. Cost note: each entry is about twice the ZONAL class; a run that does not exercise failover turns this off."
}

locals {
  # One entry per engine family, so the weekly run has a standby to promote for the
  # MySQL family and for the PostgreSQL family. The names are neutral and say what the
  # instance is for; no application appears in any of them.
  ha_targets = {
    "mysql-84-ha" = { database_version = "MYSQL_8_4", family = "mysql", flags = local.mysql_flags }
    "pg-17-ha"    = { database_version = "POSTGRES_17", family = "pg", flags = local.pg_flags }
  }

  # Same class as the suite lane: the failover run must measure the same instance it
  # would otherwise test, only with a standby attached.
  ha_tier    = "db-custom1-3840"
  ha_disk_gb = 20
  ha_enabled = var.enable_failover_lane ? local.ha_targets : {}
}

resource "google_sql_database_instance" "ha" {
  for_each = local.ha_enabled

  name             = "${local.name_prefix}-${each.key}-${var.run_id}"
  database_version = each.value.database_version
  region           = var.region

  deletion_protection = false

  settings {
    tier              = local.ha_tier
    availability_type = "REGIONAL"
    edition           = "ENTERPRISE"
    disk_type         = "PD_SSD"
    disk_size         = local.ha_disk_gb
    disk_autoresize   = false
    pricing_plan      = "PER_USE"
    activation_policy = "ALWAYS"

    user_labels = local.common_labels

    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.matrix.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    # Binlog/WAL retention is what makes the promotion observable: the promoted standby
    # has to be able to answer from the same log position the suite had reached.
    # binary_log_enabled exists for MySQL only, so it is left unset on PostgreSQL rather
    # than sent as false.
    backup_configuration {
      enabled                        = true
      binary_log_enabled             = each.value.family == "mysql" ? true : null
      point_in_time_recovery_enabled = true
    }

    # The flags are the same list the suite lane uses, taken from the entry, so a setting
    # can never be present in one lane and missing from the failover lane.
    dynamic "database_flags" {
      for_each = each.value.flags
      content {
        name  = database_flags.value.name
        value = database_flags.value.value
      }
    }
  }

  depends_on = [google_service_networking_connection.private_service_access]
}

# Same single schema as the suite lane, spelled the way each engine wants it.
resource "google_sql_database" "ha_test" {
  for_each = local.ha_enabled

  name      = var.test_database
  instance  = google_sql_database_instance.ha[each.key].name
  charset   = each.value.family == "mysql" ? "utf8mb4" : "UTF8"
  collation = each.value.family == "mysql" ? "utf8mb4_0900_ai_ci" : null
}

# The job reads these to pick the instance it promotes, and to check afterwards that the
# instance it is talking to is the same connection name it started with. No credential is
# ever an output.
output "ha_targets" {
  description = "Per failover target: the instance the job runs `gcloud sql instances failover` against, and the private IP that must not change across it."
  value = {
    for key, instance in google_sql_database_instance.ha : key => {
      target_name      = key
      connection_name  = instance.connection_name
      private_ip       = instance.ip_address[0].ip_address
      database         = google_sql_database.ha_test[key].name
      database_version = instance.database_version
      availability     = instance.settings[0].availability_type
      tier             = instance.settings[0].tier
    }
  }
}
