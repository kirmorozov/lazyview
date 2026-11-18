# The MySQL family of the matrix: the two lines Google offers as GA editions, each one
# the source the whole MySQL-family suite feeds its binlog from (role S in the plan).
#
# The settings lazyview requires on connect (src/binlog/connect.rs) are carried as Cloud
# SQL database flags, checked against the supported-flag list on 2026-11-18:
#   binlog_format=ROW, binlog_row_image=full, binlog_row_value_options='',
#   binlog_order_commits=on, binlog_expire_logs_seconds.
# log_bin, gtid_mode and enforce_gtid_consistency are deliberately NOT set: Cloud SQL
# does not offer them as flags. Binary logging comes from PITR (below) and GTID is on by
# default on Cloud SQL MySQL 8.x, so the engine's GTID path still gets GTIDs. The
# runbook asserts all of them with SHOW VARIABLES after the instance is up.

locals {
  # One entry per supported line. A new GA line is one row here, nothing else.
  mysql_targets = {
    "mysql-80" = { database_version = "MYSQL_8_0" }
    "mysql-84" = { database_version = "MYSQL_8_4" }
  }

  mysql_flags = [
    { name = "binlog_format", value = "ROW" },
    # Cloud SQL spells the enumeration value lower-case; the server reports FULL.
    { name = "binlog_row_image", value = "full" },
    # Partial JSON row events are the one binlog shape lazyview cannot read.
    { name = "binlog_row_value_options", value = "" },
    # Commit order on disk must match commit order in the log.
    { name = "binlog_order_commits", value = "on" },
    # Cost note: 24 h of binlogs on a run that lasts ~90 min is storage, and it is what
    # lets a resumed run catch up after a restart. It is the Cloud SQL counterpart of
    # the RDS "binlog retention hours" bootstrap call.
    { name = "binlog_expire_logs_seconds", value = "86400" },
  ]
}

resource "google_sql_database_instance" "mysql" {
  for_each = local.mysql_targets

  name             = "${local.name_prefix}-${each.key}-${var.run_id}"
  database_version = each.value.database_version
  region           = var.region

  # Teardown must never be blocked: the run's destroy step and the cleanup job are the
  # only thing that removes these, and both must be able to finish.
  deletion_protection = false

  settings {
    tier              = var.mysql_tier
    availability_type = var.availability_type
    edition           = "ENTERPRISE"
    disk_type         = "PD_SSD"
    disk_size         = var.mysql_disk_gb
    disk_autoresize   = false
    pricing_plan      = "PER_USE"
    # The instance serves the suite while the run is up; nothing is kept idle.
    activation_policy = "ALWAYS"

    user_labels = local.common_labels

    # Private IP only, and TLS required: no public endpoint exists for these databases.
    # ssl_mode is what the API now calls requireSsl: ENCRYPTED_ONLY accepts only
    # connections encrypted with SSL/TLS, which is what the engine's TLS path needs.
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.matrix.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    # Cloud SQL keeps binary logs only while point-in-time recovery is on, so this is
    # not a belt-and-braces setting: without it the suite has no binlog to read.
    backup_configuration {
      enabled                        = true
      binary_log_enabled             = true
      point_in_time_recovery_enabled = true
    }

    location_preference {
      zone = var.zone
    }

    dynamic "database_flags" {
      for_each = local.mysql_flags
      content {
        name  = database_flags.value.name
        value = database_flags.value.value
      }
    }
  }

  depends_on = [google_service_networking_connection.private_service_access]
}

# The one schema the run creates: docker/initdb/01-lazyview.sql makes lvtest, and the
# suites create their lvt_* schemas themselves. The lv / lvdoc accounts are made by the
# runbook's bootstrap SQL, because a managed service grants no SUPER and the exact
# grants are what the privilege-model test measures.
resource "google_sql_database" "test" {
  for_each = local.mysql_targets

  name      = var.test_database
  instance  = google_sql_database_instance.mysql[each.key].name
  charset   = "utf8mb4"
  collation = "utf8mb4_0900_ai_ci"
}
