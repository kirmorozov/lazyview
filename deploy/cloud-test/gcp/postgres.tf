# The PostgreSQL family of the matrix: every supported major, each one both a target the
# views are written to and a source whose WAL the suite decodes with pgoutput (role B —
# lv-super's ruling 4 makes the PostgreSQL lanes blocking in the cloud job, like the
# MySQL family, so neither role here may be left out of a run).
#
# The settings the source path needs come as Cloud SQL database flags, checked against
# the supported-flag list on 2026-11-18:
#   cloudsql.logical_decoding=on, max_replication_slots, max_wal_senders.
# `wal_level` is deliberately NOT set: Cloud SQL does not offer it as a flag at all, and
# `cloudsql.logical_decoding=on` is the platform's own way of getting `wal_level=logical`
# (its docs say so explicitly — setting wal_level is the instruction for an *external*
# PostgreSQL). So the runbook asserts `SHOW wal_level` returns `logical` after the
# instance is up instead of trusting the flag name, exactly as it does for GTID on MySQL.
#
# `cloudsql.enable_pglogical` and `cloudsql.enable_pgaudit` are also deliberately not
# set: lazyview decodes pgoutput itself (src/pgoutput), so the pglogical extension would
# only add a preload library, and an audit extension would add writes the suite does not
# ask for. Both default to off and the structural test refuses them.
#
# Cost note: nothing here is created per binlog event — this file creates four
# instances for a run, no more. Each is db-custom1-3840 ZONAL, 20 GiB PD-SSD with
# autoresize off, about $0.06 for a 90-minute run.

locals {
  # One entry per supported major, the same grid the CI PostgreSQL legs run. A new GA
  # major is one row here, nothing else.
  pg_targets = {
    "pg-15" = { database_version = "POSTGRES_15" }
    "pg-16" = { database_version = "POSTGRES_16" }
    "pg-17" = { database_version = "POSTGRES_17" }
    "pg-18" = { database_version = "POSTGRES_18" }
  }

  pg_flags = [
    # The one flag that makes the instance readable as a source: it is what puts
    # wal_level=logical on the server, and what lets the run's role hold REPLICATION.
    { name = "cloudsql.logical_decoding", value = "on" },
    # The suite takes one slot and one walsender connection per source it reads. Both
    # values are the platform minimum, and they are written here so the create does not
    # depend on a default the vendor is free to change; the runbook re-reads them from
    # pg_settings.
    { name = "max_replication_slots", value = "10" },
    { name = "max_wal_senders", value = "10" },
  ]

  # The same smallest class that survives the MySQL suite; fixed, so a run cannot scale
  # itself up. Cost note: 4 ZONAL instances of this class is the whole PG bill of a run.
  pg_tier    = "db-custom1-3840"
  pg_disk_gb = 20
}

resource "google_sql_database_instance" "pg" {
  for_each = local.pg_targets

  name             = "${local.name_prefix}-${each.key}-${var.run_id}"
  database_version = each.value.database_version
  region           = var.region

  # Teardown must never be blocked: the run's destroy step and the cleanup job are the
  # only thing that removes these, and both must be able to finish.
  deletion_protection = false

  settings {
    tier              = local.pg_tier
    availability_type = var.availability_type
    edition           = "ENTERPRISE"
    disk_type         = "PD_SSD"
    disk_size         = local.pg_disk_gb
    disk_autoresize   = false
    pricing_plan      = "PER_USE"
    activation_policy = "ALWAYS"

    user_labels = local.common_labels

    # Private IP only, and TLS required. A PostgreSQL source is read over replication
    # connections, so this is the one place where an unencrypted link would also mean
    # the decoded WAL crossing the internet.
    ip_configuration {
      ipv4_enabled    = false
      private_network = google_compute_network.matrix.id
      ssl_mode        = "ENCRYPTED_ONLY"
    }

    # PITR on PostgreSQL is what keeps WAL around, so a run that is restarted can still
    # catch its slots up instead of losing them. binary_log_enabled is a MySQL-only
    # field and is not set here.
    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
    }

    location_preference {
      zone = var.zone
    }

    dynamic "database_flags" {
      for_each = local.pg_flags
      content {
        name  = database_flags.value.name
        value = database_flags.value.value
      }
    }
  }

  depends_on = [google_service_networking_connection.private_service_access]
}

# The one schema per instance: the same database docker/initdb/01-lazyview.sql makes on
# MySQL, because the harness connects to `lvtest` on every engine. The suites create
# their own lvt_* schemas, and on a source the test itself creates the publication and
# the replication slot it decodes (tests/pgsrc_replication.rs) — this file writes no
# data to any source. It is instance administration at create time, once per run, and it
# cannot live in a control store: a publication and a slot are server objects that exist
# only inside the engine that has the WAL.
#
# The `lv` and `lvdoc` accounts are created by the runbook's bootstrap SQL, because a
# managed service grants no superuser; the role that reads the WAL needs the REPLICATION
# attribute, which Cloud SQL lets its own superuser role grant only while
# cloudsql.logical_decoding is on.
resource "google_sql_database" "pg_test" {
  for_each = local.pg_targets

  name     = var.test_database
  instance = google_sql_database_instance.pg[each.key].name
  # UTF8 is the PostgreSQL spelling of the utf8mb4 the MySQL schema uses, so the same
  # fixture bytes round-trip on both engines. The collation is left at the platform
  # default on purpose: the suites order rows explicitly, so pinning a locale here would
  # only make a run depend on a locale image.
  charset = "UTF8"
}

# What the weekly job reads back for this family, kept next to the resources it
# describes so this PR touches no file the MySQL PR owns. No password is ever an
# output. `database_version` is in the output so the job can prove which major served a
# scenario instead of trusting the target's name.
output "pg_targets" {
  description = "Per PostgreSQL target: the connection name and the private IP the suite connects to over TLS."
  value = {
    for key, instance in google_sql_database_instance.pg : key => {
      target_name      = key
      connection_name  = instance.connection_name
      private_ip       = instance.ip_address[0].ip_address
      database         = google_sql_database.pg_test[key].name
      database_version = instance.database_version
      availability     = instance.settings[0].availability_type
      tier             = instance.settings[0].tier
    }
  }
}
