# What the weekly job reads back: the private endpoints it hands to the in-VPC test task
# and nothing else. No password is ever an output; the accounts come from the run's
# bootstrap SQL.

output "network" {
  description = "The VPC the run created, for the destroy step and the cleanup job."
  value = {
    name          = google_compute_network.matrix.name
    runner_subnet = google_compute_subnetwork.runner.self_link
    peering_range = google_compute_global_address.services_peering.address
    labels        = local.common_labels
  }
}

output "mysql_targets" {
  description = "Per target: the connection name and the private IP the suite connects to over TLS."
  value = {
    for key, instance in google_sql_database_instance.mysql : key => {
      target_name     = key
      connection_name = instance.connection_name
      private_ip      = instance.ip_address[0].ip_address
      database        = google_sql_database.test[key].name
      availability    = instance.settings[0].availability_type
      tier            = instance.settings[0].tier
    }
  }
}
