// Structural test for the PostgreSQL family and the failover lane of the weekly
// managed-DB matrix (PR B2).
//
// The offline half of the proof, next to `tofu validate`: validate says the
// configuration is a valid Cloud SQL deployment, this says it is the deployment the
// suites need. It reads the .tf files as text, so it needs no account, no provider and
// no network — the same shape as the MySQL test, sharing its block reader.
//
//   node --test deploy/cloud-test/gcp/test/
//
// Each assertion names something a managed service could quietly get wrong and that would
// make a green run meaningless: a source that cannot be decoded, a standby that is not
// there to promote, a public endpoint, an instance nobody destroys.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { blocks, block, listBody, assignment, assertFindable } from "./hcl.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const tfDir = resolve(here, "..");
const repoRoot = resolve(tfDir, "..", "..", "..");

const read = (name) => readFileSync(join(tfDir, name), "utf8");
// The prose carries the reasons, so the checks that look for something being *absent*
// look at the configuration only: a comment that explains why a setting is not there is
// not the setting being there.
const code = (text) => text.split("\n").filter((line) => !line.trimStart().startsWith("#")).join("\n");
const files = {
  variables: read("variables.tf"),
  mysql: read("mysql.tf"),
  postgres: read("postgres.tf"),
  ha: read("ha.tf"),
};

const pairs = (text, name) =>
  [...listBody(text, name).matchAll(/\{\s*name\s*=\s*"([^"]+)"\s*,\s*value\s*=\s*"([^"]*)"\s*\}/g)]
    .map((m) => `${m[1]}=${m[2]}`);

const pgInstance = block(files.postgres, 'resource "google_sql_database_instance"');
const pgSettings = block(pgInstance, "settings");
const haInstance = block(files.ha, 'resource "google_sql_database_instance"');
const haSettings = block(haInstance, "settings");
const haLocals = block(files.ha, "locals");

// --- the PostgreSQL lane: the settings the source path depends on --------------------

test("there is one instance per supported PostgreSQL major, named neutrally", () => {
  assert.match(pgInstance, /^\s*for_each\s*=\s*local\.pg_targets\s*$/m);
  const mapBody = block(block(files.postgres, "locals"), "pg_targets");
  const versions = [...mapBody.matchAll(/database_version\s*=\s*"([A-Z0-9_]+)"/g)].map((m) => m[1]);
  assert.deepEqual(versions.sort(), ["POSTGRES_15", "POSTGRES_16", "POSTGRES_17", "POSTGRES_18"]);
  // The role semantics come from the CI jobs this lane mirrors: PostgreSQL is both a
  // target (the targets_pg_ gates) and a source (integration-pgsrc), and the cloud run
  // hands the harness the same LAZYVIEW_TEST_PG_DSNS variable, so one entry per major
  // serves both roles here too.
  const ci = readFileSync(join(repoRoot, ".github", "workflows", "ci.yml"), "utf8");
  assert.match(ci, /LAZYVIEW_TEST_PG_DSNS/, "the cloud lane drives the same harness entry point CI uses");
  assert.match(ci, /integration-pgsrc/, "the source role this file configures is the one CI already gates");
  const name = assignment(pgInstance, "name");
  assert.match(name, /^\$\{local\.name_prefix\}-\$\{each\.key\}-\$\{var\.run_id\}$/, "instance names must be prefix-target-runid");
});

test("logical decoding is on, and wal_level is not set as a flag because Cloud SQL has none", () => {
  const flags = pairs(files.postgres, "pg_flags");
  assert.ok(flags.includes("cloudsql.logical_decoding=on"), "without it the WAL cannot be decoded, so the source lane reads nothing");
  assert.ok(flags.some((f) => f.startsWith("max_replication_slots=")), "the suite takes a replication slot on the source");
  assert.ok(flags.some((f) => f.startsWith("max_wal_senders=")), "the slot needs a walsender behind it");
  // Cloud SQL does not offer wal_level as a flag at all; cloudsql.logical_decoding is the
  // platform's own way of getting wal_level=logical, and the runbook asserts the server's
  // value rather than trusting the flag name.
  for (const forbidden of ["wal_level", "cloudsql.enable_pglogical", "cloudsql.enable_pgaudit"]) {
    assert.ok(!flags.some((f) => f.startsWith(`${forbidden}=`)), `${forbidden} is not a supported Cloud SQL flag or not ours to set`);
  }
  // The MySQL-only settings must not leak onto a PostgreSQL instance: the create would fail.
  assert.ok(!flags.some((f) => /^(binlog_|log_bin|gtid_)/.test(f)), "a MySQL flag on a PostgreSQL instance fails the create");
  assert.match(pgSettings, /dynamic "database_flags"/);
  assert.match(pgSettings, /for_each\s*=\s*local\.pg_flags/);
});

test("the PostgreSQL instances keep WAL, and no MySQL-only binlog field is sent", () => {
  const backup = block(pgSettings, "backup_configuration");
  assert.match(backup, /^\s*point_in_time_recovery_enabled\s*=\s*true\s*$/m, "no PITR means WAL is not kept for a resumed run to catch up on");
  assert.match(backup, /^\s*enabled\s*=\s*true\s*$/m);
  assert.doesNotMatch(backup, /binary_log_enabled/, "binary_log_enabled is MySQL-only and the API rejects it for PostgreSQL");
});

test("the PostgreSQL lanes are blocking, so nothing makes them advisory or partial", () => {
  // Both roles: the same instance is the target the views are written to and the source
  // that is decoded, one entry per major, no separate advisory set.
  const instanceNames = [...files.postgres.matchAll(/resource\s+"([^"]+)"\s+"([^"]*)"/g)].map((m) => `${m[1]}.${m[2]}`);
  assert.deepEqual(instanceNames, ["google_sql_database_instance.pg", "google_sql_database.pg_test"]);
  assert.match(pgSettings, /^\s*availability_type\s*=\s*var\.availability_type\s*$/m, "the suite lane takes the one pinned availability");
  assert.doesNotMatch(files.postgres, /advisory|continue_on_error/i, "the PostgreSQL cloud lanes are blocking (ruling 4)");
});

test("the PostgreSQL instances are reachable only from inside the VPC, over TLS", () => {
  const ip = block(pgSettings, "ip_configuration");
  assert.match(ip, /^\s*ipv4_enabled\s*=\s*false\s*$/m, "a public IP on a managed database is never acceptable");
  assert.match(ip, /^\s*private_network\s*=\s*google_compute_network\.matrix\.id\s*$/m);
  assert.match(ip, /^\s*ssl_mode\s*=\s*"ENCRYPTED_ONLY"\s*$/m, "a decoded WAL must not cross the internet unencrypted");
  assert.doesNotMatch(files.postgres, /ipv4_enabled\s*=\s*true/);
});

test("nothing can block teardown, and nothing scales up on its own", () => {
  assert.match(pgInstance, /^\s*deletion_protection\s*=\s*false\s*$/m);
  assert.match(pgSettings, /^\s*disk_autoresize\s*=\s*false\s*$/m, "autoresize is a bill that grows by itself");
  assert.match(pgSettings, /^\s*pricing_plan\s*=\s*"PER_USE"\s*$/m);
  assert.match(pgSettings, /^\s*edition\s*=\s*"ENTERPRISE"\s*$/m);
  assert.match(pgSettings, /^\s*tier\s*=\s*local\.pg_tier\s*$/m, "the machine class comes from one pinned local");
  assert.match(block(files.postgres, "locals"), /pg_tier\s*=\s*"db-custom1-3840"/);
  assert.match(pgInstance, /depends_on\s*=\s*\[google_service_networking_connection\.private_service_access\]/, "the peering must exist before the private IP is allocated");
});

test("only the initdb database is created, in the encoding the fixtures need", () => {
  const db = block(files.postgres, 'resource "google_sql_database"');
  assert.match(db, /^\s*name\s*=\s*var\.test_database\s*$/m);
  assert.match(db, /^\s*charset\s*=\s*"UTF8"\s*$/m);
  assert.doesNotMatch(db, /^\s*collation\s*=/m, "the collation is the platform default; the suites order explicitly");
  const initdb = readFileSync(join(repoRoot, "docker", "initdb", "01-lazyview.sql"), "utf8");
  assert.match(initdb, /CREATE DATABASE IF NOT EXISTS lvtest/);
  const declared = block(files.variables, 'variable "test_database"');
  assert.match(declared, /default\s*=\s*"lvtest"/);
});

// --- the failover lane: the standby the promotion scenario needs ---------------------

test("the failover lane has a standby to promote for both engine families", () => {
  const mapBody = block(haLocals, "ha_targets");
  const entries = [...mapBody.matchAll(/"([a-z0-9-]+)"\s*=\s*\{([^}]*)\}/g)].map((m) => ({
    key: m[1],
    version: (m[2].match(/database_version\s*=\s*"([A-Z0-9_]+)"/) || [])[1],
    family: (m[2].match(/family\s*=\s*"([a-z]+)"/) || [])[1],
    flags: (m[2].match(/flags\s*=\s*(local\.\w+)/) || [])[1],
  }));
  assert.deepEqual(entries.map((e) => e.family).sort(), ["mysql", "pg"], "both families need a promotable standby");
  assert.deepEqual(new Set(entries.map((e) => e.key)).size, entries.length, "every failover target has its own name");
  for (const e of entries) {
    assert.match(e.key, /-ha$/, `${e.key} must say it is a failover target`);
    assert.ok(e.version && e.flags, `${e.key} must name its version and reuse the suite lane's flag list`);
    assert.ok(
      e.flags === (e.family === "mysql" ? "local.mysql_flags" : "local.pg_flags"),
      `${e.key} takes the wrong family's flag list`,
    );
  }
  // The scenario is the GTID plan's, so a promotion means a standby exists, in two zones.
  assert.match(haSettings, /^\s*availability_type\s*=\s*"REGIONAL"\s*$/m, "a ZONAL instance turns the failover test into a reboot test that still passes");
  assert.doesNotMatch(haSettings, /availability_type\s*=\s*var\./, "the failover lane must not inherit the suite lane's availability");
});

test("the failover lane reuses the settings it is meant to be measuring", () => {
  assert.match(haSettings, /dynamic "database_flags"/);
  assert.match(haSettings, /for_each\s*=\s*each\.value\.flags/, "no flag can exist in one lane and be missing from the other");
  const backup = block(haSettings, "backup_configuration");
  assert.match(backup, /^\s*point_in_time_recovery_enabled\s*=\s*true\s*$/m);
  assert.match(backup, /binary_log_enabled\s*=\s*each\.value\.family\s*==\s*"mysql"\s*\?\s*true\s*:\s*null/, "the binlog flag is set only where the API accepts it");
});

test("the failover lane pins no zone and keeps one address across the promotion", () => {
  // No location_preference: Cloud SQL chooses both zones inside var.region, so the run
  // works in whatever region the owner picks.
  assert.doesNotMatch(code(files.ha), /location_preference|secondary_zone/, "pinning zones would break the run in any other region");
  // The private endpoint is what must survive gcloud sql instances failover.
  assert.match(block(haSettings, "ip_configuration"), /^\s*ipv4_enabled\s*=\s*false\s*$/m);
  assert.match(block(haSettings, "ip_configuration"), /^\s*ssl_mode\s*=\s*"ENCRYPTED_ONLY"\s*$/m);
  assert.match(haInstance, /^\s*deletion_protection\s*=\s*false\s*$/m);
  assert.match(haSettings, /^\s*disk_autoresize\s*=\s*false\s*$/m);
  assert.match(block(haLocals, "ha_targets"), /"pg-17-ha"/);
});

test("the failover lane is a cost the run can decline, and its default keeps the proof", () => {
  const v = block(files.ha, 'variable "enable_failover_lane"');
  assert.match(v, /^\s*type\s*=\s*bool\s*$/m);
  assert.match(v, /^\s*default\s*=\s*true\s*$/m, "the weekly run gets the failover proof unless it says otherwise");
  assert.match(v, /description\s*=\s*"[^"]*Cost note/i, "a cost the run can decline still has to say what it costs");
  assert.match(haLocals, /ha_enabled\s*=\s*var\.enable_failover_lane\s*\?\s*local\.ha_targets\s*:\s*\{\}/);
  assert.match(haInstance, /^\s*for_each\s*=\s*local\.ha_enabled\s*$/m);
});

// --- the rules every resource in this PR follows ------------------------------------

test("everything B2 creates is findable by the cleanup job", () => {
  const resources = assertFindable({ postgres: files.postgres, ha: files.ha });
  assert.equal(resources, 4, "the two PostgreSQL instance sets and the two test schemas");
});

test("no new variable can make offline validate fail for want of a value", () => {
  for (const v of blocks(files.ha, "variable")) {
    assert.match(v.body, /^\s*default\s*=/m, `variable ${v.labels} has no default, so validate needs an account`);
  }
});

test("the new outputs leak no credential and expose no endpoint type but the private one", () => {
  const outputs = files.postgres + files.ha;
  assert.doesNotMatch(outputs, /^\s*(password|client_certificate|client_key)\s*=/im);
  assert.equal(blocks(files.postgres, "output").length, 1, "the PostgreSQL lane has one output block");
  assert.equal(blocks(files.ha, "output").length, 1, "the failover lane has one output block");
  for (const text of [files.postgres, files.ha]) {
    assert.match(text, /private_ip\s*=\s*instance\.ip_address\[0\]\.ip_address/);
    assert.match(text, /availability\s*=\s*instance\.settings\[0\]\.availability_type/, "the job must be able to prove the lane it promoted was HA");
  }
});
