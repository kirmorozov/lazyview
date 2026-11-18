// Structural test for the Google Cloud half of the weekly managed-DB matrix.
//
// It is the offline half of the proof: `tofu validate` says the configuration is a
// valid Cloud SQL deployment, this says it is the deployment the suites need. It reads
// the .tf files as text, so it needs no account, no provider and no network.
//
//   node --test deploy/cloud-test/gcp/test/
//
// Every assertion below names the thing a managed service could quietly get wrong and
// that would make a green run meaningless: no binlog to read, an engine setting the
// engine refuses to run with, a public endpoint, or an instance nobody destroys.

import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const tfDir = resolve(here, "..");
const repoRoot = resolve(tfDir, "..", "..", "..");

const read = (name) => readFileSync(join(tfDir, name), "utf8");
const files = {
  versions: read("versions.tf"),
  variables: read("variables.tf"),
  network: read("network.tf"),
  mysql: read("mysql.tf"),
  outputs: read("outputs.tf"),
};

// --- minimal HCL block reader -------------------------------------------------------
// Enough to walk block bodies with their nesting; no expression evaluation.

function endOfBody(text, openBrace) {
  let depth = 0;
  let inString = false;
  let escaped = false;
  let inLineComment = false;
  let inBlockComment = false;
  for (let i = openBrace; i < text.length; i += 1) {
    const c = text[i];
    const next = text[i + 1];
    if (inLineComment) {
      if (c === "\n") inLineComment = false;
      continue;
    }
    if (inBlockComment) {
      if (c === "*" && next === "/") {
        inBlockComment = false;
        i += 1;
      }
      continue;
    }
    if (inString) {
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') inString = false;
      continue;
    }
    if (c === "#" || (c === "/" && next === "/")) {
      inLineComment = true;
      continue;
    }
    if (c === "/" && next === "*") {
      inBlockComment = true;
      i += 1;
      continue;
    }
    if (c === '"') inString = true;
    else if (c === "{") depth += 1;
    else if (c === "}") {
      depth -= 1;
      if (depth === 0) return i;
    }
  }
  throw new Error("unbalanced braces");
}

// Returns the bodies (text between the braces) of every block whose header matches.
function blocks(text, header) {
  const found = [];
  const re = new RegExp(`^\\s*${header}\\s*(?:"([^"]*)")?[^\\n{]*\\{`, "gm");
  let m;
  while ((m = re.exec(text)) !== null) {
    const open = text.indexOf("{", m.index + m[0].length - 1);
    const close = endOfBody(text, open);
    found.push({ body: text.slice(open + 1, close), labels: m[1] });
  }
  return found;
}

// Reads `name = [ ... ]` and returns the text between the brackets.
function listBody(text, name) {
  const start = text.search(new RegExp(`^\\s*${name}\\s*=\\s*\\[`, "m"));
  assert.ok(start >= 0, `${name} = [ ... ] not found`);
  const open = text.indexOf("[", start);
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    if (text[i] === "[") depth += 1;
    else if (text[i] === "]") {
      depth -= 1;
      if (depth === 0) return text.slice(open + 1, i);
    }
  }
  throw new Error(`unbalanced brackets in ${name}`);
}

const block = (text, header) => {
  const found = blocks(text, header);
  assert.equal(found.length, 1, `expected exactly one ${header} block`);
  return found[0].body;
};

const assignment = (body, name) => {
  const m = body.match(new RegExp(`^\\s*${name}\\s*=\\s*(.+?)\\s*$`, "m"));
  if (!m) return undefined;
  return m[1].replace(/^"(.*)"$/, "$1");
};

const hasLine = (body, re) => re.test(body);

// --- the toolchain is pinned, and the pin is the same everywhere --------------------

test("the OpenTofu version is pinned in both places that read it", () => {
  const required = assignment(files.versions, "required_version");
  assert.match(required ?? "", /^=\s*(\d+\.\d+\.\d+)$/, "required_version must be an exact pin");
  const version = required.match(/(\d+\.\d+\.\d+)/)[1];
  const cliPin = read(".terraform-version").trim();
  assert.equal(cliPin, version, ".terraform-version and required_version must agree");
  assert.ok(existsSync(join(tfDir, ".terraform.lock.hcl")), "the provider version must be committed in .terraform.lock.hcl");
});

test("the google provider is pinned, not floating", () => {
  assert.match(files.versions, /source\s*=\s*"hashicorp\/google"/);
  const version = assignment(block(files.versions, "google"), "version");
  assert.match(version ?? "", /^~>\s*\d+\.\d+\.\d+$/, "the provider must carry a patch-level pin");
});

// --- labels: the only thing that makes an abandoned run collectable -----------------

test("every resource carries the managed-by, run-id and expiry labels", () => {
  const labels = block(files.versions, "locals");
  // The expiry is computed in UTC from the run, with the buffer the runbook sets.
  assert.match(labels, /expiry_label\s*=\s*.*formatdate\(/, "the expiry label must be a UTC date computed from the run");
  const labelBody = block(labels, "common_labels");
  for (const key of ["managed-by", "run-id", "expiry"]) {
    assert.match(labelBody, new RegExp(`"${key}"\\s*=`), `the ${key} label is missing`);
  }
  assert.match(labelBody, /"managed-by"\s*=\s*"cloud-db-matrix"/);
  assert.match(labelBody, /"expiry"\s*=\s*local\.expiry_label/);
  // GCP label values are lowercase alphanumeric, "_" and "-": no colons or spaces.
  assert.doesNotMatch(labelBody, /"expiry"\s*=\s*"[^"]*:/, "an expiry label with colons is not a valid GCP label");
});

test("everything the run creates is findable by the cleanup job", () => {
  // Kinds with no labels field in the API. Each is either a child of something
  // labelled (the peering range belongs to the peering, the database to the instance)
  // or is found by the name prefix the next assertion pins.
  const noLabelsField = new Set([
    "google_compute_network",
    "google_compute_subnetwork",
    "google_service_networking_connection",
    "google_sql_database",
  ]);
  const re = /resource\s+"([^"]+)"\s+"([^"]*)"[^\n{]*\{([\s\S]*?)\n\}/g;
  let resources = 0;
  for (const [file, text] of Object.entries(files)) {
    let m;
    while ((m = re.exec(text)) !== null) {
      const [, type, name, body] = m;
      resources += 1;
      if (noLabelsField.has(type)) {
        // ...and its name must still say which run it belongs to.
        const nameArg = assignment(body, "name");
        if (nameArg !== undefined && type !== "google_sql_database") {
          assert.match(nameArg, /local\.name_prefix/, `${file}: ${type}.${name} is named outside the run prefix`);
        }
        continue;
      }
      assert.match(
        body,
        /^\s*(user_)?labels\s*=\s*local\.common_labels\s*$/m,
        `${file}: ${type}.${name} is created without the run labels`,
      );
    }
  }
  assert.equal(resources, 6, "the network, the peering, the runner subnet, the instances and the test schemas");
});

// --- variables: validate must run with no account and no tfvars --------------------

test("no variable can make offline validate fail for want of a value", () => {
  for (const v of blocks(files.variables, "variable")) {
    assert.match(v.body, /^\s*default\s*=/m, `variable ${v.labels} has no default, so validate needs an account`);
  }
});

// --- the instances: the settings the suites depend on ------------------------------

const instanceBody = block(files.mysql, 'resource "google_sql_database_instance"');
const settingsBody = block(instanceBody, "settings");

test("there is one instance per supported MySQL line, named neutrally", () => {
  assert.match(instanceBody, /^\s*for_each\s*=\s*local\.mysql_targets\s*$/m);
  const mapBody = block(block(files.mysql, "locals"), "mysql_targets");
  const versions = [...mapBody.matchAll(/database_version\s*=\s*"([A-Z0-9_]+)"/g)].map((m) => m[1]);
  assert.deepEqual(versions.sort(), ["MYSQL_8_0", "MYSQL_8_4"]);
  const name = assignment(instanceBody, "name");
  assert.match(name, /^\$\{local\.name_prefix\}-\$\{each\.key\}-\$\{var\.run_id\}$/, "instance names must be prefix-target-runid");
});

test("binary logging is on, because Cloud SQL keeps binlogs only with PITR", () => {
  const backup = block(settingsBody, "backup_configuration");
  assert.match(backup, /^\s*point_in_time_recovery_enabled\s*=\s*true\s*$/m, "no PITR means no binary log, so the suite would read nothing");
  assert.match(backup, /^\s*binary_log_enabled\s*=\s*true\s*$/m);
  assert.match(backup, /^\s*enabled\s*=\s*true\s*$/m);
});

test("the flags lazyview refuses to start without are set on every instance", () => {
  const body = listBody(files.mysql, "mysql_flags");
  const pairs = [...body.matchAll(/\{\s*name\s*=\s*"([^"]+)"\s*,\s*value\s*=\s*"([^"]*)"\s*\}/g)].map((m) => `${m[1]}=${m[2]}`);
  for (const want of [
    "binlog_format=ROW",
    "binlog_row_image=full",
    "binlog_row_value_options=",
    "binlog_order_commits=on",
    "binlog_expire_logs_seconds=86400",
  ]) {
    assert.ok(pairs.includes(want), `the flag ${want} must be set explicitly`);
  }
  // Cloud SQL does not offer these; setting them would fail the create, and GTID comes
  // from the platform default, not from here.
  for (const forbidden of ["log_bin", "gtid_mode", "enforce_gtid_consistency", "log_bin_compress"]) {
    assert.ok(!pairs.some((p) => p.startsWith(`${forbidden}=`)), `${forbidden} is not a Cloud SQL flag`);
  }
  assert.match(settingsBody, /dynamic "database_flags"/);
  assert.match(settingsBody, /for_each\s*=\s*local\.mysql_flags/);
});

test("the databases are reachable only from inside the VPC, over TLS", () => {
  const ip = block(settingsBody, "ip_configuration");
  assert.match(ip, /^\s*ipv4_enabled\s*=\s*false\s*$/m, "a public IP on a managed database is never acceptable");
  assert.match(ip, /^\s*private_network\s*=\s*google_compute_network\.matrix\.id\s*$/m);
  // ssl_mode is the API's replacement for require_ssl: ENCRYPTED_ONLY is requireSsl=true.
  assert.match(ip, /^\s*ssl_mode\s*=\s*"ENCRYPTED_ONLY"\s*$/m);
  assert.doesNotMatch(files.mysql + files.network, /ipv4_enabled\s*=\s*true/);
});

test("nothing can block teardown, and nothing scales up on its own", () => {
  assert.match(instanceBody, /^\s*deletion_protection\s*=\s*false\s*$/m);
  assert.match(settingsBody, /^\s*disk_autoresize\s*=\s*false\s*$/m, "autoresize is a bill that grows by itself");
  assert.match(settingsBody, /^\s*pricing_plan\s*=\s*"PER_USE"\s*$/m);
  assert.match(settingsBody, /^\s*tier\s*=\s*var\.mysql_tier\s*$/m, "the machine class comes from one pinned variable");
  assert.match(settingsBody, /^\s*edition\s*=\s*"ENTERPRISE"\s*$/m);
  assert.match(settingsBody, /^\s*availability_type\s*=\s*var\.availability_type\s*$/m);
  assert.match(block(files.variables, 'variable "availability_type"'), /default\s*=\s*"ZONAL"/);
  const tier = block(files.variables, 'variable "mysql_tier"');
  assert.match(tier, /default\s*=\s*"db-custom1-3840"/);
});

test("only the initdb database is created, and it is spelled the same", () => {
  const db = block(files.mysql, 'resource "google_sql_database"');
  assert.match(db, /^\s*name\s*=\s*var\.test_database\s*$/m);
  const initdb = readFileSync(join(repoRoot, "docker", "initdb", "01-lazyview.sql"), "utf8");
  assert.match(initdb, /CREATE DATABASE IF NOT EXISTS lvtest/);
  const declared = block(files.variables, 'variable "test_database"');
  assert.match(declared, /default\s*=\s*"lvtest"/);
});

test("outputs leak no credential and expose no endpoint type but the private one", () => {
  assert.doesNotMatch(files.outputs, /^\s*(password|client_certificate|client_key)\s*=/im);
  assert.match(files.outputs, /private_ip\s*=\s*instance\.ip_address\[0\]\.ip_address/);
});
