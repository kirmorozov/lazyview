//! Service configuration from environment variables (see docs/configuration.md).

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartMode {
    /// Continue from the saved binlog position; build only views without valid state.
    Resume,
    /// Rebuild every view, then stream from the current position.
    Rebuild,
    /// Build/refresh every view and exit.
    Once,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingBinlog {
    Fail,
    Rebuild,
}

/// `LAZYVIEW_SUPERVISOR`: how flush work is executed (docs/design-supervisor.md).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SupervisorMode {
    /// Concurrent refreshes on connection pools with an adaptive per-endpoint limit, binlog
    /// read while a flush runs.
    Adaptive,
    /// As `Adaptive`, with every limit pinned at its maximum.
    Fixed,
    /// One sequential flush at a time on the engine's own connections (the pre-supervisor
    /// engine).
    Off,
}

/// Supervisor settings (see docs/design-supervisor.md §5).
#[derive(Debug, Clone)]
pub struct SupervisorConfig {
    pub mode: SupervisorMode,
    /// Source read pool size (= maximum read concurrency).
    pub max_source: usize,
    /// Write pool size per target (incl. `source`) unless overridden per target.
    pub max_target: usize,
    /// `LAZYVIEW_TARGET_<NAME>_MAX_CONCURRENCY` (lower-case target name → pool size).
    pub max_target_override: BTreeMap<String, usize>,
    /// Rows read from the sources but not yet written to every target (admission budget).
    pub max_inflight_rows: usize,
    /// Diffs running off the async threads at once.
    pub cpu_workers: usize,
}

impl SupervisorConfig {
    /// Write pool size of `target`.
    pub fn max_for_target(&self, target: &str) -> usize {
        self.max_target_override
            .get(&target.to_ascii_lowercase())
            .copied()
            .unwrap_or(self.max_target)
    }
}

/// Interim pool sizes while `adaptive` runs with fixed limits (the design's 8 / 4 come with the
/// adaptive limiter).
pub const DEFAULT_MAX_SOURCE: usize = 4;
pub const DEFAULT_MAX_TARGET: usize = 2;
/// Default upper bound (seconds) for one table-lock wait of a session the engine opens
/// (`LAZYVIEW_TEST_LOCK_WAIT_S`); the server default (a year) would hang a blocked refresh.
pub const DEFAULT_LOCK_WAIT_S: u64 = 60;

/// `min(4, max(1, cores / 4))`.
pub fn default_cpu_workers() -> usize {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    (cores / 4).clamp(1, 4)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dsn: String,
    /// The family the source DSN's scheme chose, once at `from_lookup`; read at connect.
    /// A `postgres://` source is refused there until the enabling PR (see
    /// [`crate::dialect::Family::from_source_dsn`]).
    pub source_family: crate::dialect::Family,
    /// Named target instances for materialized tables (`_lazyview.target`); NULL/absent target
    /// means the source instance.
    pub targets: BTreeMap<String, String>,
    pub start_mode: StartMode,
    pub sync: bool,
    pub batch_window: Duration,
    pub batch_max_keys: usize,
    pub query_chunk: usize,
    pub write_batch_rows: usize,
    /// Estimated bytes per INSERT / DELETE statement of a write (at most half of each server's
    /// `max_allowed_packet`).
    pub write_batch_bytes: usize,
    pub server_id: u32,
    pub instance_id: String,
    pub drop_removed: bool,
    pub on_missing_binlog: MissingBinlog,
    pub heartbeat: Duration,
    /// Minimum time between two full rebuilds of the same view (views that cannot be maintained
    /// incrementally: DISTINCT, UNION, LIMIT, ...). Changes mark the view stale; it is rebuilt
    /// once per interval at most. 0 = as soon as possible.
    pub full_refresh_interval: Duration,
    /// Upper bound for one write round-trip to a target (an unresponsive target must not stall
    /// the binlog loop).
    pub target_timeout: Duration,
    /// Upper bound for planning one definition (`LAZYVIEW_PLAN_TIMEOUT_MS`). Planning runs off
    /// the binlog loop; a definition that takes longer goes into error and is not planned again
    /// until it changes.
    pub plan_timeout: Duration,
    /// Test hook `LAZYVIEW_TEST_PLAN_DELAY=<marker>:<ms>`: planning a definition whose text
    /// contains `marker` first sleeps that long.
    pub test_plan_delay: Option<(String, Duration)>,
    /// Test hook `LAZYVIEW_TEST_BUILD_DELAY=<marker>:<ms>`: a build of a definition whose text
    /// contains `marker` first sleeps that long, after it took its build slot.
    pub test_build_delay: Option<(String, Duration)>,
    /// Test hook `LAZYVIEW_TEST_BUILD_HOLD_BYTES=<bytes>`: the byte budget of changes buffered
    /// for building versions instead of the engine's constant (see `Engine::hold_for_builds`).
    pub test_build_hold_bytes: Option<usize>,
    /// Test hook `LAZYVIEW_TEST_LAG_HOLD_BYTES=<bytes>`: the byte budget of the changes kept
    /// for targets that are behind instead of the engine's constant (see
    /// `Engine::lag_over_budget`: past it the engine only reports).
    pub test_lag_hold_bytes: Option<usize>,
    /// Test hook `LAZYVIEW_TEST_FAIL_IDENTITY=<n>`: the first `n` source identity reads of a
    /// start fail (see `engine::server_identity`), the stand-in for a connection blip there.
    pub test_fail_identity: Option<usize>,
    /// Test hook `LAZYVIEW_TEST_FAIL_SNAPSHOT=<n>`: the first `n` consistent-snapshot position
    /// reads of a fence fail (see `source::maria::snapshot_position`), the stand-in for a transient
    /// source error there (not a connection-class one).
    pub test_fail_snapshot: Option<usize>,
    /// Test hook `LAZYVIEW_TEST_BINLOG_DSN=<dsn>`: the DSN of the binlog dump connection
    /// alone (`BinlogReader::connect`), so a network test can cut the stream without touching
    /// the reads. Unset ⇒ `Config::dsn`: the same endpoint, nothing extra opened.
    pub test_binlog_dsn: Option<String>,
    /// Test hook `LAZYVIEW_TEST_QUERY_DSN=<dsn>`: the DSN of the source-read connection
    /// (`Engine::src`: lookups, refresh SELECTs, catalog), of the supervised source reads and
    /// of the per-build source reads. Unset ⇒ `Config::dsn`.
    pub test_query_dsn: Option<String>,
    /// Test hook `LAZYVIEW_TEST_META_DSN=<dsn>`: the DSN of the metadata connection (the
    /// catalog writes, the fencing epoch, the leadership lock). Unset ⇒ `Config::dsn`.
    pub test_meta_dsn: Option<String>,
    /// Test hook `LAZYVIEW_TEST_LOCK_WAIT_S=<s>` (default 60): how long at most a session the
    /// engine opens waits for a table lock (`SET SESSION lock_wait_timeout`, the third statement
    /// of `crate::target::source_session_setup`); a refresh blocked longer fails and is retried with backoff.
    pub lock_wait_s: u64,
    /// Background builds running at once (each holds 1 + targets connections).
    pub build_concurrency: usize,
    /// Wait before reading sources when no GTID fence is available (binlog events can arrive
    /// before their transaction is visible to readers).
    pub visibility_delay: Duration,
    /// How often the binlog position is persisted when nothing else is written.
    pub position_persist_interval: Duration,
    /// HTTP health/metrics endpoint (`/healthz`, `/readyz`, `/metrics`); None = disabled.
    pub health_addr: Option<std::net::SocketAddr>,
    /// `LAZYVIEW_HEALTH_TOKEN` / `LAZYVIEW_HEALTH_TOKEN_FILE` (a secret reference, see
    /// [`crate::secrets`]): when set, `/metrics` and `/stats` answer only to
    /// `Authorization: Bearer <token>`; `/livez`, `/readyz` and `/startupz` stay open.
    pub health_token: Option<crate::secrets::Secret>,
    /// The source and the targets that would connect to a non-local server with TLS off.
    /// The entrypoint (`src/main.rs`) logs one WARN per entry; nothing else changes.
    pub cleartext: Vec<CleartextEndpoint>,
    /// `LAZYVIEW_SERVER_ID` was set explicitly (otherwise a random id is used and re-rolled on
    /// a collision with another replica).
    pub server_id_fixed: bool,
    /// Refresh supervisor (`LAZYVIEW_SUPERVISOR`, pool sizes, budgets).
    pub supervisor: SupervisorConfig,
}

impl Config {
    pub fn from_env() -> Result<Config> {
        // Targets configured as LAZYVIEW_TARGET_<NAME>_{DSN,HOST} are discovered automatically.
        let discovered: Vec<String> = std::env::vars()
            .filter_map(|(k, _)| {
                let rest = k.strip_prefix("LAZYVIEW_TARGET_")?;
                let name = rest
                    .strip_suffix("_DSN")
                    .or_else(|| rest.strip_suffix("_HOST"))?;
                (!name.is_empty()).then(|| name.to_ascii_lowercase())
            })
            .collect();
        Self::from_lookup(|k| match std::env::var(k).ok() {
            None if k == "LAZYVIEW_TARGETS" && !discovered.is_empty() => Some(discovered.join(",")),
            v => v,
        })
    }

    pub fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Config> {
        // Endpoints reached without TLS, named in a WARN at startup (see CleartextEndpoint).
        let mut cleartext: Vec<CleartextEndpoint> = Vec::new();
        let dsn = connection(&get, "LAZYVIEW", false, Some(("source", &mut cleartext)))?.ok_or_else(|| {
            anyhow!("no database configured: set LAZYVIEW_HOST, LAZYVIEW_USER, LAZYVIEW_PASSWORD, LAZYVIEW_DATABASE (or LAZYVIEW_DSN=mysql://user:pass@host:3306/db)")
        })?;
        // The DSN's scheme chooses the source's family once (a `postgres://` source is
        // refused here with the "not supported yet" message; binlog::replica_opts stays
        // the MySQL-only DSN parser).
        let source_family = crate::dialect::Family::from_source_dsn(&dsn)?;
        let instance_id = get("LAZYVIEW_INSTANCE_ID").unwrap_or_else(|| "default".into());
        let num = |k: &str, d: u64| -> Result<u64> {
            match get(k) {
                Some(v) => v
                    .trim()
                    .parse()
                    .with_context(|| format!("{k} must be a number")),
                None => Ok(d),
            }
        };
        let flag = |k: &str, d: bool| -> Result<bool> {
            match get(k).map(|v| v.trim().to_ascii_lowercase()) {
                None => Ok(d),
                Some(v) if ["1", "true", "yes", "on"].contains(&v.as_str()) => Ok(true),
                Some(v) if ["0", "false", "no", "off"].contains(&v.as_str()) => Ok(false),
                Some(v) => bail!("{k}: expected true/false, got {v}"),
            }
        };
        let start_mode = match get("LAZYVIEW_START_MODE").map(|v| v.to_ascii_lowercase()) {
            None => StartMode::Resume,
            Some(v) => match v.as_str() {
                "resume" => StartMode::Resume,
                "rebuild" => StartMode::Rebuild,
                "once" => StartMode::Once,
                _ => bail!("LAZYVIEW_START_MODE must be resume|rebuild|once"),
            },
        };
        let on_missing_binlog =
            match get("LAZYVIEW_ON_MISSING_BINLOG").map(|v| v.to_ascii_lowercase()) {
                None => MissingBinlog::Fail,
                Some(v) => match v.as_str() {
                    "fail" => MissingBinlog::Fail,
                    "rebuild" => MissingBinlog::Rebuild,
                    _ => bail!("LAZYVIEW_ON_MISSING_BINLOG must be fail|rebuild"),
                },
            };
        let server_id_fixed = get("LAZYVIEW_SERVER_ID").is_some();
        let server_id = match get("LAZYVIEW_SERVER_ID") {
            Some(v) => v
                .trim()
                .parse()
                .context("LAZYVIEW_SERVER_ID must be a number")?,
            None => random_server_id(),
        };
        if server_id == 0 {
            bail!("LAZYVIEW_SERVER_ID must not be 0");
        }
        let query_chunk = num("LAZYVIEW_QUERY_CHUNK", 1000)? as usize;
        if query_chunk == 0 {
            bail!("LAZYVIEW_QUERY_CHUNK must be > 0");
        }
        let supervisor_mode =
            match get("LAZYVIEW_SUPERVISOR").map(|v| v.trim().to_ascii_lowercase()) {
                None => SupervisorMode::Adaptive,
                Some(v) => match v.as_str() {
                    "" | "adaptive" => SupervisorMode::Adaptive,
                    "fixed" => SupervisorMode::Fixed,
                    "off" => SupervisorMode::Off,
                    _ => bail!("LAZYVIEW_SUPERVISOR must be adaptive|fixed|off"),
                },
            };
        let mut max_target_override = BTreeMap::new();
        let mut targets = BTreeMap::new();
        if let Some(names) = get("LAZYVIEW_TARGETS") {
            for name in names.split(',').map(str::trim).filter(|n| !n.is_empty()) {
                let prefix = format!(
                    "LAZYVIEW_TARGET_{}",
                    name.to_ascii_uppercase().replace('-', "_")
                );
                let label = format!("target {name}");
                let dsn = connection(
                    &get,
                    &prefix,
                    true,
                    Some((label.as_str(), &mut cleartext)),
                )?
                .ok_or_else(|| anyhow!("target {name}: set {prefix}_HOST/_USER/_PASSWORD/_DATABASE or {prefix}_DSN"))?;
                targets.insert(name.to_string(), dsn);
            }
        }
        for name in targets.keys().map(String::as_str).chain(["source"]) {
            let k = format!(
                "LAZYVIEW_TARGET_{}_MAX_CONCURRENCY",
                name.to_ascii_uppercase().replace('-', "_")
            );
            if get(&k).is_some() {
                max_target_override
                    .insert(name.to_ascii_lowercase(), (num(&k, 1)? as usize).max(1));
            }
        }
        let supervisor = SupervisorConfig {
            mode: supervisor_mode,
            max_source: (num("LAZYVIEW_MAX_SOURCE_CONCURRENCY", DEFAULT_MAX_SOURCE as u64)?
                as usize)
                .max(1),
            max_target: (num("LAZYVIEW_MAX_TARGET_CONCURRENCY", DEFAULT_MAX_TARGET as u64)?
                as usize)
                .max(1),
            max_target_override,
            max_inflight_rows: (num("LAZYVIEW_MAX_INFLIGHT_ROWS", 200_000)? as usize).max(1),
            cpu_workers: (num("LAZYVIEW_CPU_WORKERS", default_cpu_workers() as u64)? as usize)
                .max(1),
        };
        // `<marker>:<ms>` of the LAZYVIEW_TEST_*_DELAY hooks.
        let marker_delay = |k: &str| -> Result<Option<(String, Duration)>> {
            let Some(v) = get(k) else {
                return Ok(None);
            };
            let (marker, ms) = v
                .rsplit_once(':')
                .filter(|(m, _)| !m.is_empty())
                .with_context(|| format!("{k} must be <marker>:<ms>"))?;
            let ms: u64 = ms
                .trim()
                .parse()
                .with_context(|| format!("{k} must be <marker>:<ms>"))?;
            Ok(Some((marker.to_string(), Duration::from_millis(ms))))
        };
        // The three source roles of the network tests (chaos plan F4): the binlog dump, the
        // reads and the metadata connection each take their own endpoint when the hook is set,
        // read through `connection` so `mariadb://`, the TLS modes and the encoding are exactly
        // LAZYVIEW_DSN's. Unset ⇒ `dsn`, so a run without the hooks never leaves LAZYVIEW_DSN.
        let role_dsn = |prefix: &str| -> Result<Option<String>> {
            // No cleartext warning for a test hook: those endpoints exist for the network tests.
            let v = connection(&get, prefix, false, None)?;
            if let Some(v) = &v {
                // Same rule as the source DSN above: a role's DSN is a source DSN, so a
                // `postgres://` one is refused here with the "not supported yet" message.
                crate::dialect::Family::from_source_dsn(v)?;
            }
            Ok(v.or_else(|| Some(dsn.clone())))
        };
        let test_binlog_dsn = role_dsn("LAZYVIEW_TEST_BINLOG")?;
        let test_query_dsn = role_dsn("LAZYVIEW_TEST_QUERY")?;
        let test_meta_dsn = role_dsn("LAZYVIEW_TEST_META")?;
        Ok(Config {
            dsn,
            source_family,
            targets,
            start_mode,
            sync: flag("LAZYVIEW_SYNC", true)?,
            batch_window: Duration::from_millis(num("LAZYVIEW_BATCH_WINDOW_MS", 200)?),
            batch_max_keys: num("LAZYVIEW_BATCH_MAX_KEYS", 50_000)? as usize,
            query_chunk,
            write_batch_rows: num("LAZYVIEW_WRITE_BATCH_ROWS", 500)?.max(1) as usize,
            write_batch_bytes: num(
                "LAZYVIEW_WRITE_BATCH_BYTES",
                crate::mv::DEFAULT_WRITE_BATCH_BYTES as u64,
            )?
            .max(1024) as usize,
            server_id,
            instance_id,
            drop_removed: flag("LAZYVIEW_DROP_REMOVED", true)?,
            on_missing_binlog,
            build_concurrency: num("LAZYVIEW_BUILD_CONCURRENCY", 2)?.max(1) as usize,
            full_refresh_interval: Duration::from_millis(num("LAZYVIEW_FULL_REFRESH_MS", 60_000)?),
            target_timeout: Duration::from_millis(
                num("LAZYVIEW_TARGET_TIMEOUT_MS", 60_000)?.max(1_000),
            ),
            plan_timeout: Duration::from_millis(num("LAZYVIEW_PLAN_TIMEOUT_MS", 10_000)?.max(1)),
            test_plan_delay: marker_delay("LAZYVIEW_TEST_PLAN_DELAY")?,
            test_build_delay: marker_delay("LAZYVIEW_TEST_BUILD_DELAY")?,
            test_build_hold_bytes: Some(num("LAZYVIEW_TEST_BUILD_HOLD_BYTES", 0)? as usize)
                .filter(|&b| b > 0),
            test_lag_hold_bytes: Some(num("LAZYVIEW_TEST_LAG_HOLD_BYTES", 0)? as usize)
                .filter(|&b| b > 0),
            test_fail_identity: Some(num("LAZYVIEW_TEST_FAIL_IDENTITY", 0)? as usize)
                .filter(|&n| n > 0),
            test_fail_snapshot: Some(num("LAZYVIEW_TEST_FAIL_SNAPSHOT", 0)? as usize)
                .filter(|&n| n > 0),
            test_binlog_dsn,
            test_query_dsn,
            test_meta_dsn,
            lock_wait_s: num("LAZYVIEW_TEST_LOCK_WAIT_S", DEFAULT_LOCK_WAIT_S)?.max(1),
            heartbeat: Duration::from_millis(num("LAZYVIEW_HEARTBEAT_MS", 5_000)?.max(100)),
            visibility_delay: Duration::from_millis(num("LAZYVIEW_VISIBILITY_DELAY_MS", 20)?),
            position_persist_interval: Duration::from_millis(num(
                "LAZYVIEW_POSITION_PERSIST_MS",
                5_000,
            )?),
            // Loopback by default (2026-11-15): `/metrics` and `/stats` describe the workload, so
            // a pod or a host-network process must ask for them on a routable address
            // (LAZYVIEW_HEALTH_ADDR=0.0.0.0:8080); the container health check still works, it
            // probes its own loopback.
            health_addr: match get("LAZYVIEW_HEALTH_ADDR").map(|v| v.trim().to_string()) {
                None => Some(([127, 0, 0, 1], 8080).into()),
                Some(v) if v.is_empty() || v.eq_ignore_ascii_case("off") => None,
                Some(v) => Some(v.parse().with_context(|| {
                    format!("LAZYVIEW_HEALTH_ADDR must be host:port or off, got {v}")
                })?),
            },
            health_token: crate::secrets::resolve_var(&get, "LAZYVIEW_HEALTH_TOKEN")?,
            cleartext,
            server_id_fixed,
            supervisor,
        })
    }
}

/// `<PREFIX>_DSN`, or a DSN assembled from `<PREFIX>_HOST`, `_PORT`, `_USER`, `_PASSWORD`,
/// `_DATABASE` (values used verbatim: no URL encoding needed by the user).
///
/// The password — the `<PREFIX>_PASSWORD` / `_PASSWORD_FILE` pair, or the password field of
/// `<PREFIX>_DSN` — goes through [`crate::secrets`], so it can name a file or an AWS Secrets
/// Manager secret instead of holding the value. With `warn_to`, a non-local endpoint whose TLS is
/// off is recorded there (see [`CleartextEndpoint`]).
fn connection(
    get: &impl Fn(&str) -> Option<String>,
    prefix: &str,
    allow_pg: bool,
    warn_to: Option<(&str, &mut Vec<CleartextEndpoint>)>,
) -> Result<Option<String>> {
    let part = |k: &str| get(&format!("{prefix}_{k}")).filter(|v| !v.is_empty());
    let ca = part("TLS_CA").or_else(|| get("LAZYVIEW_TLS_CA").filter(|v| !v.is_empty()));
    // A CA file without an explicit mode means full verification (certificate + host name).
    // (verify-ca, i.e. skipping only the host-name check, does not work with mysql_async 0.37 +
    // current rustls: its NotValidForName string match never fires.)
    let tls = part("TLS")
        .or_else(|| get("LAZYVIEW_TLS").filter(|v| !v.is_empty()))
        .or_else(|| ca.as_ref().map(|_| "verify-full".to_string()));
    // Its own error messages already name the variable (or the `_FILE` twin).
    let password = crate::secrets::resolve_var(get, &format!("{prefix}_PASSWORD"))?;
    let dsn = match part("DSN") {
        Some(dsn) => Some(resolve_dsn_password(&dsn, &format!("{prefix}_DSN"))?),
        None => assemble(&part, password.as_ref(), prefix)?,
    };
    let Some(d) = dsn else {
        return Ok(None);
    };
    let mut d = finish_dsn(&d, tls.as_deref(), prefix, allow_pg, ca.as_deref())?;
    if let Some(ca) = &ca {
        // finish_pg_dsn mapped a PostgreSQL target's CA onto sslrootcert instead (D11).
        if !d.starts_with("postgres") {
            d.push(if d.contains('?') { '&' } else { '?' });
            d.push_str("tls_ca=");
            d.push_str(&encode(ca));
        }
    }
    if let Some((endpoint, out)) = warn_to {
        if let Some(host) = cleartext_host(&d) {
            out.push(CleartextEndpoint {
                endpoint: endpoint.to_string(),
                var: format!("{prefix}_TLS"),
                host,
            });
        }
    }
    Ok(Some(d))
}

/// An endpoint (the source or a target) that connects to a server off this host with TLS off:
/// credentials and data cross the network in the clear. One WARN per entry is logged at startup
/// (`src/main.rs`); behavior is otherwise unchanged, because `LAZYVIEW_TLS` has always defaulted
/// to `disabled` and an internal network is a legitimate choice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleartextEndpoint {
    /// `source`, or `target <name>` as it was configured.
    pub endpoint: String,
    /// The variable that turns TLS on for it: `LAZYVIEW_TLS` or `LAZYVIEW_TARGET_<NAME>_TLS`.
    pub var: String,
    /// The host it connects to (a host is not a secret; it is what the operator has to fix).
    pub host: String,
}

/// The pieces of `scheme://[user[:password]@]host[:port]/path[?query]`, as slices of the original
/// text so a resolved password can be spliced back without touching anything else.
struct DsnParts<'a> {
    /// `scheme://`.
    head: &'a str,
    /// The user, empty when there is no userinfo.
    user: &'a str,
    /// The password exactly as written (percent-encoded when the operator encoded it).
    password: Option<&'a str>,
    /// `host[:port]/path`, without the `@`.
    host: &'a str,
    /// The `?query` (or `#fragment`) part, empty when there is none.
    query: &'a str,
}

impl DsnParts<'_> {
    /// True when a second `scheme://` sits where the host should be: the password there is an
    /// unencoded secret reference, which a URL parser cannot separate from the host — the `#` of
    /// `mysql://lv:aws-secret://id#password@h/db` swallows the host, and
    /// `mysql://lv:file:///etc/pw` leaves no host at all. A real host, port, path or socket path
    /// never contains `://`, and a reference that was percent-encoded (the way a DSN password is
    /// written) keeps its colons encoded, so it does not trip this.
    fn has_nested_scheme(&self) -> bool {
        self.host.contains("://")
    }
}

/// Splits a DSN. `None` when the text has no `://` at all (a bare `x`, which the tests and the
/// `mysql_async` URL parser both accept).
fn dsn_parts(dsn: &str) -> Option<DsnParts<'_>> {
    let at = dsn.find("://")?;
    let head = &dsn[..at + 3];
    let rest = &dsn[at + 3..];
    // The query and the fragment end the authority; a percent-encoded secret reference may hold
    // any other character, including `/`, `@` and `#`.
    let end = rest.find(['?', '#']).unwrap_or(rest.len());
    let region = &rest[..end];
    // The userinfo runs up to the LAST `@` (what a URL parser does with the encoded `@` of a
    // password); an unencoded reference that contains `#` never reaches here and is refused.
    let (userinfo, host) = match region.rfind('@') {
        Some(i) => (&region[..i], &region[i + 1..]),
        None => ("", region),
    };
    let (user, password) = match userinfo.split_once(':') {
        Some((u, p)) => (u, Some(p)),
        None => (userinfo, None),
    };
    Some(DsnParts {
        head,
        user,
        password,
        host,
        query: &rest[end..],
    })
}

/// The host of a DSN with the port and the brackets gone, percent-decoded (`None` when there is
/// no `://`). An empty host means the DSN carries no host at all.
fn dsn_host(dsn: &str) -> Option<(String, String)> {
    let p = dsn_parts(dsn)?;
    let host = p.host.split(['/', '?', '#']).next().unwrap_or(p.host);
    let host = match host.split_once(']') {
        // `[::1]:3306`
        Some((bracketed, _)) => bracketed.trim_start_matches('[').to_string(),
        None => host
            .rsplit_once(':')
            .map(|(h, _)| h)
            .unwrap_or(host)
            .to_string(),
    };
    Some((crate::secrets::percent_decode(&host), p.query.to_string()))
}

/// True when `host` needs no TLS because it cannot be seen on the wire: a loopback address or
/// name (the engine and the server are the same machine).
fn is_loopback(host: &str) -> bool {
    let h = host.trim_end_matches('.').to_ascii_lowercase();
    if h == "localhost" || h == "ip6-localhost" || h == "ip6-loopback" {
        return true;
    }
    match h.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => false,
    }
}

/// True when the endpoint is a unix socket rather than a TCP connection: the host is a socket
/// path, or a `socket=` parameter names one (`prefer_socket` alone is not enough: mysql_async
/// prefers a socket on loopback anyway).
fn is_unix_socket(host: &str, query: &str) -> bool {
    if host.starts_with('/') {
        return true;
    }
    query_params(query).any(|(k, v)| {
        k.eq_ignore_ascii_case("socket")
            && !v.is_empty()
            && !["none", "default", "false", "0", "off"].contains(&v.to_ascii_lowercase().as_str())
    })
}

/// The `?a=1&b=2` (or `#a=1`) of a DSN as key/value pairs. The leading delimiter is dropped — the
/// slice a DSN carries starts with it, and `?require_ssl` is not the parameter `require_ssl`.
fn query_params(query: &str) -> impl Iterator<Item = (&str, &str)> + '_ {
    query
        .trim_start_matches(['?', '#'])
        .split('&')
        .map(|kv| kv.split_once('=').unwrap_or((kv, "")))
}

/// True when the DSN's own parameters put TLS on: `require_ssl=true` (MySQL family, what
/// [`finish_dsn`] writes) or an `sslmode` that verifies (`postgres`, what [`finish_pg_dsn`]
/// writes). Anything else — no parameter, `sslmode=prefer` (the tokio-postgres default),
/// `require_ssl=false` — crosses the network in the clear.
fn tls_enabled(dsn: &str) -> bool {
    let Some(p) = dsn_parts(dsn) else {
        return false;
    };
    query_params(p.query).any(|(k, v)| {
        let k = k.to_ascii_lowercase();
        let v = v.to_ascii_lowercase();
        (k == "require_ssl" && ["true", "1", "on", "yes"].contains(&v.as_str()))
            || (k == "sslmode"
                && ["require", "verify-ca", "verify-ca-cert", "verify-full"].contains(&v.as_str()))
    })
}

/// The host to name in a cleartext warning, or `None` when the endpoint needs no warning: TLS on,
/// or the server is on this host (loopback / unix socket).
fn cleartext_host(dsn: &str) -> Option<String> {
    if tls_enabled(dsn) {
        return None;
    }
    let (host, query) = dsn_host(dsn)?;
    if host.is_empty() || is_loopback(&host) || is_unix_socket(&host, &query) {
        return None;
    }
    Some(host)
}

/// Resolves a secret reference in the password field of a DSN
/// (`mysql://lv:file:///run/secrets/pw@h:3306/app`). The field is percent-decoded first, so the
/// reference survives the way every other character in a DSN does; the resolved value goes back
/// URL-encoded. A literal password is returned untouched, and a reference that was written
/// without percent-encoding (so that its `#` or its second `://` swallowed the host) is refused
/// instead of being used as a password.
fn resolve_dsn_password(dsn: &str, var: &str) -> Result<String> {
    let Some(p) = dsn_parts(dsn) else {
        return Ok(dsn.to_string());
    };
    let Some(raw) = p.password else {
        if p.has_nested_scheme() {
            bail!(cleartext_reference_hint(dsn, var));
        }
        return Ok(dsn.to_string());
    };
    let reference = crate::secrets::percent_decode(raw);
    match crate::secrets::scheme_of(&reference) {
        crate::secrets::Scheme::Literal(_) => {
            if p.has_nested_scheme() {
                bail!(cleartext_reference_hint(dsn, var));
            }
            Ok(dsn.to_string())
        }
        _ => {
            let secret = crate::secrets::resolve_secret(&reference)
                .with_context(|| format!("{var} password"))?;
            Ok(format!(
                "{}{}:{}@{}{}",
                p.head,
                p.user,
                encode(secret.expose()),
                p.host,
                p.query
            ))
        }
    }
}

fn cleartext_reference_hint(dsn: &str, var: &str) -> String {
    format!(
        "{var}: {dsn} has a secret reference in its password that is not percent-encoded, so its \
         `://` or `#` cannot be told apart from the host; write it as \
         {var}=…:file%3A%2F%2F… / …:aws-secret%3A%2F%2F… (or put the password in \
         <PREFIX>_PASSWORD / <PREFIX>_PASSWORD_FILE, which needs no encoding)"
    )
}

/// Accepts `mariadb://` as an alias and appends TLS parameters for `<PREFIX>_TLS`. With
/// `allow_pg` a `postgres://` / `postgresql://` DSN is a target's: the MySQL rewrites below
/// skip it and [`finish_pg_dsn`] maps the TLS modes instead.
fn finish_dsn(
    dsn: &str,
    tls: Option<&str>,
    prefix: &str,
    allow_pg: bool,
    ca: Option<&str>,
) -> Result<String> {
    // The mariadb:// rewrite and the TLS parameter block below are MySQL-only: a PG DSN
    // (targets only) goes to finish_pg_dsn, which maps the TLS modes and the CA file; the DSN
    // itself is validated once at the real boundary, target::opts.
    if allow_pg && dsn.starts_with("postgres") {
        return finish_pg_dsn(dsn, tls, prefix, ca);
    }
    let mut d = match dsn.strip_prefix("mariadb://") {
        Some(rest) => format!("mysql://{rest}"),
        None => dsn.to_string(),
    };
    let params = match tls.map(|t| t.trim().to_ascii_lowercase()) {
        None => None,
        Some(t) => match t.as_str() {
            "disabled" | "off" | "false" => None,
            "required" | "require" | "on" | "true" => {
                Some("require_ssl=true&verify_ca=false&verify_identity=false")
            }
            "verify-ca" | "verify_ca" => {
                Some("require_ssl=true&verify_ca=true&verify_identity=false")
            }
            "verify-full" | "verify_identity" | "verify-identity" => {
                Some("require_ssl=true&verify_ca=true&verify_identity=true")
            }
            other => {
                bail!("{prefix}_TLS must be disabled|required|verify-ca|verify-full, got {other}")
            }
        },
    };
    if let Some(p) = params {
        d.push(if d.contains('?') { '&' } else { '?' });
        d.push_str(p);
    }
    Ok(d)
}

/// Maps `<PREFIX>_TLS` onto `sslmode` for a `postgres://` target DSN (D11) and appends
/// `<PREFIX>_TLS_CA` as `sslrootcert` (`PgConn::connect` reads and strips it: tokio-postgres
/// has no libpq).
fn finish_pg_dsn(dsn: &str, tls: Option<&str>, prefix: &str, ca: Option<&str>) -> Result<String> {
    let mut d = dsn.to_string();
    let sslmode = match tls.map(|t| t.trim().to_ascii_lowercase()) {
        None => None,
        Some(t) => match t.as_str() {
            "disabled" | "off" | "false" => Some("disable"),
            "required" | "require" | "on" | "true" => Some("require"),
            // tokio-postgres has no verify-ca (certificate without the host-name check).
            "verify-ca" | "verify_ca" => bail!(
                "{prefix}_TLS verify-ca is not supported for a PostgreSQL target (use verify-full)"
            ),
            "verify-full" | "verify_identity" | "verify-identity" => Some("verify-full"),
            other => bail!("{prefix}_TLS must be disabled|required|verify-full, got {other}"),
        },
    };
    if let Some(m) = sslmode {
        d.push(if d.contains('?') { '&' } else { '?' });
        d.push_str("sslmode=");
        d.push_str(m);
    }
    if let Some(ca) = ca {
        d.push(if d.contains('?') { '&' } else { '?' });
        d.push_str("sslrootcert=");
        d.push_str(&encode(ca));
    }
    Ok(d)
}

fn assemble(
    part: &impl Fn(&str) -> Option<String>,
    password: Option<&crate::secrets::Secret>,
    prefix: &str,
) -> Result<Option<String>> {
    let Some(host) = part("HOST") else {
        return Ok(None);
    };
    let db = part("DATABASE")
        .ok_or_else(|| anyhow!("{prefix}_DATABASE is required (the schema lazyview works in)"))?;
    let user = part("USER").unwrap_or_else(|| "root".into());
    let port: u16 = match part("PORT") {
        Some(p) => p
            .trim()
            .parse()
            .with_context(|| format!("{prefix}_PORT must be a port number"))?,
        None => 3306,
    };
    let auth = match password {
        Some(pw) => format!("{}:{}", encode(&user), encode(pw.expose())),
        None => encode(&user),
    };
    let host = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host
    };
    Ok(Some(format!(
        "mysql://{auth}@{host}:{port}/{}",
        encode(&db)
    )))
}

/// Percent-encode everything a password cannot carry raw, so a secret resolved from a file or a
/// secret store cannot change where the DSN points. The rule lives with the resolver
/// ([`crate::secrets::percent_encode`]), which is what makes a value round-trip out of a DSN and
/// back into one.
fn encode(s: &str) -> String {
    crate::secrets::percent_encode(s)
}

/// Random replica server id in [2^30, 2^31), away from small hand-assigned ids.
pub fn random_server_id() -> u32 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.write_u32(std::process::id());
    (1 << 30) | (h.finish() as u32 & ((1 << 30) - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::ErrorChain;
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| m.get(k).cloned())
    }

    #[test]
    fn plan_budget() {
        let c = cfg(&[("LAZYVIEW_DSN", "x")]).unwrap();
        assert_eq!(c.plan_timeout, Duration::from_secs(10));
        assert_eq!(c.test_plan_delay, None);
        assert_eq!(c.lock_wait_s, DEFAULT_LOCK_WAIT_S);
        assert_eq!(
            (
                c.test_build_delay.clone(),
                c.test_build_hold_bytes,
                c.test_lag_hold_bytes
            ),
            (None, None, None)
        );
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_PLAN_TIMEOUT_MS", "1500"),
            ("LAZYVIEW_TEST_PLAN_DELAY", "/*slow:x*/:250"),
            ("LAZYVIEW_TEST_BUILD_DELAY", "/*long_build*/:20000"),
            ("LAZYVIEW_TEST_BUILD_HOLD_BYTES", "1700000"),
            ("LAZYVIEW_TEST_LAG_HOLD_BYTES", "32"),
            ("LAZYVIEW_TEST_LOCK_WAIT_S", "2"),
        ])
        .unwrap();
        assert_eq!(c.plan_timeout, Duration::from_millis(1500));
        assert_eq!(c.lock_wait_s, 2);
        assert_eq!(
            c.test_plan_delay,
            Some(("/*slow:x*/".to_string(), Duration::from_millis(250)))
        );
        assert_eq!(
            c.test_build_delay,
            Some(("/*long_build*/".to_string(), Duration::from_millis(20_000)))
        );
        assert_eq!(c.test_build_hold_bytes, Some(1_700_000));
        assert_eq!(c.test_lag_hold_bytes, Some(32));
        for bad in ["250", ":250", "m:x"] {
            assert!(
                cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_TEST_PLAN_DELAY", bad)]).is_err(),
                "{bad}"
            );
            assert!(
                cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_TEST_BUILD_DELAY", bad)]).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn dsn_scheme_is_accepted_for_targets_only() {
        // postgres:// and postgresql:// (libpq's own alias, D12) are target DSNs, verbatim.
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "pg,pg2"),
            ("LAZYVIEW_TARGET_PG_DSN", "postgres://a@b/d"),
            ("LAZYVIEW_TARGET_PG2_DSN", "postgresql://a@b/d"),
        ])
        .unwrap();
        assert_eq!(
            c.targets.get("pg").map(String::as_str),
            Some("postgres://a@b/d")
        );
        assert_eq!(
            c.targets.get("pg2").map(String::as_str),
            Some("postgresql://a@b/d")
        );
        // The source stays MySQL-family: a postgres:// LAZYVIEW_DSN is refused by
        // dialect::Family::from_source_dsn with the "not supported yet" message (the
        // enabling PR flips this to acceptance), and binlog::replica_opts stays the
        // MySQL-only DSN parser.
        let e = cfg(&[("LAZYVIEW_DSN", "postgres://a@b/d")]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "PostgreSQL sources are not supported yet (postgres:// LAZYVIEW_DSN)"
        );
        assert!(crate::binlog::replica_opts("postgres://a@b/d").is_err());
    }

    #[test]
    fn source_dsn_scheme_sets_the_family() {
        // The DSN's scheme chooses the family once, stored on Config and read at connect.
        for dsn in ["mysql://u@h:3306/db", "mariadb://u@h/db", "x"] {
            let c = cfg(&[("LAZYVIEW_DSN", dsn)]).unwrap();
            assert_eq!(c.source_family, crate::dialect::Family::Mysql, "{dsn}");
        }
    }

    #[test]
    fn postgres_source_dsn_is_rejected_with_the_message() {
        for dsn in [
            "postgres://a@b/d",
            "postgresql://a@b/d",
            "PostgreSQL://a@b/d",
        ] {
            let e = cfg(&[("LAZYVIEW_DSN", dsn)]).unwrap_err();
            assert_eq!(
                e.to_string(),
                "PostgreSQL sources are not supported yet (postgres:// LAZYVIEW_DSN)",
                "{dsn}"
            );
        }
    }

    #[test]
    fn role_source_dsns_default_to_the_service_dsn() {
        // Unset ⇒ every role stays on LAZYVIEW_DSN: the isolation of the network tests costs
        // nothing when the hooks are absent, and no second endpoint is ever opened.
        let c = cfg(&[("LAZYVIEW_DSN", "mysql://lv:p@h:3306/db")]).unwrap();
        assert_eq!(c.test_binlog_dsn.as_deref(), Some("mysql://lv:p@h:3306/db"));
        assert_eq!(c.test_query_dsn.as_deref(), Some("mysql://lv:p@h:3306/db"));
        assert_eq!(c.test_meta_dsn.as_deref(), Some("mysql://lv:p@h:3306/db"));
        // Set ⇒ one endpoint per role, read exactly the way LAZYVIEW_DSN is read: `mariadb://`
        // is the alias there, `<PREFIX>_TLS` that hook's TLS mode, the parts an alternative.
        let c = cfg(&[
            ("LAZYVIEW_DSN", "mysql://lv:p@h:3306/db"),
            (
                "LAZYVIEW_TEST_BINLOG_DSN",
                "mariadb://lv:p@127.0.0.1:3307/db",
            ),
            ("LAZYVIEW_TEST_QUERY_DSN", "mysql://lv:p@127.0.0.1:3308/db"),
            ("LAZYVIEW_TEST_QUERY_TLS", "required"),
            ("LAZYVIEW_TEST_META_HOST", "127.0.0.1"),
            ("LAZYVIEW_TEST_META_PORT", "3309"),
            ("LAZYVIEW_TEST_META_USER", "lv"),
            ("LAZYVIEW_TEST_META_PASSWORD", "p"),
            ("LAZYVIEW_TEST_META_DATABASE", "db"),
        ])
        .unwrap();
        assert_eq!(
            c.test_binlog_dsn.as_deref(),
            Some("mysql://lv:p@127.0.0.1:3307/db"),
            "mariadb:// is the alias here too"
        );
        assert_eq!(
            c.test_query_dsn.as_deref(),
            Some("mysql://lv:p@127.0.0.1:3308/db?require_ssl=true&verify_ca=false&verify_identity=false"),
            "LAZYVIEW_TEST_QUERY_TLS is that role's TLS mode"
        );
        assert_eq!(
            c.test_meta_dsn.as_deref(),
            Some("mysql://lv:p@127.0.0.1:3309/db"),
            "the parts assemble the same way"
        );
        assert_eq!(c.dsn, "mysql://lv:p@h:3306/db", "the others stay put");
        // A role's DSN is a source DSN: postgres:// is refused with LAZYVIEW_DSN's own message.
        for k in [
            "LAZYVIEW_TEST_BINLOG_DSN",
            "LAZYVIEW_TEST_QUERY_DSN",
            "LAZYVIEW_TEST_META_DSN",
        ] {
            let e = cfg(&[("LAZYVIEW_DSN", "x"), (k, "postgres://a@b/d")]).unwrap_err();
            assert_eq!(
                e.to_string(),
                "PostgreSQL sources are not supported yet (postgres:// LAZYVIEW_DSN)",
                "{k}"
            );
        }
    }

    #[test]
    fn pg_tls_modes_map_to_sslmode() {
        let dsn = |tls: Option<&str>, ca: Option<&str>| {
            let mut env: Vec<(&str, &str)> = vec![
                ("LAZYVIEW_DSN", "x"),
                ("LAZYVIEW_TARGETS", "b"),
                ("LAZYVIEW_TARGET_B_DSN", "postgres://a@b/d"),
            ];
            if let Some(t) = tls {
                env.push(("LAZYVIEW_TARGET_B_TLS", t));
            }
            if let Some(c) = ca {
                env.push(("LAZYVIEW_TARGET_B_TLS_CA", c));
            }
            cfg(&env).unwrap().targets.get("b").cloned().unwrap()
        };
        assert!(dsn(Some("disabled"), None).ends_with("sslmode=disable"));
        assert!(dsn(Some("required"), None).ends_with("sslmode=require"));
        assert!(dsn(Some("verify-full"), None).ends_with("sslmode=verify-full"));
        // A CA file without an explicit mode means full verification, and maps onto
        // sslrootcert (D11); tokio-postgres has no verify-ca.
        let with_ca = dsn(None, Some("/opt/certs/ca.pem"));
        assert!(with_ca.contains("sslmode=verify-full"), "{with_ca}");
        assert!(with_ca.contains("sslrootcert="), "{with_ca}");
        let e = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "b"),
            ("LAZYVIEW_TARGET_B_DSN", "postgres://a@b/d"),
            ("LAZYVIEW_TARGET_B_TLS", "verify-ca"),
        ])
        .expect_err("verify-ca must be rejected for a PostgreSQL target");
        assert!(
            e.to_string()
                .contains("verify-ca is not supported for a PostgreSQL target"),
            "unexpected error: {e}"
        );
        // The MySQL branch is untouched: a MySQL target DSN still gets tls_ca=.
        let m = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "b"),
            ("LAZYVIEW_TARGET_B_DSN", "mysql://a@b/d"),
            ("LAZYVIEW_TARGET_B_TLS_CA", "/opt/certs/ca.pem"),
        ])
        .unwrap()
        .targets
        .get("b")
        .cloned()
        .unwrap();
        assert!(m.contains("tls_ca="), "{m}");
    }

    #[test]
    fn defaults_and_overrides() {
        assert!(cfg(&[]).is_err());
        let c = cfg(&[("LAZYVIEW_DSN", "mysql://a@b/c")]).unwrap();
        assert_eq!(c.start_mode, StartMode::Resume);
        assert_eq!(c.batch_window, Duration::from_millis(200));
        assert!(c.sync && c.drop_removed);
        assert!(c.server_id >= 1 << 30 && !c.server_id_fixed);
        // loopback by default: an endpoint that describes the workload is not exposed by default
        assert_eq!(c.health_addr, Some(([127, 0, 0, 1], 8080).into()));
        assert_eq!((c.write_batch_rows, c.write_batch_bytes), (500, 4 << 20));
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_START_MODE", "REBUILD"),
            ("LAZYVIEW_SYNC", "false"),
            ("LAZYVIEW_BATCH_WINDOW_MS", "0"),
            ("LAZYVIEW_SERVER_ID", "77"),
        ])
        .unwrap();
        assert_eq!(c.start_mode, StartMode::Rebuild);
        assert!(!c.sync);
        assert_eq!(c.batch_window, Duration::ZERO);
        assert_eq!(c.server_id, 77);
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_START_MODE", "bogus")]).is_err());
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SYNC", "maybe")]).is_err());
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SERVER_ID", "0")]).is_err());
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "analytics, re-port"),
            ("LAZYVIEW_TARGET_ANALYTICS_DSN", "mysql://a"),
            ("LAZYVIEW_TARGET_RE_PORT_DSN", "mysql://b"),
        ])
        .unwrap();
        assert_eq!(
            c.targets.get("re-port").map(String::as_str),
            Some("mysql://b")
        );
        // A PostgreSQL target DSN is kept verbatim (T1: src/target/postgres.rs landed).
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "b"),
            ("LAZYVIEW_TARGET_B_DSN", "postgres://lv:lv@h:5432/lvtest"),
        ])
        .unwrap();
        assert_eq!(
            c.targets.get("b").map(String::as_str),
            Some("postgres://lv:lv@h:5432/lvtest")
        );
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_TARGETS", "nope")]).is_err());
        let c = cfg(&[
            ("LAZYVIEW_HOST", "db"),
            ("LAZYVIEW_USER", "lv"),
            ("LAZYVIEW_PASSWORD", "p@ss:w/rd#%"),
            ("LAZYVIEW_DATABASE", "app"),
        ])
        .unwrap();
        assert_eq!(c.dsn, "mysql://lv:p%40ss%3Aw%2Frd%23%25@db:3306/app");
        let o = mysql_async::Opts::from_url(&c.dsn).unwrap();
        assert_eq!(o.pass(), Some("p@ss:w/rd#%"));
        assert!(
            cfg(&[("LAZYVIEW_HOST", "db")]).is_err(),
            "database required"
        );
        let c = cfg(&[
            ("LAZYVIEW_DSN", "mariadb://u:p@h:3307/d"),
            ("LAZYVIEW_TLS", "required"),
            ("LAZYVIEW_HEALTH_ADDR", "off"),
        ])
        .unwrap();
        assert_eq!(
            c.dsn,
            "mysql://u:p@h:3307/d?require_ssl=true&verify_ca=false&verify_identity=false"
        );
        assert!(mysql_async::Opts::from_url(&c.dsn).is_ok());
        assert_eq!(c.health_addr, None);
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_TLS", "maybe")]).is_err());
        let c = cfg(&[
            ("LAZYVIEW_DSN", "mysql://u@h/d"),
            ("LAZYVIEW_TLS_CA", "/etc/ca dir/rds.pem"),
        ])
        .unwrap();
        assert_eq!(
            c.dsn,
            "mysql://u@h/d?require_ssl=true&verify_ca=true&verify_identity=true&tls_ca=%2Fetc%2Fca%20dir%2Frds.pem"
        );
    }

    #[test]
    fn supervisor_settings() {
        let c = cfg(&[("LAZYVIEW_DSN", "mysql://a@b/c")]).unwrap();
        let s = &c.supervisor;
        assert_eq!(s.mode, SupervisorMode::Adaptive);
        assert_eq!((s.max_source, s.max_target), (4, 2));
        assert_eq!(s.max_inflight_rows, 200_000);
        assert!((1..=4).contains(&s.cpu_workers));
        assert_eq!(s.max_for_target("source"), 2);
        let c = cfg(&[
            ("LAZYVIEW_DSN", "mysql://a@b/c"),
            ("LAZYVIEW_SUPERVISOR", "Fixed"),
            ("LAZYVIEW_MAX_SOURCE_CONCURRENCY", "0"),
            ("LAZYVIEW_MAX_TARGET_CONCURRENCY", "6"),
            ("LAZYVIEW_TARGETS", "an-x"),
            ("LAZYVIEW_TARGET_AN_X_DSN", "mysql://x@y/z"),
            ("LAZYVIEW_TARGET_AN_X_MAX_CONCURRENCY", "2"),
            ("LAZYVIEW_TARGET_SOURCE_MAX_CONCURRENCY", "1"),
            ("LAZYVIEW_MAX_INFLIGHT_ROWS", "5000"),
            ("LAZYVIEW_CPU_WORKERS", "3"),
        ])
        .unwrap();
        let s = &c.supervisor;
        assert_eq!(s.mode, SupervisorMode::Fixed);
        assert_eq!(s.max_source, 1, "at least one connection");
        assert_eq!(s.max_for_target("an-x"), 2);
        assert_eq!(s.max_for_target("source"), 1);
        assert_eq!(s.max_for_target("other"), 6);
        assert_eq!((s.max_inflight_rows, s.cpu_workers), (5000, 3));
        let c = cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SUPERVISOR", "off")]).unwrap();
        assert_eq!(c.supervisor.mode, SupervisorMode::Off);
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SUPERVISOR", "fast")]).is_err());
    }

    /// The password of a DSN as the driver would read it.
    fn dsn_password(dsn: &str) -> Option<String> {
        mysql_async::Opts::from_url(dsn)
            .unwrap()
            .pass()
            .map(|p| p.to_string())
    }

    fn source_pairs<'a>(extra: &'a [(&'a str, &'a str)]) -> Vec<(&'a str, &'a str)> {
        let mut v = vec![
            ("LAZYVIEW_HOST", "db.example.com"),
            ("LAZYVIEW_USER", "lv"),
            ("LAZYVIEW_DATABASE", "app"),
        ];
        v.extend_from_slice(extra);
        v
    }

    #[test]
    fn health_addr_defaults_to_loopback() {
        let c = cfg(&[("LAZYVIEW_DSN", "x")]).unwrap();
        assert_eq!(
            c.health_addr,
            Some(([127, 0, 0, 1], 8080).into()),
            "a metrics endpoint that describes the workload is not exposed by default"
        );
        // an explicit address still binds anything: a container serves the pod network on request
        let cases: &[(&str, Option<std::net::SocketAddr>)] = &[
            ("0.0.0.0:8080", Some(([0, 0, 0, 0], 8080).into())),
            ("10.0.0.5:9090", Some(([10, 0, 0, 5], 9090).into())),
            (" 127.0.0.1:1 ", Some(([127, 0, 0, 1], 1).into())),
            ("off", None),
            ("OFF", None),
            ("", None),
        ];
        for (v, want) in cases {
            assert_eq!(
                cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_HEALTH_ADDR", v)])
                    .unwrap()
                    .health_addr,
                *want,
                "{v}"
            );
        }
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_HEALTH_ADDR", "nope")]).is_err());
    }

    #[test]
    fn passwords_come_from_files_and_references() {
        use crate::secrets::tests::TempDir;
        let d = TempDir::new("config-password");
        let pw = d.write("password", "file-pw\n");
        let deep = d.write("deep", &format!("file://{pw}\n"));
        let plain_deep = d.write("plain-deep", "file://nope\n");

        // the historical behavior: the variable's value, verbatim
        let c = cfg(&source_pairs(&[("LAZYVIEW_PASSWORD", "p@ss w/rd")])).unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("p@ss w/rd"));
        // _FILE: the file's content, one trailing newline trimmed
        let c = cfg(&source_pairs(&[("LAZYVIEW_PASSWORD_FILE", &pw)])).unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("file-pw"));
        // a value that is a file:// reference
        let c = cfg(&source_pairs(&[(
            "LAZYVIEW_PASSWORD",
            &format!("file://{pw}"),
        )]))
        .unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("file-pw"));
        // double indirection: the _FILE holds a reference
        let c = cfg(&source_pairs(&[("LAZYVIEW_PASSWORD_FILE", &deep)])).unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("file-pw"));
        // plain:// forces a literal that looks like a reference
        let c = cfg(&source_pairs(&[(
            "LAZYVIEW_PASSWORD",
            &format!("plain://{plain_deep}"),
        )]))
        .unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some(plain_deep.as_str()));

        // both set is a configuration error, never a precedence rule
        let e = cfg(&source_pairs(&[
            ("LAZYVIEW_PASSWORD", "from-var"),
            ("LAZYVIEW_PASSWORD_FILE", &pw),
        ]))
        .unwrap_err()
        .full();
        assert!(
            e.contains("LAZYVIEW_PASSWORD and LAZYVIEW_PASSWORD_FILE are both set"),
            "{e}"
        );
        // an unknown scheme names the known ones
        let e = cfg(&source_pairs(&[("LAZYVIEW_PASSWORD", "vault://app/pw")]))
            .unwrap_err()
            .full();
        assert!(
            e.contains("unknown secret scheme vault://")
                && e.contains("file:///abs/path")
                && e.contains("aws-secret://<secret-id>[#<json-field>]"),
            "{e}"
        );
        // aws-secret:// in a build without the feature fails loudly (never the URI as a password).
        // Only that build: with the feature on, the same reference would reach the SDK and the
        // default credential chain, and a test must not call AWS. The feature-on path is covered
        // by the mock client in `src/secrets/tests.rs`.
        #[cfg(not(feature = "aws-secrets"))]
        {
            let e = cfg(&source_pairs(&[(
                "LAZYVIEW_PASSWORD",
                "aws-secret://prod/lazyview",
            )]))
            .unwrap_err()
            .full();
            assert!(e.contains("built without the aws-secrets feature"), "{e}");
            // and the same through the same DSN: no silent fallback
            let e = cfg(&[(
                "LAZYVIEW_DSN",
                "mysql://lv:aws-secret://prod/lazyview@h:3306/db",
            )])
            .unwrap_err()
            .full();
            assert!(e.contains("built without the aws-secrets feature"), "{e}");
        }

        // a target's password goes through the same resolver
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "analytics"),
            ("LAZYVIEW_TARGET_ANALYTICS_HOST", "reports.example.com"),
            ("LAZYVIEW_TARGET_ANALYTICS_DATABASE", "reports"),
            ("LAZYVIEW_TARGET_ANALYTICS_USER", "lv"),
            ("LAZYVIEW_TARGET_ANALYTICS_PASSWORD_FILE", &pw),
        ])
        .unwrap();
        assert_eq!(
            c.targets.get("analytics").map(String::as_str),
            Some("mysql://lv:file-pw@reports.example.com:3306/reports")
        );
    }

    #[test]
    fn a_dsn_password_can_be_a_reference() {
        use crate::secrets::tests::TempDir;
        let d = TempDir::new("config-dsn");
        let pw = d.write("password", "s3cr3t/p@ss\n");

        // written as it is in a URL: percent-encoded, like every other character of a DSN
        let c = cfg(&[(
            "LAZYVIEW_DSN",
            &format!(
                "mysql://lv:{}@h:3306/db",
                crate::secrets::percent_encode(&format!("file://{pw}"))
            ),
        )])
        .unwrap();
        assert_eq!(c.dsn, "mysql://lv:s3cr3t%2Fp%40ss@h:3306/db");
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("s3cr3t/p@ss"));

        // written raw: the `/` of the reference is still separable from the host
        let c = cfg(&[("LAZYVIEW_DSN", &format!("mysql://lv:file://{pw}@h:3306/db"))]).unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("s3cr3t/p@ss"));

        // a `#` that was not encoded would swallow the host: refused, not guessed
        let e = cfg(&[(
            "LAZYVIEW_DSN",
            "mysql://lv:aws-secret://prod/lazyview#password@h:3306/db",
        )])
        .unwrap_err()
        .full();
        assert!(
            e.contains("LAZYVIEW_DSN") && e.contains("not percent-encoded"),
            "{e}"
        );
        // the same with no host at all
        let e = cfg(&[("LAZYVIEW_DSN", "mysql://lv:file:///etc/lazyview/pw")])
            .unwrap_err()
            .full();
        assert!(e.contains("not percent-encoded"), "{e}");

        // a literal password that merely contains `://` keeps working through plain://
        let c = cfg(&[(
            "LAZYVIEW_DSN",
            "mysql://lv:plain%3A%2F%2Fhe%3A%2F%2Fvalue@h:3306/db",
        )])
        .unwrap();
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("he://value"));
        // …and an ordinary encoded password is untouched
        let c = cfg(&[("LAZYVIEW_DSN", "mysql://lv:p%40ss%3Aw%2Frd@h:3306/db")]).unwrap();
        assert_eq!(c.dsn, "mysql://lv:p%40ss%3Aw%2Frd@h:3306/db");
        assert_eq!(dsn_password(&c.dsn).as_deref(), Some("p@ss:w/rd"));

        // a PostgreSQL target's DSN password resolves the same way (the MySQL driver cannot read
        // a postgres:// URL, so this asserts the text the PG driver will get)
        let c = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_TARGETS", "b"),
            (
                "LAZYVIEW_TARGET_B_DSN",
                &format!("postgres://lv:file://{pw}@h:5432/db"),
            ),
        ])
        .unwrap();
        assert_eq!(
            c.targets.get("b").map(String::as_str),
            Some("postgres://lv:s3cr3t%2Fp%40ss@h:5432/db"),
            "resolved, then written back encoded"
        );
    }

    #[test]
    fn health_token_is_a_secret_reference_too() {
        use crate::secrets::tests::TempDir;
        let d = TempDir::new("config-token");
        let f = d.write("token", "tok-from-file\n");
        let c = cfg(&[("LAZYVIEW_DSN", "x")]).unwrap();
        assert_eq!(c.health_token, None, "unset means an open metrics endpoint");
        let c = cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_HEALTH_TOKEN", "tok")]).unwrap();
        assert_eq!(
            c.health_token,
            Some(crate::secrets::resolve_secret("tok").unwrap())
        );
        assert_eq!(
            format!("{:?}", c.health_token.as_ref().unwrap()),
            "Secret(***)"
        );
        let c = cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_HEALTH_TOKEN_FILE", &f)]).unwrap();
        assert_eq!(
            c.health_token,
            Some(crate::secrets::resolve_secret("tok-from-file").unwrap())
        );
        let e = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_HEALTH_TOKEN", "tok"),
            ("LAZYVIEW_HEALTH_TOKEN_FILE", &f),
        ])
        .unwrap_err()
        .full();
        assert!(
            e.contains("LAZYVIEW_HEALTH_TOKEN and LAZYVIEW_HEALTH_TOKEN_FILE are both set"),
            "{e}"
        );
        // an unreadable token is a startup error, never a quietly open endpoint
        let e = cfg(&[
            ("LAZYVIEW_DSN", "x"),
            ("LAZYVIEW_HEALTH_TOKEN_FILE", "/nonexistent/lazyview-token"),
        ])
        .unwrap_err()
        .full();
        assert!(e.contains("LAZYVIEW_HEALTH_TOKEN_FILE"), "{e}");
    }

    #[test]
    fn cleartext_endpoints_are_listed_for_the_warning() {
        let w = |pairs: &[(&str, &str)]| cfg(pairs).unwrap().cleartext;
        // A fn rather than a closure: the pairs are all 'static and inference should not tie
        // them to the lifetime of the `tls` argument.
        fn src(tls: Option<&'static str>) -> Vec<(&'static str, &'static str)> {
            let mut v = vec![("LAZYVIEW_DSN", "mysql://lv:p@db.example.com:3306/app")];
            if let Some(t) = tls {
                v.push(("LAZYVIEW_TLS", t));
            }
            v
        }
        // the source, TLS off (the default): one line naming LAZYVIEW_TLS
        assert_eq!(
            w(&src(None)),
            vec![CleartextEndpoint {
                endpoint: "source".into(),
                var: "LAZYVIEW_TLS".into(),
                host: "db.example.com".into(),
            }]
        );
        // every TLS mode silences it
        for tls in ["required", "verify-ca", "verify-full", "REQUIRED", "on"] {
            assert!(w(&src(Some(tls))).is_empty(), "{tls}");
        }
        // explicit off, and the other words for off, warn again (`no` is not one of them: the
        // parser's set is disabled|off|false, and a typo is an error rather than a silent off)
        for tls in ["disabled", "off", "false", "OFF"] {
            assert_eq!(w(&src(Some(tls))).len(), 1, "{tls}");
        }
        // loopback and a unix socket are not on the wire
        assert!(w(&[("LAZYVIEW_DSN", "mysql://lv:p@localhost/app")]).is_empty());
        assert!(w(&[("LAZYVIEW_DSN", "mysql://lv:p@127.0.0.1:3306/app")]).is_empty());
        assert!(w(&[("LAZYVIEW_DSN", "mysql://lv:p@[::1]:3306/app")]).is_empty());
        assert!(w(&[(
            "LAZYVIEW_DSN",
            "mysql://lv:p@localhost/app?socket=/run/mysqld/mysqld.sock"
        )])
        .is_empty());
        assert!(w(&[("LAZYVIEW_DSN", "mysql://lv:p@/run/mysqld/mysqld.sock/app")]).is_empty());
        // a non-loopback address with only prefer_socket still warns
        assert_eq!(
            w(&[(
                "LAZYVIEW_DSN",
                "mysql://lv:p@10.0.0.2/app?prefer_socket=true"
            )])
            .len(),
            1
        );
        // a DSN without TLS parameters that carries require_ssl itself is on
        assert!(w(&[(
            "LAZYVIEW_DSN",
            "mysql://lv:p@db.example.com/app?require_ssl=true&verify_ca=false"
        )])
        .is_empty());
        // the parts form, and the CA file (which implies verify-full)
        assert_eq!(w(&source_pairs(&[("LAZYVIEW_PASSWORD", "p")])).len(), 1);
        assert!(w(&source_pairs(&[
            ("LAZYVIEW_PASSWORD", "p"),
            ("LAZYVIEW_TLS_CA", "/etc/ca.pem")
        ]))
        .is_empty());
        // targets, each naming its own variable, in configuration order
        let got = w(&[
            ("LAZYVIEW_HOST", "127.0.0.1"),
            ("LAZYVIEW_DATABASE", "app"),
            ("LAZYVIEW_TARGETS", "analytics, reporting"),
            ("LAZYVIEW_TARGET_ANALYTICS_HOST", "analytics.example.com"),
            ("LAZYVIEW_TARGET_ANALYTICS_DATABASE", "reports"),
            ("LAZYVIEW_TARGET_REPORTING_HOST", "10.1.2.3"),
            ("LAZYVIEW_TARGET_REPORTING_DATABASE", "d"),
            ("LAZYVIEW_TARGET_REPORTING_TLS", "required"),
        ]);
        assert_eq!(
            got,
            vec![CleartextEndpoint {
                endpoint: "target analytics".into(),
                var: "LAZYVIEW_TARGET_ANALYTICS_TLS".into(),
                host: "analytics.example.com".into(),
            }]
        );
        // a PostgreSQL target follows its sslmode
        let pg = |mode: Option<&str>| {
            let mut v = vec![
                ("LAZYVIEW_DSN", "x"),
                ("LAZYVIEW_TARGETS", "b"),
                (
                    "LAZYVIEW_TARGET_B_DSN",
                    "postgres://lv:p@pg.example.com:5432/d",
                ),
            ];
            if let Some(m) = mode {
                v.push(("LAZYVIEW_TARGET_B_TLS", m));
            }
            w(&v)
        };
        assert_eq!(
            pg(None).len(),
            1,
            "no sslmode means tokio-postgres prefers, then falls back"
        );
        assert!(pg(Some("required")).is_empty());
        assert!(pg(Some("verify-full")).is_empty());
        assert_eq!(pg(Some("disabled")).len(), 1);
        // the test-role hooks are not endpoints the operator configured
        assert_eq!(
            w(&[
                ("LAZYVIEW_DSN", "x"),
                ("LAZYVIEW_TEST_BINLOG_HOST", "elsewhere.example.com"),
                ("LAZYVIEW_TEST_BINLOG_DATABASE", "d"),
            ])
            .len(),
            0
        );
    }
}
