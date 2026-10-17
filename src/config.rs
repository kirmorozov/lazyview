//! Service configuration from environment variables (see README / PLAN.md §2).

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
    /// A view failing this long stops holding back the saved binlog position.
    pub stale_after: Duration,
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

impl Default for SupervisorConfig {
    fn default() -> Self {
        SupervisorConfig {
            mode: SupervisorMode::Adaptive,
            max_source: 8,
            max_target: 4,
            max_target_override: BTreeMap::new(),
            max_inflight_rows: 200_000,
            cpu_workers: default_cpu_workers(),
            stale_after: Duration::from_millis(300_000),
        }
    }
}

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
    /// Named target instances for materialized tables (`_lazyview.target`); NULL/absent target
    /// means the source instance.
    pub targets: BTreeMap<String, String>,
    pub start_mode: StartMode,
    pub sync: bool,
    pub batch_window: Duration,
    pub batch_max_keys: usize,
    pub query_chunk: usize,
    pub write_batch_rows: usize,
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
    /// Background builds running at once (each holds 1 + targets connections).
    pub build_concurrency: usize,
    /// Wait before reading sources when no GTID fence is available (binlog events can arrive
    /// before their transaction is visible to readers).
    pub visibility_delay: Duration,
    /// How often the binlog position is persisted when nothing else is written.
    pub position_persist_interval: Duration,
    /// HTTP health/metrics endpoint (`/healthz`, `/readyz`, `/metrics`); None = disabled.
    pub health_addr: Option<std::net::SocketAddr>,
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
        let dsn = connection(&get, "LAZYVIEW")?.ok_or_else(|| {
            anyhow!("no database configured: set LAZYVIEW_HOST, LAZYVIEW_USER, LAZYVIEW_PASSWORD, LAZYVIEW_DATABASE (or LAZYVIEW_DSN=mysql://user:pass@host:3306/db)")
        })?;
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
                let dsn = connection(&get, &prefix)?
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
            max_source: (num("LAZYVIEW_MAX_SOURCE_CONCURRENCY", 8)? as usize).max(1),
            max_target: (num("LAZYVIEW_MAX_TARGET_CONCURRENCY", 4)? as usize).max(1),
            max_target_override,
            max_inflight_rows: (num("LAZYVIEW_MAX_INFLIGHT_ROWS", 200_000)? as usize).max(1),
            cpu_workers: (num("LAZYVIEW_CPU_WORKERS", default_cpu_workers() as u64)? as usize)
                .max(1),
            stale_after: Duration::from_millis(num("LAZYVIEW_STALE_AFTER_MS", 300_000)?),
        };
        Ok(Config {
            dsn,
            targets,
            start_mode,
            sync: flag("LAZYVIEW_SYNC", true)?,
            batch_window: Duration::from_millis(num("LAZYVIEW_BATCH_WINDOW_MS", 200)?),
            batch_max_keys: num("LAZYVIEW_BATCH_MAX_KEYS", 50_000)? as usize,
            query_chunk,
            write_batch_rows: num("LAZYVIEW_WRITE_BATCH_ROWS", 500)?.max(1) as usize,
            server_id,
            instance_id,
            drop_removed: flag("LAZYVIEW_DROP_REMOVED", true)?,
            on_missing_binlog,
            build_concurrency: num("LAZYVIEW_BUILD_CONCURRENCY", 2)?.max(1) as usize,
            full_refresh_interval: Duration::from_millis(num("LAZYVIEW_FULL_REFRESH_MS", 60_000)?),
            target_timeout: Duration::from_millis(
                num("LAZYVIEW_TARGET_TIMEOUT_MS", 60_000)?.max(1_000),
            ),
            heartbeat: Duration::from_millis(num("LAZYVIEW_HEARTBEAT_MS", 5_000)?.max(100)),
            visibility_delay: Duration::from_millis(num("LAZYVIEW_VISIBILITY_DELAY_MS", 20)?),
            position_persist_interval: Duration::from_millis(num(
                "LAZYVIEW_POSITION_PERSIST_MS",
                5_000,
            )?),
            health_addr: match get("LAZYVIEW_HEALTH_ADDR").map(|v| v.trim().to_string()) {
                None => Some(([0, 0, 0, 0], 8080).into()),
                Some(v) if v.is_empty() || v.eq_ignore_ascii_case("off") => None,
                Some(v) => Some(v.parse().with_context(|| {
                    format!("LAZYVIEW_HEALTH_ADDR must be host:port or off, got {v}")
                })?),
            },
            server_id_fixed,
            supervisor,
        })
    }
}

/// `<PREFIX>_DSN`, or a DSN assembled from `<PREFIX>_HOST`, `_PORT`, `_USER`, `_PASSWORD`,
/// `_DATABASE` (values used verbatim: no URL encoding needed by the user).
fn connection(get: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<Option<String>> {
    let part = |k: &str| get(&format!("{prefix}_{k}")).filter(|v| !v.is_empty());
    let ca = part("TLS_CA").or_else(|| get("LAZYVIEW_TLS_CA").filter(|v| !v.is_empty()));
    // A CA file without an explicit mode means full verification (certificate + host name).
    // (verify-ca, i.e. skipping only the host-name check, does not work with mysql_async 0.37 +
    // current rustls: its NotValidForName string match never fires.)
    let tls = part("TLS")
        .or_else(|| get("LAZYVIEW_TLS").filter(|v| !v.is_empty()))
        .or_else(|| ca.as_ref().map(|_| "verify-full".to_string()));
    let dsn = match part("DSN") {
        Some(dsn) => Some(dsn),
        None => assemble(&part, prefix)?,
    };
    dsn.map(|d| {
        let mut d = finish_dsn(&d, tls.as_deref(), prefix)?;
        if let Some(ca) = &ca {
            d.push(if d.contains('?') { '&' } else { '?' });
            d.push_str("tls_ca=");
            d.push_str(&encode(ca));
        }
        Ok(d)
    })
    .transpose()
}

/// Accepts `mariadb://` as an alias and appends TLS parameters for `<PREFIX>_TLS`.
fn finish_dsn(dsn: &str, tls: Option<&str>, prefix: &str) -> Result<String> {
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

fn assemble(part: &impl Fn(&str) -> Option<String>, prefix: &str) -> Result<Option<String>> {
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
    let auth = match part("PASSWORD") {
        Some(pw) => format!("{}:{}", encode(&user), encode(&pw)),
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

fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
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
    use std::collections::HashMap;

    fn cfg(pairs: &[(&str, &str)]) -> Result<Config> {
        let m: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        Config::from_lookup(|k| m.get(k).cloned())
    }

    #[test]
    fn defaults_and_overrides() {
        assert!(cfg(&[]).is_err());
        let c = cfg(&[("LAZYVIEW_DSN", "mysql://a@b/c")]).unwrap();
        assert_eq!(c.start_mode, StartMode::Resume);
        assert_eq!(c.batch_window, Duration::from_millis(200));
        assert!(c.sync && c.drop_removed);
        assert!(c.server_id >= 1 << 30 && !c.server_id_fixed);
        assert_eq!(c.health_addr, Some(([0, 0, 0, 0], 8080).into()));
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
        assert_eq!((s.max_source, s.max_target), (8, 4));
        assert_eq!(s.max_inflight_rows, 200_000);
        assert!((1..=4).contains(&s.cpu_workers));
        assert_eq!(s.stale_after, Duration::from_secs(300));
        assert_eq!(s.max_for_target("source"), 4);
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
            ("LAZYVIEW_STALE_AFTER_MS", "1000"),
        ])
        .unwrap();
        let s = &c.supervisor;
        assert_eq!(s.mode, SupervisorMode::Fixed);
        assert_eq!(s.max_source, 1, "at least one connection");
        assert_eq!(s.max_for_target("an-x"), 2);
        assert_eq!(s.max_for_target("source"), 1);
        assert_eq!(s.max_for_target("other"), 6);
        assert_eq!((s.max_inflight_rows, s.cpu_workers), (5000, 3));
        assert_eq!(s.stale_after, Duration::from_secs(1));
        let c = cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SUPERVISOR", "off")]).unwrap();
        assert_eq!(c.supervisor.mode, SupervisorMode::Off);
        assert!(cfg(&[("LAZYVIEW_DSN", "x"), ("LAZYVIEW_SUPERVISOR", "fast")]).is_err());
    }
}
