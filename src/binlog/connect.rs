use super::*;

use crate::binlog::probe::show;

/// Product name and environment-variable prefix named in this module's messages.
static PRODUCT: OnceLock<(String, String)> = OnceLock::new();

/// Sets the product name and environment-variable prefix this module names in its messages
/// (defaults `lazyview` and `LAZYVIEW_`), for a program that embeds it. Only the first call takes
/// effect; it returns whether this one did. lazyview itself never calls it.
pub fn set_product(name: &str, env_prefix: &str) -> bool {
    PRODUCT
        .set((name.to_string(), env_prefix.to_string()))
        .is_ok()
}

/// `(name, env_prefix)`, see [`set_product`].
pub(super) fn product() -> (&'static str, &'static str) {
    match PRODUCT.get() {
        Some((name, prefix)) => (name, prefix),
        None => ("lazyview", "LAZYVIEW_"),
    }
}

/// Connection options for a DSN, tuned for a replication client.
///
/// The DSN is parsed with `Opts::from_url` (so `require_ssl`, `verify_ca`, `prefer_socket`, …
/// URL parameters keep working). On top of that a TCP keepalive is enabled so a silently dropped
/// connection is detected by the OS as well (the reader also has its own idle timeout derived from
/// the heartbeat period). See the module docs for authentication notes.
pub fn replica_opts(dsn: &str) -> Result<Opts> {
    // `tls_ca` / `ssl-ca` / `ssl_ca` (lazyview extension, not a mysql_async URL parameter):
    // PEM file(s) with CA certificates to trust in addition to the built-in roots (e.g. the
    // AWS RDS / Azure / self-signed CA). Implies TLS with certificate verification.
    let (url, cas) = split_ca_params(dsn);
    let opts = Opts::from_url(&url).map_err(|e| anyhow!("invalid DSN: {e}"))?;
    let keepalive = opts.tcp_keepalive().or(Some(Duration::from_secs(30)));
    let mut b = OptsBuilder::from_opts(opts.clone()).tcp_keepalive(keepalive);
    if !cas.is_empty() {
        for c in &cas {
            if !std::path::Path::new(c).is_file() {
                bail!(
                    "TLS CA file `{c}` not found ({}TLS_CA / tls_ca DSN parameter)",
                    product().1
                );
            }
        }
        let ssl = opts
            .ssl_opts()
            .cloned()
            .unwrap_or_default()
            .with_root_certs(
                cas.into_iter()
                    .map(|c| std::path::PathBuf::from(c).into())
                    .collect(),
            );
        b = b.ssl_opts(Some(ssl));
    }
    Ok(b.into())
}

pub(super) fn split_ca_params(dsn: &str) -> (String, Vec<String>) {
    let Some((base, query)) = dsn.split_once('?') else {
        return (dsn.to_string(), Vec::new());
    };
    let mut keep = Vec::new();
    let mut cas = Vec::new();
    for kv in query.split('&').filter(|kv| !kv.is_empty()) {
        match kv.split_once('=') {
            Some(("tls_ca" | "ssl-ca" | "ssl_ca", v)) => cas.extend(
                percent_decode(v)
                    .split(',')
                    .filter(|p| !p.is_empty())
                    .map(str::to_string),
            ),
            _ => keep.push(kv),
        }
    }
    let url = if keep.is_empty() {
        base.to_string()
    } else {
        format!("{base}?{}", keep.join("&"))
    };
    (url, cas)
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' && i + 2 < b.len() {
            if let Ok(v) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

pub(super) fn value_str(v: Option<&Value>) -> String {
    match v {
        None | Some(Value::NULL) => String::new(),
        Some(Value::Bytes(b)) => String::from_utf8_lossy(b).into_owned(),
        Some(Value::Int(i)) => i.to_string(),
        Some(Value::UInt(u)) => u.to_string(),
        Some(other) => format!("{other:?}"),
    }
}

/// `ER_UNKNOWN_SYSTEM_VARIABLE`: the server's own verdict that a variable does not exist.
const ER_UNKNOWN_SYSTEM_VARIABLE: u16 = 1193;

/// Reads `@@GLOBAL.<var>`.
/// `None` only when the server says the variable does not exist (1193); any other error is fatal.
pub(super) async fn probe_var(conn: &mut Conn, var: &str) -> Result<Option<String>> {
    match conn
        .query_first::<Value, _>(format!("SELECT @@GLOBAL.{var}"))
        .await
    {
        Ok(v) => Ok(Some(value_str(v.as_ref()))),
        Err(e) if probe_var_is_absent(&e) => Ok(None),
        Err(e) => Err(anyhow::Error::from(e).context(format!("reading @@GLOBAL.{var}"))),
    }
}

/// Whether `err` is the "unknown system variable" code and nothing else.
pub(super) fn probe_var_is_absent(err: &mysql_async::Error) -> bool {
    matches!(
        err,
        mysql_async::Error::Server(se) if se.code == ER_UNKNOWN_SYSTEM_VARIABLE
    )
}

/// [`probe_var`] repeated across transient failures: 5 attempts, 200 ms × 2ⁿ capped at 2 s. The
/// `SELECT` is idempotent, so a repeat is a re-read. This file is copy set, so it owns its small
/// loop: no metrics and no logging — the heartbeat is the engine's job, and the engine's startup
/// path already retries the whole [`server_info`] call through `connect_wait`.
pub(crate) async fn probe_var_retry(conn: &mut Conn, var: &str) -> Result<Option<String>> {
    let mut attempt = 1;
    loop {
        match probe_var(conn, var).await {
            Ok(v) => return Ok(v),
            Err(e) if attempt >= 5 => return Err(e),
            Err(_) => {
                let ms = (200u64 << (attempt - 1)).min(2_000);
                tokio::time::sleep(Duration::from_millis(ms)).await;
                attempt += 1;
            }
        }
    }
}

/// Checks `log_bin=ON`, `binlog_format=ROW`, `binlog_row_image=FULL`; returns a clear error
/// listing every violated requirement.
///
/// On MariaDB `log_bin_compress=OFF` is required as well: compressed events (types 165–171)
/// cannot be decoded by this reader. So is a file binlog: with `binlog_storage_engine=innodb`
/// (MariaDB >= 12.3) the server only serves binlog dumps that start from a GTID ("GTID starting
/// position is required on master with --binlog-storage-engine enabled"), and this reader
/// starts and resumes from a file and offset.
pub async fn server_info(conn: &mut Conn) -> Result<ServerInfo> {
    let row: Row = conn
        .query_first(
            "SELECT VERSION(), @@GLOBAL.log_bin, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image",
        )
        .await
        .context("reading server version and binlog settings")?
        .ok_or_else(|| anyhow!("reading server variables returned no row"))?;
    let version = value_str(row.as_ref(0));
    let log_bin = value_str(row.as_ref(1));
    let format = value_str(row.as_ref(2));
    let image = value_str(row.as_ref(3));
    let flavor = Flavor::from_version(&version);
    let mut compress: Option<String> = None;
    let mut storage_engine: Option<String> = None;
    if flavor == Flavor::MariaDb {
        // The MariaDB preflight probes are flavor code: [`crate::source::maria::probes`].
        (compress, storage_engine) = crate::source::maria::probes(conn).await?;
    }
    let mut problems = requirement_problems(
        flavor,
        &log_bin,
        &format,
        &image,
        compress.as_deref().unwrap_or(""),
        storage_engine.as_deref().unwrap_or(""),
    );
    if flavor == Flavor::MySql {
        // Partial JSON row images carry diffs instead of values; lazyview needs full values.
        let opts = probe_var_retry(conn, "binlog_row_value_options").await?;
        if opts
            .unwrap_or_default()
            .to_ascii_uppercase()
            .contains("PARTIAL_JSON")
        {
            problems.push(
                "binlog_row_value_options is PARTIAL_JSON (must be empty); fix: SET PERSIST \
                 binlog_row_value_options='' (Amazon RDS/Aurora: set it in the DB parameter group)"
                    .into(),
            );
        }
    }
    if !problems.is_empty() {
        bail!(unmet_requirements(&version, &problems));
    }
    Ok(ServerInfo { flavor, version })
}

/// The error message of a server that does not meet the binlog requirements (`problems`, see
/// [`requirement_problems`]).
pub(super) fn unmet_requirements(version: &str, problems: &[String]) -> String {
    format!(
        "server {version} does not meet {}'s binlog requirements:\n  - {}",
        product().0,
        problems.join("\n  - ")
    )
}

/// One message per violated requirement, each with the way to fix it on that flavor.
pub(super) fn requirement_problems(
    flavor: Flavor,
    log_bin: &str,
    format: &str,
    image: &str,
    compress: &str,
    storage_engine: &str,
) -> Vec<String> {
    let on = |v: &str| matches!(v.to_ascii_uppercase().as_str(), "1" | "ON");
    let maria = flavor == Flavor::MariaDb;
    // Dynamic variables: set now and make it survive a restart.
    let set = |var: &str, val: &str, opt: &str| {
        if maria {
            format!(
                "SET GLOBAL {var}='{val}' and add {var}={val} to my.cnf [mysqld] \
                 (container: --{opt}={val}; Amazon RDS: set it in the DB parameter group)"
            )
        } else {
            format!(
                "SET PERSIST {var}='{val}' (or {var}={val} in my.cnf [mysqld]; container: \
                 --{opt}={val}; Amazon RDS/Aurora: set {var}={val} in the DB parameter group)"
            )
        }
    };
    let mut problems = Vec::new();
    if !on(log_bin) {
        let fix = if maria {
            "add log_bin=ON (or log_bin=<basename>) to my.cnf [mysqld] or start mariadbd with \
             --log-bin, then restart"
        } else {
            "binary logging is on by default in MySQL 8: remove --skip-log-bin / \
             --disable-log-bin (or start mysqld with --log-bin), then restart"
        };
        problems.push(format!(
            "log_bin is {} (must be ON); fix: {fix}",
            show(log_bin)
        ));
    }
    if !format.eq_ignore_ascii_case("ROW") {
        problems.push(format!(
            "binlog_format is {} (must be ROW); fix: {}; sessions already connected keep \
             the old format until they reconnect",
            show(format),
            set("binlog_format", "ROW", "binlog-format")
        ));
    }
    if !image.eq_ignore_ascii_case("FULL") {
        problems.push(format!(
            "binlog_row_image is {} (must be FULL); fix: {}; sessions already connected keep \
             the old setting until they reconnect",
            show(image),
            set("binlog_row_image", "FULL", "binlog-row-image")
        ));
    }
    if maria && on(compress) {
        problems.push(
            "log_bin_compress is ON (compressed binlog events are not supported); fix: SET \
             GLOBAL log_bin_compress=OFF and remove log_bin_compress from my.cnf (container: \
             --log-bin-compress=OFF); binlogs written while it was ON cannot be read"
                .into(),
        );
    }
    if maria && !storage_engine.is_empty() {
        problems.push(format!(
            "binlog_storage_engine is {storage_engine} (must be unset: a binlog kept in the \
             storage engine is only served to replicas that start from a GTID, {} reads \
             it by file and position); fix: remove binlog_storage_engine from my.cnf and restart",
            product().0
        ));
    }
    problems
}
