//! lazyview service entrypoint. Configuration comes from `LAZYVIEW_*` environment variables.

use std::io::IsTerminal;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::sync::watch;
use tracing_subscriber::EnvFilter;

const HELP: &str = "lazyview — materialized views for MySQL / MariaDB / Percona, maintained from the binlog

USAGE:
  lazyview              run the service — the operations run (configured by environment
                        variables; never issues DDL on lazyview's own tables)
  lazyview run          the same, explicitly
  lazyview setup        the setup and migration run: create and migrate lazyview's own tables
                        (definitions, state, status, fence), validate every enabled definition
                        and log its plan, then exit (0 ready, 1 a definition is invalid).
                        Run it as the setup account, once at install and once per upgrade.
  lazyview healthcheck  exit 0 if the running service's /healthz answers (for container health checks)
  lazyview --version    print the version

CONNECTION (either):
  LAZYVIEW_HOST, LAZYVIEW_PORT (3306), LAZYVIEW_USER, LAZYVIEW_PASSWORD, LAZYVIEW_DATABASE
  LAZYVIEW_DSN=mysql://user:pass@host:3306/database      (mariadb:// accepted)
  LAZYVIEW_TLS=disabled|required|verify-full   LAZYVIEW_TLS_CA=/path/ca.pem (private CA)
  A password (or the health token below) may be a reference instead of a value:
  file:///abs/path, aws-secret://<secret-id>[#<json-field>] (with --features aws-secrets),
  or plain://<value>; <NAME>_FILE names a file to read the reference from.

OTHER TARGETS (optional, one view can be written to several instances):
  LAZYVIEW_TARGET_<NAME>_HOST/_PORT/_USER/_PASSWORD/_DATABASE or LAZYVIEW_TARGET_<NAME>_DSN

COMMON SETTINGS:
  LAZYVIEW_START_MODE=resume|rebuild|once   LAZYVIEW_BATCH_WINDOW_MS=200
  LAZYVIEW_HEALTH_ADDR=127.0.0.1:8080 (or off; 0.0.0.0:8080 to serve the pod network)
  LAZYVIEW_HEALTH_TOKEN=<token> (gates /metrics and /stats)
  LAZYVIEW_LOG_FORMAT=text|json   RUST_LOG=info

 TELEMETRY (off until you turn it on):
   LAZYVIEW_OTEL=otlp    export spans over OTLP/gRPC (any other value, `off`, stays off)
                         the rest is the standard OpenTelemetry environment, read as the
                         specification names it: OTEL_EXPORTER_OTLP_ENDPOINT, its _HEADERS and
                         _PROTOCOL (grpc only), OTEL_TRACES_SAMPLER and its _ARG,
                         OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES, OTEL_SDK_DISABLED,
                         OTEL_TRACES_EXPORTER=none. `/metrics` (Prometheus) is unchanged.

Then: INSERT INTO _lazyview (definition) VALUES ('CREATE VIEW v AS SELECT ...');
See README.md for everything else.
";

/// Error chain without repeated segments (driver errors often repeat their source's text).
fn chain(e: &anyhow::Error) -> String {
    let mut out: Vec<String> = Vec::new();
    for c in e.chain() {
        let s = c.to_string();
        if out.last().map(|l| l.contains(&s)).unwrap_or(false) {
            continue;
        }
        out.push(s);
    }
    out.join(": ")
}

fn init_logging() {
    let filter = EnvFilter::try_from_env("RUST_LOG").unwrap_or_else(|_| EnvFilter::new("info"));
    let json = std::env::var("LAZYVIEW_LOG_FORMAT")
        .map(|v| v.eq_ignore_ascii_case("json"))
        .unwrap_or(false);
    let color = std::io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none();
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_ansi(color)
            .init();
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let mut setup_run = false;
    match std::env::args().nth(1).as_deref() {
        Some("--version" | "-V" | "version") => {
            println!("lazyview {}", env!("CARGO_PKG_VERSION"));
            return ExitCode::SUCCESS;
        }
        Some("--help" | "-h" | "help") => {
            print!("{HELP}");
            return ExitCode::SUCCESS;
        }
        Some("setup") => setup_run = true,
        Some("run") => {}
        Some("healthcheck") => {
            let addr = std::env::var("LAZYVIEW_HEALTH_ADDR")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(|| ([127, 0, 0, 1], 8080).into());
            return if lazyview::health::probe(addr).await {
                ExitCode::SUCCESS
            } else {
                ExitCode::FAILURE
            };
        }
        Some(other) => {
            eprintln!("unknown argument `{other}`\n\n{HELP}");
            return ExitCode::from(2);
        }
        None => {}
    }
    init_logging();

    let mut cfg = match lazyview::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("configuration error: {}", chain(&e));
            return ExitCode::from(2);
        }
    };
    // The two runs (owner 2026-11-16): `lazyview setup` is the setup and migration run (it
    // alone issues DDL on lazyview's own tables); the operations run (no argument, or
    // `lazyview run`) only checks them and exits 2 naming the setup run when one is missing.
    if setup_run {
        cfg.sync = false;
    } else if !cfg.sync {
        tracing::warn!(
            "LAZYVIEW_SYNC=false is deprecated (one release) and now runs `lazyview setup`: \
             create/migrate lazyview's own tables, validate the definitions, and exit. Run \
             `lazyview setup` (with the setup account) instead of setting LAZYVIEW_SYNC=false."
        );
    }
    // One WARN per endpoint (source or target) that reaches a server off this host with TLS off.
    // A warning, not a refusal: `LAZYVIEW_TLS` has always defaulted to `disabled`, and a private
    // network is a legitimate choice — but it should be one the operator made on purpose.
    for w in &cfg.cleartext {
        tracing::warn!(
            "{}: {} connects without TLS; set {}=required (or verify-full) to keep credentials \
             and data off the wire",
            w.endpoint,
            w.host,
            w.var
        );
    }
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        tracing::info!("shutdown requested");
        let _ = tx.send(true);
    });
    match lazyview::engine::run(cfg, Arc::new(lazyview::Metrics::default()), rx).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("fatal: {}", chain(&e));
            // A failed pre-flight of lazyview's own tables is a configuration error: exit 2,
            // the fix is `lazyview setup` with the setup account (named by the error).
            if e.chain()
                .any(|c| c.downcast_ref::<lazyview::meta::SetupRequired>().is_some())
            {
                return ExitCode::from(2);
            }
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
