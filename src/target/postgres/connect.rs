//! The PostgreSQL connection (PR T1): the driver task, the rustls connector and the session.
//! rustls with the `ring` provider built explicitly — the crate's `with_webpki_roots()` would
//! use the process-default provider, which belongs to `mysql_async`; building it here keeps
//! one crypto provider (the one in Cargo.lock) and no OpenSSL (D11).

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use tokio_postgres_rustls::MakeRustlsConnect;

use super::{session_setup, PgConn};

impl PgConn {
    /// Connects and applies the session settings of [`session_setup`] on the new connection.
    pub(crate) async fn connect(
        dsn: &str,
        lock_wait_s: u64,
        connect_timeout: Duration,
    ) -> Result<PgConn> {
        // `sslrootcert` is a libpq parameter tokio-postgres does not parse: it is read here
        // (config.rs maps `<NAME>_TLS_CA` onto it, D11) and stripped before parsing.
        let (dsn, rootcert) = split_sslrootcert(dsn);
        let tls = MakeRustlsConnect::new(client_config(rootcert.as_deref())?);
        let (client, connection) =
            tokio::time::timeout(connect_timeout, tokio_postgres::connect(&dsn, tls))
                .await
                .map_err(|_| {
                    anyhow!(
                        "connecting to the PostgreSQL target: timed out after {connect_timeout:?}"
                    )
                })?
                .context("connecting to the PostgreSQL target")?;
        // The driver task ends when the Client is dropped (its send side closes).
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let pid: i32 = client
            .query_one("SELECT pg_backend_pid()", &[])
            .await
            .context("reading pg_backend_pid()")?
            .get(0);
        for stmt in session_setup(lock_wait_s) {
            client
                .execute(&stmt, &[])
                .await
                .with_context(|| format!("applying the session setting `{stmt}`"))?;
        }
        Ok(PgConn {
            client,
            pid,
            dsn: dsn.into_owned(),
        })
    }
}

/// The rustls `ClientConfig` with the `ring` provider named explicitly (one provider, D11):
/// the webpki roots by default, the DSN's `sslrootcert` file when given.
fn client_config(rootcert: Option<&str>) -> Result<rustls::ClientConfig> {
    use rustls::pki_types::CertificateDer;
    let mut roots = rustls::RootCertStore::empty();
    match rootcert {
        Some(path) => {
            let pem =
                std::fs::read(path).with_context(|| format!("reading sslrootcert `{path}`"))?;
            for cert in rustls_pemfile::certs(&mut std::io::Cursor::new(pem)) {
                let cert = cert.with_context(|| format!("parsing sslrootcert `{path}`"))?;
                roots.add(CertificateDer::from(cert.to_vec()))?;
            }
            if roots.is_empty() {
                bail!("sslrootcert `{path}` contains no certificate");
            }
        }
        None => {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
    }
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth())
}

/// Pulls `sslrootcert=<path>` (and only it) out of the DSN's parameter block; the path is
/// percent-decoded the way `src/config.rs` encodes it.
fn split_sslrootcert(dsn: &str) -> (std::borrow::Cow<'_, str>, Option<String>) {
    let Some((base, query)) = dsn.split_once('?') else {
        return (dsn.into(), None);
    };
    let mut kept = Vec::new();
    let mut root = None;
    for p in query.split('&') {
        match p.strip_prefix("sslrootcert=") {
            Some(v) => root = Some(percent_decode(v)),
            None => kept.push(p),
        }
    }
    if root.is_none() {
        return (dsn.into(), None);
    }
    let d = if kept.is_empty() {
        base.to_string().into()
    } else {
        format!("{base}?{}", kept.join("&")).into()
    };
    (d, root)
}

/// The `%XX` form `src/config.rs`'s `encode` produces (a file path is otherwise literal).
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
