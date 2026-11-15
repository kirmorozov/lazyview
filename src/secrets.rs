//! Where a password (or any other secret lazyview reads from the environment) actually lives.
//!
//! A secret *reference* is the text of an environment variable (or of the file named by its
//! `_FILE` twin) and one of:
//!
//! | form | meaning |
//! |---|---|
//! | `p@ssw0rd` | the value itself, verbatim (no scheme: the historical behavior) |
//! | `plain://p@ssw0rd` | the value itself, forced: the escape hatch for a literal that starts with a scheme-like prefix |
//! | `file:///run/secrets/pw` | the contents of that file, one trailing newline trimmed |
//! | `aws-secret://prod/lazyview` | AWS Secrets Manager `GetSecretValue`, the whole secret string |
//! | `aws-secret://prod/lazyview#password` | the `password` field of that JSON secret |
//!
//! A reference may point at another reference: the contents of a file (or of a `_FILE` variable)
//! can themselves be a `file://` or `aws-secret://` reference, up to [`MAX_INDIRECTION`] levels.
//! An unknown scheme is a configuration error that lists the known ones, and `aws-secret://` in a
//! build without the `aws-secrets` cargo feature fails loudly rather than treating the URI as a
//! password.
//!
//! Resolved values come back as [`Secret`]: a `String` wrapper that is zeroized when the last
//! handle to it is dropped. (The environment's own copy of an unread reference, and the copy that
//! ends up inside a DSN, are not ours to clear.)

use std::fmt;
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use zeroize::Zeroizing;

/// A secret reference may chain through this many levels of indirection (a `_FILE` holding a
/// `file://`, whose content holds an `aws-secret://`, …).
pub const MAX_INDIRECTION: usize = 8;
/// A secret file bigger than this is a mistake (a directory listing, `/dev/zero`, …), not a secret.
const MAX_SECRET_FILE_BYTES: u64 = 1 << 20;

/// A resolved secret, cleared when the last handle to it is dropped.
///
/// Cloning shares the same buffer, so a value used by several endpoints is zeroized once all of
/// them are done with it. [`Debug`] never prints the value: `Config` derives `Debug` and is
/// logged on configuration errors.
#[derive(Clone)]
pub struct Secret(Arc<Zeroizing<String>>);

impl Secret {
    fn new(value: String) -> Secret {
        Secret(Arc::new(Zeroizing::new(value)))
    }

    /// The secret itself. Callers must not log it or put it in an error message.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(***)")
    }
}

impl PartialEq for Secret {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}
impl Eq for Secret {}

/// What a reference text says. The `&str` of [`Scheme::Literal`] is the secret itself; the others
/// name a place and are safe to print.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme<'a> {
    /// No scheme: use the text as written.
    Literal(&'a str),
    /// `plain://<value>`: the value, forced — the escape hatch for a literal that starts with a
    /// scheme-like prefix. Same value as [`Scheme::Literal`], kept apart because a DSN password has
    /// to lose the prefix while a value that never had one stays exactly as written.
    Plain(&'a str),
    /// `file:///abs/path` (also the content of a `_FILE` variable).
    File(&'a str),
    /// `aws-secret://<secret-id>` (`key` = the `#field` of a JSON secret).
    AwsSecret { id: &'a str, key: Option<&'a str> },
    /// `<anything-else>://…`: a configuration error, never a literal.
    Unknown(&'a str),
}

/// The schemes lazyview understands, for error messages.
pub const KNOWN_SCHEMES: &str =
    "plain:// (literal), file:///abs/path, aws-secret://<secret-id>[#<json-field>]";

/// Splits a reference into its scheme. A value is a reference only when it *starts* with
/// `<scheme>://`; anything else (including `a:b`, `//x`, an empty string) is [`Scheme::Literal`].
pub fn scheme_of(reference: &str) -> Scheme<'_> {
    let Some((scheme, rest)) = split_scheme(reference) else {
        return Scheme::Literal(reference);
    };
    if scheme.eq_ignore_ascii_case("plain") {
        return Scheme::Plain(rest);
    }
    if scheme.eq_ignore_ascii_case("file") {
        return Scheme::File(rest);
    }
    if scheme.eq_ignore_ascii_case("aws-secret") {
        let (id, key) = match rest.split_once('#') {
            Some((id, key)) if !key.is_empty() => (id, Some(key)),
            Some((id, _)) => (id, None),
            None => (rest, None),
        };
        return Scheme::AwsSecret { id, key };
    }
    Scheme::Unknown(scheme)
}

/// `<scheme>://<rest>` with the separator kept out of `rest`, when the value starts with a
/// scheme-like prefix (`^[A-Za-z][A-Za-z0-9+.-]*://`). Anything else — no prefix, `://x`,
/// `1x://y`, `a b://y` — is not a scheme, so the whole value stays a literal.
fn split_scheme(reference: &str) -> Option<(&str, &str)> {
    let at = reference.find("://")?;
    let scheme = &reference[..at];
    let shaped = scheme
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic())
        && scheme
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"+-.".contains(&b));
    shaped.then(|| (scheme, &reference[at + 3..]))
}

/// Percent-decoding for one field of a DSN (`%40` → `@`, `%3A%2F%2F` → `://`), the inverse of the
/// `encode` in `src/config.rs`. A `%` that does not start a valid escape stays as written, which
/// is what the URL parsers of both drivers do with it.
pub fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = &bytes[i + 1..i + 3];
            if let Ok(b) = u8::from_str_radix(std::str::from_utf8(hex).unwrap_or_default(), 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    // Decoding is done on a URL field, so it is UTF-8 or it stays as written.
    String::from_utf8(out).unwrap_or_else(|_| value.to_string())
}

/// The inverse of [`percent_decode`], used when a resolved secret is written back into the
/// userinfo of a DSN: everything outside the unreserved set of RFC 3986 becomes `%XX`, so a
/// password holding `@`, `/` or `%` cannot change where the DSN points. The config writer's
/// `encode` is this same function, which is what makes a value round-trip.
pub fn percent_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// `format!("{e:#}")` under a name, for the tests: `Display` of an anyhow error is only its last
/// `with_context` label, while the reason an operator has to act on sits at the root. The startup
/// path prints the whole chain, so that is what the assertions check. (`anyhow::Error::chain` is
/// taken: it is the iterator, not the text.)
#[cfg(test)]
pub(crate) trait ErrorChain {
    fn full(&self) -> String;
}

#[cfg(test)]
impl ErrorChain for anyhow::Error {
    fn full(&self) -> String {
        format!("{self:#}")
    }
}

/// The AWS Secrets Manager seam: `GetSecretValue` for one secret id, so the dispatch (and the
/// JSON field selection) is testable without a network.
pub(crate) trait SecretSource {
    /// The `SecretString` of `secret_id`, resolved with the default AWS credential chain.
    fn secret_string(&self, secret_id: &str) -> Result<String>;
}

/// The source of a build without `aws-secrets`: `aws-secret://` is refused, loudly. (With the
/// feature on there is no such source: `default_source` returns the real client.)
#[cfg(not(feature = "aws-secrets"))]
struct NoAws;

#[cfg(not(feature = "aws-secrets"))]
impl SecretSource for NoAws {
    fn secret_string(&self, secret_id: &str) -> Result<String> {
        Err(anyhow!(
            "aws-secret://{secret_id}: lazyview was built without the aws-secrets feature, \
             so this reference cannot be resolved; rebuild with --features aws-secrets, or \
             put the value in a file and point at it with file://"
        ))
    }
}

/// The source `resolve_secret` uses: the real client only with the cargo feature on.
fn default_source() -> &'static dyn SecretSource {
    #[cfg(feature = "aws-secrets")]
    {
        &aws::AwsSecrets
    }
    #[cfg(not(feature = "aws-secrets"))]
    {
        &NoAws
    }
}

/// Resolves one reference text: `file://` and plain text always, `aws-secret://` through
/// [`default_source`] (which fails loudly when the `aws-secrets` feature is off).
pub fn resolve_secret(reference: &str) -> Result<Secret> {
    resolve_in(reference, default_source(), "secret")
}

/// Resolves `<NAME>` / `<NAME>_FILE` from `get` as a password input (see the module header for
/// the order). `file://` and plain text always; `aws-secret://` through the default source.
pub fn resolve_var(get: &impl Fn(&str) -> Option<String>, name: &str) -> Result<Option<Secret>> {
    resolve_var_in(get, name, default_source())
}

fn resolve_var_in(
    get: &impl Fn(&str) -> Option<String>,
    name: &str,
    source: &dyn SecretSource,
) -> Result<Option<Secret>> {
    let value = get(name).filter(|v| !v.is_empty());
    let file = get(&format!("{name}_FILE")).filter(|v| !v.is_empty());
    match (value, file) {
        (Some(_), Some(_)) => bail!(
            "{name} and {name}_FILE are both set; give the secret in one of them \
             ({name}_FILE names a file to read, {name} holds the value or a reference)"
        ),
        (Some(v), None) => Ok(Some(resolve_in(&v, source, name)?)),
        (None, Some(path)) => {
            // The variable names a path. A `file://` prefix on it is accepted too: an operator who
            // has just learned the reference grammar reaches for it here, and refusing it would be
            // a trap that only costs a restart.
            let path = path.strip_prefix("file://").unwrap_or(path.as_str());
            let text = read_secret_file(path).with_context(|| format!("{name}_FILE"))?;
            Ok(Some(resolve_in(&text, source, name)?))
        }
        (None, None) => Ok(None),
    }
}

/// The dispatch table: scheme → where to read. Errors name `label` (the variable the reference
/// came from) and never carry a resolved value.
fn resolve_in(reference: &str, source: &dyn SecretSource, label: &str) -> Result<Secret> {
    let mut text = reference.to_string();
    for _ in 0..MAX_INDIRECTION {
        match scheme_of(&text) {
            // `plain://` and "no scheme at all" are the same thing to read: the text. The
            // distinction matters only in a DSN password, where the prefix must go.
            Scheme::Literal(value) | Scheme::Plain(value) => {
                return Ok(Secret::new(value.to_string()))
            }
            Scheme::File(path) => {
                text = read_secret_file(path)
                    .with_context(|| format!("{label}: cannot read secret file {path}"))?;
            }
            Scheme::AwsSecret { id, key } => {
                let raw = source
                    .secret_string(id)
                    .with_context(|| format!("{label}: aws-secret://{id}"))?;
                text = match key {
                    Some(key) => pick_json_field(&raw, key, id)?,
                    None => raw,
                };
            }
            Scheme::Unknown(scheme) => bail!(
                "{label}: unknown secret scheme {scheme}:// (known: {KNOWN_SCHEMES}); \
                 a literal value that starts with a scheme-like prefix must be written as \
                 plain://<value>"
            ),
        }
    }
    bail!("{label}: a secret reference may not nest more than {MAX_INDIRECTION} levels")
}

/// Reads a secret file: one trailing newline is trimmed (editors and `echo` add one), the rest is
/// the value verbatim.
fn read_secret_file(path: &str) -> Result<String> {
    if !path.starts_with('/') {
        bail!("file:// expects an absolute path (file:///run/secrets/password), got {path}");
    }
    let meta = std::fs::metadata(path).with_context(|| format!("cannot read {path}"))?;
    if !meta.is_file() {
        bail!("{path} is not a file");
    }
    if meta.len() > MAX_SECRET_FILE_BYTES {
        bail!(
            "{path} is {} bytes, too large for a secret file",
            meta.len()
        );
    }
    let mut text = std::fs::read_to_string(path).with_context(|| format!("cannot read {path}"))?;
    // Exactly one trailing newline (and the \r of a CRLF one): a password may end in spaces.
    if text.ends_with('\n') {
        text.truncate(text.len() - 1);
        if text.ends_with('\r') {
            text.truncate(text.len() - 1);
        }
    }
    Ok(text)
}

/// The `#field` of a JSON secret string (`aws-secret://id#field`). Scalars are the value; a
/// string keeps its characters exactly, other scalars take their JSON text.
fn pick_json_field(raw: &str, key: &str, id: &str) -> Result<String> {
    let prefix = format!("aws-secret://{id}#{key}");
    let parsed: serde_json::Value = serde_json::from_str(raw).with_context(|| {
        format!("{prefix}: the secret of {id} is not JSON, so it has no fields (drop the #{key})")
    })?;
    let object = parsed.as_object().ok_or_else(|| {
        anyhow!(
            "{prefix}: the secret of {id} is a JSON {}, not an object",
            parsed_type(&parsed)
        )
    })?;
    let field = object.get(key).ok_or_else(|| {
        anyhow!(
            "{prefix}: no such field in the secret of {id} (fields: {})",
            object.keys().cloned().collect::<Vec<_>>().join(", ")
        )
    })?;
    match field {
        serde_json::Value::String(s) => Ok(s.clone()),
        serde_json::Value::Null => bail!("{prefix}: the field is null"),
        serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
            bail!(
                "{prefix}: the field is a JSON {}, not a scalar",
                parsed_type(field)
            )
        }
        other => Ok(other.to_string()),
    }
}

fn parsed_type(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

#[cfg(feature = "aws-secrets")]
mod aws {
    //! `aws-secret://` with the real client: the DEFAULT credential chain (environment, IRSA,
    //! instance profile …), never anything from the lazyview configuration.

    use anyhow::{bail, Context, Result};

    use super::SecretSource;

    pub(super) struct AwsSecrets;

    impl SecretSource for AwsSecrets {
        /// One `GetSecretValue` round-trip.
        ///
        /// Configuration is read synchronously (`Config::from_lookup`), which may already run
        /// inside the service's tokio runtime, so this uses a private current-thread runtime:
        /// blocking on the caller's runtime would be a deadlock.
        fn secret_string(&self, secret_id: &str) -> Result<String> {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("aws-secrets: cannot start a runtime for GetSecretValue")?;
            runtime.block_on(async move {
                let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                    .load()
                    .await;
                let client = aws_sdk_secretsmanager::Client::new(&config);
                let outcome = client
                    .get_secret_value()
                    .secret_id(secret_id)
                    .send()
                    .await
                    .with_context(|| format!("GetSecretValue failed for {secret_id}"))?;
                match (outcome.secret_string, outcome.secret_binary) {
                    (Some(s), _) => Ok(s),
                    (None, Some(_)) => bail!(
                        "aws-secret://{secret_id}: only SecretString secrets are supported \
                         (this one is a binary secret)"
                    ),
                    (None, None) => {
                        bail!("aws-secret://{secret_id}: GetSecretValue returned no value")
                    }
                }
            })
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
