use super::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// A test directory with secret files in it; `crate::config`'s tests use it too.
pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> TempDir {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let base = match std::env::var("LAZYVIEW_TEST_TMPDIR") {
            Ok(d) => PathBuf::from(d),
            Err(_) => std::env::temp_dir(),
        };
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = base.join(format!(
            "lazyview-secrets-{}-{name}-{n}",
            std::process::id()
        ));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).expect("create the test directory");
        TempDir(dir)
    }

    /// Writes `name` and returns its path as a string.
    pub(crate) fn write(&self, name: &str, content: &str) -> String {
        let p = self.0.join(name);
        std::fs::write(&p, content).expect("write the secret file");
        p.to_string_lossy().into_owned()
    }

    pub(crate) fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }

    fn dir(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).ok();
    }
}

/// The stand-in for Secrets Manager: canned secret strings, no network.
#[derive(Default)]
struct Mock {
    secrets: HashMap<String, String>,
    calls: std::cell::RefCell<Vec<String>>,
}

impl Mock {
    fn with(pairs: &[(&str, &str)]) -> Mock {
        Mock {
            secrets: pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            calls: std::cell::RefCell::new(Vec::new()),
        }
    }
}

impl SecretSource for Mock {
    fn secret_string(&self, secret_id: &str) -> Result<String> {
        self.calls.borrow_mut().push(secret_id.to_string());
        self.secrets
            .get(secret_id)
            .cloned()
            .ok_or_else(|| anyhow!("mock: no such secret {secret_id}"))
    }
}

impl Mock {
    fn called(&self) -> Vec<String> {
        self.calls.borrow().clone()
    }
}

fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
    let m: HashMap<String, String> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    move |k| m.get(k).cloned()
}

#[test]
fn scheme_dispatch_table() {
    let cases: &[(&str, Scheme)] = &[
        // no scheme: verbatim, exactly as before this module existed
        ("p@ssw0rd", Scheme::Literal("p@ssw0rd")),
        ("", Scheme::Literal("")),
        ("  padded  ", Scheme::Literal("  padded  ")),
        ("pass:word", Scheme::Literal("pass:word")),
        ("//unix/socket", Scheme::Literal("//unix/socket")),
        // not scheme-shaped prefixes stay literal
        ("://x", Scheme::Literal("://x")),
        ("1x://y", Scheme::Literal("1x://y")),
        ("a b://y", Scheme::Literal("a b://y")),
        // the escape hatch: everything after plain:// is the value, even another scheme. It is a
        // scheme of its own (`Plain`), because a DSN password has to lose the prefix.
        ("plain://file:///etc/pw", Scheme::Plain("file:///etc/pw")),
        ("plain://x", Scheme::Plain("x")),
        ("PLAIN://x", Scheme::Plain("x")),
        ("FILE:///a b/c", Scheme::File("/a b/c")),
        ("file:///a b/c", Scheme::File("/a b/c")),
        (
            "aws-secret://prod/lazyview",
            Scheme::AwsSecret {
                id: "prod/lazyview",
                key: None,
            },
        ),
        (
            "AWS-SECRET://prod/lazyview#password",
            Scheme::AwsSecret {
                id: "prod/lazyview",
                key: Some("password"),
            },
        ),
        // an empty #field is not a field
        ("aws-secret://x#", Scheme::AwsSecret { id: "x", key: None }),
        // anything else that looks like a scheme is an error, never a password
        ("http://x", Scheme::Unknown("http")),
        ("awsecret://x", Scheme::Unknown("awsecret")),
    ];
    for (reference, want) in cases {
        assert_eq!(&scheme_of(reference), want, "{reference:?}");
    }
}

#[test]
fn resolves_literal_and_file() {
    let d = TempDir::new("literal");
    let path = d.write("password", "s3cr3t\n");
    let mock = Mock::default();
    assert_eq!(
        resolve_in("p@ss", &mock, "LAZYVIEW_PASSWORD")
            .unwrap()
            .expose(),
        "p@ss"
    );
    assert_eq!(
        resolve_in(&format!("file://{path}"), &mock, "LAZYVIEW_PASSWORD")
            .unwrap()
            .expose(),
        "s3cr3t",
        "one trailing newline is trimmed"
    );
    assert!(mock.called().is_empty(), "a plain build never touches AWS");
}

#[test]
fn file_reads_trim_exactly_one_newline() {
    let d = TempDir::new("trim");
    // (name, the bytes written, the value they must resolve to)
    let cases = [
        ("a", "s\n", "s"),     // the newline an editor or `echo` added
        ("b", "s\r\n", "s"),   // a CRLF file
        ("c", "s\n\n", "s\n"), // only one newline goes: the second is the value
        ("d", "s \n", "s "),   // trailing spaces are the value
        ("e", "s", "s"),       // no newline at all
        ("f", "\n", ""),       // an empty secret
    ];
    for (name, content, want) in cases {
        let path = d.write(name, content);
        let got = resolve_in(&format!("file://{path}"), &Mock::default(), "P")
            .unwrap()
            .expose()
            .to_string();
        assert_eq!(got, want, "{name}: {content:?}");
    }
    // a variable's own value is never trimmed
    assert_eq!(
        resolve_in("s \n", &Mock::default(), "P").unwrap().expose(),
        "s \n"
    );
}

#[test]
fn a_file_can_hold_another_reference() {
    let d = TempDir::new("indirect");
    // The mounted file says where the secret really lives (the double indirection of the
    // design: the file itself is not the secret).
    let path = d.write("ref", "aws-secret://prod/lazyview#password\n");
    let mock = Mock::with(&[("prod/lazyview", r#"{"password":"from-aws","user":"lv"}"#)]);
    assert_eq!(
        resolve_in(&format!("file://{path}"), &mock, "LAZYVIEW_PASSWORD")
            .unwrap()
            .expose(),
        "from-aws"
    );
    assert_eq!(mock.called(), vec!["prod/lazyview".to_string()]);
}

#[test]
fn nesting_is_bounded() {
    let d = TempDir::new("deep");
    let self_path = d.path("loop").to_string_lossy().into_owned();
    std::fs::write(&self_path, format!("file://{self_path}\n")).unwrap();
    let e = resolve_in(&format!("file://{self_path}"), &Mock::default(), "P").unwrap_err();
    assert!(
        e.to_string()
            .contains(&format!("may not nest more than {MAX_INDIRECTION} levels")),
        "{e}"
    );
}

#[test]
fn json_field_selection() {
    let mock = Mock::with(&[
        ("obj", r#"{"password":"pw","n":7,"b":true,"empty":""}"#),
        ("array", "[1,2]"),
        ("text", "not json"),
        ("nested", r#"{"a":{"b":1}}"#),
        ("nullf", r#"{"a":null}"#),
    ]);
    let get = |id: &str, key: &str| -> Result<String> {
        resolve_in(
            &format!("aws-secret://{id}#{key}"),
            &mock,
            "LAZYVIEW_PASSWORD",
        )
        .map(|s| s.expose().to_string())
    };
    assert_eq!(get("obj", "password").unwrap(), "pw");
    assert_eq!(
        get("obj", "n").unwrap(),
        "7",
        "a number keeps its JSON text"
    );
    assert_eq!(get("obj", "b").unwrap(), "true");
    assert_eq!(
        get("obj", "empty").unwrap(),
        "",
        "an empty field is a value"
    );
    // the whole secret string when no #field is given
    assert_eq!(
        resolve_in("aws-secret://text", &mock, "P")
            .unwrap()
            .expose(),
        "not json"
    );
    let e = get("obj", "missing").unwrap_err().full();
    assert!(e.contains("no such field") && e.contains("password"), "{e}");
    let e = get("text", "password").unwrap_err().full();
    assert!(
        e.contains("is not JSON") && e.contains("drop the #password"),
        "{e}"
    );
    let e = get("array", "password").unwrap_err().full();
    assert!(e.contains("not an object"), "{e}");
    let e = get("nested", "a").unwrap_err().full();
    assert!(e.contains("not a scalar"), "{e}");
    let e = get("nullf", "a").unwrap_err().full();
    assert!(e.contains("null"), "{e}");
    let e = get("nope", "a").unwrap_err().full();
    assert!(e.contains("mock: no such secret nope"), "{e}");
    // the id is named, no value ever is
    assert!(!e.contains("from-aws"), "{e}");
}

#[test]
fn unknown_scheme_lists_the_known_ones() {
    for reference in ["vault://app/pw", "http://x/y", "awsecret://x"] {
        let e = resolve_in(reference, &Mock::default(), "LAZYVIEW_PASSWORD").unwrap_err();
        let msg = e.full();
        assert!(
            msg.contains("unknown secret scheme")
                && msg.contains("file:///abs/path")
                && msg.contains("aws-secret://<secret-id>[#<json-field>]")
                && msg.contains("LAZYVIEW_PASSWORD"),
            "{reference}: {msg}"
        );
    }
}

#[test]
fn file_scheme_errors_name_the_variable() {
    let d = TempDir::new("errs");
    let missing = d.path("nope").to_string_lossy().into_owned();
    let e = resolve_in(
        &format!("file://{missing}"),
        &Mock::default(),
        "LAZYVIEW_PASSWORD",
    )
    .unwrap_err()
    .full();
    assert!(
        e.contains("LAZYVIEW_PASSWORD") && e.contains(&missing),
        "{e}"
    );
    // a relative path is refused: file:// means an absolute path
    let e = resolve_in("file://etc/pw", &Mock::default(), "P")
        .unwrap_err()
        .full();
    assert!(e.contains("absolute path"), "{e}");
    // a directory is not a secret
    let e = resolve_in(
        &format!("file://{}", d.dir().display()),
        &Mock::default(),
        "P",
    )
    .unwrap_err()
    .full();
    assert!(e.contains("not a file"), "{e}");
}

#[test]
fn var_and_file_together_is_a_config_error() {
    let d = TempDir::new("both");
    let path = d.write("pw", "from-file");
    let e = resolve_var(
        &lookup(&[
            ("LAZYVIEW_PASSWORD", "from-var"),
            ("LAZYVIEW_PASSWORD_FILE", &path),
        ]),
        "LAZYVIEW_PASSWORD",
    )
    .unwrap_err()
    .full();
    assert!(
        e.contains("LAZYVIEW_PASSWORD and LAZYVIEW_PASSWORD_FILE are both set"),
        "{e}"
    );
    // an empty value counts as unset (the historical behavior of the password variables)
    assert_eq!(
        resolve_var(
            &lookup(&[("LAZYVIEW_PASSWORD", ""), ("LAZYVIEW_PASSWORD_FILE", &path)]),
            "LAZYVIEW_PASSWORD",
        )
        .unwrap()
        .unwrap()
        .expose(),
        "from-file"
    );
    assert_eq!(
        resolve_var(
            &lookup(&[("LAZYVIEW_PASSWORD", "lit")]),
            "LAZYVIEW_PASSWORD"
        )
        .unwrap()
        .unwrap()
        .expose(),
        "lit"
    );
    assert!(resolve_var(&lookup(&[]), "LAZYVIEW_PASSWORD")
        .unwrap()
        .is_none());
}

#[test]
fn a_file_variable_may_carry_the_file_prefix() {
    // `LAZYVIEW_PASSWORD_FILE=/run/secrets/pw` is the normal spelling. The prefix version is what
    // an operator who has just learned the reference grammar writes, so it means the same thing.
    let d = TempDir::new("fileprefix");
    let path = d.write("pw", "from-file");
    for value in [path.clone(), format!("file://{path}")] {
        assert_eq!(
            resolve_var(&lookup(&[("P_FILE", &value)]), "P")
                .unwrap()
                .unwrap()
                .expose(),
            "from-file",
            "{value}"
        );
    }
    // a relative path is still refused, prefix or no prefix
    let e = resolve_var(&lookup(&[("P_FILE", "file://etc/pw")]), "P")
        .unwrap_err()
        .full();
    assert!(e.contains("absolute path") && e.contains("P_FILE"), "{e}");
}

#[test]
fn a_file_variable_may_hold_an_aws_reference() {
    let d = TempDir::new("filevar");
    // The resolution order: `_FILE` is read, then its content is re-checked for a scheme.
    let path = d.write("ref", "aws-secret://prod/lazyview#password\n");
    let mock = Mock::with(&[("prod/lazyview", r#"{"password":"from-aws"}"#)]);
    let got = resolve_var_in(
        &lookup(&[("LAZYVIEW_PASSWORD_FILE", &path)]),
        "LAZYVIEW_PASSWORD",
        &mock,
    )
    .unwrap()
    .unwrap();
    assert_eq!(got.expose(), "from-aws");
    assert_eq!(mock.called(), vec!["prod/lazyview".to_string()]);
    // and the same content in the variable itself resolves the same way
    let mock = Mock::with(&[("prod/lazyview", r#"{"password":"from-aws"}"#)]);
    assert_eq!(
        resolve_in(
            "aws-secret://prod/lazyview#password",
            &mock,
            "LAZYVIEW_PASSWORD"
        )
        .unwrap()
        .expose(),
        "from-aws"
    );
}

#[test]
#[cfg(not(feature = "aws-secrets"))]
fn aws_secret_fails_loud_without_the_feature() {
    // Never a silent fallback to using the URI as the password.
    let e = resolve_secret("aws-secret://prod/lazyview").unwrap_err();
    let msg = e.full();
    assert!(
        msg.contains("built without the aws-secrets feature"),
        "{msg}"
    );
    assert!(msg.contains("prod/lazyview"), "{msg}");
    let d = TempDir::new("featureoff");
    let path = d.write("pw", "aws-secret://prod/lazyview\n");
    let e = resolve_var(&lookup(&[("P_FILE", &path)]), "P").unwrap_err();
    assert!(
        e.full().contains("built without the aws-secrets feature"),
        "{e}"
    );
}

#[test]
#[cfg(feature = "aws-secrets")]
fn aws_secret_resolves_through_the_mock_client() {
    // The mock is the only AWS client these tests use: no GetSecretValue ever leaves the test.
    let mock = Mock::with(&[
        ("prod/lazyview", r#"{"password":"aws-pw","user":"lv"}"#),
        ("plain-secret", "whole-secret-string"),
    ]);
    assert_eq!(
        resolve_in(
            "aws-secret://prod/lazyview#password",
            &mock,
            "LAZYVIEW_PASSWORD"
        )
        .unwrap()
        .expose(),
        "aws-pw"
    );
    assert_eq!(
        resolve_in("aws-secret://plain-secret", &mock, "P")
            .unwrap()
            .expose(),
        "whole-secret-string"
    );
    assert_eq!(
        mock.called(),
        vec!["prod/lazyview".to_string(), "plain-secret".to_string()]
    );
    // With the feature on, an unresolvable id is the SDK's error (the mock's here), not a
    // literal and not the feature message.
    let e = resolve_in("aws-secret://nope", &mock, "P")
        .unwrap_err()
        .full();
    assert!(e.contains("mock: no such secret nope"), "{e}");
    assert!(!e.contains("built without the aws-secrets feature"), "{e}");
}

#[test]
fn a_secret_is_redacted_and_zeroized() {
    let d = TempDir::new("redact");
    let path = d.write("pw", "top-secret-value");
    let s = resolve_in(&format!("file://{path}"), &Mock::default(), "P").unwrap();
    assert_eq!(format!("{s:?}"), "Secret(***)");
    assert!(!format!("{s:?}").contains("top-secret-value"));
    assert_eq!(s.expose(), "top-secret-value");
    // a clone shares the buffer and the value survives to the last drop
    let c = s.clone();
    drop(s);
    assert_eq!(c.expose(), "top-secret-value");
}

#[test]
fn a_percent_encoded_value_round_trips() {
    // The rule the config writer uses and the resolver inverts must be one rule, or a password
    // with a reserved character would authenticate as something else on the way back in.
    for value in [
        "s3cr3t",
        "p@ss/w:rd",
        "100%sure",
        "trailing ",
        "ünïcode",
        "",
        "%40",
    ] {
        let encoded = super::percent_encode(value);
        assert_eq!(
            super::percent_decode(&encoded),
            value,
            "round trip of {value:?}"
        );
        // Nothing that delimits a DSN survives the encoding unescaped (`%` itself becomes `%25`,
        // so it is checked by the round trip above rather than by absence).
        for c in ["@", ":", "/", "?", "#"] {
            if value.contains(c) {
                assert!(!encoded.contains(c), "{c} left raw in {encoded:?}");
            }
        }
    }
    assert_eq!(super::percent_encode("p@ss"), "p%40ss");
    assert_eq!(super::percent_decode("p%40ss"), "p@ss");
    // a stray percent is not an escape and stays as written
    assert_eq!(super::percent_decode("100%sure"), "100%sure");
}
