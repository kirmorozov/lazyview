//! The one guard for suites that change a server GLOBAL variable (tests review B5). A GLOBAL
//! change affects every connection on the shared test servers, so such a suite is opt-in and must
//! put the old value back; both halves used to be hand-rolled per suite. Through this guard a
//! wrong restore breaks one place instead of five.

use anyhow::ensure;

use super::*;

/// Whether the opt-in switch `var` (e.g. `LAZYVIEW_TEST_GLOBAL_TZ`) is set to `1`. Every suite that
/// changes a GLOBAL variable needs one: CI sets it because it runs one test binary at a time, and
/// no other suite may be on the server while the change is in effect.
pub fn opted_in(var: &str) -> bool {
    std::env::var(var).map(|v| v == "1").unwrap_or(false)
}

/// A numeric value as a number (the server rejects a quoted string for an integer variable),
/// anything else as a string literal (`ON`, `OFF`).
fn lit(value: &str) -> String {
    if value.parse::<i64>().is_ok() {
        value.to_string()
    } else {
        q(value)
    }
}

/// Puts back `SET GLOBAL name = value` for each pair: every pair is tried, the errors are reported
/// together.
async fn apply(conn: &mut Conn, values: &[(String, String)]) -> Result<()> {
    let mut errs = Vec::new();
    for (name, value) in values {
        if let Err(e) = exec(conn, &format!("SET GLOBAL {name} = {}", lit(value))).await {
            errs.push(format!("{name}: {e:#}"));
        }
    }
    ensure!(errs.is_empty(), "SET GLOBAL failed: {}", errs.join("; "));
    Ok(())
}

/// Server globals saved before a test changed them. [`Globals::restore`] puts them back on the
/// normal path; `Drop` restores them from a thread of its own if the test ended early.
pub struct Globals {
    dsn: String,
    saved: Vec<(String, String)>,
    restored: bool,
}

impl Globals {
    /// Saves the current values of `changes`' names, then applies `changes`.
    pub async fn set(admin: &mut Conn, dsn: &str, changes: &[(&str, &str)]) -> Result<Globals> {
        let mut saved = Vec::new();
        for (name, _) in changes {
            let v = scalar(admin, &format!("SELECT @@GLOBAL.{name}"))
                .await?
                .with_context(|| format!("@@GLOBAL.{name} is NULL"))?;
            saved.push((name.to_string(), v));
        }
        let mut g = Globals {
            dsn: dsn.to_string(),
            saved,
            restored: false,
        };
        // In order, stopping at the first failure: a half-applied change set is put back from
        // `saved` and the test fails with the SET error.
        for (name, value) in changes {
            if let Err(e) = exec(admin, &format!("SET GLOBAL {name} = {}", lit(value))).await {
                let _ = g.restore(admin).await;
                return Err(e.context(format!("SET GLOBAL {name}")));
            }
        }
        Ok(g)
    }

    pub async fn restore(&mut self, admin: &mut Conn) -> Result<()> {
        apply(admin, &self.saved).await?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for Globals {
    fn drop(&mut self) {
        if self.restored {
            return;
        }
        let (dsn, values) = (self.dsn.clone(), self.saved.clone());
        // Drop cannot await: restore on a runtime of this thread (never nested in the test's).
        let res = std::thread::spawn(move || -> Result<()> {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async {
                let mut c = connect(&dsn).await?;
                apply(&mut c, &values).await
            })
        })
        .join();
        match res {
            Ok(Ok(())) => eprintln!("globals restored on drop"),
            _ => eprintln!(
                "GLOBALS NOT RESTORED, run by hand: {}",
                self.saved
                    .iter()
                    .map(|(n, v)| format!("SET GLOBAL {n} = {};", lit(v)))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
        }
    }
}
