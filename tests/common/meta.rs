use super::*;

// ---------------------------------------------------------------------------------------------
// Metadata tables
// ---------------------------------------------------------------------------------------------

/// Creates `_lazyview` as the engine does (src/meta.rs, plus `name`, `targets`), if missing. Used to
/// pre-load definitions before the first engine start; the engine itself creates it otherwise.
pub async fn ensure_lazyview_table(conn: &mut Conn) -> Result<()> {
    exec(
        conn,
        "CREATE TABLE IF NOT EXISTS _lazyview (
           id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
           name       VARCHAR(64) NULL,
           definition MEDIUMTEXT NOT NULL,
           enabled    TINYINT(1) NOT NULL DEFAULT 1,
           targets    VARCHAR(255) NULL,
           created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
           updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
           UNIQUE KEY uq_lazyview_name (name)
         ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
    )
    .await
}

/// Inserts a definition; returns its `_lazyview.id`.
pub async fn add_definition(
    conn: &mut Conn,
    definition: &str,
    targets: Option<&str>,
) -> Result<u64> {
    let t = targets.map(q).unwrap_or_else(|| "NULL".into());
    exec(
        conn,
        &format!(
            "INSERT INTO _lazyview (definition, targets) VALUES ({}, {t})",
            q(definition)
        ),
    )
    .await?;
    let id = scalar_i64(conn, "SELECT LAST_INSERT_ID()").await?;
    Ok(id as u64)
}

/// Inserts every spec's definition; returns name → id.
pub async fn add_views(conn: &mut Conn, specs: &[ViewSpec]) -> Result<HashMap<String, u64>> {
    let mut ids = HashMap::new();
    for s in specs {
        let id = add_definition(conn, &s.definition, s.targets.as_deref()).await?;
        ids.insert(s.name.clone(), id);
    }
    Ok(ids)
}

#[derive(Debug, Clone)]
pub struct StateRow {
    pub name: String,
    pub state: String,
    pub mode: Option<String>,
    pub last_error: Option<String>,
    pub definition_hash: Option<String>,
    pub refreshed_at: Option<String>,
    /// Target instance of this state row (`source` or a target name).
    pub target: Option<String>,
    /// `_lazyview.id` this row belongs to.
    pub definition_id: Option<u64>,
}

pub async fn view_states(conn: &mut Conn) -> Result<Vec<StateRow>> {
    if !table_exists(conn, "_lazyview_state").await? {
        return Ok(Vec::new());
    }
    let rs = rows(
        conn,
        "SELECT name, state, mode, last_error, definition_hash, CAST(refreshed_at AS CHAR), target, definition_id FROM _lazyview_state",
    )
    .await?;
    Ok(rs
        .into_iter()
        .map(|r| StateRow {
            name: r[0].clone().unwrap_or_default(),
            state: r[1].clone().unwrap_or_default(),
            mode: r[2].clone(),
            last_error: r[3].clone(),
            definition_hash: r[4].clone(),
            refreshed_at: r[5].clone(),
            target: r[6].clone(),
            definition_id: r[7].as_deref().and_then(|v| v.parse().ok()),
        })
        .collect())
}

/// State rows of one view (several if the engine keeps one per target).
pub async fn view_state(conn: &mut Conn, name: &str) -> Result<Vec<StateRow>> {
    Ok(view_states(conn)
        .await?
        .into_iter()
        .filter(|s| s.name == name)
        .collect())
}

/// Waits until every named view has state rows and all of them are `ready`. Fails fast if a
/// view stays in `error` for 5 s (with the engine's message) or if the engine dies.
pub async fn wait_ready(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
) -> Result<()> {
    wait_ready_grace(conn, engine, names, timeout, Some(Duration::from_secs(5))).await
}

/// Like [`wait_ready`] but tolerates `error` states until the timeout (for views expected to
/// recover, e.g. after a source table comes back or a definition is fixed).
pub async fn wait_ready_tolerant(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
) -> Result<()> {
    wait_ready_grace(conn, engine, names, timeout, None).await
}

async fn wait_ready_grace(
    conn: &mut Conn,
    engine: &mut Engine,
    names: &[&str],
    timeout: Duration,
    error_grace: Option<Duration>,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let mut error_since: HashMap<String, Instant> = HashMap::new();
    let mut no_row_since: HashMap<String, Instant> = HashMap::new();
    loop {
        engine.check_alive()?;
        let states = match view_states(conn).await {
            Ok(s) => s,
            // A transient read error spends budget, it does not abort the wait.
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        let mut pending = Vec::new();
        // The engine is up and has made other views ready (any view, not only these).
        let any_ready = states.iter().any(|s| s.state == "ready");
        for n in names {
            let mine: Vec<&StateRow> = states.iter().filter(|s| &s.name == n).collect();
            // A view the engine never registers while others are already ready: fail after 60 s
            // instead of the full timeout (look for errors in the engine log).
            if mine.is_empty() && any_ready && error_grace.is_some() {
                let since = *no_row_since
                    .entry(n.to_string())
                    .or_insert_with(Instant::now);
                if since.elapsed() >= Duration::from_secs(60) {
                    bail!(
                        "view {n} has no _lazyview_state row 60 s after other views became ready (engine never registered its definition?) — log {}",
                        engine.log_path.display()
                    );
                }
            }
            if let Some(e) = mine.iter().find(|s| s.state == "error") {
                let since = *error_since
                    .entry(n.to_string())
                    .or_insert_with(Instant::now);
                if let Some(g) = error_grace {
                    if since.elapsed() >= g {
                        bail!(
                            "view {n} is in state 'error': {}",
                            e.last_error.clone().unwrap_or_default()
                        );
                    }
                }
            } else {
                error_since.remove(*n);
            }
            if mine.is_empty() || mine.iter().any(|s| s.state != "ready") {
                let detail: Vec<String> = mine
                    .iter()
                    .map(|s| match &s.last_error {
                        Some(e) if s.state == "error" => format!(
                            "{}:error({})",
                            s.target.as_deref().unwrap_or("?"),
                            clip(e, 200)
                        ),
                        _ => format!("{}:{}", s.target.as_deref().unwrap_or("?"), s.state),
                    })
                    .collect();
                pending.push(format!(
                    "{n}={}",
                    if detail.is_empty() {
                        "<no state row>".to_string()
                    } else {
                        detail.join("/")
                    }
                ));
            }
        }
        if pending.is_empty() {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!(
                "views not ready within {timeout:?}: {} — log {}",
                pending.join(", "),
                engine.log_path.display()
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until the view's state rows are all `ready` with a definition hash different from
/// `old_hash` (i.e. a changed definition has been built and swapped in).
pub async fn wait_rebuilt(
    conn: &mut Conn,
    engine: &mut Engine,
    name: &str,
    old_hash: &Option<String>,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        let st = match view_state(conn, name).await {
            Ok(s) => s,
            // A transient read error spends budget, it does not abort the wait.
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        if !st.is_empty()
            && st
                .iter()
                .all(|s| s.state == "ready" && &s.definition_hash != old_hash)
        {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!(
                "view {name} not rebuilt with its new definition within {timeout:?}; state: {st:?}"
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Current definition hash of a view (first state row).
pub async fn current_hash(conn: &mut Conn, name: &str) -> Result<Option<String>> {
    Ok(view_state(conn, name)
        .await?
        .into_iter()
        .next()
        .and_then(|s| s.definition_hash))
}

/// Waits until the state row of definition `def_id` (whatever name the engine gave it, e.g. `#5`
/// for an unparsable definition) reaches `state`; returns that row.
pub async fn wait_def_state(
    conn: &mut Conn,
    engine: &mut Engine,
    def_id: u64,
    state: &str,
    timeout: Duration,
) -> Result<StateRow> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        let states = match view_states(conn).await {
            Ok(s) => s,
            // A transient read error spends budget, it does not abort the wait.
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        let rows: Vec<StateRow> = states
            .into_iter()
            .filter(|s| s.definition_id == Some(def_id))
            .collect();
        if let Some(s) = rows.iter().find(|s| s.state == state) {
            return Ok(s.clone());
        }
        if Instant::now() > deadline {
            bail!("definition #{def_id} did not reach state '{state}' within {timeout:?}; its state rows: {rows:?} — log {}", engine.log_path.display());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Waits until a view reaches `state` (any of its rows); returns that row.
pub async fn wait_state(
    conn: &mut Conn,
    engine: &mut Engine,
    name: &str,
    state: &str,
    timeout: Duration,
) -> Result<StateRow> {
    let deadline = Instant::now() + timeout;
    loop {
        engine.check_alive()?;
        let st = match view_state(conn, name).await {
            Ok(s) => s,
            // A transient read error spends budget, it does not abort the wait.
            Err(e) if is_transient(&e) && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }
            Err(e) => return Err(e),
        };
        if let Some(s) = st.into_iter().find(|s| s.state == state) {
            return Ok(s);
        }
        if Instant::now() > deadline {
            bail!(
                "view {name} did not reach state '{state}' within {timeout:?}; current: {:?}",
                view_state(conn, name).await?
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Asserts that every named view is `ready` right now.
pub async fn assert_ready(conn: &mut Conn, names: &[&str]) -> Result<()> {
    let states = view_states(conn).await?;
    for n in names {
        let mine: Vec<&StateRow> = states.iter().filter(|s| &s.name == n).collect();
        if mine.is_empty() {
            bail!("view {n} has no _lazyview_state row");
        }
        for s in mine {
            if s.state != "ready" {
                bail!(
                    "view {n} state is '{}' (last_error: {:?})",
                    s.state,
                    s.last_error
                );
            }
        }
    }
    Ok(())
}

/// Waits until `cond` returns true (polling every 50 ms).
pub async fn wait_until<F, Fut>(what: &str, timeout: Duration, mut cond: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<bool>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if cond().await? {
            return Ok(());
        }
        if Instant::now() > deadline {
            bail!("timed out after {timeout:?} waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
