use super::*;

/// The first build of a view (here: rebuilt at startup) swaps its table in off the binlog loop:
/// an open reader transaction on the old table holds its metadata lock, and the swap waits only
/// its own bounded wait (`LAZYVIEW_SWAP_LOCK_WAIT_S` × `Policy::SWAP` tries), then retries as a
/// new build — never on the loop, and never swaps in while the table is locked. On the loop,
/// every attempt stalled the engine for up to `lock_wait_timeout` (5 s), so no other view was
/// refreshed meanwhile (brutal `window_0ms`: 30 first builds serialized at 5-21 s per swap on
/// slow servers).
#[test]
fn reg_first_build_swap_does_not_stall_other_views() {
    scenario("reg_swap_bg", |ctx| async move {
        use std::time::Instant;
        let specs = vec![
            ViewSpec::new(
                "r_names",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            ),
            ViewSpec::new(
                "r_locked",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
        ];
        let (db, eng) = setup(&ctx, "reg_swap_bg", &[], &specs, &[]).await?;
        eng.stop().await?;
        // A reader transaction on r_locked: the swap's RENAME waits for its metadata lock.
        let mut reader = db.conn().await?;
        exec(&mut reader, "START TRANSACTION").await?;
        let _: Option<i64> = typed_first(&mut reader, "SELECT COUNT(*) FROM r_locked").await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_START_MODE", "rebuild")])?;
        let r = async {
            // Both builds started (their state rows say `building`), then r_names is live.
            let t = Instant::now();
            while eng.snapshot().full_rebuilds < 2 {
                ensure!(
                    t.elapsed() < t_ready(),
                    "the startup rebuilds did not start"
                );
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            wait_ready(&mut c, &mut eng, &["r_names"], t_ready()).await?;
            // While r_locked's swap waits, changes to r_names are applied promptly.
            for k in 0..6 {
                let id = 100 + k;
                let t = Instant::now();
                exec(
                    &mut c,
                    &format!("INSERT INTO customers VALUES ({id}, 'n{id}', NULL)"),
                )
                .await?;
                loop {
                    let n: Option<i64> = typed_first(
                        &mut c,
                        format!("SELECT COUNT(*) FROM r_names WHERE id = {id}"),
                    )
                    .await?;
                    if n == Some(1) {
                        break;
                    }
                    ensure!(
                        t.elapsed() < Duration::from_secs(3),
                        "insert {k}: r_names not refreshed within 3 s while r_locked's swap waits \
                         for a metadata lock; metrics {}",
                        fmt_snapshot(&eng.snapshot())
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                tokio::time::sleep(Duration::from_millis(700)).await;
            }
            // Guard R1 on #254 (findings 1 and 2): a swap that only spent its own bounded wait on
            // the table's metadata lock is transient contention, not a build failure. The row
            // keeps the state a view has while it is being built and names the reason; `error` is
            // the state that tells an operator to act (the stall alerts key on it), so the flap
            // building -> error -> building is the regression this watches for. The reader stays
            // open, so the swap gives up again every cycle and writes the state again: the poll
            // sees every state the row holds, not one lucky snapshot.
            let mut saw_reason = false;
            let mut states: Vec<String> = Vec::new();
            let t = Instant::now();
            while !(saw_reason && states.last().is_some_and(|s| s == "building"))
                && t.elapsed() < Duration::from_secs(25)
            {
                let (state, last_error) = state_detail(&mut c, "r_locked")
                    .await?
                    .map(|(s, _, _, e)| (s, e.unwrap_or_default()))
                    .unwrap_or_else(|| ("missing".to_string(), String::new()));
                if !states.last().is_some_and(|last| *last == state) {
                    states.push(state.clone());
                }
                if state == "building" && last_error.contains("lock") {
                    saw_reason = true;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            ensure!(
                !states.iter().any(|s| s == "error"),
                "r_locked's state flapped to `error` while its swap only waited for the table \
                 lock; transient contention keeps the row `building` with the reason: {states:?}"
            );
            ensure!(
                saw_reason,
                "r_locked never said `building` with the table lock in last_error while its swap \
                 waited for it: {states:?}"
            );
            ensure!(
                states.last().is_some_and(|s| s == "building"),
                "r_locked must stay `building` while its swap waits for the table lock: {states:?}"
            );
            ensure!(
                !states.iter().any(|s| s == "ready"),
                "r_locked went live although its table is locked: {states:?}"
            );
            Ok(())
        }
        .await;
        exec(&mut reader, "COMMIT").await?;
        r?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(&ctx, &mut eng, &db, &specs, "after the swap").await?;
        eng.stop().await
    });
}

/// Guard R1 (finding 3, PR #254): "left to the next rebuild" has to be a promise the engine
/// keeps for an INCREMENTAL view too, not only for a full-mode one. A rebuild whose swap cannot
/// get the table lock (an open reader holds it) leaves the live version serving and refreshing —
/// the binlog loop never waits for that lock (N-02), and its own tail swap is one short try — and
/// once the reader lets go, the engine re-arms the abandoned swap on its own: the requested
/// rebuild lands and `_lazyview.rebuild` is cleared with no further kick. Without the re-arm the
/// flag stays set forever and the view keeps its old table indefinitely.
#[test]
fn reg_swap_bg_abandoned_swap_rearms_the_next_rebuild() {
    scenario("reg_swap_bg", |ctx| async move {
        use std::time::Instant;
        let specs = vec![
            ViewSpec::new(
                "r_names",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            ),
            ViewSpec::new(
                "r_locked",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
        ];
        let (db, eng) = setup(&ctx, "reg_swap_bg", &[], &specs, &[]).await?;
        eng.stop().await?;
        // An open reader transaction on r_locked's table: its rebuild's swap waits for the
        // metadata lock, spends its own short bounded wait and gives up (off the loop).
        let mut reader = db.conn().await?;
        exec(&mut reader, "START TRANSACTION").await?;
        let _: Option<i64> = typed_first(&mut reader, "SELECT COUNT(*) FROM r_locked").await?;
        let mut c = db.conn().await?;
        let mut eng = ctx.start_engine(
            &db,
            &[
                ("LAZYVIEW_SWAP_LOCK_WAIT_S", "1"),
                ("LAZYVIEW_FULL_REFRESH_MS", "300"),
            ],
        )?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        request_rebuild(&mut c, "r_locked").await?;
        let r = async {
            // The swap is waiting, and cannot succeed while the reader is open: the OTHER view is
            // refreshed promptly all the same. A loop that waits for the table lock stalls these.
            for k in 0..4 {
                let id = 300 + k;
                let t = Instant::now();
                exec(
                    &mut c,
                    &format!("INSERT INTO customers VALUES ({id}, 'm{id}', NULL)"),
                )
                .await?;
                loop {
                    let n: Option<i64> = typed_first(
                        &mut c,
                        format!("SELECT COUNT(*) FROM r_names WHERE id = {id}"),
                    )
                    .await?;
                    if n == Some(1) {
                        break;
                    }
                    ensure!(
                        t.elapsed() < Duration::from_secs(3),
                        "insert {k}: r_names was not refreshed within 3 s while r_locked's swap \
                         waited for its table lock: {}",
                        fmt_snapshot(&eng.snapshot())
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            // A transient lock wait must not look like an operator's problem (lv-super's note).
            let (state, last_error) = state_detail(&mut c, "r_locked")
                .await?
                .map(|(s, _, _, e)| (s, e.unwrap_or_default()))
                .unwrap_or_else(|| ("missing".to_string(), String::new()));
            ensure!(
                state != "error",
                "r_locked says {state:?} (last_error {last_error:?}) for a swap that only waited \
                 on a reader's table lock"
            );
            ensure!(
                rebuild_flag(&mut c, "r_locked").await? != 0,
                "the rebuild flag was cleared although r_locked's swap never got the table lock"
            );
            Ok(())
        }
        .await;
        // The reader lets go of the table. Nothing else touches the view: the engine itself has
        // to ask for the swap again (the re-armed rebuild), or the flag stays set forever.
        exec(&mut reader, "COMMIT").await?;
        r?;
        wait_rebuild_served(&mut c, &mut eng, "r_locked").await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "after the reader released the table",
        )
        .await?;
        eng.stop().await
    });
}

/// `(state, mode, detail, last_error)` of a view's state row on target `source`.
async fn state_detail(
    c: &mut mysql_async::Conn,
    name: &str,
) -> Result<Option<(String, String, Option<String>, Option<String>)>> {
    typed_exec_first(
        c,
        "SELECT CAST(state AS CHAR), CAST(mode AS CHAR), CAST(detail AS CHAR), CAST(last_error AS CHAR) \
             FROM _lazyview_state WHERE name = ? AND target = 'source'",
        (name,),
    )
    .await
}

/// A view reading a view in `error` said `waiting for upstream view ... to be built` forever,
/// so the troubleshooting query `WHERE state = 'error'` missed it. It is in `error` naming the
/// upstream's error (down a chain of such views) and is built once the upstream is fixed.
#[test]
fn reg_dependent_of_view_in_error_shows_its_error() {
    scenario("reg_blocked", |ctx| async move {
        let db = ctx.new_db("reg_blocked").await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_bad', 'SELECT nope FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_dep', 'SELECT * FROM lz_bad')",
                "INSERT INTO _lazyview (name, definition) VALUES ('lz_dep2', 'SELECT * FROM lz_dep')",
            ],
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        let bad = wait_state(&mut c, &mut eng, "lz_bad", "error", t_ready()).await?;
        let bad_err = bad.last_error.clone().unwrap_or_default();
        let dep = wait_state(&mut c, &mut eng, "lz_dep", "error", t_ready()).await?;
        let dep_err = dep.last_error.clone().unwrap_or_default();
        ensure!(
            dep_err == format!("upstream view `lz_bad` is in error: {bad_err}"),
            "lz_dep: {dep:?}"
        );
        let dep2 = wait_state(&mut c, &mut eng, "lz_dep2", "error", t_ready()).await?;
        ensure!(
            dep2.last_error.as_deref()
                == Some(format!("upstream view `lz_dep` is in error: {dep_err}").as_str()),
            "lz_dep2: {dep2:?}"
        );
        let listed: i64 = typed_first(
            &mut c,
            "SELECT COUNT(*) FROM _lazyview_overview WHERE state = 'error'",
        )
        .await?
        .unwrap_or(0);
        ensure!(
            listed == 3,
            "the troubleshooting query lists {listed} of the 3 views in error"
        );
        // Fixing the upstream builds the whole chain.
        let bad = definition_id(&mut c, "lz_bad").await?;
        exec(
            &mut c,
            &format!(
                "UPDATE `_lazyview` SET definition = 'SELECT id, status FROM orders' WHERE `id` = {bad}"
            ),
        )
        .await?;
        let specs = vec![
            ViewSpec::new("lz_bad", Kind::Incremental, "SELECT id, status FROM orders"),
            ViewSpec::new("lz_dep", Kind::Chained, "SELECT * FROM lz_bad"),
            ViewSpec::new("lz_dep2", Kind::Chained, "SELECT * FROM lz_dep"),
        ];
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "chain after the upstream was fixed",
        )
        .await?;
        eng.stop().await
    });
}

/// A `_lazyview_state` row deleted by hand left the view maintained but with NULL state, mode
/// and detail in `_lazyview_overview` until a restart, which then rebuilt it. The row is written
/// back as it was (also for a view in error), and it still lets a restart resume the table.
#[test]
fn reg_deleted_state_row_is_written_back() {
    scenario("reg_state_row", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_state",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_state_row", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec(
            &mut c,
            "INSERT INTO _lazyview (name, definition) VALUES ('r_err', 'SELECT nope FROM orders')",
        )
        .await?;
        wait_state(&mut c, &mut eng, "r_err", "error", t_ready()).await?;
        let before = (
            state_detail(&mut c, "r_state").await?,
            state_detail(&mut c, "r_err").await?,
        );
        exec(
            &mut c,
            "DELETE FROM _lazyview_state WHERE name IN ('r_state', 'r_err')",
        )
        .await?;
        let t = std::time::Instant::now();
        loop {
            let now = (
                state_detail(&mut c, "r_state").await?,
                state_detail(&mut c, "r_err").await?,
            );
            if now == before {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(15),
                "deleted state rows not written back within 15 s: {now:?}, before {before:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let managed: Option<i64> = typed_first(
            &mut c,
            "SELECT managed FROM _lazyview_state WHERE name = 'r_state'",
        )
        .await?;
        ensure!(
            managed == Some(1),
            "r_state's table is no longer recorded as lazyview's"
        );
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the state row came back").await?;
        // The row written back is a valid resume record: a restart resumes the table.
        eng.stop().await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        let d = eng.snapshot();
        ensure!(
            d.full_rebuilds == 0,
            "restart rebuilt instead of resuming: {}",
            fmt_snapshot(&d)
        );
        verify(&ctx, &mut eng, &db, &specs, "resumed").await?;
        eng.stop().await
    });
}

/// A view whose build fails keeps the mode and detail it was planned with, the reason in
/// `last_error` (here: a table of that name that lazyview does not manage); only a definition
/// that cannot be planned shows mode `unknown`.
#[test]
fn reg_failed_build_keeps_planned_mode() {
    scenario("reg_err_mode", |ctx| async move {
        let db = ctx.new_db("reg_err_mode").await?;
        let mut c = db.conn().await?;
        exec_all(&mut c, DDL).await?;
        ensure_lazyview_table(&mut c).await?;
        exec_all(
            &mut c,
            &[
                "CREATE TABLE manual (id INT PRIMARY KEY)",
                "INSERT INTO _lazyview (name, definition) VALUES ('manual', 'SELECT id, status FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_unplannable', 'SELECT nope FROM orders')",
            ],
        )
        .await?;
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_state(&mut c, &mut eng, "manual", "error", t_ready()).await?;
        let (state, mode, detail, err) = state_detail(&mut c, "manual")
            .await?
            .context("no state row for manual")?;
        ensure!(
            (state.as_str(), mode.as_str(), detail.as_deref())
                == ("error", "incremental", Some("spj")),
            "manual: {state} {mode} {detail:?}"
        );
        let err = err.unwrap_or_default();
        ensure!(
            err.contains("refusing to replace existing base table")
                && err.contains("that lazyview does not manage"),
            "manual: last_error {err}"
        );
        wait_state(&mut c, &mut eng, "r_unplannable", "error", t_ready()).await?;
        let row = state_detail(&mut c, "r_unplannable").await?;
        ensure!(
            matches!(&row, Some((s, m, None, Some(_))) if s == "error" && m == "unknown"),
            "r_unplannable: {row:?}"
        );
        eng.stop().await
    });
}

/// `_lazyview.rebuild` of view `name` (0 once lazyview has done the requested rebuild).
async fn rebuild_flag(c: &mut mysql_async::Conn, name: &str) -> Result<i64> {
    // The definitions table keeps no view name of its own (the service never writes it): the row
    // is found through the state row, which carries the name the definition resolves to.
    typed_exec_first(
        c,
        "SELECT rebuild FROM _lazyview WHERE id = (SELECT definition_id FROM `_lazyview_state` WHERE `name` = ? LIMIT 1)",
        (name,),
    )
    .await?
    .context("no _lazyview row")
}

/// The `_lazyview_state.rebuild_served` value of view `name`: the lowest among its targets, since
/// a target not built from the request yet keeps the request pending.
async fn rebuild_served(c: &mut mysql_async::Conn, name: &str) -> Result<i64> {
    typed_exec_first(
        c,
        "SELECT MIN(rebuild_served) FROM _lazyview_state WHERE name = ?",
        (name,),
    )
    .await?
    .context("no _lazyview_state row")
}

/// Waits until lazyview acknowledged the rebuild request: it never writes `_lazyview` (the counter
/// is the management user's), so it is served when `_lazyview_state.rebuild_served` has reached
/// `_lazyview.rebuild`.
async fn wait_rebuild_served(
    c: &mut mysql_async::Conn,
    eng: &mut Engine,
    name: &str,
) -> Result<()> {
    let t = std::time::Instant::now();
    loop {
        if rebuild_flag(c, name).await? == rebuild_served(c, name).await? {
            return Ok(());
        }
        eng.check_alive()?;
        ensure!(
            t.elapsed() < t_ready(),
            "{name}: rebuild request not served after {:?}",
            t_ready()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// There was no way to rebuild one view (hand edits of its table are not noticed, and the
/// tricks a user tries, `SET definition = definition`, do nothing). `UPDATE _lazyview SET
/// rebuild = rebuild + 1` (the management user's statement) rebuilds that view online and is
/// acknowledged in `_lazyview_state.rebuild_served`, also when set while lazyview is stopped; the
/// other views are left alone.
#[test]
fn reg_rebuild_flag_rebuilds_one_view() {
    scenario("reg_rebuild_flag", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_counts",
                Kind::Incremental,
                "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
            ),
            ViewSpec::new(
                "r_names",
                Kind::Incremental,
                "SELECT id, name FROM customers",
            ),
        ];
        let (db, mut eng) = setup(&ctx, "reg_rebuild_flag", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        ensure!(
            rebuild_flag(&mut c, "r_counts").await? == 0,
            "rebuild set without a request"
        );
        for restart in [false, true] {
            // A hand edit lazyview cannot notice.
            exec(&mut c, "DELETE FROM r_counts WHERE customer_id = 1").await?;
            let updated_sql =
                "SELECT CAST(updated_at AS CHAR) FROM _lazyview WHERE id = (SELECT definition_id FROM `_lazyview_state` WHERE `name` = 'r_counts' LIMIT 1)";
            let updated: Option<String> = if restart {
                // Set while lazyview is stopped: the restart rebuilds instead of resuming.
                let running = eng;
                running.stop().await?;
                request_rebuild(&mut c, "r_counts").await?;
                let u = typed_first(&mut c, updated_sql).await?;
                eng = ctx.start_engine(&db, &[])?;
                u
            } else {
                request_rebuild(&mut c, "r_counts").await?;
                typed_first(&mut c, updated_sql).await?
            };
            let before = eng.snapshot();
            wait_rebuild_served(&mut c, &mut eng, "r_counts").await?;
            wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
            let what = if restart { "restart" } else { "running" };
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("rebuild flag ({what})"),
            )
            .await?;
            let d = eng.snapshot().since(&before);
            ensure!(
                d.full_rebuilds == 1,
                "{what}: expected one rebuild (r_counts only): {}",
                fmt_snapshot(&d)
            );
            let after: Option<String> = typed_first(
                &mut c,
                "SELECT CAST(updated_at AS CHAR) FROM _lazyview WHERE id = (SELECT definition_id FROM `_lazyview_state` WHERE `name` = 'r_counts' LIMIT 1)",
            )
            .await?;
            ensure!(
                after == updated,
                "{what}: clearing the flag moved updated_at ({updated:?} -> {after:?})"
            );
            exec(&mut c, "INSERT INTO orders VALUES (20 + 1, 1, 'new')").await?;
            exec(&mut c, "DELETE FROM orders WHERE id = 21").await?;
            verify(
                &ctx,
                &mut eng,
                &db,
                &specs,
                &format!("after the rebuild ({what})"),
            )
            .await?;
        }
        eng.stop().await
    });
}

/// The startup read of the service's own state rows (`_lazyview_state`, the served rebuild
/// counters) decides whether a request is still pending, so a transient failure of it must never
/// come out as an empty default: every view that ever had a request would start rebuilding on a
/// blip, with no WARN. It goes through `retry_src` — the one retry policy at the network edge,
/// WARNing the cause of every spent try — and a read that keeps failing keeps the whole start
/// failed, so whoever supervises the process retries the start rather than half of it.
#[test]
fn reg_failed_state_read_never_asks_for_a_rebuild() {
    scenario("reg_state_read_fail", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_counts",
            Kind::Incremental,
            "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_state_read_fail", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        // A served request: the counter is above 0 and acknowledged — the knowledge a failed
        // read must not lose.
        request_rebuild(&mut c, "r_counts").await?;
        wait_rebuild_served(&mut c, &mut eng, "r_counts").await?;
        ensure!(
            rebuild_flag(&mut c, "r_counts").await? == 1
                && rebuild_served(&mut c, "r_counts").await? == 1,
            "the scenario stands on one served request"
        );
        eng.stop().await?;

        // The blip: the first try of the startup read fails, the retry succeeds. The proof it
        // was retried and WARNed, not defaulted (the WARN text of `retry_src`): the WARN lands
        // before the engine gets past the read, and `wait_log_line` checks the engine is alive
        // every turn — if the start died instead, it says so.
        let mut eng = ctx.start_engine(&db, &[("LAZYVIEW_TEST_FAIL_STATE_READ", "1")])?;
        eng.wait_log_line(
            "reading _lazyview_state (the served rebuild counters) failed (try 1 of ",
            t_ready(),
        )
        .await?;
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        // The start completed with the view live, not rebuilt: this line closes `load_slots`,
        // so once it is in the log every rebuild decision of this start is made — and the
        // counter says none was taken. The fail-before: defaulted to empty, every slot starts
        // at `rebuild_served = 0`, the counter of 1 looks unserved, and the view rebuilds with
        // nothing asked of it (the line appears only after that build, and it is counted).
        eng.wait_log_line("ready: 1 view(s) live", t_ready())
            .await?;
        let d = eng.snapshot();
        let log = std::fs::read_to_string(&eng.log_path).unwrap_or_default();
        ensure!(
            !log.contains("rebuild requested (_lazyview.rebuild =") && d.full_rebuilds == 0,
            "a transient failure of the state read asked for a rebuild: {}",
            fmt_snapshot(&d)
        );
        eng.stop().await?;

        // The lasting failure: the start stays an error (the supervisor retries the whole of it),
        // and the acknowledged request is untouched meanwhile.
        let eng = ctx.start_engine(&db, &[("LAZYVIEW_TEST_FAIL_STATE_READ", "999999")])?;
        let out = eng.wait_finished(Duration::from_secs(60)).await?;
        ensure!(
            matches!(&out, Err(m) if m.contains("LAZYVIEW_TEST_FAIL_STATE_READ")),
            "a start whose state read keeps failing must fail naming that read, got {out:?}"
        );
        ensure!(
            rebuild_served(&mut c, "r_counts").await? == 1,
            "a start that failed kept its hands off the acknowledged request"
        );
        Ok(())
    });
}

/// What `serve_rebuild` acknowledges and what the startup minimum reads must be the same set of
/// rows: the definition's rows under its **current** name and **current** targets. A rename's
/// leftover (the state row of the old name, `managed`, kept while its table may still be needed)
/// and a retarget's leftover (a row of a target the definition dropped) keep the value they were
/// written at; if the minimum read them, the view would be asked to rebuild on every start —
/// forever, since nothing acknowledges a row the definition no longer has.
#[test]
fn reg_leftover_state_rows_do_not_pin_a_rebuild() {
    scenario("reg_leftover_rows", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_counts",
            Kind::Incremental,
            "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_leftover_rows", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        // A rename through the definition (the management user's statement): the view comes up
        // under the new name, the old-name state row stays as the rename left it.
        let id = definition_id(&mut c, "r_counts").await?;
        let renamed = ViewSpec::new(
            "r_counts_v2",
            Kind::Incremental,
            "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
        );
        exec(
            &mut c,
            &format!(
                "UPDATE _lazyview SET definition = {} WHERE id = {id}",
                q(&renamed.definition)
            ),
        )
        .await?;
        wait_ready(&mut c, &mut eng, &["r_counts_v2"], t_ready()).await?;
        // A served request under the new name: the counter is 1 and acknowledged there.
        request_rebuild(&mut c, "r_counts_v2").await?;
        wait_rebuild_served(&mut c, &mut eng, "r_counts_v2").await?;
        ensure!(
            rebuild_flag(&mut c, "r_counts_v2").await? == 1,
            "one request stands"
        );
        eng.stop().await?;
        // The second leftover, as a retarget leaves it: a row of the current definition under a
        // target it no longer writes, at the old lower value (the minimum must read the same set
        // of rows `serve_rebuild` updates, whatever left the row behind).
        exec(
            &mut c,
            &format!(
                "INSERT INTO `_lazyview_state` \
                 (`name`, `target`, `definition_id`, `definition_hash`, `mode`, `state`, `managed`, `rebuild_served`) \
                 VALUES ('r_counts_v2', 'r_counts_dropped', {id}, 'ffffffffffffffff', 'i', 'ready', 1, 0) \
                 ON DUPLICATE KEY UPDATE `rebuild_served` = 0, `managed` = 1"
            ),
        )
        .await?;
        // Restart twice. The fail-before is the first: the old-name row still carries request 0
        // while the counter is 1, so a minimum over every row of the definition asks for a
        // rebuild the moment the engine starts (and the `managed` leftover is never cleaned, so
        // the second restart shows it is a loop, not one build). Fixed, both restarts just
        // resume: the acknowledged rows cover the counter exactly.
        for round in 1..=2 {
            let mut eng = ctx.start_engine(&db, &[])?;
            wait_ready(&mut c, &mut eng, &["r_counts_v2"], t_ready()).await?;
            // The line that closes `load_slots`: once it is in the log, every rebuild decision
            // of this start is made — and none was taken. The fail-before: the minimum over
            // every row of the definition reads the leftover's request 0 against the counter 1,
            // asks for a rebuild here (the line names it, and it is counted), on both rounds —
            // the leftover is `managed`, so nothing cleans it.
            eng.wait_log_line("ready: 1 view(s) live", t_ready())
                .await?;
            let d = eng.snapshot();
            let log = std::fs::read_to_string(&eng.log_path).unwrap_or_default();
            ensure!(
                !log.contains("rebuild requested (_lazyview.rebuild =") && d.full_rebuilds == 0,
                "restart {round}: a leftover state row pinned the view to a rebuild: {}",
                fmt_snapshot(&d)
            );
            eng.stop().await?;
        }
        // The engine is still healthy, and the request stays served.
        let mut eng = ctx.start_engine(&db, &[])?;
        wait_ready(&mut c, &mut eng, &["r_counts_v2"], t_ready()).await?;
        eng.check_alive()?;
        eng.stop().await
    });
}

/// `TRUNCATE` of a view's table (the lazy user's "reset") went unnoticed: the view stayed
/// `ready` and groups that were not touched again were missing for good. lazyview never
/// truncates its tables, so it rebuilds the view online.
#[test]
fn reg_truncated_view_table_is_rebuilt() {
    scenario("reg_truncate_mv", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_counts",
            Kind::Incremental,
            "SELECT customer_id, COUNT(*) AS n FROM orders GROUP BY customer_id",
        )];
        let (db, mut eng) = setup(&ctx, "reg_truncate_mv", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot();
        exec_all(
            &mut c,
            &[
                "TRUNCATE r_counts",
                "INSERT INTO orders VALUES (14, 1, 'new')",
            ],
        )
        .await?;
        eventually_equal(
            &ctx,
            &mut eng,
            &db,
            &std::collections::HashMap::new(),
            &specs,
            t_ready(),
            "after TRUNCATE of the view table",
        )
        .await?;
        let d = eng.snapshot().since(&before);
        ensure!(
            d.full_rebuilds >= 1,
            "the truncated view converged without a rebuild?: {}",
            fmt_snapshot(&d)
        );
        wait_ready(&mut c, &mut eng, &names(&specs), t_ready()).await?;
        exec(&mut c, "UPDATE orders SET customer_id = 3 WHERE id = 10").await?;
        verify(&ctx, &mut eng, &db, &specs, "after the rebuild").await?;
        eng.stop().await
    });
}

/// Two definitions whose view names differ only in letter case (`Lz_Case`, `lz_case`): the
/// second one was shown as `#<id>` with "name `lz_case` already used by row #N" (`_lazyview.name`
/// ignores case, the tables do not). It keeps its name, the error names both rows and how to
/// remove this one, and the first view is unaffected (its state row is not overwritten).
#[test]
fn reg_case_only_name_clash() {
    scenario("reg_case_clash", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "Lz_Case",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_case_clash", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        let first = definition_id(&mut c, "Lz_Case").await?;
        exec(
            &mut c,
            "INSERT INTO _lazyview (definition) VALUES ('CREATE VIEW lz_case AS SELECT id, customer_id FROM orders')",
        )
        .await?;
        let second: u64 = typed_first(&mut c, "SELECT LAST_INSERT_ID()")
            .await?
            .context("no id")?;
        let row = wait_state(&mut c, &mut eng, "lz_case", "error", t_ready()).await?;
        let expected = format!(
            "name `lz_case` is already used by row #{first} as `Lz_Case`: view names that differ \
             only in letter case are the same name to lazyview; rename this view, or remove this \
             row with DELETE FROM _lazyview WHERE id = {second}"
        );
        ensure!(
            row.last_error.as_deref() == Some(expected.as_str()),
            "lz_case: {row:?}"
        );
        let shown: Vec<(u64, String, Option<String>)> = typed_rows(&mut c, "SELECT id, CAST(name AS CHAR), CAST(state AS CHAR) FROM _lazyview_overview ORDER BY id")
            .await?;
        ensure!(
            shown
                == vec![
                    (first, "Lz_Case".to_string(), Some("ready".to_string())),
                    (second, "lz_case".to_string(), Some("error".to_string())),
                ],
            "overview: {shown:?}"
        );
        exec(&mut c, "INSERT INTO orders VALUES (14, 2, 'new')").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "Lz_Case next to the clashing row",
        )
        .await?;
        // Removed as the message says: only lz_case's row goes.
        exec(
            &mut c,
            &format!("DELETE FROM _lazyview WHERE id = {second}"),
        )
        .await?;
        let t = std::time::Instant::now();
        loop {
            let n: Option<i64> = typed_first(
                &mut c,
                "SELECT COUNT(*) FROM _lazyview_state WHERE name = 'lz_case'",
            )
            .await?;
            if n == Some(0) {
                break;
            }
            ensure!(
                t.elapsed() < Duration::from_secs(15),
                "lz_case's state row stayed after its definition was deleted"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_ready(&mut c, &["Lz_Case"]).await?;
        eng.stop().await
    });
}

/// A view named like the source table it reads (`orders`) said "view `orders` cannot read
/// itself", which reads like recursion. It gets the documented refusal for a table lazyview
/// does not manage, other views over `orders` keep working, and a definition that really
/// reads its own view still gets "cannot read itself".
#[test]
fn reg_view_named_like_its_source_table() {
    scenario("reg_self_named", |ctx| async move {
        let specs = vec![ViewSpec::new(
            "r_orders",
            Kind::Incremental,
            "SELECT id, status FROM orders",
        )];
        let (db, mut eng) = setup(&ctx, "reg_self_named", &[], &specs, &[]).await?;
        let mut c = db.conn().await?;
        exec_all(
            &mut c,
            &[
                "INSERT INTO _lazyview (name, definition) VALUES ('orders', 'SELECT id FROM orders')",
                "INSERT INTO _lazyview (name, definition) VALUES ('r_self', 'SELECT * FROM r_self')",
            ],
        )
        .await?;
        let row = wait_state(&mut c, &mut eng, "orders", "error", t_ready()).await?;
        let expected = format!(
            "refusing to replace existing base table `{}`.`orders` on target source that \
             lazyview does not manage (the view reads it: give the view another name)",
            db.name
        );
        ensure!(
            row.last_error.as_deref() == Some(expected.as_str()),
            "orders: {row:?}"
        );
        let row = wait_state(&mut c, &mut eng, "r_self", "error", t_ready()).await?;
        ensure!(
            row.last_error.as_deref() == Some("view `r_self` cannot read itself"),
            "r_self: {row:?}"
        );
        exec(&mut c, "UPDATE orders SET status = 'paid' WHERE id = 10").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "a view over orders next to them",
        )
        .await?;
        let n: Option<i64> = typed_first(&mut c, "SELECT COUNT(*) FROM orders").await?;
        ensure!(
            n == Some(4),
            "the source table orders was touched: {n:?} rows"
        );
        eng.stop().await
    });
}
