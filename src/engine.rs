//! Orchestration: startup, background builds, the binlog loop, flushes and online swaps.
//!
//! Each view definition (`_lazyview` row) is a [`Slot`] with up to two versions:
//! * `live` — the table readers see, maintained incrementally from the binlog;
//! * `next` — a new version (new definition, targets, or a rebuild after DDL) being built in
//!   the background into shadow tables. Changes arriving meanwhile are buffered for it, applied
//!   once the build finishes, and then the shadow is swapped in atomically (`RENAME TABLE`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use mysql_async::prelude::*;
use mysql_async::{Conn, Opts, Value};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::accumulate::{feed, feed_cascade, Fed, UnitSet, ViewDirty};
use crate::analyze::{self, Mode, ViewPlan};
use crate::binlog::{self, BinlogReader, Flavor, Item, Position};
use mysql_async::binlog::row::BinlogRow;
use crate::catalog::{Catalog, TableInfo, TableRef};
use crate::config::{Config, MissingBinlog, StartMode};
use crate::coord;
use crate::datum::{Datum, Key};
use crate::meta::{self, StateUpdate, SOURCE_TARGET};
use crate::metrics::Metrics;
use crate::mv::{self, BuildTarget, Layout};

/// Runs the service until `shutdown` turns true (or, for `once` / `sync=false`, until done).
pub async fn run(cfg: Config, metrics: Arc<Metrics>, mut shutdown: watch::Receiver<bool>) -> Result<()> {
    Metrics::set(&metrics.gauges.heartbeat_unix, crate::metrics::unix_now());
    let health = cfg.health_addr.map(|addr| tokio::spawn(crate::health::serve(addr, metrics.clone())));
    let result = run_engine(cfg, metrics, &mut shutdown).await;
    if let Some(h) = health {
        h.abort();
    }
    result
}

async fn run_engine(cfg: Config, metrics: Arc<Metrics>, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
    let user = binlog::replica_opts(&cfg.dsn).ok().and_then(|o| o.user().map(str::to_string)).unwrap_or_else(|| "lazyview".into());
    let schema = binlog::replica_opts(&cfg.dsn).ok().and_then(|o| o.db_name().map(str::to_string)).unwrap_or_default();
    let r = async {
        let Some((mut engine, start)) = Engine::start(cfg, metrics, shutdown).await? else {
            return Ok(());
        };
        let Some(start) = start else {
            return Ok(());
        };
        engine.stream(start, shutdown).await
    }
    .await;
    r.map_err(|e| with_access_hint(e, &user, &schema))
}

fn server_code(e: &anyhow::Error) -> Option<u16> {
    e.chain().find_map(|c| match c.downcast_ref::<mysql_async::Error>() {
        Some(mysql_async::Error::Server(se)) => Some(se.code),
        _ => None,
    })
}

/// Privilege errors get the exact GRANT statements appended.
fn with_access_hint(e: anyhow::Error, user: &str, schema: &str) -> anyhow::Error {
    match server_code(&e) {
        Some(1045) if !format!("{e:#}").contains("REPLICATION") => e.context(format!(
            "access denied for user '{user}': check LAZYVIEW_USER / LAZYVIEW_PASSWORD (or the DSN) and that the user may connect from this host"
        )),
        Some(1044 | 1045 | 1142 | 1143 | 1227 | 1370) => e.context(format!(
            "the lazyview database user lacks privileges; as an admin run:\n  \
             GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO '{user}'@'%';\n  \
             GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX, CREATE VIEW, SHOW VIEW ON `{schema}`.* TO '{user}'@'%';\n  \
             (plus SELECT on any other schema your views read)"
        )),
        Some(1049) => e.context(format!("database `{schema}` does not exist; create it first: CREATE DATABASE `{schema}`")),
        _ => e,
    }
}

/// Connects, waiting (with backoff) while the server is unreachable. Authentication and
/// unknown-database errors are fatal. `Ok(None)` when shutdown was requested.
async fn connect_wait(opts: &Opts, what: &str, metrics: &Metrics, shutdown: &mut watch::Receiver<bool>) -> Result<Option<Conn>> {
    let mut backoff = Duration::from_secs(1);
    let mut waited = false;
    loop {
        match connect(opts).await {
            Ok(c) => {
                if waited {
                    info!("{what} at {}:{} is reachable", opts.ip_or_hostname(), opts.tcp_port());
                }
                return Ok(Some(c));
            }
            Err(e) if matches!(server_code(&e), Some(1044 | 1045 | 1049 | 1227)) => return Err(e.context(format!("connecting to {what}"))),
            Err(e) => {
                warn!("waiting for {what} at {}:{}: {}; retrying in {backoff:?}", opts.ip_or_hostname(), opts.tcp_port(), e.root_cause());
                waited = true;
            }
        }
        metrics.beat();
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = shutdown.changed() => if *shutdown.borrow() { return Ok(None) },
        }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

struct Target {
    opts: Opts,
    schema: String,
    conn: Option<Conn>,
}

struct Version {
    plan: Arc<ViewPlan>,
    layout: Layout,
    name: String,
    targets: Vec<String>,
    dirty: ViewDirty,
    /// Source schema changed under it: kept readable but no longer maintained.
    frozen: bool,
}

struct Next {
    plan: Arc<ViewPlan>,
    name: String,
    targets: Vec<String>,
    dirty: ViewDirty,
    generation: u64,
    task: JoinHandle<()>,
    done: Option<Result<Layout>>,
    /// Only re-creates these targets of an unchanged live version.
    partial: bool,
}

struct Slot {
    def_id: u64,
    name: String,
    definition: String,
    hash: String,
    targets: Vec<String>,
    live: Option<Version>,
    next: Option<Next>,
    error: Option<String>,
    /// (name, target) tables to drop once `next` is swapped in.
    retired: Vec<(String, String)>,
    /// Tables the definition reads (to retry a failed definition when they change/appear).
    wanted: Vec<TableRef>,
}

#[derive(Clone, Copy, Debug)]
struct RouteT {
    slot: usize,
    next: bool,
    kind: RouteKind,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum RouteKind {
    /// Rows of this plan alias.
    Alias(usize),
    /// Maintained by full rebuilds: any change marks the view dirty.
    All,
    /// Parent of a cascading FK into a table the view reads (plan cascade index).
    Cascade(usize),
}

type XaRow = (TableRef, Option<BinlogRow>, Option<BinlogRow>);

struct BuildDone {
    def_id: u64,
    generation: u64,
    result: Result<Layout>,
}

struct Engine {
    cfg: Config,
    metrics: Arc<Metrics>,
    src_opts: Opts,
    schema: String,
    flavor: Flavor,
    /// Source reads: lookups, refresh SELECTs, catalog.
    src: Conn,
    /// Metadata writes on the source.
    meta: Conn,
    targets: BTreeMap<String, Target>,
    catalog: Catalog,
    slots: Vec<Slot>,
    /// Tables we have created and own: (name, target) → definition hash.
    owned: HashMap<(String, String), String>,
    routes: HashMap<TableRef, Arc<Vec<RouteT>>>,
    table_ids: HashMap<u64, Option<Arc<Vec<RouteT>>>>,
    table_names: HashMap<u64, TableRef>,
    /// XA transaction being read: (xid, position before it, rows of routed tables).
    xa_current: Option<(String, Position, Vec<XaRow>)>,
    /// Prepared XA transactions awaiting XA COMMIT / ROLLBACK.
    xa_pending: HashMap<String, (Position, Vec<XaRow>)>,
    defs_dirty: bool,
    replan: HashSet<usize>,
    boundary: Position,
    persisted: Position,
    last_persist: Instant,
    in_txn: bool,
    /// The current transaction touched something besides our own metadata tables.
    txn_foreign: bool,
    persist_needed: bool,
    first_dirty: Option<Instant>,
    last_gtid: Option<String>,
    generation: u64,
    build_tx: mpsc::UnboundedSender<BuildDone>,
    build_rx: Option<mpsc::UnboundedReceiver<BuildDone>>,
    build_slots: Arc<tokio::sync::Semaphore>,
    leader: coord::Leadership,
    ready_logged: bool,
    /// Live views refreshed since `refreshed_at` was last bumped.
    refreshed: HashSet<String>,
    last_touch: Instant,
    last_stats: (Instant, crate::metrics::Snapshot),
    last_meta_ping: Instant,
}

async fn connect(opts: &Opts) -> Result<Conn> {
    let mut c = Conn::new(opts.clone()).await?;
    mv::init_session(&mut c).await?;
    Ok(c)
}

fn schema_of(opts: &Opts, what: &str) -> Result<String> {
    opts.db_name()
        .map(str::to_string)
        .ok_or_else(|| anyhow!("{what} DSN must name a database (mysql://user:pass@host:port/<schema>)"))
}

fn mode_name(plan: &ViewPlan) -> (&'static str, Option<String>) {
    match &plan.mode {
        Mode::Spj => ("incremental", Some("spj".into())),
        Mode::Grouped => ("incremental", Some("grouped".into())),
        Mode::Global => ("incremental", Some("global".into())),
        Mode::Full(r) => ("full", Some(r.chars().take(255).collect())),
    }
}

impl Engine {
    /// `Ok(None)`: shutdown requested while waiting for the database or the lock.
    async fn start(cfg: Config, metrics: Arc<Metrics>, shutdown: &mut watch::Receiver<bool>) -> Result<Option<(Engine, Option<Position>)>> {
        Metrics::set(&metrics.gauges.starting, 1);
        let src_opts = binlog::replica_opts(&cfg.dsn)?;
        let schema = schema_of(&src_opts, "LAZYVIEW_DSN")?;
        let Some(mut src) = connect_wait(&src_opts, "source database", &metrics, shutdown).await? else {
            return Ok(None);
        };
        let info = binlog::server_info(&mut src).await?;
        info!("connected to {} at {}:{}, schema `{schema}`", info.version, src_opts.ip_or_hostname(), src_opts.tcp_port());
        let Some(mut meta_conn) = connect_wait(&src_opts, "source database", &metrics, shutdown).await? else {
            return Ok(None);
        };
        meta::ensure(&mut meta_conn, &schema).await?;
        let Some(leader) = coord::acquire(&mut meta_conn, &schema, &cfg.instance_id, &metrics, shutdown).await? else {
            return Ok(None);
        };

        let mut targets = BTreeMap::new();
        targets.insert(
            SOURCE_TARGET.to_string(),
            Target { opts: src_opts.clone(), schema: schema.clone(), conn: None },
        );
        for (name, dsn) in &cfg.targets {
            let opts = binlog::replica_opts(dsn)?;
            let ts = schema_of(&opts, &format!("target {name}"))?;
            targets.insert(name.clone(), Target { opts, schema: ts, conn: None });
        }

        let owned = meta::load_state(&mut meta_conn)
            .await?
            .into_iter()
            .filter(|(_, v)| v.managed)
            .map(|(k, v)| (k, v.hash))
            .collect();
        let (build_tx, build_rx) = mpsc::unbounded_channel();
        let cfg_build_concurrency = cfg.build_concurrency;
        let zero = Position { file: String::new(), pos: 0 };
        let mut e = Engine {
            cfg,
            metrics,
            src_opts,
            schema,
            flavor: info.flavor,
            src,
            meta: meta_conn,
            targets,
            catalog: Catalog::default(),
            slots: Vec::new(),
            owned,
            routes: HashMap::new(),
            table_ids: HashMap::new(),
            table_names: HashMap::new(),
            xa_current: None,
            xa_pending: HashMap::new(),
            defs_dirty: false,
            replan: HashSet::new(),
            boundary: zero.clone(),
            persisted: zero,
            last_persist: Instant::now(),
            in_txn: false,
            txn_foreign: false,
            persist_needed: false,
            first_dirty: None,
            last_gtid: None,
            generation: 0,
            build_tx,
            build_rx: Some(build_rx),
            build_slots: Arc::new(tokio::sync::Semaphore::new(cfg_build_concurrency)),
            leader,
            ready_logged: false,
            refreshed: HashSet::new(),
            last_touch: Instant::now(),
            last_stats: (Instant::now(), Default::default()),
            last_meta_ping: Instant::now(),
        };

        let defs = meta::load_definitions(&mut e.meta).await?;
        if !e.cfg.sync {
            // Validate definitions only (usable in CI: non-zero exit on any problem).
            let mut bad = 0;
            let mut names: HashMap<String, u64> = HashMap::new();
            for d in defs.iter().filter(|d| d.enabled) {
                match e.parse_and_plan(&d.definition, &d.targets, None).await {
                    Ok(p) => {
                        if let Some(other) = names.insert(p.name.clone(), d.id) {
                            bad += 1;
                            error!("definition #{}: view `{}` is also defined by #{other}", d.id, p.name);
                        } else {
                            info!("definition #{}: `{}` ok — {} → {}", d.id, p.name, p.mode, d.targets.join(","));
                        }
                    }
                    Err(err) => {
                        bad += 1;
                        error!("definition #{}: {err:#}", d.id);
                    }
                }
            }
            if bad > 0 {
                bail!("{bad} invalid definition(s)");
            }
            info!("LAZYVIEW_SYNC=false: metadata ready, {} definition(s) valid, not streaming", names.len());
            return Ok(Some((e, None)));
        }

        let stored = meta::load_position(&mut e.meta, &e.cfg.instance_id).await?;
        let mut rebuild_all = matches!(e.cfg.start_mode, StartMode::Rebuild | StartMode::Once) || stored.is_none();
        let mut start = match &stored {
            Some(p) if !rebuild_all => p.clone(),
            _ => binlog::current_position(&mut e.src).await?,
        };
        if !rebuild_all && !binlog::binlog_exists(&mut e.src, &start.file).await? {
            match e.cfg.on_missing_binlog {
                MissingBinlog::Fail => bail!(
                    "saved binlog position {start} is no longer available on the server; \
                     set LAZYVIEW_ON_MISSING_BINLOG=rebuild or LAZYVIEW_START_MODE=rebuild"
                ),
                MissingBinlog::Rebuild => {
                    warn!(%start, "saved binlog file purged; rebuilding all views");
                    rebuild_all = true;
                    start = binlog::current_position(&mut e.src).await?;
                }
            }
        }
        e.boundary = start.clone();
        e.load_slots(defs, !rebuild_all).await?;
        e.rebuild_routes();

        if e.slots.is_empty() {
            info!(
                "no views defined yet; add one with: INSERT INTO `{}`.`_lazyview` (definition) VALUES ('CREATE VIEW my_view AS SELECT ...')",
                e.schema
            );
        }
        if e.cfg.start_mode == StartMode::Once {
            e.wait_builds().await?;
            info!("LAZYVIEW_START_MODE=once: all views built");
            return Ok(Some((e, None)));
        }
        e.persist_position(true).await?;
        e.update_gauges();
        Ok(Some((e, Some(start))))
    }

    // ---------------------------------------------------------------- definitions & planning

    async fn table_info(&mut self, t: &TableRef) -> Result<Option<Arc<TableInfo>>> {
        self.catalog
            .get(&mut self.src, &t.0, &t.1, self.flavor == Flavor::MariaDb)
            .await
    }

    /// Parses and plans a definition against the current catalog. `own_def` excludes the slot
    /// itself from the chained-view check.
    async fn parse_and_plan(&mut self, definition: &str, targets: &[String], own_def: Option<u64>) -> Result<ViewPlan> {
        let parsed = analyze::parse_definition(definition)?;
        if let Some(sch) = &parsed.schema {
            if *sch != self.schema {
                bail!(
                    "schema-qualified view name `{sch}`.`{}` is not supported: views are created in the lazyview schema `{}` (and in each target's schema); write CREATE VIEW {} AS ...",
                    parsed.name, self.schema, parsed.name
                );
            }
        }
        for t in targets {
            if !self.targets.contains_key(t) {
                bail!("unknown target `{t}` (configure LAZYVIEW_TARGETS / LAZYVIEW_TARGET_<NAME>_DSN)");
            }
        }
        let tables = analyze::referenced_tables(&parsed.query, &self.schema);
        let mut infos = HashMap::new();
        for t in &tables {
            if t.0 == self.schema && t.1 == parsed.name {
                bail!("view `{}` cannot read itself", parsed.name);
            }
            // Chained views: an upstream MV must exist on the source to be watched.
            if let Some(up) = self
                .slots
                .iter()
                .find(|s| Some(s.def_id) != own_def && t.0 == self.schema && s.name == t.1)
            {
                if !up.targets.iter().any(|x| x == SOURCE_TARGET) {
                    bail!("view reads `{}`, which is materialized only on other instances; chained views need the upstream view on target `source`", t.1);
                }
            }
            match self.table_info(t).await? {
                Some(info) => {
                    infos.insert(t.clone(), info);
                }
                None => bail!("table `{}`.`{}` does not exist", t.0, t.1),
            }
        }
        // Parents of cascading foreign keys (their changes cascade without binlog rows).
        let mut queue: Vec<(TableRef, usize)> = infos.values().flat_map(|i| i.cascades.iter().map(|f| (f.parent.clone(), 0))).collect();
        while let Some((t, depth)) = queue.pop() {
            if infos.contains_key(&t) || depth > 8 {
                continue;
            }
            if let Some(info) = self.table_info(&t).await? {
                queue.extend(info.cascades.iter().map(|f| (f.parent.clone(), depth + 1)));
                infos.insert(t, info);
            }
        }
        let plan = analyze::plan(&parsed, &self.schema, &infos)?;
        if plan.cascades.iter().any(|c| c.unit_from_parent.is_none()) && plan.is_incremental() {
            warn!(view = %plan.name, "reads a child of a cascading foreign key whose units cannot be derived from the parent; parent deletes/updates trigger an online rebuild");
        }
        Ok(plan)
    }

    async fn set_state(&mut self, name: &str, target: &str, def_id: u64, hash: &str, plan: Option<&ViewPlan>, state: &str, err: Option<&str>) {
        let (mode, detail) = plan.map(mode_name).unwrap_or(("unknown", None));
        let r = meta::set_state(
            &mut self.meta,
            StateUpdate {
                name,
                target,
                definition_id: def_id,
                hash,
                mode,
                detail: detail.as_deref(),
                state,
                error: err,
                managed: state != "error",
            },
        )
        .await;
        if let Err(e) = r {
            warn!("writing _lazyview_state for {name}@{target}: {e:#}");
        }
        if state != "error" {
            self.owned.insert((name.to_string(), target.to_string()), hash.to_string());
            let _ = meta::delete_stale_state(&mut self.meta, def_id, name).await;
        }
    }

    async fn slot_error(&mut self, idx: usize, err: &anyhow::Error) {
        let msg = format!("{err:#}");
        error!(view = %self.slots[idx].name, "{msg}");
        let s = &self.slots[idx];
        let (name, id, hash, targets) = (s.name.clone(), s.def_id, s.hash.clone(), s.targets.clone());
        self.slots[idx].error = Some(msg.clone());
        for t in targets {
            self.set_state(&name, &t, id, &hash, None, "error", Some(&msg)).await;
        }
    }

    /// Initial slot creation at startup. With `resume`, valid existing tables become live.
    async fn load_slots(&mut self, defs: Vec<meta::DefRow>, resume: bool) -> Result<()> {
        for d in defs.into_iter().filter(|d| d.enabled) {
            let idx = self.new_slot(&d);
            match self.parse_and_plan(&d.definition, &d.targets, Some(d.id)).await {
                Ok(plan) => {
                    let plan = Arc::new(plan);
                    if self.name_taken(idx, &plan.name) {
                        self.slot_error(idx, &anyhow!("another definition already materializes `{}`", plan.name)).await;
                        continue;
                    }
                    self.slots[idx].name = plan.name.clone();
                    if resume {
                        match self.try_resume(idx, &plan).await {
                            Ok(true) => continue,
                            Ok(false) => {}
                            Err(e) => warn!(view = %plan.name, "cannot resume, rebuilding: {e:#}"),
                        }
                    }
                    let targets = d.targets.clone();
                    let name = plan.name.clone();
                    self.start_next(idx, plan, name, targets, false).await;
                }
                Err(e) => self.slot_error(idx, &e).await,
            }
        }
        Ok(())
    }

    fn new_slot(&mut self, d: &meta::DefRow) -> usize {
        let parsed = analyze::parse_definition(&d.definition).ok();
        self.slots.push(Slot {
            def_id: d.id,
            name: parsed.as_ref().map(|p| p.name.clone()).unwrap_or_else(|| format!("#{}", d.id)),
            wanted: parsed.map(|p| analyze::referenced_tables(&p.query, &self.schema)).unwrap_or_default(),
            definition: d.definition.clone(),
            hash: analyze::definition_hash(&d.definition),
            targets: d.targets.clone(),
            live: None,
            next: None,
            error: None,
            retired: Vec::new(),
        });
        self.slots.len() - 1
    }

    fn name_taken(&self, idx: usize, name: &str) -> bool {
        self.slots.iter().enumerate().any(|(i, s)| {
            i != idx && (s.name == name || s.next.as_ref().map(|n| n.name == name).unwrap_or(false))
        })
    }

    async fn target_conn(&mut self, t: &str) -> Result<&mut Conn> {
        let target = self.targets.get_mut(t).ok_or_else(|| anyhow!("unknown target {t}"))?;
        if target.conn.is_none() {
            target.conn = Some(connect(&target.opts).await.with_context(|| format!("connecting to target {t}"))?);
        }
        Ok(target.conn.as_mut().unwrap())
    }

    /// Existing tables are reused when every target has a ready state with the same definition
    /// hash and a table whose columns match the plan's layout.
    async fn try_resume(&mut self, idx: usize, plan: &Arc<ViewPlan>) -> Result<bool> {
        let s = &self.slots[idx];
        let (name, targets, hash) = (plan.name.clone(), s.targets.clone(), s.hash.clone());
        for t in &targets {
            if self.owned.get(&(name.clone(), t.clone())) != Some(&hash) {
                return Ok(false);
            }
        }
        let state = meta::load_state(&mut self.meta).await?;
        if targets.iter().any(|t| state.get(&(name.clone(), t.clone())).map(|r| r.state != "ready").unwrap_or(true)) {
            return Ok(false);
        }
        let layout = mv::probe(&mut self.src, &self.schema, plan).await?;
        for t in &targets {
            let ts = self.targets[t].schema.clone();
            let conn = self.target_conn(t).await?;
            let cols = mv::table_columns(conn, &ts, &name).await?;
            if !mv::layout_matches(&layout, &cols) {
                return Ok(false);
            }
        }
        info!(view = %name, "resuming existing materialized table");
        self.slots[idx].live = Some(Version {
            plan: plan.clone(),
            layout,
            name,
            targets,
            dirty: ViewDirty { images: vec![HashSet::new(); plan.aliases.len()], ..Default::default() },
            frozen: false,
        });
        Ok(true)
    }

    /// Starts building a new version in the background.
    async fn start_next(&mut self, idx: usize, plan: Arc<ViewPlan>, name: String, targets: Vec<String>, partial: bool) {
        if let Some(n) = self.slots[idx].next.take() {
            n.task.abort();
        }
        // Never overwrite a table lazyview does not own.
        for t in &targets {
            let key = (name.clone(), t.clone());
            let live_has = self.slots[idx].live.as_ref().map(|l| l.name == name && l.targets.contains(t)).unwrap_or(false);
            if self.owned.contains_key(&key) || live_has {
                continue;
            }
            let ts = self.targets[t].schema.clone();
            let kind = match self.target_conn(t).await {
                Ok(c) => mv::table_kind(c, &ts, &name).await,
                Err(e) => Err(e),
            };
            match kind {
                Ok(None) => {}
                Ok(Some(k)) => {
                    let e = anyhow!("refusing to replace existing {} `{ts}`.`{name}` on target {t} that lazyview does not manage", k.to_lowercase());
                    self.slot_error(idx, &e).await;
                    return;
                }
                Err(e) => {
                    self.slot_error(idx, &e).await;
                    return;
                }
            }
        }
        self.generation += 1;
        let generation = self.generation;
        let def_id = self.slots[idx].def_id;
        let hash = self.slots[idx].hash.clone();
        for t in &targets {
            self.set_state(&name, t, def_id, &hash, Some(&plan), "building", None).await;
        }
        let build_targets: Vec<(Opts, String)> = targets
            .iter()
            .map(|t| (self.targets[t].opts.clone(), self.targets[t].schema.clone()))
            .collect();
        let task = tokio::spawn(build_task(
            self.src_opts.clone(),
            self.schema.clone(),
            build_targets,
            plan.clone(),
            name.clone(),
            self.cfg.write_batch_rows,
            self.metrics.clone(),
            self.build_tx.clone(),
            self.build_slots.clone(),
            def_id,
            generation,
        ));
        let routine = self.slots[idx].live.as_ref().map(|l| Arc::ptr_eq(&l.plan, &plan)).unwrap_or(false);
        if routine {
            debug!(view = %name, "rebuilding");
        } else {
            info!(view = %name, targets = %targets.join(","), mode = %plan.mode, "building");
        }
        Metrics::add(&self.metrics.full_rebuilds, 1);
        self.slots[idx].error = None;
        self.slots[idx].next = Some(Next {
            dirty: ViewDirty { images: vec![HashSet::new(); plan.aliases.len()], ..Default::default() },
            plan,
            name,
            targets,
            generation,
            task,
            done: None,
            partial,
        });
        self.rebuild_routes();
    }

    /// Re-reads `_lazyview` and reconciles slots: new, changed, renamed, retargeted, removed.
    async fn reload_definitions(&mut self) -> Result<()> {
        Metrics::add(&self.metrics.definition_reloads, 1);
        let defs = meta::load_definitions(&mut self.meta).await?;
        let enabled: HashMap<u64, meta::DefRow> = defs.into_iter().filter(|d| d.enabled).map(|d| (d.id, d)).collect();

        // Removed or disabled.
        let mut i = 0;
        while i < self.slots.len() {
            if enabled.contains_key(&self.slots[i].def_id) {
                i += 1;
                continue;
            }
            let s = self.slots.remove(i);
            info!(view = %s.name, "definition removed");
            self.retire_slot(s).await;
            self.table_ids.clear();
        }

        let mut ids: Vec<u64> = enabled.keys().copied().collect();
        ids.sort();
        for id in ids {
            let d = &enabled[&id];
            let hash = analyze::definition_hash(&d.definition);
            let idx = match self.slots.iter().position(|s| s.def_id == id) {
                Some(idx) => {
                    let s = &self.slots[idx];
                    if s.hash == hash && s.targets == d.targets {
                        continue;
                    }
                    idx
                }
                None => self.new_slot(d),
            };
            let old_name = self.slots[idx].name.clone();
            let old_targets = self.slots[idx].targets.clone();
            let is_new = self.slots[idx].live.is_none() && self.slots[idx].next.is_none();
            self.slots[idx].definition = d.definition.clone();
            self.slots[idx].hash = hash;
            self.slots[idx].targets = d.targets.clone();
            if let Ok(p) = analyze::parse_definition(&d.definition) {
                self.slots[idx].wanted = analyze::referenced_tables(&p.query, &self.schema);
                if self.slots[idx].live.is_none() {
                    self.slots[idx].name = p.name;
                }
            }
            match self.parse_and_plan(&d.definition, &d.targets, Some(id)).await {
                Ok(plan) => {
                    if self.name_taken(idx, &plan.name) {
                        self.slot_error(idx, &anyhow!("another definition already materializes `{}`", plan.name)).await;
                        continue;
                    }
                    let name = plan.name.clone();
                    let has_live = self.slots[idx].live.is_some();
                    let mut retired: Vec<(String, String)> = Vec::new();
                    if has_live {
                        for t in &old_targets {
                            if old_name != name || !d.targets.contains(t) {
                                retired.push((old_name.clone(), t.clone()));
                            }
                        }
                    }
                    self.slots[idx].retired = retired;
                    self.slots[idx].name = name.clone();
                    if is_new {
                        info!(view = %name, "new definition");
                    } else {
                        info!(view = %name, "definition changed; building the new version online (current version stays live)");
                    }
                    self.start_next(idx, Arc::new(plan), name, d.targets.clone(), false).await;
                }
                Err(e) => self.slot_error(idx, &e).await,
            }
        }
        self.rebuild_routes();
        Ok(())
    }

    async fn retire_slot(&mut self, s: Slot) {
        if let Some(n) = s.next {
            n.task.abort();
            for t in &n.targets {
                let ts = self.targets[t].schema.clone();
                if let Ok(c) = self.target_conn(t).await {
                    let _ = mv::drop_table(c, &ts, &mv::shadow_name(&n.name)).await;
                }
            }
        }
        let mut tables: Vec<(String, String)> = s.targets.iter().map(|t| (s.name.clone(), t.clone())).collect();
        tables.extend(s.retired);
        if let Some(l) = &s.live {
            tables.extend(l.targets.iter().map(|t| (l.name.clone(), t.clone())));
        }
        tables.sort();
        tables.dedup();
        for (name, t) in tables {
            self.drop_owned(&name, &t).await;
        }
    }

    async fn drop_owned(&mut self, name: &str, target: &str) {
        let key = (name.to_string(), target.to_string());
        if self.owned.remove(&key).is_none() {
            let _ = meta::delete_state(&mut self.meta, name, target).await;
            return;
        }
        if self.cfg.drop_removed {
            if let Some(t) = self.targets.get(target) {
                let ts = t.schema.clone();
                match self.target_conn(target).await {
                    Ok(c) => {
                        if let Err(e) = mv::drop_table(c, &ts, name).await {
                            warn!("dropping {name}@{target}: {e:#}");
                        }
                    }
                    Err(e) => warn!("dropping {name}@{target}: {e:#}"),
                }
            }
        }
        let _ = meta::delete_state(&mut self.meta, name, target).await;
    }

    fn rebuild_routes(&mut self) {
        let mut routes: HashMap<TableRef, Vec<RouteT>> = HashMap::new();
        for (si, s) in self.slots.iter().enumerate() {
            let versions = [
                s.live.as_ref().filter(|l| !l.frozen).map(|l| (false, &l.plan)),
                s.next.as_ref().map(|n| (true, &n.plan)),
            ];
            for (next, plan) in versions.into_iter().flatten() {
                if plan.is_incremental() || matches!(plan.mode, Mode::Global) {
                    for (ai, a) in plan.aliases.iter().enumerate() {
                        routes
                            .entry((a.table.schema.clone(), a.table.name.clone()))
                            .or_default()
                            .push(RouteT { slot: si, next, kind: RouteKind::Alias(ai) });
                    }
                } else {
                    for t in &plan.tables {
                        routes.entry(t.clone()).or_default().push(RouteT { slot: si, next, kind: RouteKind::All });
                    }
                }
                for (ci, c) in plan.cascades.iter().enumerate() {
                    routes
                        .entry((c.parent.schema.clone(), c.parent.name.clone()))
                        .or_default()
                        .push(RouteT { slot: si, next, kind: RouteKind::Cascade(ci) });
                }
            }
        }
        self.routes = routes.into_iter().map(|(k, v)| (k, Arc::new(v))).collect();
        self.table_ids.clear();
    }

    fn version_of(&mut self, r: RouteT) -> Option<(&ViewPlan, &mut ViewDirty)> {
        let s = self.slots.get_mut(r.slot)?;
        if r.next {
            s.next.as_mut().map(|n| (&*n.plan, &mut n.dirty))
        } else {
            s.live.as_mut().map(|l| (&*l.plan, &mut l.dirty))
        }
    }

    // ---------------------------------------------------------------- streaming

    async fn stream(&mut self, start: Position, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let mut build_rx = self.build_rx.take().expect("stream runs once");
        let mut backoff = Duration::from_millis(500);
        let mut pos = start;
        'reconnect: loop {
            let mut reader = match BinlogReader::connect(self.src_opts.clone(), self.cfg.server_id, self.flavor, &pos, self.cfg.heartbeat).await {
                Ok(r) => {
                    backoff = Duration::from_millis(500);
                    r
                }
                Err(e) => {
                    if binlog::is_missing_binlog(&e) {
                        self.on_missing_binlog(&e).await?;
                        pos = self.boundary.clone();
                        continue 'reconnect;
                    }
                    warn!("binlog connect failed: {e:#}; retrying in {backoff:?}");
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {}
                        _ = shutdown.changed() => if *shutdown.borrow() { break 'reconnect; },
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue 'reconnect;
                }
            };
            info!(position = %pos, "streaming binlog");
            Metrics::set(&self.metrics.gauges.streaming, 1);
            Metrics::set(&self.metrics.gauges.starting, 0);
            loop {
                Metrics::set(&self.metrics.gauges.heartbeat_unix, crate::metrics::unix_now());
                let deadline = self.flush_deadline();
                let sleep_until = deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
                tokio::select! {
                    biased;
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            break 'reconnect;
                        }
                    }
                    Some(done) = build_rx.recv() => self.on_build_done(done),
                    item = reader.next() => match item {
                        Ok(Some(item)) => {
                            Metrics::add(&self.metrics.events, 1);
                            if let Err(e) = self.on_item(&reader, item).await {
                                error!("processing binlog event: {e:#}");
                                return Err(e);
                            }
                        }
                        Ok(None) => {
                            warn!("binlog stream closed by server; reconnecting");
                            break;
                        }
                        Err(e) => {
                            if binlog::is_missing_binlog(&e) {
                                self.on_missing_binlog(&e).await?;
                            } else {
                                warn!("binlog stream error: {e:#}; reconnecting");
                            }
                            break;
                        }
                    },
                    _ = tokio::time::sleep_until(sleep_until.into()), if deadline.is_some() && !self.in_txn => {}
                }
                if !self.in_txn && self.flush_due() {
                    self.flush().await?;
                }
                if !self.in_txn {
                    self.housekeeping().await?;
                }
            }
            Metrics::set(&self.metrics.gauges.streaming, 0);
            let _ = reader.close().await;
            // Changes of a partially read transaction are kept: re-applying them is harmless.
            self.in_txn = false;
            pos = self.boundary.clone();
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        if !self.in_txn {
            let _ = self.flush().await;
        }
        for s in &self.slots {
            if let Some(n) = &s.next {
                n.task.abort();
            }
        }
        info!("stopped");
        Ok(())
    }

    async fn on_missing_binlog(&mut self, e: &anyhow::Error) -> Result<()> {
        match self.cfg.on_missing_binlog {
            MissingBinlog::Fail => Err(anyhow!("binlog position {} is no longer available: {e:#}", self.boundary)),
            MissingBinlog::Rebuild => {
                warn!("binlog position {} purged; rebuilding all views", self.boundary);
                self.boundary = binlog::current_position(&mut self.src).await?;
                for i in 0..self.slots.len() {
                    if let Some(l) = &self.slots[i].live {
                        let (plan, name, targets) = (l.plan.clone(), l.name.clone(), l.targets.clone());
                        self.start_next(i, plan, name, targets, false).await;
                    }
                }
                Ok(())
            }
        }
    }

    async fn on_item(&mut self, reader: &BinlogReader, item: Item) -> Result<()> {
        match item {
            Item::Gtid { gtid } => {
                self.last_gtid = Some(gtid);
                self.in_txn = true;
            }
            Item::TableMap { table_id, schema, table, columns } => {
                self.in_txn = true;
                let key = (schema, table);
                self.table_names.insert(table_id, key.clone());
                if !(key.0 == self.schema && (key.1 == meta::STATUS || key.1 == meta::STATE)) {
                    self.txn_foreign = true;
                }
                if key.0 == self.schema && key.1 == meta::DEFS {
                    self.table_ids.insert(table_id, Some(Arc::new(Vec::new())));
                    return Ok(());
                }
                let route = self.routes.get(&key).cloned();
                if let Some(r) = &route {
                    // A table whose shape differs from what the plans were built for (DDL we
                    // have not seen, e.g. before the resume position) forces a re-plan.
                    if let Some(info) = r.iter().find_map(|rt| {
                        let s = &self.slots[rt.slot];
                        let p = if rt.next { s.next.as_ref().map(|n| &n.plan) } else { s.live.as_ref().map(|l| &l.plan) }?;
                        match rt.kind {
                            RouteKind::Alias(a) => Some(p.aliases[a].table.clone()),
                            RouteKind::Cascade(c) => Some(p.cascades[c].parent.clone()),
                            RouteKind::All => None,
                        }
                    }) {
                        if info.columns.len() as u64 != columns {
                            warn!(table = %key.1, "column count changed; re-planning dependent views");
                            self.on_tables_changed(std::slice::from_ref(&key));
                            self.table_ids.insert(table_id, None);
                            return Ok(());
                        }
                    }
                }
                self.table_ids.insert(table_id, route);
            }
            Item::Rows { table_id, event, .. } => {
                self.in_txn = true;
                let Some(Some(route)) = self.table_ids.get(&table_id).cloned() else {
                    return Ok(());
                };
                if route.is_empty() {
                    self.defs_dirty = true;
                    self.first_dirty.get_or_insert_with(Instant::now);
                    return Ok(());
                }
                let need_rows = self.xa_current.is_some() || route.iter().any(|r| r.kind != RouteKind::All);
                self.mark_all(&route);
                if !need_rows {
                    return Ok(());
                }
                let rows = reader.rows(table_id, &event)?;
                self.feed_rows(&route, &rows);
                if let (Some(xa), Some(t)) = (self.xa_current.as_mut(), self.table_names.get(&table_id)) {
                    xa.2.extend(rows.into_iter().map(|(b, a)| (t.clone(), b, a)));
                }
            }
            Item::XaStart { xid } => {
                self.in_txn = true;
                self.xa_current = Some((xid, self.boundary.clone(), Vec::new()));
            }
            Item::XaPrepare { xid, pos } => {
                // Prepared rows become visible only at XA COMMIT: keep them to feed again then.
                self.in_txn = false;
                self.boundary = pos;
                if let Some((x, start, rows)) = self.xa_current.take() {
                    let key = if x == xid { x } else { xid };
                    self.xa_pending.insert(key, (start, rows));
                }
                self.persist_needed |= std::mem::take(&mut self.txn_foreign);
                self.after_boundary();
            }
            Item::XaEnd { xid, commit, pos } => {
                self.in_txn = false;
                self.boundary = pos;
                let pending = self.xa_pending.remove(&xid);
                if pending.is_none() && commit {
                    // Prepared before the position we resumed from (should not happen: positions
                    // are never saved past an open XA PREPARE). Be safe: rebuild everything.
                    warn!(%xid, "XA COMMIT for a transaction prepared before this run; rebuilding all views");
                    for s in self.slots.iter_mut() {
                        if let Some(l) = s.live.as_mut() {
                            l.dirty.rebuild = true;
                            l.dirty.all = true;
                        }
                    }
                    self.first_dirty.get_or_insert_with(Instant::now);
                }
                if let Some((_, rows)) = pending {
                    if commit {
                        let mut by_table: Vec<(TableRef, Vec<(Option<BinlogRow>, Option<BinlogRow>)>)> = Vec::new();
                        for (t, b, a) in rows {
                            match by_table.last_mut() {
                                Some((lt, v)) if *lt == t => v.push((b, a)),
                                _ => by_table.push((t, vec![(b, a)])),
                            }
                        }
                        for (t, rows) in by_table {
                            if let Some(route) = self.routes.get(&t).cloned() {
                                self.mark_all(&route);
                                self.feed_rows(&route, &rows);
                            }
                        }
                    }
                }
                self.persist_needed = true;
                self.after_boundary();
            }
            Item::Commit { pos } => {
                self.in_txn = false;
                self.boundary = pos;
                // One-phase XA commits like a normal transaction.
                self.xa_current = None;
                self.persist_needed |= std::mem::take(&mut self.txn_foreign);
                self.after_boundary();
            }
            Item::Statement { schema, query, pos } => {
                self.in_txn = false;
                self.boundary = pos;
                self.txn_foreign = false;
                self.persist_needed = true;
                let tables = crate::ddl::statement_tables(&query, &schema);
                if !tables.is_empty() {
                    debug!(%query, "statement affects tables {tables:?}");
                    self.on_tables_changed(&tables);
                }
                self.after_boundary();
            }
            Item::Heartbeat | Item::Other => {}
        }
        Ok(())
    }

    fn update_gauges(&mut self) {
        let g = &self.metrics.gauges;
        let n = |f: &dyn Fn(&Slot) -> bool| self.slots.iter().filter(|s| f(s)).count() as u64;
        let (total, ready, errors) = (self.slots.len() as u64, n(&|s| s.live.is_some() && s.error.is_none()), n(&|s| s.error.is_some()));
        Metrics::set(&g.views, total);
        Metrics::set(&g.views_ready, ready);
        Metrics::set(&g.views_building, n(&|s| s.next.is_some()));
        Metrics::set(&g.views_error, errors);
        let all_ready = total > 0 && ready + errors == total && self.slots.iter().all(|s| s.next.is_none() || s.live.is_some());
        if all_ready && !self.ready_logged {
            if errors > 0 {
                info!("ready: {ready} view(s) live, {errors} in error (see `{}`._lazyview_overview)", self.schema);
            } else {
                info!("ready: {ready} view(s) live");
            }
        }
        self.ready_logged = all_ready;
    }

    /// Periodic one-line activity summary and metadata-connection keepalive (which also
    /// verifies we still hold the leadership lock).
    async fn housekeeping(&mut self) -> Result<()> {
        if self.last_stats.0.elapsed() >= Duration::from_secs(60) {
            let now = self.metrics.snapshot();
            let d = now.since(&self.last_stats.1);
            if d.events > 0 {
                info!(
                    "last 60s: {} binlog events, {} rows ({} irrelevant), {} units refreshed ({} changed), +{}/-{} rows written, {} flushes",
                    d.events, d.rows_seen, d.rows_skipped, d.units_refreshed, d.units_changed, d.mv_rows_inserted, d.mv_rows_deleted, d.flushes
                );
            }
            self.last_stats = (Instant::now(), now);
        }
        if self.last_meta_ping.elapsed() >= Duration::from_secs(60) {
            coord::verify(&mut self.meta, &self.leader).await?;
            self.last_meta_ping = Instant::now();
        }
        Ok(())
    }

    fn mark_all(&mut self, route: &[RouteT]) {
        for r in route.iter().filter(|r| r.kind == RouteKind::All) {
            if let Some((_, d)) = self.version_of(*r) {
                d.all = true;
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
    }

    /// Hot path: one decoded row fans out to every routed (view, version, alias).
    fn feed_rows(&mut self, route: &[RouteT], rows: &[(Option<BinlogRow>, Option<BinlogRow>)]) {
        let (mut seen, mut skipped) = (0u64, 0u64);
        for (before, after) in rows {
            seen += 1;
            for r in route {
                let Some((plan, dirty)) = self.version_of(*r) else { continue };
                let fed = match r.kind {
                    RouteKind::Alias(a) => feed(plan, a, dirty, before.as_ref(), after.as_ref()),
                    RouteKind::Cascade(c) => feed_cascade(plan, c, dirty, before.as_ref(), after.as_ref()),
                    RouteKind::All => continue,
                };
                if fed == Fed::Skipped {
                    skipped += 1;
                }
            }
        }
        self.first_dirty.get_or_insert_with(Instant::now);
        Metrics::add(&self.metrics.rows_seen, seen);
        Metrics::add(&self.metrics.rows_skipped, skipped);
    }

    /// Nothing buffered and nothing building: the views reflect everything up to the boundary.
    fn after_boundary(&mut self) {
        if self.first_dirty.is_none() && !self.defs_dirty && self.replan.is_empty() && self.slots.iter().all(|s| s.next.is_none()) {
            self.metrics.set_applied(self.boundary.clone());
        }
    }

    fn on_tables_changed(&mut self, tables: &[TableRef]) {
        for t in tables {
            if t.1 == "*" {
                self.catalog.clear();
            } else {
                self.catalog.invalidate(t);
            }
            if t.0 == self.schema && (t.1 == meta::DEFS || t.1 == "*") {
                self.defs_dirty = true;
            }
        }
        for (i, s) in self.slots.iter().enumerate() {
            if s.error.is_some() && s.live.is_none() && s.next.is_none() && s.wanted.iter().any(|w| tables.iter().any(|t| t.0 == w.0 && (t.1 == "*" || t.1 == w.1))) {
                self.replan.insert(i);
                continue;
            }
            let reads = |p: &ViewPlan| p.tables.iter().any(|x| tables.iter().any(|t| t.0 == x.0 && (t.1 == "*" || t.1 == x.1)));
            let affected = s.live.as_ref().map(|l| reads(&l.plan)).unwrap_or(false) || s.next.as_ref().map(|n| reads(&n.plan)).unwrap_or(false);
            if affected {
                self.replan.insert(i);
            }
        }
        if !self.replan.is_empty() || self.defs_dirty {
            self.first_dirty.get_or_insert_with(Instant::now);
        }
    }

    fn on_build_done(&mut self, done: BuildDone) {
        if let Some(s) = self.slots.iter_mut().find(|s| s.def_id == done.def_id) {
            if let Some(n) = s.next.as_mut() {
                if n.generation == done.generation {
                    n.done = Some(done.result);
                    self.first_dirty.get_or_insert_with(Instant::now);
                }
            }
        }
    }

    fn key_count(&self) -> usize {
        self.slots
            .iter()
            .map(|s| s.live.as_ref().map(|l| l.dirty.key_count()).unwrap_or(0) + s.next.as_ref().map(|n| n.dirty.key_count()).unwrap_or(0))
            .sum()
    }

    fn flush_deadline(&self) -> Option<Instant> {
        let urgent = self.defs_dirty || !self.replan.is_empty() || self.slots.iter().any(|s| s.next.as_ref().map(|n| n.done.is_some()).unwrap_or(false));
        match self.first_dirty {
            Some(_) if urgent => Some(Instant::now()),
            Some(t) => Some(t + self.cfg.batch_window),
            None if self.persist_needed => Some(self.last_persist + self.cfg.position_persist_interval),
            None => None,
        }
    }

    fn flush_due(&self) -> bool {
        match self.flush_deadline() {
            Some(d) => Instant::now() >= d || self.key_count() >= self.cfg.batch_max_keys,
            None => false,
        }
    }

    // ---------------------------------------------------------------- flush

    async fn flush(&mut self) -> Result<()> {
        let started = Instant::now();
        if self.defs_dirty {
            self.defs_dirty = false;
            self.reload_definitions().await?;
        }
        if !self.replan.is_empty() {
            let mut idxs: Vec<usize> = self.replan.drain().collect();
            idxs.sort();
            for i in idxs {
                self.replan_slot(i).await;
            }
        }
        let has_reads = self.slots.iter().any(|s| {
            s.live.as_ref().map(|l| !l.frozen && !l.dirty.is_empty()).unwrap_or(false) || s.next.as_ref().map(|n| n.done.is_some()).unwrap_or(false)
        });
        if has_reads {
            self.fence().await;
        }
        for i in 0..self.slots.len() {
            if self.slots[i].next.as_ref().map(|n| n.done.is_some()).unwrap_or(false) {
                self.finish_next(i).await;
            }
            self.refresh_live(i).await;
        }
        self.first_dirty = None;
        self.persist_position(false).await?;
        if !self.refreshed.is_empty() && self.last_touch.elapsed() >= Duration::from_secs(10) {
            let names: Vec<String> = self.refreshed.drain().collect();
            let _ = meta::touch_refreshed(&mut self.meta, &names).await;
            self.last_touch = Instant::now();
        }
        Metrics::add(&self.metrics.flushes, 1);
        self.update_gauges();
        self.after_boundary();
        debug!(elapsed = ?started.elapsed(), "flush");
        Ok(())
    }

    /// Waits until the transactions read from the binlog are visible to readers: binlog events
    /// are shipped before the engine commit completes.
    async fn fence(&mut self) {
        if self.flavor == Flavor::MySql {
            if let Some(g) = self.last_gtid.clone() {
                let r: Result<Option<i64>, _> = self.src.exec_first("SELECT WAIT_FOR_EXECUTED_GTID_SET(?, 5)", (g,)).await;
                match r {
                    Ok(_) => return,
                    Err(e) => debug!("gtid fence unavailable: {e}"),
                }
            }
        }
        if !self.cfg.visibility_delay.is_zero() {
            tokio::time::sleep(self.cfg.visibility_delay).await;
        }
    }

    async fn replan_slot(&mut self, i: usize) {
        let (def, targets, id) = {
            let s = &self.slots[i];
            (s.definition.clone(), s.targets.clone(), s.def_id)
        };
        if let Some(l) = self.slots[i].live.as_mut() {
            l.frozen = true;
        }
        self.rebuild_routes();
        match self.parse_and_plan(&def, &targets, Some(id)).await {
            Ok(plan) => {
                let name = plan.name.clone();
                info!(view = %name, "source schema changed; rebuilding online");
                self.start_next(i, Arc::new(plan), name, targets, false).await;
            }
            Err(e) => self.slot_error(i, &e).await,
        }
    }

    /// Applies changes buffered during the build to the shadow tables, swaps them in.
    async fn finish_next(&mut self, i: usize) {
        let mut n = self.slots[i].next.take().unwrap();
        let layout = match n.done.take().unwrap() {
            Ok(l) => l,
            Err(e) => {
                self.slots[i].next = None;
                self.rebuild_routes();
                self.slot_error(i, &e).await;
                return;
            }
        };
        let was_live_same_plan = self.slots[i].live.as_ref().map(|l| Arc::ptr_eq(&l.plan, &n.plan)).unwrap_or(false);
        let shadow = mv::shadow_name(&n.name);
        let mut dirty = n.dirty.take();
        let full_again = matches!(n.plan.mode, Mode::Full(_)) && dirty.all;
        if !matches!(n.plan.mode, Mode::Full(_)) {
            if let Err(e) = self.refresh(&n.plan, &layout, &mut dirty, &n.targets, &shadow).await {
                self.slots[i].next = None;
                self.rebuild_routes();
                self.slot_error(i, &e.context("applying changes buffered during build")).await;
                return;
            }
        }
        for t in &n.targets {
            let ts = self.targets[t].schema.clone();
            let r = match self.target_conn(t).await {
                Ok(c) => mv::swap(c, &ts, &n.name).await,
                Err(e) => Err(e),
            };
            if let Err(e) = r {
                self.slots[i].next = None;
                self.rebuild_routes();
                self.slot_error(i, &e.context(format!("swapping in new version on target {t}"))).await;
                return;
            }
        }
        let (def_id, hash) = (self.slots[i].def_id, self.slots[i].hash.clone());
        for t in &n.targets {
            self.set_state(&n.name, t, def_id, &hash, Some(&n.plan), "ready", None).await;
        }
        let retired = std::mem::take(&mut self.slots[i].retired);
        for (name, t) in retired {
            if !(name == n.name && n.targets.contains(&t)) {
                self.drop_owned(&name, &t).await;
            }
        }
        if was_live_same_plan {
            debug!(view = %n.name, "rebuilt");
        } else {
            info!(view = %n.name, targets = %n.targets.join(","), "live");
        }
        let targets = if n.partial {
            self.slots[i].live.as_ref().map(|l| l.targets.clone()).unwrap_or(n.targets.clone())
        } else {
            n.targets.clone()
        };
        let mut live_dirty = ViewDirty { images: vec![HashSet::new(); n.plan.aliases.len()], ..Default::default() };
        live_dirty.all = full_again;
        live_dirty.rebuild = dirty.rebuild;
        self.slots[i].live = Some(Version { plan: n.plan, layout, name: n.name, targets, dirty: live_dirty, frozen: false });
        self.rebuild_routes();
    }

    async fn refresh_live(&mut self, i: usize) {
        let Some(l) = self.slots[i].live.as_mut() else { return };
        if l.frozen || l.dirty.is_empty() {
            return;
        }
        let mut dirty = l.dirty.take();
        let (plan, layout, name, targets) = (l.plan.clone(), l.layout.clone(), l.name.clone(), l.targets.clone());
        if let Mode::Full(_) = plan.mode {
            // Rebuild into a shadow; further changes during the build mark it dirty again.
            if self.slots[i].next.is_none() {
                self.start_next(i, plan, name, targets, false).await;
            }
            return;
        }
        if let Err(e) = self.refresh(&plan, &layout, &mut dirty, &targets, &name).await {
            warn!(view = %name, "refresh failed, rebuilding: {e:#}");
            self.replan.insert(i);
            self.first_dirty.get_or_insert_with(Instant::now);
            return;
        }
        self.refreshed.insert(name.clone());
        if dirty.rebuild && self.slots[i].next.is_none() {
            info!(view = %name, "cascading foreign key changed rows with unknown units; rebuilding online");
            self.start_next(i, plan, name, targets, false).await;
        }
    }

    /// Refreshes the units in `dirty` on every target table `table`.
    async fn refresh(&mut self, plan: &ViewPlan, layout: &Layout, dirty: &mut ViewDirty, targets: &[String], table: &str) -> Result<()> {
        let batch = self.cfg.write_batch_rows;
        if matches!(plan.mode, Mode::Global) {
            if !dirty.all {
                return Ok(());
            }
            let rows = mv::read_source(&mut self.src, plan, layout, None, &self.metrics).await?;
            Metrics::add(&self.metrics.units_refreshed, 1);
            for t in targets {
                let ts = self.targets[t].schema.clone();
                let metrics = self.metrics.clone();
                let c = self.target_conn(t).await?;
                mv::apply(c, &ts, table, layout, None, &rows, batch, &metrics).await?;
            }
            return Ok(());
        }
        let mut units = std::mem::take(&mut dirty.units);
        for (ai, images) in dirty.images.iter().enumerate() {
            if !images.is_empty() {
                self.lookup(plan, ai, images, &mut units).await?;
            }
        }
        Metrics::add(&self.metrics.units_refreshed, units.len() as u64);
        for chunk in units.chunks(self.cfg.query_chunk) {
            self.metrics.beat();
            let rows = mv::read_source(&mut self.src, plan, layout, Some(&chunk), &self.metrics).await?;
            for t in targets {
                let ts = self.targets[t].schema.clone();
                let metrics = self.metrics.clone();
                let c = self.target_conn(t).await?;
                if let Err(e) = mv::apply(c, &ts, table, layout, Some(&chunk), &rows, batch, &metrics).await {
                    // Drop the connection so the next attempt reconnects.
                    self.targets.get_mut(t).unwrap().conn = None;
                    return Err(e.context(format!("target {t}")));
                }
            }
        }
        Ok(())
    }

    /// Resolves row images of `alias` to units with `SELECT DISTINCT <units>` over the view's
    /// FROM/WHERE where that table is replaced by the literal images.
    async fn lookup(&mut self, plan: &ViewPlan, alias: usize, images: &HashSet<Key>, units: &mut UnitSet) -> Result<()> {
        let a = &plan.aliases[alias];
        let cols: Vec<&crate::catalog::Column> = a.image_cols.iter().map(|&c| &a.table.columns[c]).collect();
        if cols.is_empty() {
            // No membership column: every unit may be affected (e.g. a cross join).
            bail!("view `{}`: table `{}` contributes to every row; incremental lookup impossible", plan.name, a.table.name);
        }
        let per = (60_000 / cols.len()).clamp(1, self.cfg.query_chunk);
        let first = cols
            .iter()
            .map(|c| format!("{} AS {}", c.param, analyze::quote_ident(&c.name)))
            .collect::<Vec<_>>()
            .join(", ");
        let rest = cols.iter().map(|c| c.param.as_str()).collect::<Vec<_>>().join(", ");
        let images: Vec<&Key> = images.iter().collect();
        for chunk in images.chunks(per) {
            let mut derived = format!("(SELECT {first}");
            for _ in 1..chunk.len() {
                derived.push_str(" UNION ALL SELECT ");
                derived.push_str(&rest);
            }
            derived.push(')');
            let sql = plan.lookup_sql(alias, &derived);
            let params: Vec<Value> = chunk.iter().flat_map(|k| k.iter().map(Datum::to_value)).collect();
            let rows: Vec<mysql_async::Row> = self.src.exec(sql, params).await?;
            Metrics::add(&self.metrics.lookup_queries, 1);
            for r in rows {
                let key: Key = r.unwrap().into_iter().map(Datum::from_value).collect();
                units.insert(key);
            }
        }
        Ok(())
    }

    async fn persist_position(&mut self, force: bool) -> Result<()> {
        if !force && (self.boundary == self.persisted || !self.persist_needed) {
            return Ok(());
        }
        self.persist_needed = false;
        if self.boundary.file.is_empty() {
            return Ok(());
        }
        // Never past an outstanding prepared XA transaction: its rows must be re-read after a
        // restart so they can be applied at XA COMMIT.
        let pos = self
            .xa_pending
            .values()
            .map(|(p, _)| p)
            .chain(self.xa_current.as_ref().map(|(_, p, _)| p))
            .filter(|p| !p.file.is_empty())
            .min()
            .filter(|p| **p < self.boundary)
            .cloned()
            .unwrap_or_else(|| self.boundary.clone());
        meta::save_position(&mut self.meta, &self.cfg.instance_id, &pos, self.leader.epoch).await?;
        self.persisted = self.boundary.clone();
        self.last_persist = Instant::now();
        Ok(())
    }

    /// `once` mode: waits for all builds and swaps them in.
    async fn wait_builds(&mut self) -> Result<()> {
        let mut rx = self.build_rx.take().expect("wait_builds runs once");
        while self.slots.iter().any(|s| s.next.as_ref().map(|n| n.done.is_none()).unwrap_or(false)) {
            match rx.recv().await {
                Some(done) => self.on_build_done(done),
                None => break,
            }
        }
        for i in 0..self.slots.len() {
            if self.slots[i].next.is_some() {
                self.finish_next(i).await;
            }
        }
        if self.slots.iter().any(|s| s.error.is_some()) {
            bail!("some views failed to build; see _lazyview_state");
        }
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn build_task(
    src_opts: Opts,
    src_schema: String,
    targets: Vec<(Opts, String)>,
    plan: Arc<ViewPlan>,
    name: String,
    batch_rows: usize,
    metrics: Arc<Metrics>,
    tx: mpsc::UnboundedSender<BuildDone>,
    slots: Arc<tokio::sync::Semaphore>,
    def_id: u64,
    generation: u64,
) {
    let result = async {
        let _permit = slots.acquire_owned().await?;
        let mut src = connect(&src_opts).await?;
        let layout = mv::probe(&mut src, &src_schema, &plan).await?;
        let mut conns = Vec::new();
        for (opts, schema) in &targets {
            let mut c = connect(opts).await?;
            mv::create_shadow(&mut c, schema, &name, &layout).await?;
            conns.push((c, schema.clone()));
        }
        let mut bt: Vec<BuildTarget> = conns.iter_mut().map(|(c, s)| BuildTarget { conn: c, schema: s.clone() }).collect();
        let n = mv::fill(&mut src, &plan, &layout, &mut bt, &name, batch_rows, &metrics).await?;
        info!(view = %name, rows = n, "build finished");
        for (c, _) in conns {
            let _ = c.disconnect().await;
        }
        let _ = src.disconnect().await;
        Ok::<_, anyhow::Error>(layout)
    }
    .await;
    if let Err(e) = &result {
        warn!(view = %name, "build failed: {e:#}");
    }
    let _ = tx.send(BuildDone { def_id, generation, result });
}
