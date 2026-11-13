//! Soak workload (PR S1 of the soak harness): the **neutral** schema, the neutral view set, the
//! seeded statement mix and the paced writers. Everything here is neutral on purpose —
//! `entities`, `items`, `links`, `tags`, `events`, `groups`, `sets`, `sets_items`, `sets_parts`,
//! `sets_notes`, `entity_refs` — no application vocabulary, because the soak's exactness checks
//! are written against this set (`tmp/out/lvs-soak/breakdown.md` §2), while `workload.rs` stays
//! the application-shaped e-commerce set other suites use.
//!
//! Reused from `workload.rs` by import, not copy: [`Gen`], [`Sizes`], [`Txn`], [`run_txn`].
//! The shapes it mirrors are named in the comment above each item.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use mysql_async::Conn;
use tokio::sync::watch;

use super::oracle::{Kind, ViewSpec};
use super::workload::{run_txn, Gen, Sizes, Txn};
use super::{exec, q, Db};

/// `NULL` with probability `p`, else the (lazily evaluated) value (the `workload.rs:21` shape).
macro_rules! opt {
    ($g:expr, $p:expr, $v:expr) => {
        if $g.chance($p) {
            "NULL".to_string()
        } else {
            $v
        }
    };
}

// ---------------------------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------------------------

/// Source tables. Only `sets_items`, `sets_parts`, `sets_notes` and `entity_refs` have foreign
/// keys (the `sets → sets_items → sets_parts` two-level `ON DELETE/UPDATE CASCADE` chain,
/// `sets_notes` with `ON DELETE SET NULL`, `entity_refs` cascading from `entities`); everything
/// else has none, because FK cascades are not row-logged in the binlog except on MySQL 9.x/26.x
/// with `innodb_native_foreign_keys=OFF` — see `common::cascades_logged` (`mod.rs:338`).
/// `entities.note`, `entities.spare_at`, `items.label` and `groups.region` are read by no view
/// ([`SOAK_UNREFERENCED`]): updating them must cost nothing.
pub const SOAK_DDL: &[&str] = &[
    // The parent of items / links / entity_refs.
    "CREATE TABLE entities (
       id INT NOT NULL PRIMARY KEY,
       grp INT NOT NULL,
       kind VARCHAR(16) NULL,
       amount DECIMAL(10,2) NULL,
       state VARCHAR(12) NULL,
       weight DOUBLE NULL,
       created_at DATETIME(6) NOT NULL,
       note VARCHAR(255) NULL,
       meta JSON NULL,
       spare_at TIMESTAMP(3) NULL DEFAULT NULL,
       KEY k_grp (grp)
     )",
    "CREATE TABLE items (
       id BIGINT NOT NULL PRIMARY KEY,
       entity_id INT NULL,
       grp INT NOT NULL,
       qty INT NOT NULL,
       price DECIMAL(10,2) NOT NULL,
       label VARCHAR(24) NULL,
       KEY k_entity (entity_id)
     )",
    // The edge table every hop view walks (entities → links → entities).
    "CREATE TABLE links (
       src_id INT NOT NULL,
       dst_id INT NOT NULL,
       seq INT NOT NULL,
       kind VARCHAR(16) NOT NULL,
       PRIMARY KEY (src_id, dst_id, seq),
       KEY k_dst (dst_id)
     )",
    // Composite primary key, no FK.
    "CREATE TABLE tags (
       item_id INT NOT NULL,
       tag VARCHAR(24) NOT NULL,
       n INT NOT NULL,
       PRIMARY KEY (item_id, tag)
     )",
    // No primary key, no index.
    "CREATE TABLE events (
       entity_id INT NULL,
       kind VARCHAR(20) NOT NULL,
       amount DECIMAL(10,2) NULL,
       happened_at DATETIME NOT NULL
     )",
    "CREATE TABLE `groups` (
       id INT NOT NULL PRIMARY KEY,
       region VARCHAR(8) NOT NULL,
       rate DECIMAL(5,3) NOT NULL
     )",
    // The FK chain (all InnoDB). `sets` and `sets_items` primary keys are never moved: the child
    // FKs only declare `ON DELETE`, so a parent key update would be restricted (ERROR 1451).
    "CREATE TABLE sets (
       id INT NOT NULL PRIMARY KEY,
       name VARCHAR(30) NOT NULL
     ) ENGINE=InnoDB",
    "CREATE TABLE sets_items (
       id BIGINT NOT NULL PRIMARY KEY,
       set_id INT NULL,
       label VARCHAR(30) NOT NULL,
       KEY k_set (set_id),
       CONSTRAINT fk_sets_items_set FOREIGN KEY (set_id) REFERENCES sets (id) ON DELETE CASCADE ON UPDATE CASCADE
     ) ENGINE=InnoDB",
    "CREATE TABLE sets_parts (
       item_id BIGINT NOT NULL,
       seq INT NOT NULL,
       qty INT NOT NULL,
       PRIMARY KEY (item_id, seq),
       CONSTRAINT fk_sets_parts_item FOREIGN KEY (item_id) REFERENCES sets_items (id) ON DELETE CASCADE
     ) ENGINE=InnoDB",
    "CREATE TABLE sets_notes (
       id INT NOT NULL PRIMARY KEY,
       set_id INT NULL,
       body VARCHAR(40) NOT NULL,
       KEY k_note_set (set_id),
       CONSTRAINT fk_sets_notes_set FOREIGN KEY (set_id) REFERENCES sets (id) ON DELETE SET NULL
     ) ENGINE=InnoDB",
    "CREATE TABLE entity_refs (
       entity_id INT NOT NULL,
       seq INT NOT NULL,
       city VARCHAR(30) NOT NULL,
       PRIMARY KEY (entity_id, seq),
       CONSTRAINT fk_entity_refs_entity FOREIGN KEY (entity_id) REFERENCES entities (id) ON DELETE CASCADE
     ) ENGINE=InnoDB",
];

/// Every source table, in the order the DDL creates them (the fingerprint loop of
/// `tests/soak.rs` walks this list).
pub const SOAK_TABLES: &[&str] = &[
    "entities",
    "items",
    "links",
    "tags",
    "events",
    "groups",
    "sets",
    "sets_items",
    "sets_parts",
    "sets_notes",
    "entity_refs",
];

/// Columns no view reads (updates of these must cost nothing). Mirrors `workload.rs:152`.
pub const SOAK_UNREFERENCED: &[(&str, &str)] = &[
    ("entities", "note"),
    ("entities", "spare_at"),
    ("items", "label"),
    ("groups", "region"),
];

// ---------------------------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------------------------

/// The soak's view set, minus `LAZYVIEW_TEST_SKIP_VIEWS` (the `workload.rs:160` shape).
pub fn soak_views() -> Vec<ViewSpec> {
    super::skip_views(all_soak_views())
}

/// [`soak_views`] with every view also maintained on the peer target (`source,<peer>`, the
/// `oracle.rs:66` builder; the shape of `supervisor.rs:57`). `scenario` gives one server per run,
/// so a peer only exists in a `scenario_multi` run — the caller passes its name when it has one.
pub fn soak_views_with_peer(peer: Option<&str>) -> Vec<ViewSpec> {
    match peer {
        Some(p) => soak_views()
            .into_iter()
            .map(|s| s.with_targets(&format!("source,{p}")))
            .collect(),
        None => soak_views(),
    }
}

/// The `wc_*` names: the views that read a child table of a cascading foreign key. The soak's own
/// cascade predicate (F8): `workload.rs:367 reads_fk_child` is prefix-based on `vf_` and is
/// called by `is_cadence_rebuilt` (`engine.rs:304`), so the neutral set must not rely on it.
pub fn soak_cascade_views() -> Vec<&'static str> {
    vec![
        "wc_child_parent",
        "wc_child_by_parent",
        "wc_child_only",
        "wc_notes",
        "wc_notes_by_set",
        "wc_refs_by_city",
        "wc_parts_by_item",
    ]
}

/// All specs of §2 of the breakdown, grouped by access path with one comment per group, so a
/// reviewer can check the coverage at a glance. Every column named here exists in
/// [`SOAK_DDL`]; the two `noisy` definitions carry the MySQL noise of `oracle.rs:54`.
fn all_soak_views() -> Vec<ViewSpec> {
    use Kind::*;
    vec![
        // --- direct: single-table filter and projection with expressions over DECIMAL / DOUBLE /
        //     JSON / DATETIME(6), one of them with the full noise of a hand-written view.
        ViewSpec::new(
            "w_entities_active",
            Incremental,
            "SELECT id, grp, kind, amount, state, weight, created_at FROM entities WHERE state IN ('new','open')",
        )
        .noisy(
            "ALGORITHM=UNDEFINED DEFINER=CURRENT_USER SQL SECURITY DEFINER",
            Some("entity_id, e_grp, e_kind, e_amount, e_state, e_weight, e_created"),
            "WITH LOCAL CHECK OPTION",
        ),
        ViewSpec::new(
            "w_entity_exprs",
            Incremental,
            "SELECT id, UPPER(kind) AS ukind, CONCAT(kind, '-', COALESCE(state, '-')) AS slug,
                    COALESCE(amount, 0) * 2 AS dbl_amount, ROUND(weight, 2) AS w2,
                    JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang')) AS lang,
                    YEAR(created_at) AS yr, DATE(created_at) AS day
             FROM entities WHERE grp >= 0",
        ),
        ViewSpec::new(
            "w_items_direct",
            Incremental,
            "SELECT id, entity_id, grp, qty, price FROM items WHERE qty > 0",
        )
        .noisy(
            // ALGORITHM=MERGE: a plain projection with column references only.
            "OR REPLACE ALGORITHM=MERGE DEFINER=CURRENT_USER SQL SECURITY INVOKER",
            Some("item_id, i_entity, i_grp, i_qty, i_price"),
            "WITH CHECK OPTION",
        ),
        // --- partial: 2/3/4-way joins on items / tags and LEFT JOIN chains (rows flip
        //     NULL <-> matched, so a partial derivation has to be repaired).
        ViewSpec::new(
            "w_item_tags",
            Incremental,
            "SELECT t.item_id, t.tag, t.n, i.qty, i.price
             FROM tags t JOIN items i ON i.id = t.item_id",
        ),
        ViewSpec::new(
            "w_item_entity_grp",
            Incremental,
            "SELECT i.id, i.qty, e.kind AS e_kind, g.rate
             FROM items i JOIN entities e ON e.id = i.entity_id JOIN `groups` g ON g.id = e.grp
             WHERE i.qty > 0",
        ),
        ViewSpec::new(
            "w_items_4way",
            Incremental,
            "SELECT i.id AS item, t.tag, e.kind AS e_kind, g.rate, i.qty * i.price AS amount
             FROM items i
             JOIN tags t ON t.item_id = i.id
             JOIN entities e ON e.id = i.entity_id
             JOIN `groups` g ON g.id = e.grp",
        ),
        ViewSpec::new(
            "w_items_left",
            Incremental,
            "SELECT i.id, i.qty, e.kind AS e_kind, e.state AS e_state, g.rate
             FROM items i
             LEFT JOIN entities e ON e.id = i.entity_id
             LEFT JOIN `groups` g ON g.id = e.grp",
        ),
        ViewSpec::new(
            "w_items_tags_left",
            Incremental,
            "SELECT i.id, i.price, t.tag, t.n FROM items i LEFT JOIN tags t ON t.item_id = i.id",
        ),
        // --- hop: through the edge table (self-join over entities, plus a two-hop variant) and
        //     the cascade chain reached by hop.
        ViewSpec::new(
            "w_hop_partners",
            Incremental,
            "SELECT a.id AS src, b.id AS dst, a.grp AS src_grp, b.state AS dst_state,
                    a.weight AS src_weight
             FROM entities a JOIN links l ON l.src_id = a.id JOIN entities b ON b.id = l.dst_id",
        ),
        ViewSpec::new(
            "w_hop_two",
            Incremental,
            "SELECT a.id AS a_id, b.id AS b_id, c.id AS c_id, a.kind AS a_kind, c.state AS c_state
             FROM entities a
             JOIN links l1 ON l1.src_id = a.id
             JOIN entities b ON b.id = l1.dst_id
             JOIN links l2 ON l2.src_id = b.id
             JOIN entities c ON c.id = l2.dst_id
             WHERE a.id <> c.id",
        ),
        ViewSpec::new(
            "w_hop_parts",
            Incremental,
            "SELECT p.item_id, p.seq, p.qty, si.set_id, s.name AS set_name
             FROM sets_parts p JOIN sets_items si ON si.id = p.item_id JOIN sets s ON s.id = si.set_id",
        ),
        // --- cascade: over the FK-cascading children ([`soak_cascade_views`]); the child rows of
        //     a cascade are usually not row-logged, so only the cascade cache can keep these exact.
        ViewSpec::new(
            "wc_child_parent",
            Incremental,
            "SELECT si.id, si.set_id, si.label, s.name AS set_name
             FROM sets_items si JOIN sets s ON s.id = si.set_id",
        ),
        ViewSpec::new(
            "wc_child_by_parent",
            Incremental,
            "SELECT set_id, COUNT(*) AS n FROM sets_items GROUP BY set_id",
        ),
        ViewSpec::new(
            "wc_child_only",
            Incremental,
            "SELECT id, set_id, label FROM sets_items WHERE id > 0",
        ),
        ViewSpec::new(
            "wc_notes",
            Incremental,
            "SELECT id, set_id, body FROM sets_notes",
        ),
        ViewSpec::new(
            "wc_notes_by_set",
            Incremental,
            "SELECT set_id, COUNT(*) AS n FROM sets_notes GROUP BY set_id",
        ),
        ViewSpec::new(
            "wc_refs_by_city",
            Incremental,
            "SELECT city, COUNT(*) AS n FROM entity_refs GROUP BY city",
        ),
        ViewSpec::new(
            "wc_parts_by_item",
            Incremental,
            "SELECT item_id, seq, qty FROM sets_parts WHERE qty >= 0",
        ),
        // --- aggregate: GROUP BY over joins (NULL group keys through the LEFT JOINs), the
        //     PK-less table, DISTINCT, ORDER BY … LIMIT, IN (subselect), UNION, global aggregate.
        ViewSpec::new(
            "w_groups_by_region",
            Incremental,
            "SELECT i.entity_id, e.grp, COUNT(*) AS n, SUM(i.qty * i.price) AS revenue,
                    MIN(g.rate) AS min_rate, MAX(g.rate) AS max_rate, AVG(i.qty) AS avg_qty
             FROM items i
             LEFT JOIN entities e ON e.id = i.entity_id
             LEFT JOIN `groups` g ON g.id = e.grp
             GROUP BY i.entity_id, e.grp
             HAVING COUNT(*) > 1",
        ),
        ViewSpec::new(
            "w_events_by_kind",
            Incremental,
            "SELECT kind, entity_id, COUNT(*) AS n, SUM(amount) AS total
             FROM events GROUP BY kind, entity_id",
        ),
        ViewSpec::new("w_links_distinct", Incremental, "SELECT DISTINCT src_id, kind FROM links"),
        ViewSpec::new(
            "w_top_items",
            Incremental,
            "SELECT id, qty, price FROM items WHERE qty > 0 ORDER BY price DESC, id LIMIT 5",
        ),
        ViewSpec::new(
            "w_big_entities",
            Incremental,
            "SELECT e.id, e.kind FROM entities e
             WHERE e.id IN (SELECT i.entity_id FROM items i JOIN tags t ON t.item_id = i.id WHERE i.qty > 3)",
        ),
        ViewSpec::new(
            "w_union_kinds",
            Full,
            "SELECT 'e' AS src, id, kind AS val FROM entities WHERE grp > 0
             UNION SELECT 's', id, name FROM sets",
        ),
        // the global aggregate (no GROUP BY): the `Route::Recompute` shape.
        ViewSpec::new(
            "w_global_entities",
            Incremental,
            "SELECT COUNT(*) AS n, SUM(amount) AS total, MIN(grp) AS min_grp, MAX(weight) AS max_weight
             FROM entities",
        ),
        ViewSpec::new(
            "w_total_amount",
            Incremental,
            "SELECT COUNT(*) AS n, SUM(i.qty * i.price) AS gross, MAX(e.weight) AS max_weight
             FROM items i JOIN entities e ON e.id = i.entity_id",
        ),
        // --- chained: an MV over an MV, so a check needs the extra catch-up rounds of
        //     `engine.rs:205`.
        ViewSpec::new(
            "w_chain_a",
            Chained,
            "SELECT entity_id, revenue, n FROM w_groups_by_region WHERE revenue > 500",
        ),
        ViewSpec::new(
            "w_chain_b",
            Chained,
            "SELECT e.state, COUNT(*) AS n_entities, SUM(c.revenue) AS revenue
             FROM w_groups_by_region c JOIN entities e ON e.id = c.entity_id GROUP BY e.state",
        ),
    ]
}

// ---------------------------------------------------------------------------------------------
// Ids and values
// ---------------------------------------------------------------------------------------------

/// Fresh-id allocators shared by concurrent writers (the `workload.rs:422` shape): an id from an
/// allocator never collides with another writer's, so only *seed* ids are ever re-created by key
/// (`seed_entities` / `seed_items` / `seed_sets`).
#[derive(Debug)]
pub struct SoakIds {
    /// Highest seeded id handed out by the seed data (never handed out by an allocator).
    pub seed_entities: i64,
    pub seed_items: i64,
    pub seed_sets: i64,
    pub seed_sets_items: i64,
    /// Highest seeded `groups` id (the group set is fixed).
    pub groups: i64,
    pub entity: AtomicI64,
    pub item: AtomicI64,
    pub link_seq: AtomicI64,
    pub tag_seq: AtomicI64,
    pub group: AtomicI64,
    pub set: AtomicI64,
    pub set_item: AtomicI64,
    pub part_seq: AtomicI64,
    pub set_note: AtomicI64,
    pub ref_seq: AtomicI64,
    /// XA branch ids (`soak-<n>`), so a branch id is unique per run.
    pub xid: AtomicI64,
}

impl SoakIds {
    fn fresh(c: &AtomicI64) -> i64 {
        c.fetch_add(1, Ordering::SeqCst)
    }
    /// Highest id handed out so far.
    fn max(c: &AtomicI64) -> i64 {
        (c.load(Ordering::SeqCst) - 1).max(1)
    }
}

const KINDS: &[&str] = &["alpha", "beta", "gamma", "delta", "epsilon", "zeta"];
const STATES: &[&str] = &["new", "open", "held", "done", "void"];
const EVKINDS: &[&str] = &["open", "close", "sync", "note", "ping"];
const TAGPOOL: &[&str] = &[
    "red", "blue", "tall", "fast", "heavy", "old", "new", "x1", "y2", "zz",
];
const CITIES: &[&str] = &["Berlin", "Paris", "Tokyo", "Lima", "Oslo", "Cairo"];
const LINK_KINDS: &[&str] = &["uses", "owns", "pairs", "refs", "copies"];
const LANGS: &[&str] = &["en", "de", "fr", "ja"];

/// TIMESTAMP(3) literal, avoiding 01:00–03:59 local time (the DST gap on non-UTC servers), like
/// `workload.rs:552 ts6`.
fn ts3(g: &mut Gen) -> String {
    format!(
        "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}'",
        g.range(2020, 2025),
        g.range(1, 12),
        g.range(1, 28),
        g.range(4, 23),
        g.range(0, 59),
        g.range(0, 59),
        g.range(0, 999)
    )
}

/// A JSON document literal for `entities.meta` (the `workload.rs:593 meta` shape).
fn meta(g: &mut Gen) -> String {
    if g.chance(0.2) {
        return "NULL".into();
    }
    let lang = *g.pick(LANGS);
    q(&format!(
        "{{\"lang\": \"{lang}\", \"score\": {}}}",
        g.range(0, 10)
    ))
}

// ---------------------------------------------------------------------------------------------
// Setup: schema + seed data
// ---------------------------------------------------------------------------------------------

/// Creates the neutral tables and seeds deterministic data; returns the id allocators (the
/// `workload.rs:610 setup_ecommerce` shape).
pub async fn setup_soak(db: &Db, seed: u64, s: Sizes) -> Result<Arc<SoakIds>> {
    let mut c = db.conn().await?;
    for ddl in SOAK_DDL {
        exec(&mut c, ddl).await?;
    }
    let (stmts, ids) = soak_seed_statements(seed, &s);
    for st in &stmts {
        exec(&mut c, st).await?;
    }
    c.disconnect().await.ok();
    Ok(Arc::new(ids))
}

fn chunked_insert(out: &mut Vec<String>, head: &str, rows: Vec<String>) {
    for chunk in rows.chunks(200) {
        out.push(format!("{head} VALUES {}", chunk.join(", ")));
    }
}

/// Deterministic seed data as multi-row INSERTs for every table (the `workload.rs:630` shape).
/// `Sizes` is read as: `regions` → `groups` and `sets`, `customers` → `entities` and
/// `entity_refs`, `products` → `items` and `sets_items`, `orders` → `links` and `sets_parts`,
/// `events` → `tags` and `events`. The same `(seed, sizes)` gives the same statements, which is
/// what `soak_workload_is_deterministic` asserts.
pub fn soak_seed_statements(seed: u64, s: &Sizes) -> (Vec<String>, SoakIds) {
    let mut g = Gen::new(seed ^ 0x50a9);
    let mut out = Vec::new();
    let (n_groups, n_entities, n_items, n_links, n_events) =
        (s.regions, s.customers, s.products, s.orders, s.events);
    let n_sets = s.regions;
    let n_set_items = s.products;
    let n_notes = s.regions;

    let groups: Vec<String> = (1..=n_groups)
        .map(|id| {
            format!(
                "({id}, {}, 0.{:03})",
                q(&format!("r{}", id % 10)),
                g.range(0, 250)
            )
        })
        .collect();
    chunked_insert(&mut out, "INSERT INTO `groups` (id, region, rate)", groups);

    let entities: Vec<String> = (1..=n_entities)
        .map(|id| {
            let kind = opt!(g, 0.15, q(g.pick(KINDS)));
            let amount = opt!(g, 0.2, g.decimal(-50_000, 500_000));
            let state = opt!(g, 0.15, q(g.pick(STATES)));
            let weight = opt!(g, 0.2, format!("{}.{}", g.range(0, 50), g.range(0, 9)));
            let note = opt!(g, 0.3, q(&format!("note {id}")));
            let spare = opt!(g, 0.4, ts3(&mut g));
            let grp = g.range(1, n_groups + 1);
            format!(
                "({id}, {grp}, {kind}, {amount}, {state}, {weight}, {}, {note}, {}, {spare})",
                g.dt6(),
                meta(&mut g)
            )
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO entities (id, grp, kind, amount, state, weight, created_at, note, meta, spare_at)",
        entities,
    );

    // Items share their entity in pairs, so a grouped view has groups with more than one row.
    let items: Vec<String> = (1..=n_items)
        .map(|id| {
            let entity = opt!(g, 0.15, (((id * 2) % n_entities) + 1).to_string());
            let label = opt!(g, 0.3, q(&format!("l{id}")));
            format!(
                "({id}, {entity}, {}, {}, {}, {label})",
                g.range(1, n_groups + 1),
                g.range(0, 9),
                g.decimal(50, 50_000)
            )
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO items (id, entity_id, grp, qty, price, label)",
        items,
    );

    // A chain over the consecutive entity ids plus random edges: the chain is what makes the
    // two-hop view non-empty on tiny sizes.
    let mut links = Vec::new();
    let mut seq = 1i64;
    for src in 1..n_entities {
        let kind = q(g.pick(LINK_KINDS));
        links.push(format!("({src}, {}, {seq}, {kind})", src + 1));
        seq += 1;
    }
    for _ in 0..n_links.max(1) {
        let (src, dst) = (g.range(1, n_entities + 1), g.range(1, n_entities + 1));
        let kind = q(g.pick(LINK_KINDS));
        links.push(format!("({src}, {dst}, {seq}, {kind})"));
        seq += 1;
    }
    chunked_insert(
        &mut out,
        "INSERT INTO links (src_id, dst_id, seq, kind)",
        links,
    );

    // tags: 0..3 distinct tags per item, so the composite primary key never collides.
    let mut tags = Vec::new();
    for item in 1..=n_items {
        for k in 0..g.range(0, 3) {
            let tag = q(TAGPOOL[((item + k) as usize) % TAGPOOL.len()]);
            tags.push(format!("({item}, {tag}, {})", g.range(0, 20)));
        }
    }
    if tags.is_empty() {
        tags.push("(1, 'red', 1)".into());
    }
    chunked_insert(&mut out, "INSERT INTO tags (item_id, tag, n)", tags);

    let events: Vec<String> = (0..n_events)
        .map(|_| {
            let entity = opt!(g, 0.1, g.range(1, n_entities).to_string());
            let amount = opt!(g, 0.2, g.decimal(0, 20_000));
            format!("({entity}, {}, {amount}, {})", q(g.pick(EVKINDS)), g.dt0())
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO events (entity_id, kind, amount, happened_at)",
        events,
    );

    let sets: Vec<String> = (1..=n_sets)
        .map(|id| format!("({id}, 'set {id}')"))
        .collect();
    chunked_insert(&mut out, "INSERT INTO sets (id, name)", sets);

    let set_items: Vec<String> = (1..=n_set_items)
        .map(|id| {
            // only existing `sets` ids: this FK has no ON UPDATE action and no INSERT IGNORE.
            let set_id = opt!(g, 0.15, g.range(1, n_sets).to_string());
            format!("({id}, {set_id}, 'item {id}')")
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO sets_items (id, set_id, label)",
        set_items,
    );

    // sets_parts over the existing sets_items ids (parents before children, an FK target exists).
    let mut parts = Vec::new();
    let mut part_seq = 1i64;
    for si in 1..=n_set_items {
        for _ in 0..g.range(0, 3) {
            parts.push(format!("({si}, {part_seq}, {})", g.range(-2, 20)));
            part_seq += 1;
        }
    }
    if !parts.is_empty() {
        chunked_insert(
            &mut out,
            "INSERT INTO sets_parts (item_id, seq, qty)",
            parts,
        );
    }

    let notes: Vec<String> = (1..=n_notes)
        .map(|id| {
            // only existing `sets` ids (the ON DELETE SET NULL FK has no ON UPDATE action).
            let set_id = opt!(g, 0.2, g.range(1, n_sets).to_string());
            format!("({id}, {set_id}, 'note {id}')")
        })
        .collect();
    chunked_insert(&mut out, "INSERT INTO sets_notes (id, set_id, body)", notes);

    let mut refs = Vec::new();
    let mut ref_seq = 1i64;
    for e in 1..=n_entities {
        for _ in 0..g.range(0, 3) {
            refs.push(format!("({e}, {ref_seq}, {})", q(g.pick(CITIES))));
            ref_seq += 1;
        }
    }
    if !refs.is_empty() {
        chunked_insert(
            &mut out,
            "INSERT INTO entity_refs (entity_id, seq, city)",
            refs,
        );
    }

    let ids = SoakIds {
        seed_entities: n_entities,
        seed_items: n_items,
        seed_sets: n_sets,
        seed_sets_items: n_set_items,
        groups: n_groups,
        entity: AtomicI64::new(n_entities + 1),
        item: AtomicI64::new(n_items + 1),
        link_seq: AtomicI64::new(seq),
        tag_seq: AtomicI64::new(1),
        group: AtomicI64::new(n_groups + 1),
        set: AtomicI64::new(n_sets + 1),
        set_item: AtomicI64::new(n_set_items + 1),
        part_seq: AtomicI64::new(part_seq),
        set_note: AtomicI64::new(n_notes + 1),
        ref_seq: AtomicI64::new(ref_seq),
        xid: AtomicI64::new(1),
    };
    (out, ids)
}

// ---------------------------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------------------------

/// The seeded mix of §2: inserts, updates of referenced and unreferenced columns, deletes, key
/// moves, bulk statements, FK cascade deletes, transient insert-use-delete transactions,
/// rollbacks, the hammer op and XA (the `workload.rs:870 Workload` shape).
pub struct SoakWorkload {
    g: Gen,
    ids: Arc<SoakIds>,
    /// op label → count (the report prints this, §4).
    pub stats: BTreeMap<&'static str, u64>,
    /// Probability that a transaction is rolled back (`workload.rs:885`).
    p_rollback: f64,
    /// Probability that a batch is an XA branch instead of the usual mix.
    p_xa: f64,
}

impl SoakWorkload {
    pub fn new(seed: u64, ids: Arc<SoakIds>) -> SoakWorkload {
        SoakWorkload {
            g: Gen::new(seed),
            ids,
            stats: BTreeMap::new(),
            p_rollback: 0.07,
            p_xa: 0.05,
        }
    }

    fn entity(&mut self) -> i64 {
        let hi = SoakIds::max(&self.ids.entity);
        self.g.range(1, hi + 1)
    }
    fn seed_entity(&mut self) -> i64 {
        self.g.range(1, self.ids.seed_entities.max(1))
    }
    fn entity_opt(&mut self) -> String {
        let e = self.entity();
        opt!(self.g, 0.05, e.to_string())
    }
    fn item(&mut self) -> i64 {
        let hi = SoakIds::max(&self.ids.item);
        self.g.range(1, hi + 1)
    }
    fn grp(&mut self) -> i64 {
        self.g.range(1, SoakIds::max(&self.ids.group) + 1)
    }
    fn set(&mut self) -> i64 {
        let hi = SoakIds::max(&self.ids.set);
        self.g.range(1, hi + 1)
    }
    fn set_item(&mut self) -> i64 {
        let hi = SoakIds::max(&self.ids.set_item);
        self.g.range(1, hi + 1)
    }

    /// One fresh `items` row (its `entity_id` may point at a missing entity: no FK on purpose).
    fn item_row(&mut self, entity: String) -> String {
        let id = SoakIds::fresh(&self.ids.item);
        let grp = self.grp();
        let qty = self.g.range(0, 9);
        let price = self.g.decimal(50, 50_000);
        let label = opt!(self.g, 0.3, q(&format!("l{id}")));
        format!("({id}, {entity}, {grp}, {qty}, {price}, {label})")
    }

    /// Next unit of work: usually one transaction, several for the hammer and the XA ops (the
    /// `workload.rs:936 next_batch` shape).
    pub fn next_batch(&mut self) -> Vec<Txn> {
        if self.g.chance(self.p_xa) {
            return self.xa_batch();
        }
        if self.g.chance(0.025) {
            return self.hammer();
        }
        let n_ops = self.g.range(1, 4);
        let mut stmts = Vec::new();
        let mut labels = Vec::new();
        for _ in 0..n_ops {
            let (label, s) = self.op();
            *self.stats.entry(label).or_default() += 1;
            labels.push(label);
            stmts.extend(s);
        }
        let rollback = self.g.chance(self.p_rollback);
        if rollback {
            *self.stats.entry("(rollback)").or_default() += 1;
        }
        vec![Txn {
            // deletes a row and re-creates it by the same primary key (see `run_txn`).
            retry_duplicate: labels.contains(&"sets_recreate"),
            label: labels.join("+"),
            stmts,
            rollback,
        }]
    }

    /// The same entity id written ~30× across entities / items / links, as one big transaction, a
    /// few transactions or 30 single-statement transactions (`workload.rs:963` shape).
    pub fn hammer(&mut self) -> Vec<Txn> {
        *self.stats.entry("hammer").or_default() += 1;
        let id = self.entity();
        let mut stmts = Vec::new();
        for k in 0..30 {
            let s = match k % 6 {
                0 => format!(
                    "UPDATE entities SET state = {} WHERE id = {id}",
                    q(self.g.pick(STATES))
                ),
                1 => format!(
                    "UPDATE items SET qty = qty + {} WHERE entity_id = {id}",
                    self.g.range(-1, 2)
                ),
                2 => format!(
                    "UPDATE links SET kind = {} WHERE src_id = {id}",
                    q(self.g.pick(LINK_KINDS))
                ),
                3 => {
                    let row = self.item_row(id.to_string());
                    format!(
                        "INSERT INTO items (id, entity_id, grp, qty, price, label) VALUES {row}
                     ON DUPLICATE KEY UPDATE entity_id = VALUES(entity_id), grp = VALUES(grp), qty = VALUES(qty), price = VALUES(price), label = VALUES(label)"
                    )
                }
                4 => format!(
                    "UPDATE entities SET amount = {} WHERE id = {id}",
                    opt!(self.g, 0.2, self.g.decimal(-50_000, 500_000))
                ),
                _ => {
                    let dst = self.entity();
                    let seq = SoakIds::fresh(&self.ids.link_seq);
                    format!(
                        "INSERT INTO links (src_id, dst_id, seq, kind) VALUES ({id}, {dst}, {seq}, {})",
                        q(self.g.pick(LINK_KINDS))
                    )
                }
            };
            stmts.push(s);
        }
        let per = *self.g.pick(&[30usize, 10, 1]);
        stmts
            .chunks(per)
            .map(|c| Txn {
                label: format!("hammer({id})"),
                stmts: c.to_vec(),
                rollback: false,
                retry_duplicate: false,
            })
            .collect()
    }

    /// An XA branch: `XA START` … `XA END` … `XA PREPARE` then `XA COMMIT` / `XA ROLLBACK`, or
    /// `XA COMMIT … ONE PHASE`, the statement shapes of `tests/xa.rs:14`–`28`. The whole branch
    /// is **one** `Txn`, which [`run_txn`] recognizes and runs as one unit: when the server rolls
    /// the branch back under the session (deadlock, restart) the entire branch is retried
    /// (`workload.rs` `run_xa_txn`, Q1) — per-statement batches could not, they lost the smoke
    /// run of S3 at `XA COMMIT`. The branch is still always resolved inside the batch (F5/D5: no
    /// branch is ever left prepared, so no crash can meet `XA … prepared before this run`; the
    /// retry resolves a found prepared branch the same way it would have).
    fn xa_batch(&mut self) -> Vec<Txn> {
        let n = SoakIds::fresh(&self.ids.xid);
        let xid = format!("soak-{n}");
        let label = match self.g.range(0, 2) {
            0 => "xa_commit",
            1 => "xa_rollback",
            _ => "xa_one_phase",
        };
        *self.stats.entry(label).or_default() += 1;
        let item = SoakIds::fresh(&self.ids.item);
        let entity = self.entity();
        let touch = self.entity();
        let mut stmts = vec![
            format!("XA START '{xid}'"),
            format!(
                "INSERT INTO items (id, entity_id, grp, qty, price, label) VALUES ({item}, {entity}, {}, {}, {}, NULL)
                     ON DUPLICATE KEY UPDATE entity_id = VALUES(entity_id), grp = VALUES(grp), qty = VALUES(qty), price = VALUES(price), label = VALUES(label)",
                self.grp(),
                self.g.range(1, 9),
                self.g.decimal(100, 90_000)
            ),
            format!(
                "INSERT INTO tags (item_id, tag, n) VALUES ({item}, 'xa{n}', {})
                     ON DUPLICATE KEY UPDATE n = VALUES(n)",
                self.g.range(0, 9)
            ),
            format!(
                "UPDATE entities SET state = {} WHERE id = {touch}",
                q(self.g.pick(STATES))
            ),
            format!(
                "UPDATE items SET qty = qty + 100 WHERE id = {}",
                self.item()
            ),
            format!(
                "DELETE FROM events WHERE entity_id = {entity} ORDER BY entity_id, kind, amount, happened_at LIMIT 1"
            ),
            format!("XA END '{xid}'"),
        ];
        match label {
            "xa_commit" => {
                stmts.push(format!("XA PREPARE '{xid}'"));
                stmts.push(format!("XA COMMIT '{xid}'"));
            }
            "xa_rollback" => {
                stmts.push(format!("XA PREPARE '{xid}'"));
                stmts.push(format!("XA ROLLBACK '{xid}'"));
            }
            _ => stmts.push(format!("XA COMMIT '{xid}' ONE PHASE")),
        }
        vec![Txn {
            label: label.to_string(),
            stmts,
            rollback: false,
            retry_duplicate: false,
        }]
    }

    /// One random operation: (label, statements). Weights follow §2: the FK and cascade ops are a
    /// tenth of the mix, the plain DML the rest.
    pub fn op(&mut self) -> (&'static str, Vec<String>) {
        let w = self.g.range(0, 111);
        if w >= 100 {
            // (the two heaviest arms re-create a seeded `sets` row by its key.)
            // the last arm deletes a seeded `sets` row and re-creates it by id.
            let label = if w >= 111 { "sets_recreate" } else { "sets" };
            return (label, self.sets_op(w - 100));
        }
        match w {
            0..=6 => ("entity_insert", vec![self.entity_insert()]),
            7..=15 => ("entity_update", vec![self.entity_update()]),
            16 => (
                "entity_delete",
                vec![format!("DELETE FROM entities WHERE id = {}", self.entity())],
            ),
            17..=18 => ("entity_pk", self.entity_pk()),
            19..=24 => {
                let entity = self.entity_opt();
                (
                    "item_insert",
                    vec![format!(
                        "INSERT INTO items (id, entity_id, grp, qty, price, label) VALUES {}
                     ON DUPLICATE KEY UPDATE entity_id = VALUES(entity_id), grp = VALUES(grp), qty = VALUES(qty), price = VALUES(price), label = VALUES(label)",
                        self.item_row(entity)
                    )],
                )
            }
            25..=33 => ("item_update", vec![self.item_update()]),
            34..=36 => (
                "item_move",
                vec![format!(
                    "UPDATE items SET entity_id = {} WHERE id = {}",
                    self.entity_opt(),
                    self.item()
                )],
            ),
            37 => (
                "item_delete",
                vec![format!("DELETE FROM items WHERE id = {}", self.item())],
            ),
            38..=44 => ("tag", self.tag_op()),
            45..=51 => ("link", self.link_op()),
            52..=57 => ("events", self.events_op()),
            58..=60 => ("group", self.group_op()),
            61..=64 => ("refs", self.refs_op()),
            65..=69 => ("unreferenced", vec![self.unreferenced()]),
            70..=75 => ("bulk", self.bulk()),
            76..=79 => ("multi", self.multi()),
            80..=83 => ("set_items", self.sets_items_op()),
            84..=86 => ("parts", self.parts_op()),
            87..=89 => ("notes", self.notes_op()),
            90..=92 => ("set", self.set_op()),
            93..=96 => (
                "item_upsert",
                // re-insert a deleted item with ON DUPLICATE KEY UPDATE (NULL <-> matched); only
                // seed ids: fresh ids belong to the allocator (concurrent writers).
                vec![format!(
                    "INSERT INTO items (id, entity_id, grp, qty, price) VALUES ({}, {}, {}, {}, {})
                     ON DUPLICATE KEY UPDATE qty = VALUES(qty)",
                    self.g.range(1, self.ids.seed_items.max(1)),
                    self.entity_opt(),
                    self.grp(),
                    self.g.range(0, 9),
                    self.g.decimal(50, 50_000)
                )],
            ),
            _ => self.transient(),
        }
    }

    fn entity_insert(&mut self) -> String {
        let id = SoakIds::fresh(&self.ids.entity);
        let kind = opt!(self.g, 0.15, q(self.g.pick(KINDS)));
        let amount = opt!(self.g, 0.2, self.g.decimal(-50_000, 500_000));
        let state = opt!(self.g, 0.15, q(self.g.pick(STATES)));
        let weight = opt!(
            self.g,
            0.2,
            format!("{}.{}", self.g.range(0, 50), self.g.range(0, 9))
        );
        let spare = opt!(self.g, 0.4, ts3(&mut self.g));
        format!(
            "INSERT INTO entities (id, grp, kind, amount, state, weight, created_at, note, meta, spare_at)
             VALUES ({id}, {}, {kind}, {amount}, {state}, {weight}, {}, 'fresh', {}, {spare})
                     ON DUPLICATE KEY UPDATE grp = VALUES(grp), kind = VALUES(kind), amount = VALUES(amount), state = VALUES(state), weight = VALUES(weight), created_at = VALUES(created_at), note = VALUES(note), meta = VALUES(meta), spare_at = VALUES(spare_at)",
            self.grp(),
            self.g.dt6(),
            meta(&mut self.g)
        )
    }

    fn entity_update(&mut self) -> String {
        let e = self.entity();
        match self.g.range(0, 8) {
            0 => format!(
                "UPDATE entities SET grp = {} WHERE id = {e}",
                self.grp()
            ),
            1 => format!(
                "UPDATE entities SET kind = {} WHERE id = {e}",
                opt!(self.g, 0.2, q(self.g.pick(KINDS)))
            ),
            2 => format!(
                "UPDATE entities SET state = {} WHERE id = {e}",
                opt!(self.g, 0.2, q(self.g.pick(STATES)))
            ),
            3 => format!(
                "UPDATE entities SET amount = {} WHERE id = {e}",
                opt!(self.g, 0.2, self.g.decimal(-50_000, 500_000))
            ),
            4 => format!(
                "UPDATE entities SET weight = {} WHERE id = {e}",
                opt!(self.g, 0.2, format!("{}.{}", self.g.range(0, 50), self.g.range(0, 9)))
            ),
            5 => format!("UPDATE entities SET kind = UPPER(kind) WHERE id = {e}"),
            6 => format!(
                "UPDATE entities SET meta = JSON_SET(COALESCE(meta, '{{}}'), '$.lang', {}) WHERE id = {e}",
                q(self.g.pick(LANGS))
            ),
            7 => format!(
                "UPDATE entities SET created_at = {} WHERE id = {e}",
                self.g.dt6()
            ),
            _ => format!(
                "UPDATE entities SET state = {}, weight = COALESCE(weight, 0) + 1 WHERE id = {e}",
                q(self.g.pick(STATES))
            ),
        }
    }

    /// A key move of a seeded parent. The children of `entity_refs` are removed first: its FK
    /// declares `ON DELETE CASCADE` only, so a parent key update with children would be
    /// restricted (ERROR 1451). Statements that match no row are no-ops, so a stale id is harmless.
    fn entity_pk(&mut self) -> Vec<String> {
        let o = self.seed_entity();
        let n = SoakIds::fresh(&self.ids.entity);
        let seq = SoakIds::fresh(&self.ids.ref_seq);
        let city = q(self.g.pick(CITIES));
        vec![
            format!("DELETE FROM entity_refs WHERE entity_id = {o}"),
            format!("UPDATE entities SET id = {n} WHERE id = {o}"),
            format!(
                "INSERT INTO entity_refs (entity_id, seq, city) SELECT {n}, {seq}, {city} FROM entities WHERE id = {n}"
            ),
        ]
    }

    fn item_update(&mut self) -> String {
        let i = self.item();
        match self.g.range(0, 4) {
            0 => format!(
                "UPDATE items SET qty = qty + {} WHERE id = {i}",
                self.g.range(-3, 4)
            ),
            1 => format!(
                "UPDATE items SET price = {} WHERE id = {i}",
                self.g.decimal(50, 50_000)
            ),
            2 => format!(
                "UPDATE items SET qty = {}, price = {} WHERE id = {i}",
                self.g.range(0, 9),
                self.g.decimal(50, 50_000)
            ),
            3 => format!("UPDATE items SET grp = {} WHERE id = {i}", self.grp()),
            _ => format!("UPDATE items SET qty = 0 - qty WHERE id = {i}"),
        }
    }

    /// The composite-PK anchor: `tags(item_id, tag)`.
    fn tag_op(&mut self) -> Vec<String> {
        let i = self.item();
        match self.g.range(0, 3) {
            0 => {
                let tag = q(self.g.pick(TAGPOOL));
                vec![format!(
                    "INSERT INTO tags (item_id, tag, n) VALUES ({i}, {tag}, {})
                     ON DUPLICATE KEY UPDATE n = VALUES(n)",
                    self.g.range(0, 20)
                )]
            }
            1 => vec![format!(
                "UPDATE tags SET n = n + {} WHERE item_id = {i} ORDER BY tag LIMIT 1",
                self.g.range(-2, 3)
            )],
            2 => vec![format!(
                "DELETE FROM tags WHERE item_id = {i} ORDER BY tag LIMIT 1"
            )],
            _ => {
                let tag = format!("t{}", SoakIds::fresh(&self.ids.tag_seq));
                vec![format!(
                    "REPLACE INTO tags (item_id, tag, n) VALUES ({i}, {}, {})",
                    q(&tag),
                    self.g.range(0, 20)
                )]
            }
        }
    }

    /// The edge table: inserts, a key move of `src_id`, deletes.
    fn link_op(&mut self) -> Vec<String> {
        let (src, dst) = (self.entity(), self.entity());
        match self.g.range(0, 3) {
            0 => {
                let seq = SoakIds::fresh(&self.ids.link_seq);
                vec![format!(
                    "INSERT INTO links (src_id, dst_id, seq, kind) VALUES ({src}, {dst}, {seq}, {})
                     ON DUPLICATE KEY UPDATE kind = VALUES(kind)",
                    q(self.g.pick(LINK_KINDS))
                )]
            }
            1 => {
                let n = self.entity();
                vec![format!(
                    "UPDATE links SET src_id = {n} WHERE src_id = {src} ORDER BY dst_id, seq LIMIT 1"
                )]
            }
            2 => vec![format!(
                "UPDATE links SET kind = {} WHERE src_id = {src} ORDER BY dst_id, seq LIMIT 1",
                q(self.g.pick(LINK_KINDS))
            )],
            _ => vec![format!(
                "DELETE FROM links WHERE src_id = {src} ORDER BY dst_id, seq LIMIT 1"
            )],
        }
    }

    /// The table with no primary key and no index: `LIMIT` without a total `ORDER BY` would pick
    /// rows arbitrarily, which the determinism check could not tolerate.
    fn events_op(&mut self) -> Vec<String> {
        let e = self.entity();
        let order = "ORDER BY entity_id, kind, amount, happened_at";
        match self.g.range(0, 3) {
            0 => {
                // duplicates on purpose (no PK)
                let n = self.g.range(1, 4);
                let kind = q(self.g.pick(EVKINDS));
                let amount = opt!(self.g, 0.2, self.g.decimal(0, 20_000));
                let at = self.g.dt0();
                let row = format!("({e}, {kind}, {amount}, {at})");
                vec![format!(
                    "INSERT INTO events (entity_id, kind, amount, happened_at) VALUES {}",
                    vec![row; n as usize].join(", ")
                )]
            }
            1 => vec![format!(
                "UPDATE events SET amount = COALESCE(amount, 0) + {} WHERE entity_id = {e} {order} LIMIT 2",
                self.g.decimal(-1000, 9000)
            )],
            2 => vec![format!(
                "UPDATE events SET entity_id = {} WHERE entity_id = {e} {order} LIMIT 1",
                self.entity_opt()
            )],
            _ => vec![format!(
                "DELETE FROM events WHERE entity_id = {e} {order} LIMIT 1"
            )],
        }
    }

    fn group_op(&mut self) -> Vec<String> {
        let gid = self.g.range(1, SoakIds::max(&self.ids.group) + 1);
        match self.g.range(0, 2) {
            0 => vec![format!(
                "UPDATE `groups` SET rate = 0.{:03} WHERE id = {gid}",
                self.g.range(0, 250)
            )],
            1 => vec![format!(
                "REPLACE INTO `groups` (id, region, rate) VALUES ({gid}, {}, 0.{:03})",
                q(&format!("r{}", self.g.range(0, 9))),
                self.g.range(0, 250)
            )],
            _ => {
                let n = SoakIds::fresh(&self.ids.group);
                vec![
                    format!("DELETE FROM `groups` WHERE id = {gid}"),
                    format!(
                        "INSERT INTO `groups` (id, region, rate) VALUES ({n}, {}, 0.{:03})
                     ON DUPLICATE KEY UPDATE region = VALUES(region), rate = VALUES(rate)",
                        q(&format!("r{}", self.g.range(0, 9))),
                        self.g.range(0, 250)
                    ),
                ]
            }
        }
    }

    /// `entity_refs`: the child of `entities` (its FK is `ON DELETE CASCADE`), so a parent delete
    /// or key move takes these rows with it.
    fn refs_op(&mut self) -> Vec<String> {
        let e = self.entity();
        match self.g.range(0, 3) {
            0 => {
                let seq = SoakIds::fresh(&self.ids.ref_seq);
                vec![format!(
                    "INSERT INTO entity_refs (entity_id, seq, city) SELECT {e}, {seq}, {} FROM entities WHERE id = {e}
                     ON DUPLICATE KEY UPDATE city = VALUES(city)",
                    q(self.g.pick(CITIES))
                )]
            }
            1 => vec![format!(
                "UPDATE entity_refs SET city = {} WHERE entity_id = {e} ORDER BY seq LIMIT 1",
                q(self.g.pick(CITIES))
            )],
            2 => vec![format!(
                "DELETE FROM entity_refs WHERE entity_id = {e} ORDER BY seq LIMIT 1"
            )],
            _ => {
                // a key move of the FK column, guarded by the join: no row is set to a key the
                // parent table does not have (its FK has no ON UPDATE action).
                let n = self.entity();
                vec![format!(
                    "UPDATE entity_refs r JOIN entities e ON e.id = {n} SET r.entity_id = e.id WHERE r.entity_id = {e}"
                )]
            }
        }
    }

    /// An update touching only columns no view reads ([`SOAK_UNREFERENCED`], `workload.rs:1410`).
    pub fn unreferenced(&mut self) -> String {
        match self.g.range(0, 3) {
            0 => {
                let e = self.entity();
                format!(
                    "UPDATE entities SET note = {} WHERE id BETWEEN {e} AND {}",
                    q(&format!("note {}", self.g.range(0, 1_000_000))),
                    e + 5
                )
            }
            1 => {
                let e = self.entity();
                format!(
                    "UPDATE entities SET spare_at = {} WHERE id = {e}",
                    opt!(self.g, 0.3, ts3(&mut self.g))
                )
            }
            2 => {
                let i = self.item();
                format!(
                    "UPDATE items SET label = {} WHERE id = {i}",
                    opt!(self.g, 0.2, q(&format!("l{}", self.g.range(0, 99_999))))
                )
            }
            _ => {
                let gid = self.g.range(1, SoakIds::max(&self.ids.group) + 1);
                format!(
                    "UPDATE `groups` SET region = {} WHERE id = {gid}",
                    q(&format!("r{}", self.g.range(0, 9)))
                )
            }
        }
    }

    /// Bulk statements: `INSERT … SELECT`, `UPDATE … JOIN`, `REPLACE`, `INSERT … ODKU`.
    fn bulk(&mut self) -> Vec<String> {
        let i = self.item();
        match self.g.range(0, 4) {
            0 => vec![format!(
                "UPDATE items SET price = price + 0.01 WHERE id BETWEEN {i} AND {}",
                i + 15
            )],
            1 => {
                let e = self.entity();
                vec![format!(
                    "INSERT INTO items (id, entity_id, grp, qty, price) SELECT {}, {}, {}, {}, {} FROM entities WHERE id = {e}
                     ON DUPLICATE KEY UPDATE entity_id = VALUES(entity_id), grp = VALUES(grp), qty = VALUES(qty), price = VALUES(price)",
                    SoakIds::fresh(&self.ids.item),
                    self.g.range(1, SoakIds::max(&self.ids.entity) + 1),
                    self.grp(),
                    self.g.range(0, 9),
                    self.g.decimal(50, 50_000)
                )]
            }
            2 => vec![format!(
                "UPDATE items i JOIN entities e ON e.id = i.entity_id SET i.qty = i.qty + 1
                 WHERE i.id BETWEEN {i} AND {}",
                i + 3
            )],
            3 => {
                let item = self.item();
                vec![format!(
                    "REPLACE INTO tags (item_id, tag, n) VALUES ({item}, 'rp{}', {})",
                    self.g.range(0, 999),
                    self.g.range(0, 20)
                )]
            }
            _ => {
                let item = self.item();
                vec![format!(
                    "INSERT INTO tags (item_id, tag, n) SELECT {item}, 'bulk{}', {} FROM items WHERE id = {item}
                     ON DUPLICATE KEY UPDATE n = n + VALUES(n)",
                    self.g.range(0, 999),
                    self.g.range(1, 10)
                )]
            }
        }
    }

    /// Multi-table DELETE and a bulk update across two tables.
    ///
    /// No derived write may put a NULL into a NOT NULL column: `entities.amount` is nullable and
    /// the seed leaves it NULL about a fifth of the time, so the joined value is `COALESCE`d
    /// before it reaches `items.price` (NOT NULL) — under a strict `sql_mode` the bare product is
    /// ERROR 1048 (`Column 'price' cannot be null`), which is seed-dependent and therefore looks
    /// like a flaky CI run. The other derived writes are safe by schema: every
    /// `UPDATE … JOIN … SET` here reads only a primary key (`e.id`, `s.id`) or a NOT NULL column
    /// of the target row (`i.qty`), and every `INSERT … SELECT` list is literals plus a NOT NULL
    /// primary key.
    fn multi(&mut self) -> Vec<String> {
        let i = self.item();
        let e = self.entity();
        if self.g.chance(0.5) {
            vec![format!(
                "DELETE t, i FROM tags t RIGHT JOIN items i ON t.item_id = i.id WHERE i.id = {i} AND t.item_id IS NULL"
            )]
        } else {
            vec![format!(
                "UPDATE items i JOIN entities e ON e.id = i.entity_id
                 SET i.price = COALESCE(e.amount, 0) * 2 WHERE e.id = {e} AND i.id BETWEEN {i} AND {}",
                i + 2
            )]
        }
    }

    fn set_op(&mut self) -> Vec<String> {
        let sid = self.set();
        match self.g.range(0, 2) {
            0 => vec![format!(
                "UPDATE sets SET name = 'set {}' WHERE id = {sid}",
                self.g.range(0, 999)
            )],
            1 => vec![format!(
                "INSERT INTO sets (id, name) VALUES ({}, 'set {sid}') ON DUPLICATE KEY UPDATE name = VALUES(name)",
                SoakIds::fresh(&self.ids.set)
            )],
            _ => vec![format!("DELETE FROM sets WHERE id = {sid}")],
        }
    }

    fn sets_items_op(&mut self) -> Vec<String> {
        let si = self.set_item();
        let sid = self.set();
        match self.g.range(0, 3) {
            0 => (0..self.g.range(1, 3))
                .map(|_| {
                    let id = SoakIds::fresh(&self.ids.set_item);
                    // through the parent table, so the FK value always exists
                    format!(
                        "INSERT INTO sets_items (id, set_id, label) SELECT {id}, id, 'item {id}' FROM sets WHERE id = {sid}
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), label = VALUES(label)"
                    )
                })
                .collect(),
            1 => vec![format!(
                "UPDATE sets_items si JOIN sets s ON s.id = {sid} SET si.set_id = s.id WHERE si.id = {si}"
            )],
            2 => vec![format!(
                "UPDATE sets_items SET label = 'item {}' WHERE id = {si}",
                self.g.range(0, 999)
            )],
            _ => vec![format!("DELETE FROM sets_items WHERE id = {si}")],
        }
    }

    fn parts_op(&mut self) -> Vec<String> {
        let si = self.set_item();
        match self.g.range(0, 2) {
            0 => {
                let seq = SoakIds::fresh(&self.ids.part_seq);
                vec![format!(
                    "INSERT INTO sets_parts (item_id, seq, qty) SELECT {si}, {seq}, {} FROM sets_items WHERE id = {si}
                     ON DUPLICATE KEY UPDATE qty = VALUES(qty)",
                    self.g.range(-2, 20)
                )]
            }
            1 => vec![format!(
                "UPDATE sets_parts SET qty = qty + {} WHERE item_id = {si} ORDER BY seq LIMIT 1",
                self.g.range(-3, 3)
            )],
            _ => vec![format!(
                "DELETE FROM sets_parts WHERE item_id = {si} ORDER BY seq LIMIT 1"
            )],
        }
    }

    fn notes_op(&mut self) -> Vec<String> {
        let sid = self.set();
        match self.g.range(0, 2) {
            0 => {
                let id = SoakIds::fresh(&self.ids.set_note);
                vec![format!(
                    "INSERT INTO sets_notes (id, set_id, body) SELECT {id}, id, 'n{id}' FROM sets WHERE id = {sid}
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), body = VALUES(body)"
                )]
            }
            1 => {
                let other = self.set();
                vec![format!(
                    "UPDATE sets_notes n JOIN sets s ON s.id = {other} SET n.set_id = s.id WHERE n.set_id = {sid} OR n.set_id IS NULL"
                )]
            }
            _ => vec![format!("DELETE FROM sets_notes WHERE set_id = {sid}")],
        }
    }

    /// The FK-cascading chain: deletes of a parent cascade silently (`DELETE FROM sets` takes
    /// `sets_items` and, two levels down, `sets_parts` with it and sets `sets_notes.set_id`
    /// to NULL), a delete of `sets_items` takes its `sets_parts`.
    fn sets_op(&mut self, k: i64) -> Vec<String> {
        let sid = self.set();
        let seed_sid = self.g.range(1, self.ids.seed_sets.max(1));
        let si = self.set_item();
        match k {
            0 | 1 => vec![format!("DELETE FROM sets_items WHERE id = {si}")],
            2 => vec![format!("DELETE FROM sets WHERE id = {sid}")],
            // the parent key is never moved: sets_notes' FK has no ON UPDATE action.
            3 => vec![format!(
                "UPDATE sets SET name = 'renamed {}' WHERE id = {sid}",
                self.g.range(0, 999)
            )],
            4 => vec![format!(
                "UPDATE sets_items si JOIN sets s ON s.id = {sid} SET si.set_id = s.id WHERE si.set_id IS NULL OR si.set_id = {}",
                self.set()
            )],
            5 => {
                let n = SoakIds::fresh(&self.ids.set_item);
                vec![
                    format!("INSERT IGNORE INTO sets (id, name) VALUES ({seed_sid}, 'set {seed_sid}')"),
                    format!("INSERT INTO sets_items (id, set_id, label) SELECT {n}, id, 'item {n}' FROM sets WHERE id = {seed_sid}
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), label = VALUES(label)"),
                ]
            }
            6 => {
                let n = SoakIds::fresh(&self.ids.set_item);
                let seq = SoakIds::fresh(&self.ids.part_seq);
                vec![
                    format!("INSERT INTO sets_items (id, set_id, label) SELECT {n}, id, 'item {n}' FROM sets WHERE id = {sid}
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), label = VALUES(label)"),
                    format!("INSERT INTO sets_parts (item_id, seq, qty) SELECT {n}, {seq}, 1 FROM sets_items WHERE id = {n}
                     ON DUPLICATE KEY UPDATE qty = VALUES(qty)"),
                ]
            }
            7 => vec![format!(
                "DELETE FROM sets_parts WHERE item_id = {si}"
            )],
            8 => {
                let id = SoakIds::fresh(&self.ids.set_note);
                vec![format!(
                    "INSERT INTO sets_notes (id, set_id, body) SELECT {id}, id, 'n{id}' FROM sets WHERE id = {sid}
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), body = VALUES(body)"
                )]
            }
            9 => {
                let e = self.entity();
                let seq = SoakIds::fresh(&self.ids.ref_seq);
                vec![format!(
                    "INSERT INTO entity_refs (entity_id, seq, city) SELECT {e}, {seq}, {} FROM entities WHERE id = {e}
                     ON DUPLICATE KEY UPDATE city = VALUES(city)",
                    q(self.g.pick(CITIES))
                )]
            }
            10 => vec![format!("DELETE FROM sets_notes WHERE set_id = {sid}")],
            _ => {
                // the two-level cascade in one statement plus a re-creation by the same key.
                let n = SoakIds::fresh(&self.ids.set_item);
                let seq = SoakIds::fresh(&self.ids.part_seq);
                vec![
                    format!("DELETE FROM sets WHERE id = {seed_sid}"),
                    format!("INSERT INTO sets (id, name) VALUES ({seed_sid}, 'reborn {seed_sid}')"),
                    format!("INSERT INTO sets_items (id, set_id, label) VALUES ({n}, {seed_sid}, 'new {n}')
                     ON DUPLICATE KEY UPDATE set_id = VALUES(set_id), label = VALUES(label)"),
                    format!("INSERT INTO sets_parts (item_id, seq, qty) VALUES ({n}, {seq}, 1)
                     ON DUPLICATE KEY UPDATE qty = VALUES(qty)"),
                ]
            }
        }
    }

    /// Inserts a row, references it, then deletes it again in the same transaction (§2).
    fn transient(&mut self) -> (&'static str, Vec<String>) {
        let n = SoakIds::fresh(&self.ids.entity);
        let e = self.entity();
        (
            "transient",
            vec![
                format!(
                    "INSERT INTO entities (id, grp, kind, state, created_at) VALUES ({n}, 1, 'tmp', 'new', {})
                     ON DUPLICATE KEY UPDATE grp = VALUES(grp), kind = VALUES(kind), state = VALUES(state), created_at = VALUES(created_at)",
                    self.g.dt6()
                ),
                format!("UPDATE items SET entity_id = {n} WHERE entity_id = {e}"),
                format!("UPDATE items SET entity_id = {e} WHERE entity_id = {n}"),
                format!("DELETE FROM entities WHERE id = {n}"),
            ],
        )
    }

    pub fn stats_line(&self) -> String {
        self.stats
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

// ---------------------------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------------------------

/// Runs `n` batches from `wl` on `conn`; returns the number of transactions executed (the
/// `workload.rs:1590 run_batches` shape, reusing [`run_txn`] for the retry rules).
pub async fn run_soak_batches(conn: &mut Conn, wl: &mut SoakWorkload, n: usize) -> Result<usize> {
    let mut txns = 0;
    for _ in 0..n {
        for t in wl.next_batch() {
            run_txn(conn, &t).await?;
            txns += 1;
        }
    }
    Ok(txns)
}

/// `writers` concurrent paced writers (each on its own connection and derived seed) until `stop`
/// turns true; returns the total transactions executed. Like `workload.rs:1635 run_until_stopped`
/// but paced (F9): each writer owns `tps / writers` transactions per second, accumulating its debt
/// against `Instant::now()` and sleeping only when it is ahead of schedule — so a stalled server
/// lowers the achieved rate instead of building up a backlog.
pub async fn run_paced(
    db: &Db,
    ids: Arc<SoakIds>,
    seed: u64,
    writers: usize,
    tps: u32,
    stop: watch::Receiver<bool>,
) -> Result<usize> {
    let writers = writers.max(1);
    let per = Duration::from_secs_f64(writers as f64 / f64::from(tps.max(1)));
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = db.clone();
        let ids = ids.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut c = db.conn().await?;
            let mut wl = SoakWorkload::new(super::derive_seed(seed, &format!("soak{w}")), ids);
            let start = Instant::now();
            let mut n = 0usize;
            while !*stop.borrow() {
                n += run_soak_batches(&mut c, &mut wl, 1)
                    .await
                    .with_context(|| format!("paced writer {w}"))?;
                let due = start + per * n as u32;
                let mut left = due.saturating_duration_since(Instant::now());
                // short sleeps, so a stop request is seen quickly.
                while !left.is_zero() && !*stop.borrow() {
                    let step = left.min(Duration::from_millis(100));
                    tokio::time::sleep(step).await;
                    left = left.saturating_sub(step);
                }
            }
            c.disconnect().await.ok();
            Ok::<usize, anyhow::Error>(n)
        }));
    }
    let mut total = 0;
    for h in handles {
        match h.await {
            Ok(r) => total += r?,
            Err(e) => bail!("paced writer task panicked: {e}"),
        }
    }
    Ok(total)
}

/// `LAZYVIEW_TEST_SOAK_TPS` (default 40), in the style of `mod.rs:89 scale()`.
pub fn soak_rate() -> u32 {
    std::env::var("LAZYVIEW_TEST_SOAK_TPS")
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(40)
}

/// `LAZYVIEW_TEST_SOAK_WRITERS` (default 4), in the style of `mod.rs:89 scale()`.
pub fn soak_writers() -> usize {
    std::env::var("LAZYVIEW_TEST_SOAK_WRITERS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4)
}
