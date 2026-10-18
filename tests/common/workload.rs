//! Deterministic seeded e-commerce workload: schema, seed data, view set and a random
//! transaction generator covering the nasty cases (PK updates, moves between groups / join
//! partners, deletes of parents, rollbacks, bulk statements, REPLACE, ODKU, multi-table DML, the
//! same id hammered across tables, updates of columns no view reads).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use mysql_async::prelude::Queryable;
use mysql_async::Conn;
use rand::rngs::Xoshiro256PlusPlus;
use rand::{RngExt, SeedableRng};

use super::oracle::{Kind, ViewSpec};
use super::{exec, q, scaled, server_code, Db};

/// `NULL` with probability `p`, else the (lazily evaluated) value.
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

/// Source tables. No foreign keys on purpose: FK cascades are not row-logged in the binlog.
pub const ECOMMERCE_DDL: &[&str] = &[
    "CREATE TABLE regions (
       id INT NOT NULL PRIMARY KEY,
       name VARCHAR(40) NULL,
       country CHAR(2) NOT NULL DEFAULT 'US',
       tax_rate DECIMAL(5,3) NOT NULL DEFAULT 0.000,
       notes TEXT NULL
     )",
    "CREATE TABLE customers (
       id INT NOT NULL PRIMARY KEY,
       region_id INT NULL,
       referrer_id INT NULL,
       name VARCHAR(64) NOT NULL,
       email VARCHAR(128) NULL,
       tier ENUM('bronze','silver','gold','platinum') NULL,
       flags SET('vip','newsletter','blocked','test') NOT NULL DEFAULT '',
       credit DECIMAL(12,2) NULL,
       birth DATE NULL,
       meta JSON NULL,
       signup_ts TIMESTAMP(6) NULL DEFAULT NULL,
       last_seen_at DATETIME(6) NULL,
       KEY k_region (region_id),
       KEY k_referrer (referrer_id)
     )",
    "CREATE TABLE products (
       id INT NOT NULL PRIMARY KEY,
       sku VARCHAR(32) NOT NULL,
       name VARCHAR(100) NOT NULL,
       category VARCHAR(32) NULL,
       price DECIMAL(10,2) NOT NULL,
       weight DOUBLE NULL,
       stock INT UNSIGNED NOT NULL DEFAULT 0,
       active TINYINT(1) NOT NULL DEFAULT 1,
       updated_at TIMESTAMP(3) NULL DEFAULT NULL
     )",
    "CREATE TABLE orders (
       id BIGINT NOT NULL PRIMARY KEY,
       customer_id INT NULL,
       status ENUM('new','paid','shipped','cancelled','refunded') NOT NULL DEFAULT 'new',
       created_at DATETIME(6) NOT NULL,
       shipped_at TIMESTAMP(6) NULL DEFAULT NULL,
       note VARCHAR(255) NULL,
       KEY k_customer (customer_id)
     )",
    "CREATE TABLE order_items (
       order_id BIGINT NOT NULL,
       line_no INT NOT NULL,
       product_id INT NOT NULL,
       qty INT NOT NULL,
       price DECIMAL(10,2) NOT NULL,
       discount DECIMAL(5,2) NULL,
       PRIMARY KEY (order_id, line_no),
       KEY k_product (product_id)
     )",
    "CREATE TABLE payments (
       id BIGINT NOT NULL PRIMARY KEY,
       order_id BIGINT NULL,
       method ENUM('card','paypal','wire','cash') NOT NULL,
       amount DECIMAL(12,2) NOT NULL,
       status VARCHAR(16) NOT NULL DEFAULT 'ok',
       paid_at TIMESTAMP(6) NULL DEFAULT NULL,
       KEY k_order (order_id)
     )",
    // No primary key, no unique key.
    "CREATE TABLE events_log (
       customer_id INT NULL,
       kind VARCHAR(20) NOT NULL,
       amount DECIMAL(10,2) NULL,
       happened_at DATETIME NOT NULL
     )",
    // Composite primary key.
    "CREATE TABLE inventory (
       region_id INT NOT NULL,
       product_id INT NOT NULL,
       qty INT NOT NULL DEFAULT 0,
       reserved INT NOT NULL DEFAULT 0,
       PRIMARY KEY (region_id, product_id)
     )",
    // FK cascades (storage-engine cascades are NOT row-logged in the binlog):
    // fk_groups → fk_parents → fk_children is a 2-level ON DELETE/UPDATE CASCADE chain,
    // fk_notes uses ON DELETE SET NULL, customer_addresses cascades from customers.
    "CREATE TABLE fk_groups (
       id INT NOT NULL PRIMARY KEY,
       name VARCHAR(30) NOT NULL
     )",
    "CREATE TABLE fk_parents (
       id INT NOT NULL PRIMARY KEY,
       group_id INT NULL,
       label VARCHAR(30) NOT NULL,
       KEY k_group (group_id),
       CONSTRAINT fk_parents_group FOREIGN KEY (group_id) REFERENCES fk_groups (id) ON DELETE CASCADE ON UPDATE CASCADE
     )",
    "CREATE TABLE fk_children (
       parent_id INT NOT NULL,
       seq INT NOT NULL,
       qty INT NOT NULL,
       PRIMARY KEY (parent_id, seq),
       CONSTRAINT fk_children_parent FOREIGN KEY (parent_id) REFERENCES fk_parents (id) ON DELETE CASCADE ON UPDATE CASCADE
     )",
    "CREATE TABLE fk_notes (
       id INT NOT NULL PRIMARY KEY,
       parent_id INT NULL,
       body VARCHAR(40) NOT NULL,
       KEY k_parent (parent_id),
       CONSTRAINT fk_notes_parent FOREIGN KEY (parent_id) REFERENCES fk_parents (id) ON DELETE SET NULL ON UPDATE CASCADE
     )",
    "CREATE TABLE customer_addresses (
       customer_id INT NOT NULL,
       seq INT NOT NULL,
       city VARCHAR(30) NOT NULL,
       PRIMARY KEY (customer_id, seq),
       CONSTRAINT fk_addr_customer FOREIGN KEY (customer_id) REFERENCES customers (id) ON DELETE CASCADE ON UPDATE CASCADE
     )",
];

/// Columns no view reads (updates of these must cost nothing).
pub const UNREFERENCED: &[(&str, &str)] = &[
    ("customers", "last_seen_at"),
    ("orders", "note"),
    ("products", "updated_at"),
    ("regions", "notes"),
];

/// The shared, heavily overlapping view set (minus `LAZYVIEW_TEST_SKIP_VIEWS`).
pub fn ecommerce_views() -> Vec<ViewSpec> {
    super::skip_views(all_ecommerce_views())
}

fn all_ecommerce_views() -> Vec<ViewSpec> {
    use Kind::*;
    vec![
        // single-table filter, written with noise + column list + LOCAL CHECK OPTION
        ViewSpec::new(
            "v_paid_orders",
            Incremental,
            "SELECT id, customer_id, created_at, status FROM orders WHERE status IN ('paid','shipped')",
        )
        .noisy(
            "ALGORITHM=UNDEFINED DEFINER=CURRENT_USER SQL SECURITY DEFINER",
            Some("order_id, customer_id, created_at, status"),
            "WITH LOCAL CHECK OPTION",
        ),
        // projection with expressions
        ViewSpec::new(
            "v_customer_cards",
            Incremental,
            "SELECT id, UPPER(name) AS uname, CONCAT(name, ' <', COALESCE(email, '-'), '>') AS contact,
                    COALESCE(credit, 0) * 2 AS dbl_credit, tier, FIND_IN_SET('vip', flags) > 0 AS is_vip,
                    JSON_UNQUOTE(JSON_EXTRACT(meta, '$.lang')) AS lang, YEAR(birth) AS birth_year, signup_ts
             FROM customers WHERE FIND_IN_SET('blocked', flags) = 0",
        ),
        // 2-way inner join, composite-PK anchor
        ViewSpec::new(
            "v_order_lines",
            Incremental,
            "SELECT i.order_id, i.line_no, i.qty, i.price, p.sku, p.name AS product
             FROM order_items i JOIN products p ON p.id = i.product_id",
        ),
        // 3-way inner join
        ViewSpec::new(
            "v_order_customer_region",
            Incremental,
            "SELECT o.id, o.status, c.name AS customer, c.tier, r.name AS region
             FROM orders o JOIN customers c ON c.id = o.customer_id JOIN regions r ON r.id = c.region_id",
        ),
        // 4-way inner join
        ViewSpec::new(
            "v_line_details",
            Incremental,
            "SELECT i.order_id, i.line_no, c.name AS customer, p.sku, i.qty * i.price AS amount, o.status
             FROM order_items i
             JOIN orders o ON o.id = i.order_id
             JOIN customers c ON c.id = o.customer_id
             JOIN products p ON p.id = i.product_id
             WHERE i.qty > 0",
        ),
        // LEFT JOIN chain (rows go NULL <-> matched)
        ViewSpec::new(
            "v_orders_left",
            Incremental,
            "SELECT o.id, o.status, pay.id AS payment_id, pay.amount, c.name AS customer, r.name AS region
             FROM orders o
             LEFT JOIN payments pay ON pay.order_id = o.id
             LEFT JOIN customers c ON c.id = o.customer_id
             LEFT JOIN regions r ON r.id = c.region_id",
        ),
        ViewSpec::new(
            "v_customer_regions",
            Incremental,
            "SELECT c.id, c.name, r.name AS region, r.tax_rate
             FROM customers c LEFT JOIN regions r ON r.id = c.region_id",
        ),
        // self-join
        ViewSpec::new(
            "v_referrals",
            Incremental,
            "SELECT c.id, c.name, r.id AS referrer_id, r.name AS referrer, r.tier AS referrer_tier
             FROM customers c JOIN customers r ON r.id = c.referrer_id",
        ),
        // GROUP BY + SUM/COUNT/MIN/MAX/AVG + HAVING, NULL group keys (customer_id NULL)
        ViewSpec::new(
            "v_customer_stats",
            Incremental,
            "SELECT o.customer_id, COUNT(*) AS n_lines, SUM(i.qty * i.price) AS revenue,
                    MIN(o.created_at) AS first_order, MAX(o.created_at) AS last_order, AVG(i.qty) AS avg_qty
             FROM orders o JOIN order_items i ON i.order_id = o.id
             WHERE o.status <> 'cancelled'
             GROUP BY o.customer_id
             HAVING COUNT(*) > 1",
        ),
        // GROUP BY select alias
        ViewSpec::new(
            "v_status_counts",
            Incremental,
            "SELECT status AS st, COUNT(*) AS n, SUM(id % 7) AS s, MAX(shipped_at) AS last_shipped
             FROM orders GROUP BY st",
        ),
        // GROUP BY ordinal on a joined (non-anchor) column, NULL keys (category NULL)
        ViewSpec::new(
            "v_category_sales",
            Incremental,
            "SELECT p.category, COUNT(*) AS n_lines, SUM(i.qty) AS units, MAX(i.price) AS max_price,
                    MIN(i.discount) AS min_discount
             FROM order_items i JOIN products p ON p.id = i.product_id
             GROUP BY 1",
        ),
        // GROUP BY a column two joins away from the anchor, COUNT(DISTINCT), NULL keys
        ViewSpec::new(
            "v_region_revenue",
            Incremental,
            "SELECT c.region_id, COUNT(DISTINCT o.id) AS n_orders, SUM(pay.amount) AS paid, MAX(pay.paid_at) AS last_paid
             FROM payments pay JOIN orders o ON o.id = pay.order_id JOIN customers c ON c.id = o.customer_id
             GROUP BY c.region_id",
        ),
        // global aggregate
        ViewSpec::new(
            "v_global_orders",
            Incremental,
            "SELECT COUNT(*) AS n, SUM(CASE WHEN status = 'paid' THEN 1 ELSE 0 END) AS n_paid,
                    MAX(created_at) AS latest, MIN(id) AS min_id
             FROM orders",
        ),
        // composite-PK anchor + inner + left join
        ViewSpec::new(
            "v_inventory",
            Incremental,
            "SELECT inv.region_id, inv.product_id, inv.qty - inv.reserved AS available, p.sku, r.name AS region
             FROM inventory inv JOIN products p ON p.id = inv.product_id LEFT JOIN regions r ON r.id = inv.region_id",
        ),
        // expressions over DECIMAL / TIMESTAMP / ENUM
        ViewSpec::new(
            "v_payment_days",
            Incremental,
            "SELECT id, order_id, DATE(paid_at) AS pay_day, amount * 100 AS cents, UPPER(method) AS m
             FROM payments WHERE paid_at IS NOT NULL",
        ),
        // the noisiest definition: OR REPLACE / ALGORITHM / DEFINER / SQL SECURITY / columns / CHECK OPTION
        ViewSpec::new("v_noisy_products", Incremental, "SELECT id, sku, price FROM products WHERE active = 1").noisy(
            "OR REPLACE ALGORITHM=MERGE DEFINER=CURRENT_USER SQL SECURITY INVOKER",
            Some("pid, psku, pprice"),
            "WITH CHECK OPTION",
        ),
        // grouped view over a table without primary key
        ViewSpec::new(
            "v_events_by_kind",
            Incremental,
            "SELECT kind, customer_id, COUNT(*) AS n, SUM(amount) AS total FROM events_log GROUP BY kind, customer_id",
        ),
        // full-refresh fallbacks
        ViewSpec::new("v_events_big", Incremental, "SELECT customer_id, kind, amount FROM events_log WHERE amount > 50"),
        ViewSpec::new("v_distinct_methods", Incremental, "SELECT DISTINCT method, status FROM payments"),
        ViewSpec::new(
            "v_union_names",
            Full,
            "SELECT 'c' AS kind, id, name FROM customers WHERE tier = 'gold'
             UNION SELECT 'p', id, name FROM products WHERE active = 1",
        ),
        ViewSpec::new(
            "v_top_products",
            Incremental,
            "SELECT id, sku, price FROM products WHERE active = 1 ORDER BY price DESC, id LIMIT 5",
        ),
        ViewSpec::new(
            "v_big_spenders",
            Incremental,
            "SELECT c.id, c.name FROM customers c
             WHERE c.id IN (SELECT o.customer_id FROM orders o JOIN payments p ON p.order_id = o.id WHERE p.amount > 400)",
        ),
        // chained: MVs over another MV
        ViewSpec::new(
            "v_vip_stats",
            Chained,
            "SELECT customer_id, revenue, n_lines FROM v_customer_stats WHERE revenue > 500",
        ),
        ViewSpec::new(
            "v_tier_revenue",
            Chained,
            "SELECT c.tier, COUNT(*) AS n_customers, SUM(s.revenue) AS revenue
             FROM v_customer_stats s JOIN customers c ON c.id = s.customer_id GROUP BY c.tier",
        ),
        // over FK-cascading children: joined to the parent via the FK, 2-level chain
        ViewSpec::new(
            "vf_child_parent",
            Incremental,
            "SELECT ch.parent_id, ch.seq, ch.qty, p.label, g.name AS grp
             FROM fk_children ch JOIN fk_parents p ON p.id = ch.parent_id LEFT JOIN fk_groups g ON g.id = p.group_id",
        ),
        // grouped by the FK column
        ViewSpec::new(
            "vf_child_by_parent",
            Incremental,
            "SELECT parent_id, COUNT(*) AS n, SUM(qty) AS total FROM fk_children GROUP BY parent_id",
        ),
        // plain child-only view, composite PK
        ViewSpec::new("vf_children", Incremental, "SELECT parent_id, seq, qty FROM fk_children WHERE qty >= 0"),
        // ON DELETE SET NULL child (NULL group keys appear through the cascade)
        ViewSpec::new("vf_notes", Incremental, "SELECT id, parent_id, body FROM fk_notes"),
        ViewSpec::new("vf_notes_by_parent", Incremental, "SELECT parent_id, COUNT(*) AS n FROM fk_notes GROUP BY parent_id"),
        // parent grouped by its own cascading FK column
        ViewSpec::new("vf_parents_by_group", Incremental, "SELECT group_id, COUNT(*) AS n FROM fk_parents GROUP BY group_id"),
        // child of customers (customer deletes / PK updates cascade)
        ViewSpec::new(
            "vf_addresses",
            Incremental,
            "SELECT a.customer_id, a.seq, a.city, c.name FROM customer_addresses a JOIN customers c ON c.id = a.customer_id",
        ),
        ViewSpec::new("vf_addresses_by_city", Incremental, "SELECT city, COUNT(*) AS n FROM customer_addresses GROUP BY city"),
    ]
}

/// Whether a view of [`ecommerce_views`] reads a child table of a cascading foreign key.
pub fn reads_fk_child(name: &str) -> bool {
    name.starts_with("vf_")
}

/// Views of the given kinds.
pub fn views_of(kinds: &[Kind]) -> Vec<ViewSpec> {
    ecommerce_views()
        .into_iter()
        .filter(|v| kinds.contains(&v.kind))
        .collect()
}

pub fn names(specs: &[ViewSpec]) -> Vec<&str> {
    specs.iter().map(|s| s.name.as_str()).collect()
}

// ---------------------------------------------------------------------------------------------
// Ids and sizes
// ---------------------------------------------------------------------------------------------

/// Seed sizes.
#[derive(Debug, Clone, Copy)]
pub struct Sizes {
    pub regions: i64,
    pub customers: i64,
    pub products: i64,
    pub orders: i64,
    pub events: i64,
}

impl Sizes {
    /// Default sizes × `LAZYVIEW_TEST_SCALE`.
    pub fn scaled() -> Sizes {
        Sizes {
            regions: 6,
            customers: scaled(50) as i64,
            products: scaled(30) as i64,
            orders: scaled(150) as i64,
            events: scaled(120) as i64,
        }
    }

    pub fn tiny() -> Sizes {
        Sizes {
            regions: 3,
            customers: 8,
            products: 6,
            orders: 12,
            events: 10,
        }
    }
}

/// Id allocators shared by concurrent writers (fresh ids never collide).
#[derive(Debug)]
pub struct Ids {
    pub regions: i64,
    /// Order ids 1..=seed_orders came from the seed data (never handed out by the allocator).
    pub seed_orders: i64,
    pub customer: AtomicI64,
    pub product: AtomicI64,
    pub order: AtomicI64,
    pub line: AtomicI64,
    pub payment: AtomicI64,
    pub fk_group: AtomicI64,
    pub fk_parent: AtomicI64,
    pub fk_seq: AtomicI64,
    pub fk_note: AtomicI64,
    pub addr_seq: AtomicI64,
}

impl Ids {
    fn fresh(c: &AtomicI64) -> i64 {
        c.fetch_add(1, Ordering::SeqCst)
    }
    /// Highest id handed out so far.
    fn max(c: &AtomicI64) -> i64 {
        (c.load(Ordering::SeqCst) - 1).max(1)
    }
}

// ---------------------------------------------------------------------------------------------
// Random values
// ---------------------------------------------------------------------------------------------

const NAMES: &[&str] = &[
    "alice",
    "Bob",
    "carol",
    "Dave",
    "Eve",
    "mallory",
    "Zoë",
    "José",
    "李雷",
    "Ольга",
    "o'brien",
    "back\\slash",
    "😀 smiley",
    "trent",
    "peggy",
    "victor",
    "walter",
    "Sybil",
    "x",
];
const KINDS: &[&str] = &["login", "view", "cart", "refund", "promo"];
const CATEGORIES: &[Option<&str>] = &[
    Some("books"),
    Some("toys"),
    Some("food"),
    Some("garden"),
    Some("tools"),
    None,
];
const ORDER_STATUS: &[&str] = &["new", "paid", "shipped", "cancelled", "refunded"];
const METHODS: &[&str] = &["card", "paypal", "wire", "cash"];
const PAY_STATUS: &[&str] = &["ok", "pending", "failed"];
const TIERS: &[&str] = &["bronze", "silver", "gold", "platinum"];
const FLAGS: &[&str] = &["vip", "newsletter", "blocked", "test"];
const LANGS: &[&str] = &["en", "de", "fr", "ja"];
const CITIES: &[&str] = &["Berlin", "Paris", "Tokyo", "Lima", "Oslo"];
/// Seeded fk_groups ids (1..=N); only these are re-created by id (fresh ids belong to the allocator).
const N_FK_GROUPS: i64 = 5;
const REGION_NAMES: &[&str] = &[
    "North",
    "South",
    "East",
    "West",
    "Central",
    "Overseas",
    "Île-de-France",
    "Zürich",
];

pub struct Gen {
    pub rng: Xoshiro256PlusPlus,
}

impl Gen {
    pub fn new(seed: u64) -> Gen {
        Gen {
            rng: Xoshiro256PlusPlus::seed_from_u64(seed),
        }
    }
    pub fn range(&mut self, lo: i64, hi_incl: i64) -> i64 {
        if hi_incl <= lo {
            lo
        } else {
            self.rng.random_range(lo..=hi_incl)
        }
    }
    pub fn chance(&mut self, p: f64) -> bool {
        self.rng.random_bool(p.clamp(0.0, 1.0))
    }
    pub fn pick<'a, T>(&mut self, xs: &'a [T]) -> &'a T {
        &xs[self.rng.random_range(0..xs.len())]
    }
    pub fn name(&mut self) -> String {
        let base = *self.pick(NAMES);
        let s = match self.range(0, 3) {
            0 => base.to_uppercase(),
            1 => format!("{base} {}", self.range(1, 99)),
            _ => base.to_string(),
        };
        q(&s)
    }
    pub fn decimal(&mut self, lo_cents: i64, hi_cents: i64) -> String {
        let c = self.range(lo_cents, hi_cents);
        let sign = if c < 0 { "-" } else { "" };
        format!("{sign}{}.{:02}", c.abs() / 100, c.abs() % 100)
    }
    pub fn dt6(&mut self) -> String {
        format!(
            "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
            self.range(2020, 2025),
            self.range(1, 12),
            self.range(1, 28),
            self.range(0, 23),
            self.range(0, 59),
            self.range(0, 59),
            self.range(0, 999_999)
        )
    }
    /// TIMESTAMP(6) literal, avoiding 01:00–03:59 local time (DST gaps on non-UTC servers).
    pub fn ts6(&mut self) -> String {
        format!(
            "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}'",
            self.range(2020, 2025),
            self.range(1, 12),
            self.range(1, 28),
            self.range(4, 23),
            self.range(0, 59),
            self.range(0, 59),
            self.range(0, 999_999)
        )
    }
    pub fn dt0(&mut self) -> String {
        format!(
            "'{:04}-{:02}-{:02} {:02}:{:02}:{:02}'",
            self.range(2020, 2025),
            self.range(1, 12),
            self.range(1, 28),
            self.range(0, 23),
            self.range(0, 59),
            self.range(0, 59)
        )
    }
    pub fn date(&mut self) -> String {
        format!(
            "'{:04}-{:02}-{:02}'",
            self.range(1950, 2010),
            self.range(1, 12),
            self.range(1, 28)
        )
    }
    pub fn flags(&mut self) -> String {
        let mut set = Vec::new();
        for f in FLAGS {
            let p = if *f == "blocked" { 0.1 } else { 0.3 };
            if self.chance(p) {
                set.push(*f);
            }
        }
        q(&set.join(","))
    }
    pub fn meta(&mut self) -> String {
        if self.chance(0.2) {
            return "NULL".into();
        }
        let lang = *self.pick(LANGS);
        q(&format!(
            "{{\"lang\": \"{lang}\", \"score\": {}}}",
            self.range(0, 10)
        ))
    }
}

// ---------------------------------------------------------------------------------------------
// Setup: schema + seed data
// ---------------------------------------------------------------------------------------------

/// Creates the e-commerce tables and seeds deterministic data; returns the id allocators.
pub async fn setup_ecommerce(db: &Db, seed: u64, sizes: Sizes) -> Result<Arc<Ids>> {
    let mut c = db.conn().await?;
    for ddl in ECOMMERCE_DDL {
        exec(&mut c, ddl).await?;
    }
    let (stmts, ids) = seed_statements(seed, sizes);
    for s in &stmts {
        exec(&mut c, s).await?;
    }
    c.disconnect().await.ok();
    Ok(Arc::new(ids))
}

fn chunked_insert(out: &mut Vec<String>, head: &str, rows: Vec<String>) {
    for chunk in rows.chunks(200) {
        out.push(format!("{head} VALUES {}", chunk.join(", ")));
    }
}

/// Deterministic seed data as multi-row INSERTs.
pub fn seed_statements(seed: u64, s: Sizes) -> (Vec<String>, Ids) {
    let mut g = Gen::new(seed ^ 0x5eed);
    let mut out = Vec::new();
    let regions: Vec<String> = (1..=s.regions)
        .map(|id| {
            let name = opt!(
                g,
                0.1,
                q(REGION_NAMES[(id as usize - 1) % REGION_NAMES.len()])
            );
            format!("({id}, {name}, 'US', 0.{:03}, NULL)", g.range(0, 250))
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO regions (id, name, country, tax_rate, notes)",
        regions,
    );

    let customers: Vec<String> = (1..=s.customers)
        .map(|id| {
            let region = opt!(g, 0.1, g.range(1, s.regions + 1).to_string());
            let referrer = opt!(g, 0.4, g.range(1, s.customers + 2).to_string());
            let email = opt!(g, 0.2, q(&format!("user{id}@example.com")));
            let tier = opt!(g, 0.15, q(g.pick(TIERS)));
            let credit = opt!(g, 0.2, g.decimal(-50_000, 500_000));
            let birth = opt!(g, 0.3, g.date());
            let signup = opt!(g, 0.3, g.ts6());
            format!(
                "({id}, {region}, {referrer}, {}, {email}, {tier}, {}, {credit}, {birth}, {}, {signup}, NULL)",
                g.name(),
                g.flags(),
                g.meta()
            )
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO customers (id, region_id, referrer_id, name, email, tier, flags, credit, birth, meta, signup_ts, last_seen_at)",
        customers,
    );

    let products: Vec<String> = (1..=s.products)
        .map(|id| {
            let cat = match g.pick(CATEGORIES) {
                Some(c) => q(c),
                None => "NULL".into(),
            };
            let weight = opt!(g, 0.2, format!("{}.{}", g.range(0, 50), g.range(0, 9)));
            format!(
                "({id}, 'SKU-{id:05}', {}, {cat}, {}, {weight}, {}, {}, NULL)",
                q(&format!("Product {id}")),
                g.decimal(50, 99_999),
                g.range(0, 1000),
                if g.chance(0.85) { 1 } else { 0 }
            )
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO products (id, sku, name, category, price, weight, stock, active, updated_at)",
        products,
    );

    let mut orders = Vec::new();
    let mut items = Vec::new();
    let mut payments = Vec::new();
    let mut line: i64 = 1;
    let mut pay_id: i64 = 1;
    for id in 1..=s.orders {
        let cust = opt!(g, 0.05, g.range(1, s.customers + 2).to_string());
        let status = q(g.pick(ORDER_STATUS));
        let shipped = opt!(g, 0.5, g.ts6());
        orders.push(format!(
            "({id}, {cust}, {status}, {}, {shipped}, NULL)",
            g.dt6()
        ));
        for _ in 0..g.range(0, 4) {
            let disc = opt!(g, 0.6, g.decimal(0, 5000));
            items.push(format!(
                "({id}, {line}, {}, {}, {}, {disc})",
                g.range(1, s.products + 1),
                g.range(1, 5),
                g.decimal(50, 50_000)
            ));
            line += 1;
        }
        for _ in 0..g.range(0, 2) {
            let paid_at = opt!(g, 0.2, g.ts6());
            payments.push(format!(
                "({pay_id}, {id}, {}, {}, {}, {paid_at})",
                q(g.pick(METHODS)),
                g.decimal(100, 90_000),
                q(g.pick(PAY_STATUS))
            ));
            pay_id += 1;
        }
    }
    chunked_insert(
        &mut out,
        "INSERT INTO orders (id, customer_id, status, created_at, shipped_at, note)",
        orders,
    );
    chunked_insert(
        &mut out,
        "INSERT INTO order_items (order_id, line_no, product_id, qty, price, discount)",
        items,
    );
    chunked_insert(
        &mut out,
        "INSERT INTO payments (id, order_id, method, amount, status, paid_at)",
        payments,
    );

    let events: Vec<String> = (0..s.events)
        .map(|_| {
            let cust = opt!(g, 0.1, g.range(1, s.customers).to_string());
            let amount = opt!(g, 0.2, g.decimal(0, 20_000));
            format!("({cust}, {}, {amount}, {})", q(g.pick(KINDS)), g.dt0())
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO events_log (customer_id, kind, amount, happened_at)",
        events,
    );

    let mut inv = Vec::new();
    for r in 1..=s.regions {
        for p in 1..=s.products {
            if g.chance(0.3) {
                inv.push(format!(
                    "({r}, {p}, {}, {})",
                    g.range(0, 100),
                    g.range(0, 10)
                ));
            }
        }
    }
    if inv.is_empty() {
        inv.push("(1, 1, 5, 0)".into());
    }
    chunked_insert(
        &mut out,
        "INSERT INTO inventory (region_id, product_id, qty, reserved)",
        inv,
    );

    // FK tables (parents before children).
    let n_groups = N_FK_GROUPS;
    let n_parents = (s.customers / 2).max(4);
    let groups: Vec<String> = (1..=n_groups)
        .map(|id| format!("({id}, 'group {id}')"))
        .collect();
    chunked_insert(&mut out, "INSERT INTO fk_groups (id, name)", groups);
    let parents: Vec<String> = (1..=n_parents)
        .map(|id| {
            let gid = opt!(g, 0.15, g.range(1, n_groups).to_string());
            format!("({id}, {gid}, 'parent {id}')")
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO fk_parents (id, group_id, label)",
        parents,
    );
    let mut fk_seq = 1i64;
    let mut children = Vec::new();
    for pid in 1..=n_parents {
        for _ in 0..g.range(0, 4) {
            children.push(format!("({pid}, {fk_seq}, {})", g.range(-2, 20)));
            fk_seq += 1;
        }
    }
    if !children.is_empty() {
        chunked_insert(
            &mut out,
            "INSERT INTO fk_children (parent_id, seq, qty)",
            children,
        );
    }
    let n_notes = n_parents;
    let notes: Vec<String> = (1..=n_notes)
        .map(|id| {
            let pid = opt!(g, 0.1, g.range(1, n_parents).to_string());
            format!("({id}, {pid}, 'note {id}')")
        })
        .collect();
    chunked_insert(
        &mut out,
        "INSERT INTO fk_notes (id, parent_id, body)",
        notes,
    );
    let mut addr_seq = 1i64;
    let mut addrs = Vec::new();
    for cid in 1..=s.customers {
        for _ in 0..g.range(0, 2) {
            addrs.push(format!("({cid}, {addr_seq}, {})", q(g.pick(CITIES))));
            addr_seq += 1;
        }
    }
    if !addrs.is_empty() {
        chunked_insert(
            &mut out,
            "INSERT INTO customer_addresses (customer_id, seq, city)",
            addrs,
        );
    }

    let ids = Ids {
        regions: s.regions,
        seed_orders: s.orders,
        customer: AtomicI64::new(s.customers + 1),
        product: AtomicI64::new(s.products + 1),
        order: AtomicI64::new(s.orders + 1),
        line: AtomicI64::new(line),
        payment: AtomicI64::new(pay_id),
        fk_group: AtomicI64::new(n_groups + 1),
        fk_parent: AtomicI64::new(n_parents + 1),
        fk_seq: AtomicI64::new(fk_seq),
        fk_note: AtomicI64::new(n_notes + 1),
        addr_seq: AtomicI64::new(addr_seq),
    };
    (out, ids)
}

// ---------------------------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Txn {
    pub label: String,
    pub stmts: Vec<String>,
    pub rollback: bool,
}

/// Random transaction generator over the e-commerce schema.
pub struct Workload {
    pub g: Gen,
    pub ids: Arc<Ids>,
    /// op label → count (for logs).
    pub stats: BTreeMap<&'static str, u64>,
    /// Probability that a transaction is rolled back.
    pub p_rollback: f64,
}

impl Workload {
    pub fn new(seed: u64, ids: Arc<Ids>) -> Workload {
        Workload {
            g: Gen::new(seed),
            ids,
            stats: BTreeMap::new(),
            p_rollback: 0.07,
        }
    }

    fn order(&mut self) -> i64 {
        let hi = Ids::max(&self.ids.order);
        self.g.range(1, hi + 1)
    }
    fn customer(&mut self) -> i64 {
        let hi = Ids::max(&self.ids.customer);
        self.g.range(1, hi + 2)
    }
    fn customer_opt(&mut self) -> String {
        let c = self.customer();
        opt!(self.g, 0.05, c.to_string())
    }
    fn product(&mut self) -> i64 {
        let hi = Ids::max(&self.ids.product);
        self.g.range(1, hi + 1)
    }
    fn region(&mut self) -> i64 {
        self.g.range(1, self.ids.regions + 2)
    }
    fn payment(&mut self) -> i64 {
        let hi = Ids::max(&self.ids.payment);
        self.g.range(1, hi + 1)
    }

    fn item_row(&mut self, order: i64) -> String {
        let line = Ids::fresh(&self.ids.line);
        let disc = opt!(self.g, 0.6, self.g.decimal(0, 5000));
        let p = self.product();
        format!(
            "({order}, {line}, {p}, {}, {}, {disc})",
            self.g.range(1, 6),
            self.g.decimal(50, 50_000)
        )
    }

    fn payment_row(&mut self, order: String) -> String {
        let id = Ids::fresh(&self.ids.payment);
        let paid_at = opt!(self.g, 0.25, self.g.ts6());
        format!(
            "({id}, {order}, {}, {}, {}, {paid_at})",
            q(self.g.pick(METHODS)),
            self.g.decimal(100, 90_000),
            q(self.g.pick(PAY_STATUS))
        )
    }

    /// Next unit of work: usually one transaction, several for the "hammer" op.
    pub fn next_batch(&mut self) -> Vec<Txn> {
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
            label: labels.join("+"),
            stmts,
            rollback,
        }]
    }

    /// The same order id written ~30× across orders / order_items / payments, as one big
    /// transaction, a few transactions or 30 single-statement transactions.
    pub fn hammer(&mut self) -> Vec<Txn> {
        *self.stats.entry("hammer").or_default() += 1;
        let id = self.order();
        let mut stmts = Vec::new();
        for k in 0..30 {
            let s = match k % 6 {
                0 => format!(
                    "UPDATE orders SET status = {} WHERE id = {id}",
                    q(self.g.pick(ORDER_STATUS))
                ),
                1 => format!(
                    "UPDATE order_items SET qty = qty + {} WHERE order_id = {id}",
                    self.g.range(-1, 2)
                ),
                2 => format!("UPDATE payments SET amount = amount + 1.25 WHERE order_id = {id}"),
                3 => {
                    let row = self.item_row(id);
                    format!("INSERT INTO order_items (order_id, line_no, product_id, qty, price, discount) VALUES {row}")
                }
                4 => format!(
                    "UPDATE orders SET customer_id = {} WHERE id = {id}",
                    self.customer_opt()
                ),
                _ => {
                    let row = self.payment_row(id.to_string());
                    format!("INSERT INTO payments (id, order_id, method, amount, status, paid_at) VALUES {row}")
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
            })
            .collect()
    }

    /// One random operation: (label, statements).
    pub fn op(&mut self) -> (&'static str, Vec<String>) {
        let w = self.g.range(0, 111);
        if w >= 100 {
            return ("fk", self.fk_op(w - 100));
        }
        match w {
            0..=9 => ("new_order", self.new_order()),
            10..=13 => {
                let o = self.order();
                let n = self.g.range(1, 3);
                let rows: Vec<String> = (0..n).map(|_| self.item_row(o)).collect();
                (
                    "add_items",
                    vec![format!(
                        "INSERT INTO order_items (order_id, line_no, product_id, qty, price, discount) VALUES {}",
                        rows.join(", ")
                    )],
                )
            }
            14..=19 => {
                let o = self.order();
                let s = match self.g.range(0, 3) {
                    0 => format!(
                        "UPDATE order_items SET qty = qty + {} WHERE order_id = {o} ORDER BY line_no LIMIT 1",
                        self.g.range(-2, 4)
                    ),
                    1 => format!("UPDATE order_items SET price = {} WHERE order_id = {o}", self.g.decimal(50, 50_000)),
                    2 => format!(
                        "UPDATE order_items SET discount = {} WHERE order_id = {o} ORDER BY line_no DESC LIMIT 1",
                        opt!(self.g, 0.5, self.g.decimal(0, 5000))
                    ),
                    _ => format!("UPDATE order_items SET product_id = {} WHERE order_id = {o}", self.product()),
                };
                ("item_update", vec![s])
            }
            20..=22 => {
                // composite PK changes: move a line to another order / renumber it
                let o = self.order();
                let s = if self.g.chance(0.6) {
                    format!("UPDATE order_items SET order_id = {} WHERE order_id = {o} ORDER BY line_no LIMIT 1", self.order())
                } else {
                    let l = Ids::fresh(&self.ids.line);
                    format!("UPDATE order_items SET line_no = {l} WHERE order_id = {o} ORDER BY line_no LIMIT 1")
                };
                ("item_move", vec![s])
            }
            23..=24 => {
                let o = self.order();
                (
                    "item_delete",
                    vec![format!(
                        "DELETE FROM order_items WHERE order_id = {o} ORDER BY line_no LIMIT 1"
                    )],
                )
            }
            25..=31 => {
                let o = self.order();
                let st = q(self.g.pick(ORDER_STATUS));
                let shipped = opt!(self.g, 0.5, self.g.ts6());
                (
                    "order_status",
                    vec![format!(
                        "UPDATE orders SET status = {st}, shipped_at = {shipped} WHERE id = {o}"
                    )],
                )
            }
            32..=34 => {
                let o = self.order();
                (
                    "order_customer",
                    vec![format!(
                        "UPDATE orders SET customer_id = {} WHERE id = {o}",
                        self.customer_opt()
                    )],
                )
            }
            35..=36 => {
                // PK update of a parent; sometimes children follow
                let o = self.order();
                let n = Ids::fresh(&self.ids.order);
                let mut v = vec![format!("UPDATE orders SET id = {n} WHERE id = {o}")];
                if self.g.chance(0.5) {
                    v.push(format!(
                        "UPDATE order_items SET order_id = {n} WHERE order_id = {o}"
                    ));
                    v.push(format!(
                        "UPDATE payments SET order_id = {n} WHERE order_id = {o}"
                    ));
                }
                ("order_pk", v)
            }
            37..=38 => {
                // delete a parent (children stay orphaned or go too)
                let o = self.order();
                let mut v = vec![format!("DELETE FROM orders WHERE id = {o}")];
                if self.g.chance(0.5) {
                    v.push(format!("DELETE FROM order_items WHERE order_id = {o}"));
                }
                ("order_delete", v)
            }
            39..=45 => ("customer_update", vec![self.customer_update()]),
            46..=47 => {
                let id = Ids::fresh(&self.ids.customer);
                let region = opt!(self.g, 0.1, self.region().to_string());
                let referrer = opt!(self.g, 0.4, self.customer().to_string());
                let tier = opt!(self.g, 0.15, q(self.g.pick(TIERS)));
                let credit = opt!(self.g, 0.2, self.g.decimal(-50_000, 500_000));
                let s = format!(
                    "INSERT INTO customers (id, region_id, referrer_id, name, email, tier, flags, credit, birth, meta, signup_ts)
                     VALUES ({id}, {region}, {referrer}, {}, NULL, {tier}, {}, {credit}, {}, {}, {})",
                    self.g.name(),
                    self.g.flags(),
                    self.g.date(),
                    self.g.meta(),
                    self.g.ts6()
                );
                ("customer_insert", vec![s])
            }
            48 => {
                let c = self.customer();
                (
                    "customer_delete",
                    vec![format!("DELETE FROM customers WHERE id = {c}")],
                )
            }
            49 => {
                let c = self.customer();
                let n = Ids::fresh(&self.ids.customer);
                (
                    "customer_pk",
                    vec![format!("UPDATE customers SET id = {n} WHERE id = {c}")],
                )
            }
            50..=54 => ("unreferenced", vec![self.unreferenced()]),
            55..=58 => {
                let p = self.product();
                let s = match self.g.range(0, 3) {
                    0 => format!(
                        "UPDATE products SET price = {} WHERE id = {p}",
                        self.g.decimal(50, 99_999)
                    ),
                    1 => {
                        let cat = match self.g.pick(CATEGORIES) {
                            Some(c) => q(c),
                            None => "NULL".into(),
                        };
                        format!("UPDATE products SET category = {cat} WHERE id = {p}")
                    }
                    2 => format!("UPDATE products SET active = 1 - active WHERE id = {p}"),
                    _ => format!(
                        "UPDATE products SET name = {}, stock = {} WHERE id = {p}",
                        self.g.name(),
                        self.g.range(0, 500)
                    ),
                };
                ("product_update", vec![s])
            }
            59..=60 => {
                let p = if self.g.chance(0.2) {
                    Ids::fresh(&self.ids.product)
                } else {
                    self.product()
                };
                let cat = match self.g.pick(CATEGORIES) {
                    Some(c) => q(c),
                    None => "NULL".into(),
                };
                let s = format!(
                    "REPLACE INTO products (id, sku, name, category, price, weight, stock, active)
                     VALUES ({p}, 'SKU-{p:05}', {}, {cat}, {}, NULL, {}, {})",
                    self.g.name(),
                    self.g.decimal(50, 99_999),
                    self.g.range(0, 100),
                    if self.g.chance(0.8) { 1 } else { 0 }
                );
                ("product_replace", vec![s])
            }
            61..=64 => {
                let o = self.order().to_string();
                let o = opt!(self.g, 0.05, o);
                let row = self.payment_row(o);
                ("payment_insert", vec![format!("INSERT INTO payments (id, order_id, method, amount, status, paid_at) VALUES {row}")])
            }
            65..=67 => {
                let p = self.payment();
                let s = match self.g.range(0, 3) {
                    0 => format!(
                        "UPDATE payments SET amount = {} WHERE id = {p}",
                        self.g.decimal(100, 90_000)
                    ),
                    1 => format!(
                        "UPDATE payments SET method = {}, status = {} WHERE id = {p}",
                        q(self.g.pick(METHODS)),
                        q(self.g.pick(PAY_STATUS))
                    ),
                    2 => format!(
                        "UPDATE payments SET paid_at = {} WHERE id = {p}",
                        opt!(self.g, 0.4, self.g.ts6())
                    ),
                    _ => format!(
                        "UPDATE payments SET order_id = {} WHERE id = {p}",
                        self.order()
                    ),
                };
                ("payment_update", vec![s])
            }
            68..=69 => {
                let p = self.payment();
                (
                    "payment_delete",
                    vec![format!("DELETE FROM payments WHERE id = {p}")],
                )
            }
            70..=74 => ("events", self.events()),
            75..=79 => ("inventory", self.inventory()),
            80..=81 => ("region", self.region_op()),
            82..=84 => {
                let o = self.order();
                let s = if self.g.chance(0.5) {
                    format!(
                        "UPDATE orders o JOIN customers c ON c.id = o.customer_id
                         SET o.status = 'shipped', o.shipped_at = {}, c.credit = COALESCE(c.credit, 0) + 1.00
                         WHERE o.id BETWEEN {o} AND {}",
                        self.g.ts6(),
                        o + 3
                    )
                } else {
                    format!(
                        "UPDATE order_items i JOIN products p ON p.id = i.product_id
                         SET i.price = p.price WHERE i.order_id BETWEEN {o} AND {}",
                        o + 2
                    )
                };
                ("multi_update", vec![s])
            }
            85..=86 => {
                let o = self.order();
                let s = if self.g.chance(0.5) {
                    format!("DELETE o, i FROM orders o LEFT JOIN order_items i ON i.order_id = o.id WHERE o.id = {o}")
                } else {
                    format!(
                        "DELETE pay FROM payments pay JOIN orders o ON o.id = pay.order_id
                         WHERE o.status = 'cancelled' AND o.id BETWEEN {o} AND {}",
                        o + 10
                    )
                };
                ("multi_delete", vec![s])
            }
            87..=91 => ("bulk", self.bulk()),
            92..=95 => {
                // re-insert previously deleted parents with ON DUPLICATE KEY UPDATE (NULL <-> matched);
                // only seed ids: fresh ids belong to the allocator (concurrent writers)
                let o = self.g.range(1, self.ids.seed_orders);
                let s = format!(
                    "INSERT INTO orders (id, customer_id, status, created_at) VALUES ({o}, {}, 'new', {})
                     ON DUPLICATE KEY UPDATE status = VALUES(status)",
                    self.customer_opt(),
                    self.g.dt6()
                );
                ("order_upsert", vec![s])
            }
            _ => {
                let c = self.customer();
                let n = Ids::fresh(&self.ids.customer);
                // Insert a customer, reference it, then delete it again in the same txn.
                ("transient", vec![
                    format!("INSERT INTO customers (id, name, tier, region_id) VALUES ({n}, 'transient', 'gold', 1)"),
                    format!("UPDATE orders SET customer_id = {n} WHERE customer_id = {c}"),
                    format!("UPDATE orders SET customer_id = {c} WHERE customer_id = {n}"),
                    format!("DELETE FROM customers WHERE id = {n}"),
                ])
            }
        }
    }

    /// Operations on the FK-cascading tables (deletes / PK updates of parents cascade silently).
    fn fk_op(&mut self, k: i64) -> Vec<String> {
        // Any allocated group id for deletes / updates; only seeded ids are re-created by id.
        let gid = self.g.range(1, Ids::max(&self.ids.fk_group));
        let seed_gid = self.g.range(1, N_FK_GROUPS);
        let parents_hi = Ids::max(&self.ids.fk_parent) + 1;
        let pid = self.g.range(1, parents_hi);
        let pid2 = self.g.range(1, parents_hi);
        match k {
            0 | 1 => vec![format!("DELETE FROM fk_parents WHERE id = {pid}")],
            2 => vec![format!("DELETE FROM fk_groups WHERE id = {gid}")],
            3 => {
                let n = Ids::fresh(&self.ids.fk_parent);
                vec![format!("UPDATE fk_parents SET id = {n} WHERE id = {pid}")]
            }
            4 => {
                let n = Ids::fresh(&self.ids.fk_group);
                vec![format!("UPDATE fk_groups SET id = {n} WHERE id = {gid}")]
            }
            5 => {
                let n = Ids::fresh(&self.ids.fk_parent);
                let mut v = vec![
                    format!("INSERT IGNORE INTO fk_groups (id, name) VALUES ({seed_gid}, 'group {seed_gid}')"),
                    format!("INSERT INTO fk_parents (id, group_id, label) SELECT {n}, id, 'parent {n}' FROM fk_groups WHERE id = {gid}"),
                ];
                if self.g.chance(0.3) {
                    let m = Ids::fresh(&self.ids.fk_parent);
                    v.push(format!("INSERT INTO fk_parents (id, group_id, label) VALUES ({m}, NULL, 'orphan {m}')"));
                }
                v
            }
            6 => {
                let n = self.g.range(1, 3);
                (0..n)
                    .map(|_| {
                        let seq = Ids::fresh(&self.ids.fk_seq);
                        format!("INSERT INTO fk_children (parent_id, seq, qty) SELECT id, {seq}, {} FROM fk_parents WHERE id = {pid}", self.g.range(-2, 20))
                    })
                    .collect()
            }
            7 => match self.g.range(0, 2) {
                0 => vec![format!("UPDATE fk_children SET qty = qty + {} WHERE parent_id = {pid}", self.g.range(-3, 3))],
                1 => vec![format!(
                    "UPDATE fk_children ch JOIN fk_parents p ON p.id = {pid2} SET ch.parent_id = p.id WHERE ch.parent_id = {pid}"
                )],
                _ => vec![format!("DELETE FROM fk_children WHERE parent_id = {pid} ORDER BY seq LIMIT 1")],
            },
            8 => {
                let id = Ids::fresh(&self.ids.fk_note);
                match self.g.range(0, 2) {
                    0 => vec![format!("INSERT INTO fk_notes (id, parent_id, body) SELECT {id}, id, 'n{id}' FROM fk_parents WHERE id = {pid}")],
                    1 => vec![format!("UPDATE fk_notes n JOIN fk_parents p ON p.id = {pid2} SET n.parent_id = p.id WHERE n.parent_id = {pid} OR n.parent_id IS NULL")],
                    _ => vec![format!("DELETE FROM fk_notes WHERE parent_id = {pid}")],
                }
            }
            9 => {
                let c = self.customer();
                let seq = Ids::fresh(&self.ids.addr_seq);
                match self.g.range(0, 2) {
                    0 => vec![format!(
                        "INSERT INTO customer_addresses (customer_id, seq, city) SELECT id, {seq}, {} FROM customers WHERE id = {c}",
                        q(self.g.pick(CITIES))
                    )],
                    1 => vec![format!("UPDATE customer_addresses SET city = {} WHERE customer_id = {c}", q(self.g.pick(CITIES)))],
                    _ => vec![format!("DELETE FROM customer_addresses WHERE customer_id = {c} ORDER BY seq LIMIT 1")],
                }
            }
            _ => {
                // 2-level cascade in one statement plus a re-creation in the same transaction
                let n = Ids::fresh(&self.ids.fk_parent);
                let seq = Ids::fresh(&self.ids.fk_seq);
                vec![
                    format!("DELETE FROM fk_groups WHERE id = {seed_gid}"),
                    format!("INSERT INTO fk_groups (id, name) VALUES ({seed_gid}, 'reborn {seed_gid}')"),
                    format!("INSERT INTO fk_parents (id, group_id, label) VALUES ({n}, {seed_gid}, 'new {n}')"),
                    format!("INSERT INTO fk_children (parent_id, seq, qty) VALUES ({n}, {seq}, 1)"),
                ]
            }
        }
    }

    fn new_order(&mut self) -> Vec<String> {
        let id = Ids::fresh(&self.ids.order);
        let cust = self.customer_opt();
        let st = q(self.g.pick(ORDER_STATUS));
        let mut v = vec![format!(
            "INSERT INTO orders (id, customer_id, status, created_at, shipped_at, note) VALUES ({id}, {cust}, {st}, {}, NULL, {})",
            self.g.dt6(),
            opt!(self.g, 0.5, q("fresh"))
        )];
        let n = self.g.range(0, 4);
        if n > 0 {
            let rows: Vec<String> = (0..n).map(|_| self.item_row(id)).collect();
            v.push(format!(
                "INSERT INTO order_items (order_id, line_no, product_id, qty, price, discount) VALUES {}",
                rows.join(", ")
            ));
        }
        if self.g.chance(0.6) {
            let row = self.payment_row(id.to_string());
            v.push(format!(
                "INSERT INTO payments (id, order_id, method, amount, status, paid_at) VALUES {row}"
            ));
        }
        v
    }

    fn customer_update(&mut self) -> String {
        let c = self.customer();
        match self.g.range(0, 9) {
            8 => format!("UPDATE customers SET signup_ts = {} WHERE id = {c}", opt!(self.g, 0.3, self.g.ts6())),
            0 => format!("UPDATE customers SET region_id = {} WHERE id = {c}", opt!(self.g, 0.2, self.region().to_string())),
            1 => format!("UPDATE customers SET tier = {} WHERE id = {c}", opt!(self.g, 0.2, q(self.g.pick(TIERS)))),
            2 => format!("UPDATE customers SET name = UPPER(name) WHERE id = {c}"),
            3 => format!("UPDATE customers SET name = LOWER(name) WHERE id = {c}"),
            4 => format!("UPDATE customers SET referrer_id = {} WHERE id = {c}", opt!(self.g, 0.3, self.customer().to_string())),
            5 => format!("UPDATE customers SET credit = {} WHERE id = {c}", opt!(self.g, 0.2, self.g.decimal(-50_000, 500_000))),
            6 => format!("UPDATE customers SET flags = {} WHERE id = {c}", self.g.flags()),
            7 => format!(
                "UPDATE customers SET meta = JSON_SET(COALESCE(meta, '{{}}'), '$.lang', {}) WHERE id = {c}",
                q(self.g.pick(LANGS))
            ),
            _ => format!("UPDATE customers SET name = {}, email = {} WHERE id = {c}", self.g.name(), opt!(self.g, 0.3, q("new@example.com"))),
        }
    }

    /// An update touching only columns no view reads.
    pub fn unreferenced(&mut self) -> String {
        match self.g.range(0, 3) {
            0 => {
                let c = self.customer();
                format!(
                    "UPDATE customers SET last_seen_at = {} WHERE id BETWEEN {c} AND {}",
                    self.g.dt6(),
                    c + 5
                )
            }
            1 => {
                let o = self.order();
                format!(
                    "UPDATE orders SET note = {} WHERE id BETWEEN {o} AND {}",
                    q(&format!("note {}", self.g.range(0, 1_000_000))),
                    o + 5
                )
            }
            2 => {
                let p = self.product();
                format!(
                    "UPDATE products SET updated_at = {} WHERE id = {p}",
                    self.g.ts6()
                )
            }
            _ => format!(
                "UPDATE regions SET notes = {} WHERE id = {}",
                q(&format!("n{}", self.g.range(0, 9999))),
                self.region()
            ),
        }
    }

    fn events(&mut self) -> Vec<String> {
        let c = self.customer();
        match self.g.range(0, 3) {
            0 => {
                // duplicates on purpose (no PK)
                let n = self.g.range(1, 4);
                let amount = opt!(self.g, 0.2, self.g.decimal(0, 20_000));
                let kind = q(self.g.pick(KINDS));
                let at = self.g.dt0();
                let row = format!("({c}, {kind}, {amount}, {at})");
                vec![format!(
                    "INSERT INTO events_log (customer_id, kind, amount, happened_at) VALUES {}",
                    vec![row; n as usize].join(", ")
                )]
            }
            1 => vec![format!(
                "UPDATE events_log SET amount = COALESCE(amount, 0) + {} WHERE customer_id = {c} AND kind = {}",
                self.g.decimal(-1000, 9000),
                q(self.g.pick(KINDS))
            )],
            2 => vec![format!("DELETE FROM events_log WHERE customer_id = {c} LIMIT 1")],
            _ => vec![format!("UPDATE events_log SET customer_id = {} WHERE customer_id = {c} LIMIT 2", self.customer())],
        }
    }

    fn inventory(&mut self) -> Vec<String> {
        let r = self.region();
        let p = self.product();
        match self.g.range(0, 3) {
            0 => vec![format!(
                "INSERT INTO inventory (region_id, product_id, qty, reserved) VALUES ({r}, {p}, {}, 0)
                 ON DUPLICATE KEY UPDATE qty = qty + VALUES(qty)",
                self.g.range(1, 20)
            )],
            1 => vec![format!(
                "REPLACE INTO inventory (region_id, product_id, qty, reserved) VALUES ({r}, {p}, {}, {})",
                self.g.range(0, 100),
                self.g.range(0, 10)
            )],
            2 => vec![format!("UPDATE inventory SET reserved = reserved + 1 WHERE product_id = {p}")],
            _ => vec![format!("DELETE FROM inventory WHERE region_id = {r} AND product_id = {p}")],
        }
    }

    fn region_op(&mut self) -> Vec<String> {
        let r = self.region();
        match self.g.range(0, 2) {
            0 => vec![format!(
                "UPDATE regions SET name = {} WHERE id = {r}",
                opt!(self.g, 0.2, q(self.g.pick(REGION_NAMES)))
            )],
            1 => vec![format!("DELETE FROM regions WHERE id = {r}")],
            _ => vec![format!(
                "REPLACE INTO regions (id, name, country, tax_rate) VALUES ({r}, {}, 'DE', {})",
                q(self.g.pick(REGION_NAMES)),
                format!("0.{:03}", self.g.range(0, 250))
            )],
        }
    }

    fn bulk(&mut self) -> Vec<String> {
        let o = self.order();
        match self.g.range(0, 4) {
            0 => vec![format!(
                "UPDATE order_items SET price = price + 0.01 WHERE order_id BETWEEN {o} AND {}",
                o + 15
            )],
            1 => vec![format!(
                "UPDATE orders SET status = 'paid' WHERE id BETWEEN {o} AND {} AND status = 'new'",
                o + 10
            )],
            2 => vec![format!(
                "DELETE FROM payments WHERE order_id BETWEEN {o} AND {} AND status = 'failed'",
                o + 8
            )],
            3 => {
                // big order: one parent + many lines in one statement
                let id = Ids::fresh(&self.ids.order);
                let n = self.g.range(10, 30);
                let rows: Vec<String> = (0..n).map(|_| self.item_row(id)).collect();
                vec![
                    format!("INSERT INTO orders (id, customer_id, status, created_at) VALUES ({id}, {}, 'paid', {})", self.customer_opt(), self.g.dt6()),
                    format!("INSERT INTO order_items (order_id, line_no, product_id, qty, price, discount) VALUES {}", rows.join(", ")),
                ]
            }
            _ => vec![format!(
                "UPDATE customers SET credit = COALESCE(credit, 0) - 1 WHERE region_id = {}",
                self.region()
            )],
        }
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

/// Runs one transaction, retrying the whole transaction on deadlock / lock wait timeout.
pub async fn run_txn(conn: &mut Conn, t: &Txn) -> Result<()> {
    let mut attempt = 0;
    loop {
        attempt += 1;
        let res: Result<()> = async {
            if t.stmts.len() == 1 && !t.rollback {
                return exec(conn, &t.stmts[0]).await;
            }
            exec(conn, "START TRANSACTION").await?;
            for s in &t.stmts {
                exec(conn, s).await?;
            }
            exec(conn, if t.rollback { "ROLLBACK" } else { "COMMIT" }).await
        }
        .await;
        match res {
            Ok(()) => return Ok(()),
            Err(e) => {
                let _ = conn.query_drop("ROLLBACK").await;
                let code = server_code(&e);
                if matches!(code, Some(1213) | Some(1205)) && attempt < 30 {
                    tokio::time::sleep(Duration::from_millis(5 * attempt)).await;
                    continue;
                }
                return Err(e).with_context(|| {
                    format!("transaction '{}' failed (attempt {attempt})", t.label)
                });
            }
        }
    }
}

/// Runs `n` batches from `wl` on `conn`; returns the number of transactions executed.
pub async fn run_batches(conn: &mut Conn, wl: &mut Workload, n: usize) -> Result<usize> {
    let mut txns = 0;
    for _ in 0..n {
        for t in wl.next_batch() {
            run_txn(conn, &t).await?;
            txns += 1;
        }
    }
    Ok(txns)
}

/// Runs `writers` concurrent writer tasks (each with its own connection and derived seed),
/// `batches_each` batches per writer. Returns total transactions.
pub async fn run_concurrent(
    db: &Db,
    ids: Arc<Ids>,
    seed: u64,
    writers: usize,
    batches_each: usize,
) -> Result<usize> {
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = db.clone();
        let ids = ids.clone();
        handles.push(tokio::spawn(async move {
            let mut c = db.conn().await?;
            let mut wl = Workload::new(super::derive_seed(seed, &format!("writer{w}")), ids);
            let n = run_batches(&mut c, &mut wl, batches_each)
                .await
                .with_context(|| format!("writer {w}"))?;
            c.disconnect().await.ok();
            Ok::<usize, anyhow::Error>(n)
        }));
    }
    let mut total = 0;
    for h in handles {
        match h.await {
            Ok(r) => total += r?,
            Err(e) => bail!("writer task panicked: {e}"),
        }
    }
    Ok(total)
}

/// Runs `writers` concurrent writers until `stop` turns true. Returns total transactions.
pub async fn run_until_stopped(
    db: &Db,
    ids: Arc<Ids>,
    seed: u64,
    writers: usize,
    stop: tokio::sync::watch::Receiver<bool>,
) -> Result<usize> {
    let mut handles = Vec::new();
    for w in 0..writers {
        let db = db.clone();
        let ids = ids.clone();
        let stop = stop.clone();
        handles.push(tokio::spawn(async move {
            let mut c = db.conn().await?;
            let mut wl = Workload::new(super::derive_seed(seed, &format!("bg{w}")), ids);
            let mut n = 0;
            while !*stop.borrow() {
                n += run_batches(&mut c, &mut wl, 1)
                    .await
                    .with_context(|| format!("background writer {w}"))?;
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            c.disconnect().await.ok();
            Ok::<usize, anyhow::Error>(n)
        }));
    }
    let mut total = 0;
    for h in handles {
        match h.await {
            Ok(r) => total += r?,
            Err(e) => bail!("writer task panicked: {e}"),
        }
    }
    Ok(total)
}
