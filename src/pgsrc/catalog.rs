//! The `pg_catalog` loader of the PostgreSQL source (issue #57, PR S2d).
//!
//! One load per `Relation` message — the same cadence as a MySQL `TABLE_MAP` load — building
//! [`TableInfo`] through `catalog`'s public constructors; `src/catalog*` itself is F6's file
//! and is not edited here. The text-to-type mapping is the inverse of the target's map: every
//! pair [`catalog_types`] produces is a pair [`crate::target::postgres::pg_type`] renders back
//! to the PostgreSQL type it came from, and the `data_type` choices are the ones
//! [`crate::target::postgres::cell_value`] knows how to parse (`time` for TIME, `varbinary`
//! for bytea). There is no second type map (source plan §0.2); the tests assert both.
//!
//! `TableInfo` is never partial: a key or foreign key naming a column the table does not have
//! is a loud error (CA1, the rule the MySQL loader follows), and a view is refused here, at
//! the door, never streamed as a source table.

// Staged like the target's T4/T5 items: S2f's `Relation` handler calls `load_table_pg` when
// it lands; the tests here prove it now, and the `dead_code` lint cannot see that far ahead.
#![allow(dead_code)]

use anyhow::{anyhow, bail, Result};
use tokio_postgres::GenericClient;

use crate::catalog::{Column, FkAction, ForeignKey, Generated, TableInfo};
use crate::source::mysql::keys_from;
use crate::source::SourceCaps;

/// Load one source table from `pg_catalog`: columns from `pg_attribute` (with the identity
/// and generation flags), the primary key and the unique NOT NULL indexes from
/// `pg_index`/`pg_constraint`, the cascading foreign keys from `pg_constraint`, and the view
/// refusal from `pg_class`/`pg_views`.
pub(crate) async fn load_table_pg<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<TableInfo> {
    // `pg_class`/`pg_namespace`: the table must exist, and what it is decides the refusal.
    let relkind: Option<String> = client
        .query_opt(
            "SELECT c.relkind::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname::text = $1 AND c.relname::text = $2",
            &[&schema, &name],
        )
        .await?
        .map(|r| r.get(0));
    let Some(kind) = relkind else {
        bail!("table `{schema}.{name}` does not exist on the PostgreSQL source");
    };
    // A view is refused as a source table, with the message naming `REPLICA IDENTITY` (the
    // statement the base tables actually need, D2 #6: never degrade silently).
    if kind == "v" || is_view(client, schema, name).await? {
        return Err(view_refusal(schema, name));
    }
    if kind != "r" {
        bail!(
            "`{schema}.{name}` (relkind `{kind}`) is not a plain table: a logical replication \
             slot streams rows of plain tables only"
        );
    }
    let columns = columns_from_pg_rows(read_attributes(client, schema, name).await?);
    let (pk, unique_keys) = load_keys(client, schema, name, &columns).await?;
    let cascades = load_foreign_keys(client, schema, name, &columns).await?;
    Ok(TableInfo {
        schema: schema.to_string(),
        name: name.to_string(),
        columns,
        pk,
        unique_keys,
        cascades,
        is_view: false,
        view_bases: Vec::new(),
    })
}

/// The refusal of a view: no WAL of its own, so never a source table (the test pins that the
/// message names `REPLICA IDENTITY`).
pub(crate) fn view_refusal(schema: &str, name: &str) -> anyhow::Error {
    anyhow!(
        "`{schema}.{name}` is a view: Lazyview streams plain tables, never views (a view has no \
         WAL of its own). Stream its base tables instead, each one carrying ALTER TABLE … \
         REPLICA IDENTITY FULL"
    )
}

async fn is_view<C: GenericClient>(client: &C, schema: &str, name: &str) -> Result<bool> {
    Ok(client
        .query_opt(
            "SELECT 1 FROM pg_views WHERE schemaname::text = $1 AND viewname::text = $2",
            &[&schema, &name],
        )
        .await?
        .is_some())
}

/// One captured `pg_attribute` row: (attname, `format_type` text, attnotnull, attidentity,
/// attgenerated, generation expression).
pub(crate) type AttributeRow = (String, String, bool, String, String, Option<String>);

/// Builds one [`AttributeRow`] (a tuple alias has no constructor of its own): the fixture
/// spelling the tests share with the live read.
pub(crate) fn attribute_row(
    attname: &str,
    pg_type: &str,
    not_null: bool,
    identity: &str,
    generated: &str,
    expr: Option<&str>,
) -> AttributeRow {
    (
        attname.into(),
        pg_type.into(),
        not_null,
        identity.into(),
        generated.into(),
        expr.map(str::to_string),
    )
}

/// Columns: `pg_attribute` over the live table (dropped columns stay out), `format_type` for
/// the type text, the identity and generation flags, and the expression of a generated column.
const COLUMNS_SQL: &str = "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull, \
     a.attidentity::text, a.attgenerated::text, \
     CASE WHEN a.attgenerated <> '' THEN pg_get_expr(d.adbin, d.adrelid) ELSE NULL::text END \
   FROM pg_attribute a \
   JOIN pg_class c ON c.oid = a.attrelid \
   JOIN pg_namespace n ON n.oid = c.relnamespace \
   LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
   WHERE n.nspname::text = $1 AND c.relname::text = $2 AND a.attnum > 0 AND NOT a.attisdropped \
   ORDER BY a.attnum";

async fn read_attributes<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
) -> Result<Vec<AttributeRow>> {
    let rows = client.query(COLUMNS_SQL, &[&schema, &name]).await?;
    Ok(rows
        .iter()
        .map(|r| attribute_row(r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5)))
        .collect())
}

/// Assemble the columns from the `pg_attribute` rows. `attidentity` is read with them because
/// it is precisely *not* a generated column: like MySQL's `AUTO_INCREMENT`, an identity column
/// carries a server-made value that sits in every row image, so it stays a plain column. Only
/// `attgenerated` fills [`Generated`] — `s` stored (in the image), `v` virtual (never in the
/// image, and `Column::exact_key` keeps it out of keys).
pub(crate) fn columns_from_pg_rows(rows: Vec<AttributeRow>) -> Vec<Column> {
    rows.into_iter()
        .map(|(name, pg_type, not_null, _identity, generated, expr)| {
            let (data_type, column_type) = catalog_types(&pg_type);
            let gen = match generated.as_str() {
                "s" | "v" => Some(Generated {
                    expr: expr.unwrap_or_default(),
                    stored: generated == "s",
                    deps: None,
                }),
                _ => None,
            };
            Column {
                nullable: !not_null,
                generated: gen,
                ..Column::new(&name, &data_type, &column_type, None, None, None, pg_caps())
            }
        })
        .collect()
}

/// The `SourceCaps` the public `Column` constructor takes until S2e picks the real PostgreSQL
/// answers at connect. None of them reads a source capability today except the JSON param
/// spelling, and the PostgreSQL rendering of every param belongs to the dialect (A5).
fn pg_caps() -> SourceCaps {
    SourceCaps {
        fresh_snapshot: false,
        json_cast: false,
        sql_generation_expressions: false,
        // A PostgreSQL slot loses WAL past its retain window, so a lost resume point is an
        // expected event here; `Config::on_missing_binlog` decides what it costs (unlike the
        // placeholders above, this one reads from the source on the fail path).
        cursor_purgeable: true,
    }
}

/// The inverse of [`crate::target::postgres::pg_type`]: for each PostgreSQL type text
/// `format_type` answers, the (data_type, column_type) pair that map renders back to it. This
/// is the whole mapping — the target's map read backwards, never a new one (source plan §0.2).
/// The folds are the map's own: `boolean` answers `smallint` (its `tinyint(1)` alias row),
/// TIME answers `text` (D1) while the `time` data_type stays `cell_value`'s signal to parse a
/// time, and the types the map only ever makes as `text` — char, varchar, uuid, interval,
/// PostgreSQL's own `bit`, arrays, anything unknown — land there too, where `cell_value` keeps
/// their text visible instead of guessing a shape.
pub(crate) fn catalog_types(pg: &str) -> (String, String) {
    let t = pg.trim().to_ascii_lowercase();
    let paren = match (t.find('('), t.rfind(')')) {
        (Some(o), Some(c)) if c > o => t[o..=c].to_string(),
        _ => String::new(),
    };
    let base = t.split('(').next().unwrap_or(t.as_str()).trim();
    let plain = |my: &str| (my.to_string(), format!("{my}{paren}"));
    if base.starts_with("timestamp") {
        // `timestamp … without time zone` is the map's `datetime`, `… with time zone` its
        // `timestamp` (the UTC session of T1.4).
        return if t.ends_with("with time zone") {
            plain("timestamp")
        } else {
            plain("datetime")
        };
    }
    if base.starts_with("time") {
        return plain("time");
    }
    match base {
        "smallint" => plain("smallint"),
        "integer" => plain("integer"),
        "bigint" => plain("bigint"),
        "boolean" => ("tinyint".to_string(), "tinyint(1)".to_string()),
        "numeric" | "decimal" => plain("decimal"),
        "real" => ("float".to_string(), "float".to_string()),
        "double precision" => ("double".to_string(), "double".to_string()),
        "date" => plain("date"),
        "bytea" => ("varbinary".to_string(), "varbinary".to_string()),
        "json" | "jsonb" => ("json".to_string(), "json".to_string()),
        "character varying" => plain("varchar"),
        "character" => plain("char"),
        _ => ("text".to_string(), "text".to_string()),
    }
}

/// The PK and the unique indexes over NOT NULL columns, fed to the MySQL loader's `keys_from`
/// in its own row shape: the PK is renamed `PRIMARY` (PostgreSQL spells it `<table>_pkey`),
/// an expression index part has no column name (the functional part `keys_from` refuses on a
/// PK), a prefix never exists here (`SUB_PART` is always None), and the `pg_constraint` join
/// keeps out the indexes of exclusion constraints (unique, but no key).
const KEYS_SQL: &str = "SELECT CASE WHEN i.indisprimary THEN 'PRIMARY' ELSE ic.relname::text END, \
     k.ord, a.attname, CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END \
   FROM pg_index i \
   JOIN pg_constraint con ON con.indrelid = i.indrelid AND con.indexrelid = i.indexrelid \
     AND con.contype IN ('p', 'u') \
   JOIN pg_class ic ON ic.oid = i.indexrelid \
   JOIN pg_class tc ON tc.oid = i.indrelid \
   JOIN pg_namespace n ON n.oid = tc.relnamespace \
   LEFT JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true \
   LEFT JOIN pg_attribute a ON a.attrelid = tc.oid AND a.attnum = k.attnum \
     AND NOT a.attisdropped \
   WHERE n.nspname::text = $1 AND tc.relname::text = $2 \
   ORDER BY 1, k.ord";

async fn load_keys<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<(Vec<usize>, Vec<Vec<usize>>)> {
    let rows = client.query(KEYS_SQL, &[&schema, &name]).await?;
    let key_rows: Vec<(String, u64, Option<String>, Option<u64>, String)> = rows
        .iter()
        .map(|r| {
            (
                r.get(0),
                r.get::<_, i64>(1) as u64,
                r.get(2),
                None,
                r.get(3),
            )
        })
        .collect();
    keys_from(name, &key_rows, columns)
}

/// One captured `pg_constraint` row for `contype = 'f'`: (constraint name, child column,
/// parent schema, parent table, parent column, confdeltype, confupdtype).
pub(crate) type FkRow = (String, String, String, String, String, String, String);

/// Builds one [`FkRow`] (a tuple alias has no constructor of its own): the fixture spelling
/// the tests share with the live read.
#[allow(clippy::too_many_arguments)]
pub(crate) fn fk_row(
    conname: &str,
    child_col: &str,
    parent_schema: &str,
    parent_table: &str,
    parent_col: &str,
    deltype: &str,
    updtype: &str,
) -> FkRow {
    (
        conname.into(),
        child_col.into(),
        parent_schema.into(),
        parent_table.into(),
        parent_col.into(),
        deltype.into(),
        updtype.into(),
    )
}

const FK_SQL: &str = "SELECT con.conname::text, ck.attname, pn.nspname::text, pt.relname::text, \
     pk.attname, con.confdeltype::text, con.confupdtype::text \
   FROM pg_constraint con \
   JOIN pg_class tc ON tc.oid = con.conrelid \
   JOIN pg_namespace tn ON tn.oid = tc.relnamespace \
   JOIN pg_class pt ON pt.oid = con.confrelid \
   JOIN pg_namespace pn ON pn.oid = pt.relnamespace \
   JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS ckn(attnum, ord) ON true \
   JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS pkn(attnum, ord) ON pkn.ord = ckn.ord \
   JOIN pg_attribute ck ON ck.attrelid = tc.oid AND ck.attnum = ckn.attnum \
   JOIN pg_attribute pk ON pk.attrelid = pt.oid AND pk.attnum = pkn.attnum \
   WHERE tn.nspname::text = $1 AND tc.relname::text = $2 AND con.contype = 'f' \
   ORDER BY con.conname, ckn.ord";

async fn load_foreign_keys<C: GenericClient>(
    client: &C,
    schema: &str,
    name: &str,
    columns: &[Column],
) -> Result<Vec<ForeignKey>> {
    let rows = client.query(FK_SQL, &[&schema, &name]).await?;
    let fk_rows: Vec<FkRow> = rows
        .iter()
        .map(|r| {
            fk_row(
                r.get(0),
                r.get(1),
                r.get(2),
                r.get(3),
                r.get(4),
                r.get(5),
                r.get(6),
            )
        })
        .collect();
    foreign_keys_from_rows(name, &fk_rows, columns)
}

/// Group the FK rows by constraint (they come ordered by name and position) and keep only the
/// actions that change child rows outside the stream, exactly as the MySQL loader does. A
/// child column the table does not have is broken metadata: a loud error naming table,
/// constraint and column (CA1), never a silently dropped cascade.
pub(crate) fn foreign_keys_from_rows(
    table: &str,
    rows: &[FkRow],
    columns: &[Column],
) -> Result<Vec<ForeignKey>> {
    let mut cascades: Vec<ForeignKey> = Vec::new();
    let mut last: Option<String> = None;
    for (cname, col, ps, pt, pc, del, upd) in rows {
        let (on_delete, on_update) = (fk_action(del), fk_action(upd));
        if on_delete == FkAction::Restrict && on_update == FkAction::Restrict {
            continue;
        }
        let Some(ci) = columns.iter().position(|c| c.name == *col) else {
            bail!("internal: FK `{cname}` of table `{table}` names column `{col}`, which the table does not have");
        };
        if last.as_deref() != Some(cname.as_str()) {
            cascades.push(ForeignKey {
                parent: (ps.clone(), pt.clone()),
                child_cols: Vec::new(),
                parent_cols: Vec::new(),
                on_delete,
                on_update,
            });
            last = Some(cname.clone());
        }
        let fk = cascades.last_mut().unwrap();
        fk.child_cols.push(ci);
        fk.parent_cols.push(pc.clone());
    }
    Ok(cascades)
}

/// `confdeltype`/`confupdtype` in the words `FkAction::parse` reads: `c` cascade, `n` set
/// null, `d` set default (the parse's own `SET NULL | SET DEFAULT` row), `a`/`r` the restrict
/// the loader skips.
fn fk_action(code: &str) -> FkAction {
    FkAction::parse(match code {
        "c" => "CASCADE",
        "n" => "SET NULL",
        "d" => "SET DEFAULT",
        _ => "RESTRICT",
    })
}
