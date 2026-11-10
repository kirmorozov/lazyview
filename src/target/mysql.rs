//! The MySQL family as a target: the statements `crate::mv` issues, moving here as the seam
//! lands (PR S2 onward). Verbatim moves: the SQL strings are the ones `crate::mv` rendered.

use crate::analyze::quote_ident as q;
use crate::mv::{Layout, Upsert};

/// Row alias of upserts (`INSERT ... AS <alias> ON DUPLICATE KEY UPDATE`).
const ROW_ALIAS: &str = "_lv_new";

/// A multi-row INSERT of `n` rows; with `upsert`, rows whose key exists replace that row.
pub(crate) fn insert_sql(
    schema: &str,
    table: &str,
    layout: &Layout,
    n: usize,
    upsert: Option<Upsert>,
) -> String {
    let tuple = format!("({})", vec!["?"; layout.cols.len()].join(","));
    let mut sql = format!(
        "INSERT INTO {}.{} ({}) VALUES {}",
        q(schema),
        q(table),
        layout.col_list(),
        vec![tuple.as_str(); n].join(",")
    );
    // Every column is set, the key columns too: a collation-equal key takes the new bytes.
    let set = |value: &dyn Fn(&str) -> String| {
        layout
            .cols
            .iter()
            .map(|c| format!("{} = {}", q(&c.name), value(&q(&c.name))))
            .collect::<Vec<_>>()
            .join(", ")
    };
    match upsert {
        None => {}
        Some(Upsert::RowAlias) => {
            // (The row alias must differ from the table name.)
            let alias = if table.eq_ignore_ascii_case(ROW_ALIAS) {
                q(&format!("{ROW_ALIAS}_"))
            } else {
                q(ROW_ALIAS)
            };
            let updates = set(&|c| format!("{alias}.{c}"));
            sql.push_str(&format!(" AS {alias} ON DUPLICATE KEY UPDATE {updates}"));
        }
        Some(Upsert::Values) => {
            let updates = set(&|c| format!("VALUES({c})"));
            sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {updates}"));
        }
    }
    sql
}
