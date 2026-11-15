//! Captured `pg_catalog` rows prove the loader's pure assembly: the column build, the type
//! map's inverse, the key and foreign-key rules and the view refusal. The rows are fixture
//! text (neutral names); the live read is the same SQL over `tokio_postgres`, gated with the
//! pg16 source (`S(pg16)` deferred until the harness pins a PostgreSQL DSN).

use super::catalog::{
    attribute_row, catalog_types, columns_from_pg_rows, fk_row, foreign_keys_from_rows,
    view_refusal, AttributeRow, FkRow,
};
use crate::catalog::{ColKind, FkAction};
use crate::mv::MvCol;
use crate::source::mysql::keys_from;
use crate::target::postgres::{cell_value, pg_type};
use mysql_async::Value;

fn mv_col(data_type: &str, column_type: &str) -> MvCol {
    MvCol {
        name: "c".into(),
        src_name: "c".into(),
        column_type: column_type.into(),
        data_type: data_type.into(),
        charset: None,
        collation: None,
        nullable: true,
    }
}

fn attr(attname: &str, pg_type: &str, not_null: bool) -> AttributeRow {
    attribute_row(attname, pg_type, not_null, "", "", None)
}

/// One `items` table from captured rows: the identity column stays plain, `attgenerated`
/// fills `Generated` (stored in the image, virtual never), every type lands in the catalog
/// vocabulary the target's map knows, and the keys follow the NOT NULL rule.
#[test]
fn pg_catalog_rows_build_a_table_info() {
    let rows = vec![
        attribute_row("id", "integer", true, "a", "", None),
        attr("title", "character varying(255)", true),
        attr("body", "text", false),
        attr("total", "numeric(10,2)", true),
        attr("created", "timestamp(6) with time zone", true),
        attr("day", "date", false),
        attr("clock", "time without time zone", false),
        attr("payload", "jsonb", false),
        attr("raw", "bytea", false),
        attr("flag", "boolean", true),
        attr("parent_id", "integer", true),
        attr("ref_id", "integer", true),
        attribute_row("upper_title", "text", false, "", "s", Some("upper(title)")),
        attribute_row("norm", "text", false, "", "v", Some("lower(title)")),
    ];
    let columns = columns_from_pg_rows(rows);
    assert_eq!(columns.len(), 14);
    // The identity column is not generated: its value sits in every row image.
    assert_eq!(
        (
            columns[0].data_type.as_str(),
            columns[0].column_type.as_str()
        ),
        ("integer", "integer")
    );
    assert!(columns[0].generated.is_none());
    assert!(!columns[0].nullable);
    // The inverse of the target's map, per column.
    assert_eq!(
        (
            columns[1].data_type.as_str(),
            columns[1].column_type.as_str()
        ),
        ("varchar", "varchar(255)")
    );
    assert_eq!(
        (
            columns[3].data_type.as_str(),
            columns[3].column_type.as_str()
        ),
        ("decimal", "decimal(10,2)")
    );
    assert_eq!(
        (
            columns[4].data_type.as_str(),
            columns[4].column_type.as_str()
        ),
        ("timestamp", "timestamp(6)")
    );
    assert_eq!(
        (
            columns[6].data_type.as_str(),
            columns[6].column_type.as_str()
        ),
        ("time", "time")
    );
    assert_eq!(columns[7].kind, ColKind::Json);
    assert_eq!(columns[9].column_type, "tinyint(1)");
    assert!(columns[2].nullable);
    // Generated columns: stored reaches the image, virtual never does (`exact_key`).
    let stored = columns[12].generated.as_ref().unwrap();
    assert!(stored.stored && stored.expr == "upper(title)");
    assert!(columns[12].exact_key());
    let virtual_gen = columns[13].generated.as_ref().unwrap();
    assert!(!virtual_gen.stored && virtual_gen.expr == "lower(title)");
    assert!(!columns[13].exact_key());

    // Keys: the PK from the `PRIMARY` row, the unique index over a NOT NULL column, and the
    // unique index over the nullable `body` dropped (several NULL rows are allowed).
    let key_rows: Vec<(String, u64, Option<String>, Option<u64>, String)> = vec![
        ("PRIMARY".into(), 1, Some("id".into()), None, "NO".into()),
        (
            "items_title_key".into(),
            1,
            Some("title".into()),
            None,
            "NO".into(),
        ),
        (
            "items_body_key".into(),
            1,
            Some("body".into()),
            None,
            "YES".into(),
        ),
    ];
    let (pk, unique_keys) = keys_from("items", &key_rows, &columns).unwrap();
    assert_eq!(pk, vec![0]);
    assert_eq!(unique_keys, vec![vec![1]]);

    // Cascades: the two-column cascade and the SET UPDATE kept, the pure restrict skipped.
    let fk_rows: Vec<FkRow> = vec![
        fk_row(
            "links_pair_fkey",
            "parent_id",
            "public",
            "items",
            "id",
            "c",
            "a",
        ),
        fk_row(
            "links_pair_fkey",
            "ref_id",
            "public",
            "items",
            "version",
            "c",
            "a",
        ),
        fk_row(
            "links_single_fkey",
            "title",
            "public",
            "items",
            "slug",
            "a",
            "n",
        ),
        fk_row(
            "links_restricted_fkey",
            "day",
            "public",
            "items",
            "id",
            "a",
            "r",
        ),
    ];
    let cascades = foreign_keys_from_rows("links", &fk_rows, &columns).unwrap();
    assert_eq!(cascades.len(), 2);
    assert_eq!(
        cascades[0].parent,
        ("public".to_string(), "items".to_string())
    );
    assert_eq!(cascades[0].child_cols, vec![10, 11]);
    assert_eq!(
        cascades[0].parent_cols,
        vec!["id".to_string(), "version".to_string()]
    );
    assert_eq!(cascades[0].on_delete, FkAction::Cascade);
    assert_eq!(cascades[1].child_cols, vec![1]);
    assert_eq!(cascades[1].on_update, FkAction::SetNull);
}

/// The whole mapping is the target's map read backwards: every pair `catalog_types` produces
/// is a pair `pg_type` renders back to the PostgreSQL type it came from (the folds — boolean
/// to `smallint`, TIME to text — are the map's own), and `cell_value` parses the inverses it
/// knows with its own text parsers, never new ones.
#[test]
fn pg_type_inverse_matches_the_target_map() {
    let cases = [
        ("smallint", "smallint", "smallint", "smallint"),
        ("integer", "integer", "integer", "integer"),
        ("bigint", "bigint", "bigint", "bigint"),
        ("boolean", "tinyint", "tinyint(1)", "smallint"),
        ("numeric", "decimal", "decimal", "numeric"),
        ("numeric(10,2)", "decimal", "decimal(10,2)", "numeric(10,2)"),
        ("real", "float", "float", "real"),
        ("double precision", "double", "double", "double precision"),
        ("date", "date", "date", "date"),
        (
            "timestamp without time zone",
            "datetime",
            "datetime",
            "timestamp",
        ),
        (
            "timestamp(3) without time zone",
            "datetime",
            "datetime(3)",
            "timestamp(3)",
        ),
        (
            "timestamp(6) with time zone",
            "timestamp",
            "timestamp(6)",
            "timestamp(6) with time zone",
        ),
        ("time without time zone", "time", "time", "text"),
        ("time(3) without time zone", "time", "time(3)", "text"),
        ("bytea", "varbinary", "varbinary", "bytea"),
        ("jsonb", "json", "json", "jsonb"),
        ("json", "json", "json", "jsonb"),
        ("character varying(255)", "varchar", "varchar(255)", "text"),
        ("character(10)", "char", "char(10)", "text"),
        ("text", "text", "text", "text"),
        ("uuid", "text", "text", "text"),
        ("interval", "text", "text", "text"),
        ("bit(4)", "text", "text", "text"),
    ];
    for (pg, data_type, column_type, mapped) in cases {
        let (dt, ct) = catalog_types(pg);
        assert_eq!(
            (dt.as_str(), ct.as_str()),
            (data_type, column_type),
            "inverse of {pg}"
        );
        assert_eq!(
            pg_type(&mv_col(&dt, &ct)),
            mapped,
            "pg_type of the inverse of {pg}"
        );
    }
    // `cell_value` routes the inverses to its parsers (never the bytes fallback).
    assert_eq!(
        cell_value(Some("42"), &mv_col("integer", "integer"), true),
        Value::Int(42)
    );
    assert_eq!(
        cell_value(Some("\\x4142"), &mv_col("varbinary", "varbinary"), true),
        Value::Bytes(b"AB".to_vec())
    );
    assert!(
        !matches!(
            cell_value(Some("838:59:59"), &mv_col("time", "time"), true),
            Value::Bytes(_)
        ),
        "TIME parses as a time"
    );
    assert!(
        !matches!(
            cell_value(
                Some("2024-02-29 23:59:58.000001+00"),
                &mv_col("timestamp", "timestamp(6)"),
                true
            ),
            Value::Bytes(_)
        ),
        "timestamptz parses as a datetime"
    );
}

/// A unique index is a key only over whole NOT NULL columns; a key column the table does not
/// have is broken metadata - a loud error, never a silently shorter key (CA1).
#[test]
fn keys_need_whole_not_null_columns() {
    let columns = columns_from_pg_rows(vec![
        attr("id", "integer", true),
        attr("name", "character varying(64)", true),
        attr("note", "text", false),
    ]);
    let rows: Vec<(String, u64, Option<String>, Option<u64>, String)> = vec![
        ("PRIMARY".into(), 1, Some("id".into()), None, "NO".into()),
        (
            "items_note_key".into(),
            1,
            Some("note".into()),
            None,
            "YES".into(),
        ),
    ];
    let (pk, unique) = keys_from("items", &rows, &columns).unwrap();
    assert_eq!(pk, vec![0]);
    assert!(
        unique.is_empty(),
        "a nullable unique column admits several NULL rows"
    );

    let missing: Vec<(String, u64, Option<String>, Option<u64>, String)> =
        vec![("PRIMARY".into(), 1, Some("ghost".into()), None, "NO".into())];
    let err = keys_from("items", &missing, &columns)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("ghost") && err.contains("does not have"),
        "{err}"
    );

    let functional: Vec<(String, u64, Option<String>, Option<u64>, String)> =
        vec![("PRIMARY".into(), 1, None, None, "NO".into())];
    let err = keys_from("items", &functional, &columns)
        .unwrap_err()
        .to_string();
    assert!(err.contains("unusable PRIMARY"), "{err}");
}

/// Only actions that change child rows outside the stream survive, `confdeltype`/
/// `confupdtype` speak the words `FkAction::parse` reads, and a FK naming a column the table
/// does not have is a loud error, never a dropped cascade.
#[test]
fn foreign_keys_keep_only_the_cascading_actions() {
    let columns = columns_from_pg_rows(vec![attr("id", "integer", true)]);
    let rows: Vec<FkRow> = vec![
        fk_row("items_a_fkey", "id", "public", "items", "id", "c", "r"),
        fk_row("items_b_fkey", "id", "public", "items", "id", "n", "a"),
        fk_row("items_c_fkey", "id", "public", "items", "id", "d", "a"),
        fk_row("items_d_fkey", "id", "public", "items", "id", "a", "r"),
    ];
    let kept = foreign_keys_from_rows("links", &rows, &columns).unwrap();
    assert_eq!(kept.len(), 3, "the pure restrict stays out");
    assert_eq!(kept[0].on_delete, FkAction::Cascade);
    assert_eq!(kept[1].on_delete, FkAction::SetNull);
    assert_eq!(
        kept[2].on_delete,
        FkAction::SetNull,
        "SET DEFAULT is the map's SetNull row"
    );

    let broken = vec![fk_row(
        "items_x_fkey",
        "ghost",
        "public",
        "items",
        "id",
        "c",
        "a",
    )];
    let err = foreign_keys_from_rows("links", &broken, &columns)
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("items_x_fkey") && err.contains("ghost") && err.contains("does not have"),
        "{err}"
    );
}

/// A view is refused as a source table, and the refusal names the statement the base tables
/// actually need.
#[test]
fn views_are_refused_naming_replica_identity() {
    let err = view_refusal("public", "items").to_string();
    assert!(err.contains("public.items"), "{err}");
    assert!(err.contains("REPLICA IDENTITY"), "{err}");
    assert!(err.contains("is a view"), "{err}");
}
