//! PostgreSQL as a SOURCE: the logical-replication smoke (owner ruling "CI follows the source",
//! phase 3, task pgsrc-ci-src-p2). It proves the whole read path end to end on a live server:
//! create the replication slot, make the publication publish DML, run an INSERT / UPDATE / DELETE,
//! then stream the pgoutput byte stream out of the slot and decode every message with the crate's
//! own [`lazyview::pgoutput`] decoder — asserting the decoded row images round-trip the values we
//! wrote (the INSERT's new row, the UPDATE's new row, the DELETE's old row).
//!
//! Why SQL + `pg_logical_slot_peek_binary_changes` and not the engine's reader: the crate has no
//! public replication-session / slot API yet (the slot, stream and fence live in `src/pgsrc/` and
//! land in the later PRs of the S-series), so the honest gate is the same peek the pgoutput decoder
//! fixtures were captured with (`tmp/out/lvs-pgsrc/decoder/capture.sql`) driven through the public
//! decoder. When the gated session path lands, this test re-points at it; the assertion is the same
//! decoded INSERT/UPDATE/DELETE round-trip either way.
//!
//! Gated like every other PostgreSQL leg: the server comes from `LAZYVIEW_TEST_PG_DSNS` (first
//! entry), and `LAZYVIEW_TEST_REQUIRE_PG=1` (which the CI `integration-pgsrc` job always sets) turns
//! a missing server into a failure instead of a printed SKIP — so the job can require this test by
//! name and a skip cannot leave it green.

mod common;

use common::pg;
use lazyview::pgoutput::{self, Column, Message, OldTuple, Relation};

/// The `v` cell (index 1) of a tuple, as decoded text; `NULL`/unchanged surface as markers so an
/// assertion can tell them from a real value.
fn cells(t: &pgoutput::Tuple<'_>) -> Vec<String> {
    t.columns()
        .map(|c| match c.expect("a well-formed tuple cell") {
            Column::Text(b) => String::from_utf8_lossy(b).into_owned(),
            Column::Null => "<null>".to_string(),
            Column::Unchanged => "<unchanged>".to_string(),
            Column::Binary(_) => "<binary>".to_string(),
        })
        .collect()
}

#[tokio::test]
async fn pgsrc_replication_decodes_insert_update_delete() {
    const TEST: &str = "pgsrc_replication_decodes_insert_update_delete";
    let Some(servers) = pg::servers_or_skip(TEST) else {
        return;
    };
    let dsn = servers[0].base_dsn.clone();

    let (client, conn) = tokio_postgres::connect(&dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to the PostgreSQL source");
    tokio::spawn(conn);

    // Unique names so concurrent legs (and a shared developer server) never collide.
    let sfx: u32 = rand::random();
    let tbl = format!("lvsrc_smoke_items_{sfx}");
    let publication = format!("lvsrc_smoke_pub_{sfx}");
    let slot = format!("lvsrc_smoke_slot_{sfx}");

    // --- gated session path: the slot + publication, then the DML, on one committed txn each ---
    client
        .batch_execute(&format!(
            "CREATE TABLE {tbl}(id int primary key, v text); \
             ALTER TABLE {tbl} REPLICA IDENTITY FULL; \
             CREATE PUBLICATION {publication} FOR TABLE {tbl} \
               WITH (publish = 'insert,update,delete');"
        ))
        .await
        .expect("create the source table, identity FULL and publication");

    client
        .batch_execute(&format!(
            "SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput')"
        ))
        .await
        .expect("create the logical replication slot (wal_level=logical)");

    client
        .batch_execute(&format!("INSERT INTO {tbl} VALUES (1, 'hello')"))
        .await
        .expect("INSERT");
    client
        .batch_execute(&format!("UPDATE {tbl} SET v = 'world' WHERE id = 1"))
        .await
        .expect("UPDATE");
    client
        .batch_execute(&format!("DELETE FROM {tbl} WHERE id = 1"))
        .await
        .expect("DELETE");

    // Peek (do not consume): every pgoutput message the slot holds for this publication. Each row's
    // `data` bytea is one whole message (leading tag byte included), exactly the decoder's input.
    let rows = client
        .query(
            "SELECT lsn, xid, data FROM pg_logical_slot_peek_binary_changes( \
                 $1, NULL, NULL, 'proto_version', '1', 'publication_names', $2)",
            &[&slot, &publication],
        )
        .await
        .expect("peek the binary changes from the slot");

    // Tear the server down before asserting: a failure must not leave a slot, publication or table.
    client
        .batch_execute(&format!(
            "SELECT pg_drop_replication_slot('{slot}'); \
             DROP PUBLICATION IF EXISTS {publication}; \
             DROP TABLE IF EXISTS {tbl};"
        ))
        .await
        .expect("drop the slot, publication and table");
    drop(client);

    // --- decode every message with the crate's public decoder and check the round trip ---
    let mut relations: Vec<Relation<'_>> = Vec::new();
    let mut begins = 0;
    let mut commits = 0;
    let mut inserts: Vec<Vec<String>> = Vec::new();
    let mut updates: Vec<(bool, Vec<String>)> = Vec::new();
    let mut deletes: Vec<(OldTuple, Vec<String>)> = Vec::new();

    for row in &rows {
        let data: &[u8] = row.get(2);
        match pgoutput::parse(data).expect("every peeked message decodes") {
            Message::Begin { .. } => begins += 1,
            Message::Commit { .. } => commits += 1,
            Message::Relation(r) => relations.push(r),
            Message::Insert { new, .. } => inserts.push(cells(&new)),
            Message::Update { old, new, .. } => {
                updates.push((matches!(old, Some((OldTuple::Full, _))), cells(&new)))
            }
            Message::Delete { old, .. } => deletes.push((old.0, cells(&old.1))),
            // Type / Origin / Truncate / Message are legal but this smoke does not exercise them.
            _ => {}
        }
    }

    // One relation for our table, named by the publication's decode path.
    let rel = relations.iter().find(|r| r.name == tbl).unwrap_or_else(|| {
        panic!(
            "no Relation message for {tbl}: decoded relations \
            {:?}",
            relations.iter().map(|r| r.name).collect::<Vec<_>>()
        )
    });
    assert_eq!(rel.replica_identity, pgoutput::replica_identity::FULL);
    assert_eq!(
        rel.columns().count(),
        2,
        "the two-column table is described"
    );

    // A complete transaction stream: a Begin and a Commit bracket the DML.
    assert_eq!(
        begins, 3,
        "one Begin per committed statement (INSERT, UPDATE, DELETE)"
    );
    assert_eq!(commits, 3, "one Commit per committed statement");

    // INSERT: the new row is (1, 'hello').
    assert_eq!(inserts, vec![vec!["1".to_string(), "hello".to_string()]]);

    // UPDATE: with REPLICA IDENTITY FULL the whole old row is sent; the new row is (1, 'world').
    assert_eq!(updates.len(), 1, "one UPDATE decoded");
    let (old_is_full, new_cells) = &updates[0];
    assert!(
        old_is_full,
        "the UPDATE carried the full old tuple (REPLICA IDENTITY FULL)"
    );
    assert_eq!(new_cells, &vec!["1".to_string(), "world".to_string()]);

    // DELETE: the old tuple is always present and is the whole row we deleted, (1, 'world').
    assert_eq!(deletes.len(), 1, "one DELETE decoded");
    let (del_kind, del_cells) = &deletes[0];
    assert_eq!(*del_kind, OldTuple::Full);
    assert_eq!(del_cells, &vec!["1".to_string(), "world".to_string()]);
}
