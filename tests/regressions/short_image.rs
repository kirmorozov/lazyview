use super::*;

/// A session that sets `binlog_row_image=MINIMAL` (or NOBLOB) while the engine runs, writing
/// to tables an incremental view reads: the images arrive with only some columns, and every
/// read of a row image is positional, so the missing ones shift all later values. The views
/// must never stay `ready` with wrong data (review C-02): the engine never applies such an
/// image; it refreshes the units the event still names, or the whole view, from the source.
const SHORT_DDL: &[&str] = &[
    "CREATE TABLE entities (id INT PRIMARY KEY, grp INT NOT NULL, name VARCHAR(20) NOT NULL, note TEXT NULL)",
    "CREATE TABLE items (id INT PRIMARY KEY, entity_id INT NOT NULL, price DECIMAL(10,2) NOT NULL)",
    "INSERT INTO entities VALUES (1, 1, 'ann', NULL), (2, 1, 'bob', 'first'), (3, 2, 'cid', 'x')",
    "INSERT INTO items VALUES (10, 1, 5.00), (11, 2, 50.00), (12, 3, 20.00)",
];

/// C-02: writes with MINIMAL and NOBLOB row images leave the views exact and counted; writes
/// back at FULL cost no marking at all.
#[test]
fn reg_short_row_image_keeps_views_exact() {
    scenario("reg_short_image", |ctx| async move {
        let specs = vec![
            ViewSpec::new(
                "r_item_entity",
                Kind::Incremental,
                "SELECT i.id, i.price, e.name FROM items i JOIN entities e ON e.id = i.entity_id",
            ),
            ViewSpec::new(
                "r_items_per_grp",
                Kind::Incremental,
                "SELECT e.grp, COUNT(*) AS n FROM items i JOIN entities e ON e.id = i.entity_id GROUP BY e.grp",
            ),
            // Reads the TEXT column: under NOBLOB an update that does not change it leaves
            // it out of both images.
            ViewSpec::new(
                "r_entity_notes",
                Kind::Incremental,
                "SELECT id, note FROM entities WHERE note IS NOT NULL",
            ),
        ];
        let (db, mut eng) = setup_views(&ctx, "reg_short_image", SHORT_DDL, &specs, &[]).await?;
        let mut c = db.conn().await?;
        // The trigger: this session logs minimal images (the global value stays FULL, so the
        // engine's startup check passes and never sees this).
        exec(&mut c, "SET SESSION binlog_row_image=MINIMAL").await?;
        exec_all(
            &mut c,
            &[
                // before-image: the PK only; after-image: the changed column only.
                "UPDATE entities SET grp = 9 WHERE id = 1",
                "UPDATE items SET price = 1.00 WHERE id = 10",
                // before-image: the PK only.
                "DELETE FROM items WHERE id = 11",
                // after-image: the NOT NULL columns (all of them here).
                "INSERT INTO items VALUES (13, 3, 7.00)",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "writes logged with MINIMAL row images",
        )
        .await?;
        assert!(
            eng.snapshot().short_row_images > 0,
            "the MINIMAL events must be counted as short images"
        );
        // NOBLOB: the unchanged TEXT column is left out of both images.
        exec(&mut c, "SET SESSION binlog_row_image=NOBLOB").await?;
        exec_all(
            &mut c,
            &[
                "UPDATE entities SET name = 'zed' WHERE id = 2",
                "UPDATE entities SET note = 'second' WHERE id = 3",
                "UPDATE items SET price = 8.00 WHERE id = 12",
            ],
        )
        .await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "writes logged with NOBLOB row images",
        )
        .await?;
        // Back at FULL: the common case, and it must not mark anything.
        exec(&mut c, "SET SESSION binlog_row_image=FULL").await?;
        let before = eng.snapshot().short_row_images;
        exec_all(
            &mut c,
            &[
                "UPDATE entities SET grp = 4, note = NULL WHERE id = 3",
                "UPDATE items SET price = 9.00 WHERE id = 13",
                "INSERT INTO entities VALUES (4, 4, 'dee', 'n')",
                "INSERT INTO items VALUES (14, 4, 1.50)",
                "DELETE FROM items WHERE id = 10",
            ],
        )
        .await?;
        verify(&ctx, &mut eng, &db, &specs, "writes back at FULL").await?;
        assert_eq!(
            eng.snapshot().short_row_images,
            before,
            "a full image must never count as short"
        );
        eng.stop().await
    });
}

/// C-02's other half (issue #7): on MariaDB 11.4+ a full image legitimately leaves VIRTUAL
/// generated columns out. A view over a table whose VIRTUAL column trails the read ones
/// must maintain exactly and must never be counted as a short image.
#[test]
fn reg_virtual_only_image_stays_legal() {
    scenario("reg_virtual_image", |ctx| async move {
        let ddl = &[
            "CREATE TABLE links (id INT PRIMARY KEY, a INT NOT NULL, b INT NOT NULL, v INT AS (a * 2) VIRTUAL)",
            "INSERT INTO links (id, a, b) VALUES (1, 1, 10), (2, 2, 20)",
        ];
        let specs = vec![ViewSpec::new(
            "r_link_rows",
            Kind::Incremental,
            "SELECT id, a, b FROM links WHERE b > 0",
        )];
        let (db, mut eng) = setup_views(&ctx, "reg_virtual_image", ddl, &specs, &[]).await?;
        let mut c = db.conn().await?;
        let before = eng.snapshot().short_row_images;
        exec(&mut c, "UPDATE links SET a = 5, b = 50 WHERE id = 1").await?;
        exec(&mut c, "INSERT INTO links (id, a, b) VALUES (3, 3, 30)").await?;
        exec(&mut c, "DELETE FROM links WHERE id = 2").await?;
        verify(
            &ctx,
            &mut eng,
            &db,
            &specs,
            "full images that omit only the VIRTUAL column",
        )
        .await?;
        assert_eq!(
            eng.snapshot().short_row_images,
            before,
            "an image that omits only a VIRTUAL column stays legal (#7)"
        );
        eng.stop().await
    });
}
