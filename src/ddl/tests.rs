use super::statement_effects;

fn t(q: &str) -> Vec<(String, String)> {
    statement_effects(q, "d").tables
}

fn names(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .collect()
}

/// Only `DROP TABLE[S]` removes a base table; `DROP INDEX … ON t`, `DROP VIEW`, `DROP TEMPORARY
/// TABLE` and `ALTER TABLE … DROP …` name a table without removing it.
#[test]
fn dropped_tables() {
    let dropped = |q: &str| statement_effects(q, "d").dropped;
    assert_eq!(
        dropped("DROP TABLE a, s.b"),
        names(&[("d", "a"), ("s", "b")])
    );
    assert_eq!(dropped("drop tables if exists `a`"), names(&[("d", "a")]));
    assert_eq!(dropped("/*!40000 DROP TABLE a */"), names(&[("d", "a")]));
    assert_eq!(
        dropped("DROP TABLE a; DROP TABLE b, a"),
        names(&[("d", "a"), ("d", "b")])
    );
    for q in [
        "DROP INDEX i ON a",
        "DROP INDEX `i` ON s.a ALGORITHM = INPLACE",
        "ALTER TABLE a DROP INDEX i",
        "ALTER TABLE a DROP COLUMN x",
        "DROP VIEW a",
        "DROP TEMPORARY TABLE IF EXISTS a",
        "DROP /*!40005 TEMPORARY */ TABLE IF EXISTS a",
        "TRUNCATE TABLE a",
        "DROP DATABASE d",
    ] {
        assert!(dropped(q).is_empty(), "{q}");
        assert!(!statement_effects(q, "d").tables.is_empty(), "{q}");
    }
}

/// Only statements that keep every row of their tables (ALTER TABLE without a RENAME or a
/// partition / tablespace change, CREATE INDEX, DROP INDEX) leave `rows_kept` set.
#[test]
fn rows_kept() {
    let kept = |q: &str| statement_effects(q, "d").rows_kept;
    for q in [
        "ALTER TABLE t ADD INDEX i (a)",
        "ALTER TABLE t ADD COLUMN c INT NULL",
        "CREATE INDEX i ON t (a)",
        "DROP INDEX i ON t",
        "ALTER TABLE t RENAME COLUMN a TO b",
        "ALTER TABLE t ADD INDEX i (a); ALTER TABLE u DROP COLUMN x",
    ] {
        assert!(kept(q), "{q}");
    }
    for q in [
        "RENAME TABLE a TO a_old, a_new TO a",
        "ALTER TABLE t RENAME TO u",
        "ALTER TABLE t RENAME AS u",
        "ALTER TABLE t EXCHANGE PARTITION p WITH TABLE u",
        "ALTER TABLE t DROP PARTITION p",
        "ALTER TABLE t TRUNCATE PARTITION p",
        "ALTER TABLE t DISCARD TABLESPACE",
        "TRUNCATE TABLE t",
        "REPAIR TABLE t",
        "CREATE TABLE t2 LIKE t",
        "CREATE TABLE t (a INT)",
        "DROP TABLE t",
        "DROP VIEW v",
        "CREATE VIEW v AS SELECT 1",
        "ALTER TABLE t ADD INDEX i (a); TRUNCATE TABLE t",
        "ALTER TABLE t ADD INDEX i (a); DROP TABLE u",
        "INSERT INTO t VALUES (1)",
        "",
    ] {
        assert!(!kept(q), "{q:?}");
    }
}

#[test]
fn alter_table() {
    assert_eq!(t("ALTER TABLE t ADD COLUMN x INT"), names(&[("d", "t")]));
    assert_eq!(
        t("alter table `s`.`my t` drop column x"),
        names(&[("s", "my t")])
    );
    assert_eq!(
        t("ALTER ONLINE IGNORE TABLE IF EXISTS s.t ENGINE=InnoDB"),
        names(&[("s", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t RENAME TO s2.t2"),
        names(&[("d", "t"), ("s2", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t RENAME AS t2"),
        names(&[("d", "t"), ("d", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t RENAME t2"),
        names(&[("d", "t"), ("d", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t ADD x INT, RENAME TO t2"),
        names(&[("d", "t"), ("d", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t RENAME COLUMN a TO b"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t RENAME INDEX a TO b, RENAME KEY c TO e"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t EXCHANGE PARTITION p0 WITH TABLE `x`.`t2` WITHOUT VALIDATION"),
        names(&[("d", "t"), ("x", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t CONVERT TABLE t2 TO PARTITION p2 VALUES LESS THAN (10)"),
        names(&[("d", "t"), ("d", "t2")])
    );
    assert_eq!(
        t("ALTER TABLE t CONVERT PARTITION p1 TO TABLE t3"),
        names(&[("d", "t"), ("d", "t3")])
    );
    assert_eq!(
        t("ALTER TABLE t CONVERT TO CHARACTER SET utf8mb4"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t ADD CONSTRAINT fk FOREIGN KEY (a) REFERENCES p (id)"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t COMMENT 'rename to x'"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t ADD c INT DEFAULT (1), ALGORITHM=INPLACE, LOCK=NONE"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE t TRUNCATE PARTITION p0"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("ALTER TABLE `select`.`from` ADD x INT"),
        names(&[("select", "from")])
    );
    assert_eq!(t("ALTER TABLE t1 ADD x INT"), names(&[("d", "t1")]));
    assert_eq!(t("ALTER TABLE 1t ADD x INT"), names(&[("d", "1t")]));
}

#[test]
fn drop_table() {
    assert_eq!(t("DROP TABLE a"), names(&[("d", "a")]));
    assert_eq!(
        t("DROP TABLE IF EXISTS a, s.b, `c` RESTRICT"),
        names(&[("d", "a"), ("s", "b"), ("d", "c")])
    );
    assert_eq!(
        t("DROP TABLE `a`,`b` /* generated by server */"),
        names(&[("d", "a"), ("d", "b")])
    );
    assert_eq!(
        t("DROP TEMPORARY TABLE IF EXISTS `tmp`"),
        names(&[("d", "tmp")])
    );
    assert_eq!(
        t("DROP /*!40005 TEMPORARY */ TABLE IF EXISTS `tmp`"),
        names(&[("d", "tmp")])
    );
    assert_eq!(t("drop table a, a"), names(&[("d", "a")]));
}

#[test]
fn rename_table() {
    assert_eq!(t("RENAME TABLE a TO b"), names(&[("d", "a"), ("d", "b")]));
    assert_eq!(
        t("RENAME TABLE a TO b, s.c TO `s2`.`d`"),
        names(&[("d", "a"), ("d", "b"), ("s", "c"), ("s2", "d")])
    );
    assert_eq!(
        t("RENAME TABLES IF EXISTS a WAIT 5 TO b, c NOWAIT TO e"),
        names(&[("d", "a"), ("d", "b"), ("d", "c"), ("d", "e")])
    );
    assert_eq!(
        t("RENAME TABLE mv TO mv_old, mv_new TO mv"),
        names(&[("d", "mv"), ("d", "mv_old"), ("d", "mv_new")])
    );
    assert!(t("RENAME USER a TO b").is_empty());
}

#[test]
fn truncate_repair_optimize() {
    assert_eq!(t("TRUNCATE TABLE s.t"), names(&[("s", "t")]));
    assert_eq!(t("truncate t"), names(&[("d", "t")]));
    assert_eq!(t("TRUNCATE TABLE t WAIT 3"), names(&[("d", "t")]));
    assert_eq!(
        t("REPAIR NO_WRITE_TO_BINLOG TABLE a, s.b QUICK"),
        names(&[("d", "a"), ("s", "b")])
    );
    assert_eq!(t("REPAIR LOCAL TABLE a"), names(&[("d", "a")]));
    assert!(t("OPTIMIZE TABLE a, b").is_empty());
    assert!(t("ANALYZE TABLE a").is_empty());
    assert!(t("CHECK TABLE a").is_empty());
}

#[test]
fn create_table() {
    assert_eq!(
        t("CREATE TABLE t (id INT PRIMARY KEY)"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("CREATE TABLE IF NOT EXISTS s.t (id INT)"),
        names(&[("s", "t")])
    );
    assert_eq!(t("CREATE TEMPORARY TABLE t (id INT)"), names(&[("d", "t")]));
    assert_eq!(
        t("CREATE OR REPLACE TABLE t (id INT)"),
        names(&[("d", "t")])
    );
    assert_eq!(t("CREATE TABLE t2 LIKE s.t1"), names(&[("d", "t2")]));
    assert_eq!(t("CREATE TABLE t2 (LIKE t1)"), names(&[("d", "t2")]));
    assert_eq!(
        t("CREATE TABLE t2 AS SELECT * FROM t1 JOIN t3"),
        names(&[("d", "t2")])
    );
    assert_eq!(t("CREATE TABLE t2 SELECT a FROM t1"), names(&[("d", "t2")]));
    assert_eq!(
        t("CREATE TABLE `t2` (\n  `a` int DEFAULT NULL\n) START TRANSACTION"),
        names(&[("d", "t2")])
    );
    assert_eq!(t("CREATE TABLE \"q\".\"t\" (a INT)"), names(&[("q", "t")]));
}

#[test]
fn indexes() {
    assert_eq!(t("CREATE INDEX i ON t (a)"), names(&[("d", "t")]));
    assert_eq!(
        t("CREATE UNIQUE INDEX i USING BTREE ON s.t (a, b)"),
        names(&[("s", "t")])
    );
    assert_eq!(
        t("CREATE OR REPLACE ONLINE INDEX IF NOT EXISTS i ON t (a)"),
        names(&[("d", "t")])
    );
    assert_eq!(
        t("CREATE FULLTEXT INDEX ft ON `t` (body)"),
        names(&[("d", "t")])
    );
    assert_eq!(t("DROP INDEX i ON s.t"), names(&[("s", "t")]));
    assert_eq!(
        t("DROP INDEX IF EXISTS `i` ON `t` ALGORITHM=INPLACE"),
        names(&[("d", "t")])
    );
    assert_eq!(t("DROP INDEX `PRIMARY` ON t"), names(&[("d", "t")]));
}

#[test]
fn views() {
    assert_eq!(t("CREATE VIEW v AS SELECT * FROM t"), names(&[("d", "v")]));
    assert_eq!(
        t("CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `s`.`v` AS select 1 AS `1`"),
        names(&[("s", "v")])
    );
    assert_eq!(
        t("CREATE OR REPLACE DEFINER='u'@'%' VIEW v (a) AS SELECT 1"),
        names(&[("d", "v")])
    );
    assert_eq!(
        t("CREATE DEFINER=root@localhost VIEW v AS SELECT 1"),
        names(&[("d", "v")])
    );
    assert_eq!(
        t("CREATE DEFINER=CURRENT_USER() VIEW v AS SELECT 1"),
        names(&[("d", "v")])
    );
    assert_eq!(
        t("CREATE DEFINER=CURRENT_USER VIEW v AS SELECT 1"),
        names(&[("d", "v")])
    );
    assert_eq!(
        t("ALTER ALGORITHM=MERGE SQL SECURITY INVOKER VIEW v AS SELECT 2"),
        names(&[("d", "v")])
    );
    assert_eq!(
        t("DROP VIEW IF EXISTS v1, s.v2"),
        names(&[("d", "v1"), ("s", "v2")])
    );
    assert_eq!(
        t("/*!50001 CREATE ALGORITHM=UNDEFINED */\n/*!50013 DEFINER=`root`@`localhost` SQL SECURITY DEFINER */\n/*!50001 VIEW `v` AS select 1 */"),
        names(&[("d", "v")])
    );
}

#[test]
fn databases() {
    assert_eq!(t("DROP DATABASE x"), names(&[("x", "*")]));
    assert_eq!(t("DROP SCHEMA IF EXISTS `x y`"), names(&[("x y", "*")]));
    assert_eq!(t("CREATE OR REPLACE DATABASE x"), names(&[("x", "*")]));
    assert!(t("CREATE DATABASE x").is_empty());
    assert!(t("CREATE DATABASE IF NOT EXISTS x CHARACTER SET utf8mb4").is_empty());
    assert!(t("ALTER DATABASE x CHARACTER SET utf8mb4").is_empty());
}

#[test]
fn comments() {
    assert_eq!(
        t("/* app=x */ ALTER TABLE t ADD x INT"),
        names(&[("d", "t")])
    );
    assert_eq!(t("-- note\nDROP TABLE a"), names(&[("d", "a")]));
    assert_eq!(t("# note\nDROP TABLE a"), names(&[("d", "a")]));
    assert_eq!(
        t("ALTER TABLE /* old */ t /* RENAME TO x */ ADD y INT"),
        names(&[("d", "t")])
    );
    assert_eq!(t("/*!50001 DROP TABLE t2 */"), names(&[("d", "t2")]));
    assert_eq!(t("/*!DROP TABLE t2*/"), names(&[("d", "t2")]));
    assert_eq!(t("/*M!100100 DROP TABLE t3 */"), names(&[("d", "t3")]));
    assert_eq!(t("/*M! DROP TABLE t3 */"), names(&[("d", "t3")]));
    assert_eq!(
        t("ALTER TABLE t /*!50100 RENAME TO t5 */"),
        names(&[("d", "t"), ("d", "t5")])
    );
    assert!(t("/* DROP TABLE t */ SELECT 1").is_empty());
}

#[test]
fn not_ddl() {
    for q in [
        "",
        "   ",
        "BEGIN",
        "COMMIT",
        "GRANT SELECT ON d.t TO 'u'@'%'",
        "REVOKE ALL ON *.* FROM u",
        "CREATE USER 'u'@'%' IDENTIFIED BY 'p'",
        "ALTER USER u IDENTIFIED BY 'x'",
        "DROP USER IF EXISTS u",
        "SET PASSWORD FOR u = 'x'",
        "SET GLOBAL read_only = 1",
        "FLUSH TABLES",
        "FLUSH PRIVILEGES",
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET a = 1",
        "DELETE FROM t",
        "REPLACE INTO t VALUES (1)",
        "SELECT * FROM t",
        "CREATE TRIGGER tr BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1",
        "CREATE DEFINER=`root`@`localhost` TRIGGER tr AFTER DELETE ON t FOR EACH ROW BEGIN DELETE FROM t2; DROP TABLE x; END",
        "CREATE PROCEDURE p() BEGIN DROP TABLE x; END",
        "CREATE DEFINER=`root`@`%` FUNCTION f() RETURNS INT RETURN 1",
        "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO TRUNCATE t",
        "DROP PROCEDURE IF EXISTS p",
        "DROP TRIGGER tr",
        "DROP EVENT e",
        "ALTER EVENT e DISABLE",
        "INSTALL PLUGIN x SONAME 'x.so'",
        "CREATE SEQUENCE s",
        "XA START X'31'",
        "ANALYZE TABLE t",
    ] {
        assert!(t(q).is_empty(), "{q:?} -> {:?}", t(q));
    }
}

#[test]
fn multiple_statements_and_odd_input() {
    assert_eq!(
        t("DROP TABLE a; DROP TABLE b"),
        names(&[("d", "a"), ("d", "b")])
    );
    assert_eq!(t("ALTER TABLE"), names(&[]));
    assert_eq!(t("DROP TABLE"), names(&[]));
    assert_eq!(t("RENAME TABLE a"), names(&[("d", "a")]));
    assert_eq!(t("RENAME TABLE a TO"), names(&[("d", "a")]));
    assert_eq!(t("TRUNCATE"), names(&[]));
    assert_eq!(t("CREATE INDEX i"), names(&[]));
    // Tokenizer failure: DDL-looking statements report the whole default schema.
    assert_eq!(
        t("ALTER TABLE t COMMENT 'unterminated"),
        names(&[("d", "*")])
    );
    assert_eq!(t("/* x */ DROP TABLE `unterminated"), names(&[("d", "*")]));
    assert!(t("SELECT 'unterminated").is_empty());
    // Unqualified names with an empty default schema.
    assert_eq!(
        statement_effects("DROP TABLE a", "").tables,
        names(&[("", "a")])
    );
}

// ---------------------------------------------------------------------------------------------
// #278 — user-declared secondary indexes: the RED-TEST list's grammar half
// (tmp/out/lvs-indexes/plan/expectations.md → RED-TEST LIST → Unit; owner rulings D1, D4, D8).
//
// These tests are INTENTIONALLY RED (owner ruling D8: expectations → plan → failing tests →
// implementation). They must not be weakened, skipped or deleted to make CI green: the
// implementation PRs (breakdown.md P4/P5) turn them green. Every case keeps the (a)/(b)/(c)
// labels of its expectations.md row: (a) the definition/statement table as the user writes it,
// (b) the original db (pinned corpora `A`/`B` — nothing invented), (c) the resulting view-table
// DDL, asserted here at the grammar level (the integration half is tests/indexes.rs).
// ---------------------------------------------------------------------------------------------

/// (a) of a #278 case: the `definition` cell as the user writes it (form S) — the `CREATE VIEW`
/// plus its free-standing follow-up statements (D1: "follow up statements, as if someone would
/// write them manually"), split at top-level `;`.
fn script(view: &str, select: &str, decls: &[&str]) -> String {
    let mut s = format!("CREATE VIEW {view} AS {select}");
    for d in decls {
        s.push_str("; ");
        s.push_str(d);
    }
    s.push(';');
    s
}

/// The pinned corpora of expectations.md ("Corpora (pinned, existing only)"): `B`'s `items` is
/// what H1/H4 aggregate, `A`'s `entities` what H2/H8 project. Only their column names appear in
/// the (a) scripts below; the data itself belongs to the integration half (tests/indexes.rs).
const SEL_TOTALS: &str = "SELECT entity_id, SUM(amount) AS total, COUNT(*) AS n \
                          FROM items GROUP BY entity_id";
const SEL_STATUS: &str = "SELECT id, grp, status FROM entities";
const SEL_NAMES: &str = "SELECT id, name FROM entities";

/// (c) probe for the parse cases: everything the definition parse keeps of `def`. A correct
/// implementation must expose the declared index — its name, its kind and its columns in order —
/// on the parse result it hands the planner, so the parse result's own rendering is the honest
/// probe of "the parse kept the declaration". Until the declaration AST exists there is nothing
/// else to look at: `src/ddl.rs:498-511` (`index_target`) keeps only the table after the first
/// top-level `ON` and throws name, kind and columns away, and `parse_definition`
/// (src/analyze/parse.rs:208 `SINGLE_SELECT`) refuses a script with a second statement at all.
fn kept(def: &str) -> String {
    match crate::analyze::parse_definition(def) {
        Ok(p) => format!("{p:?}"),
        Err(e) => format!("PARSE ERROR: {e:#}"),
    }
}

/// The error text of a refused definition, or `""` when it parses.
fn refusal(def: &str) -> String {
    match crate::analyze::parse_definition(def) {
        Ok(_) => String::new(),
        Err(e) => format!("{e:#}"),
    }
}

/// `hay` mentions `needle` (case-insensitively).
fn mentions(hay: &str, needle: &str) -> bool {
    hay.to_ascii_lowercase()
        .contains(&needle.to_ascii_lowercase())
}

/// H1, H2, H3, H4 (a)→(c): `CREATE [UNIQUE|FULLTEXT] INDEX <name> ON <view>(cols)` as a
/// follow-up statement is recognized into `{ name, unique/fulltext, table, cols in order }`.
///
/// RED today because no index AST exists (`src/ddl.rs:498-511` discards name/kind/columns, and
/// there is no other index parser in `src/`), so none of the declaration survives the parse.
#[test]
fn parses_index_declarations() {
    for (case, view, select, decl, name, cols, kind) in [
        (
            "H1",
            "item_totals",
            SEL_TOTALS,
            "CREATE INDEX i_total ON item_totals (total)",
            "i_total",
            vec!["total"],
            "",
        ),
        (
            "H2",
            "entity_status",
            SEL_STATUS,
            "CREATE UNIQUE INDEX u_status_grp ON entity_status (status, grp)",
            "u_status_grp",
            vec!["status", "grp"],
            "unique",
        ),
        (
            "H3",
            "entity_names",
            SEL_NAMES,
            "CREATE FULLTEXT INDEX ft_name ON entity_names (name)",
            "ft_name",
            vec!["name"],
            "fulltext",
        ),
        (
            "H4",
            "item_totals",
            SEL_TOTALS,
            "CREATE INDEX i_tot_ent ON item_totals (total, entity_id)",
            "i_tot_ent",
            vec!["total", "entity_id"],
            "",
        ),
    ] {
        let def = script(view, select, &[decl]);
        let kept = kept(&def);
        assert!(
            !kept.starts_with("PARSE ERROR"),
            "{case} (a) is refused outright: {kept}\n  (a): {def}\n  (c): the view table carries \
             {name} over ({}) — impossible while the declaration does not parse",
            cols.join(", "),
        );
        assert!(
            mentions(&kept, name),
            "{case} (c): the parse does not keep the declared index name `{name}`: {kept}"
        );
        assert!(
            mentions(&kept, "CREATE INDEX")
                || mentions(&kept, "CREATE UNIQUE INDEX")
                || mentions(&kept, "CREATE FULLTEXT INDEX"),
            "{case} (a): the follow-up statement itself is gone from the parse result: {kept}"
        );
        if !kind.is_empty() {
            assert!(
                mentions(&kept, kind),
                "{case} (c): the declared kind `{kind}` is not recorded: {kept}"
            );
        }
        // (c) asserts the column set in SEQ_IN_INDEX order, so a parse that keeps them in another
        // order (or only one of them) is a failure — H4: "(entity_id, total) fails the test".
        let mut at = 0usize;
        for c in &cols {
            let found = kept.to_ascii_lowercase()[at..]
                .find(&c.to_ascii_lowercase())
                .map(|i| at + i);
            assert!(
                found.is_some(),
                "{case} (c): column `{c}` of the declared key is missing, or out of order, in {kept}"
            );
            at = found.unwrap() + 1;
        }
    }
}

/// H1-H4, E2 (a): one `CREATE VIEW` and its follow-up `CREATE INDEX` statements are ONE script —
/// split at top-level `;`, with comments and `;` whitespace ignored (D1, form S of
/// expectations.md).
///
/// RED today because `SINGLE_SELECT` (src/analyze/parse.rs:208) rejects anything but one
/// statement, so the script of every case below is refused before anything is recognized.
#[test]
fn index_declaration_after_create_view_is_one_script() {
    let cases = [
        (
            "H1",
            script(
                "item_totals",
                SEL_TOTALS,
                &["CREATE INDEX i_total ON item_totals (total)"],
            ),
        ),
        (
            "H4",
            script(
                "item_totals",
                SEL_TOTALS,
                &["CREATE INDEX i_tot_ent ON item_totals (total, entity_id)"],
            ),
        ),
        // E2 (a): "the follow-up statement written twice".
        (
            "E2",
            script(
                "item_totals",
                SEL_TOTALS,
                &[
                    "CREATE INDEX i_total ON item_totals (total)",
                    "CREATE INDEX i_total ON item_totals (total)",
                ],
            ),
        ),
    ];
    for (case, def) in cases {
        let kept = kept(&def);
        assert!(
            !kept.starts_with("PARSE ERROR"),
            "{case} (a): a CREATE VIEW with its follow-up CREATE INDEX statements is one script \
             (D1), it is refused: {kept}"
        );
        assert!(
            mentions(&kept, "i_total") || mentions(&kept, "i_tot_ent"),
            "{case} (c): the declaration of the second statement is not kept: {kept}"
        );
    }
    // (a) with the comments and whitespace a hand-written cell has: `-- note`, `/* */`, `#`,
    // and blank lines between the statements (the same G2 handling of src/analyze/parse.rs:216-231).
    let commented =
        "CREATE VIEW item_totals AS SELECT entity_id, SUM(amount) AS total, COUNT(*) AS n \
                     FROM items GROUP BY entity_id;\n  -- declared later, by hand\n \
                     /* a block comment */\nCREATE INDEX i_total ON item_totals (total);\n#\n";
    let kept = kept(commented);
    assert!(
        !kept.starts_with("PARSE ERROR"),
        "H1 (a): comments and whitespace between the statements of the script break it: {kept}"
    );
    assert!(mentions(&kept, "i_total"), "H1 (c): {kept}");
}

/// E8 (a)→(c): the unsupported index forms are refused at PARSE, each error naming the statement
/// and the offending construct, and nothing of them reaching the target. The rule is the same on
/// every flavor ("MySQL 8.0 *would* accept the functional index while MariaDB would not"): reject
/// all of these at parse everywhere, so one definition means one thing.
///
/// RED today because nothing parses these statements at all, so no error names any construct.
#[test]
fn rejects_expression_prefix_and_method_index() {
    for (what, decl) in [
        (
            "expression (functional) index",
            "CREATE INDEX i_up ON item_totals ((UPPER(total)))",
        ),
        (
            "prefix length",
            "CREATE INDEX i_pre ON item_totals (total(4))",
        ),
        (
            "USING method clause",
            "CREATE INDEX i_h ON item_totals (total) USING HASH",
        ),
        (
            "WITH storage options",
            "CREATE INDEX i_o ON item_totals (total) WITH (fillfactor=70)",
        ),
        (
            "SPATIAL index",
            "CREATE SPATIAL INDEX i_sp ON item_totals (total)",
        ),
        (
            "partial-index WHERE clause",
            "CREATE INDEX i_e ON item_totals (total) WHERE total > 0",
        ),
    ] {
        let def = script("item_totals", SEL_TOTALS, &[decl]);
        let err = refusal(&def);
        assert!(
            !err.is_empty(),
            "E8 (c): `{decl}` must be refused, it was accepted"
        );
        assert!(
            mentions(&err, "CREATE INDEX") || mentions(&err, decl),
            "E8 (c): the error must name the statement it refuses (`{decl}`); it says: {err}"
        );
        assert!(
            mentions(&err, what.split(' ').next().unwrap()),
            "E8 (c): the error must name the offending construct ({what}); it says: {err}"
        );
        assert!(
            mentions(&err, "supported") || mentions(&err, "support"),
            "E8 (c): the error must state the supported subset; it says: {err}"
        );
    }
}

/// E8 (a) tail (c): trailing garbage after the closing `)` is refused the way `parse_sql` refuses
/// anything that does not end at EOF (src/analyze.rs:52-58).
///
/// RED today: there is no such parser, so the trailing garbage is never even looked at.
#[test]
fn rejects_unterminated_or_trailing_garbage_index() {
    for (what, decl) in [
        (
            "trailing garbage",
            "CREATE INDEX i_total ON item_totals (total) GARBAGE",
        ),
        (
            "trailing comma",
            "CREATE INDEX i_total ON item_totals (total,)",
        ),
        (
            "unclosed column list",
            "CREATE INDEX i_total ON item_totals (total",
        ),
        ("missing ON", "CREATE INDEX i_total (total)"),
        ("missing column list", "CREATE INDEX i_total ON item_totals"),
    ] {
        let def = script("item_totals", SEL_TOTALS, &[decl]);
        let err = refusal(&def);
        assert!(
            !err.is_empty(),
            "E8 (c): `{decl}` must be refused, it was accepted"
        );
        assert!(
            mentions(&err, "CREATE INDEX") || mentions(&err, "index"),
            "E8 (c): the {what} error must say which statement it refuses; it says: {err}"
        );
    }
}

/// E3 (a)→(c), and E10: an index over a column the view does not define — and, for E3, one whose
/// generated-column declaration is missing — is refused where the error names the column. The
/// engine must not invent the column (D4 requires the explicit column statement).
///
/// RED today: there is no cross-check between a declared index and the view's columns.
#[test]
fn rejects_index_on_undeclared_column() {
    for (case, view, select, decl, column) in [
        // E10 (a): `nope` is not a column of the view at all.
        (
            "E10",
            "item_totals",
            SEL_TOTALS,
            "CREATE INDEX i_nope ON item_totals (nope)",
            "nope",
        ),
        // E3 (a): H8's script with the `ADD COLUMN sig …` statement removed.
        (
            "E3",
            "entity_status",
            SEL_STATUS,
            "CREATE INDEX i_sig ON entity_status (sig)",
            "sig",
        ),
    ] {
        let def = script(view, select, &[decl]);
        let err = refusal(&def);
        assert!(
            !err.is_empty(),
            "{case} (c): `{decl}` must be refused, it was accepted"
        );
        assert!(
            mentions(&err, column),
            "{case} (c): the error must name the column `{column}` it refuses; it says: {err}"
        );
    }
    // The view's own columns must stay acceptable (so the refusal above cannot be earned by
    // refusing everything): H1's declaration over `total` is legal.
    assert!(
        refusal(&script(
            "item_totals",
            SEL_TOTALS,
            &["CREATE INDEX i_total ON item_totals (total)"]
        ))
        .is_empty(),
        "H1 (c): a declaration over a real view column must parse"
    );
}

/// A declared index in the engine's own namespace, or over one of its columns, is refused: the
/// `_lv_*` set is the engine's (`unit_index`/`row_index`/`lineage_indexes`/`partial_indexes`,
/// src/target/mod.rs:754-873) and the `_lv_`-invisible columns stay invisible
/// (src/target/mysql.rs:107-109). Part of H6's (c) "dropping or renaming any `_lv_*` index is a
/// test failure".
///
/// RED today: no such rule exists in any parser.
#[test]
fn rejects_index_over_engine_namespace() {
    for (what, decl) in [
        (
            "an index name in the engine's namespace",
            "CREATE INDEX _lv_row ON item_totals (total)",
        ),
        (
            "an index name in the engine's namespace",
            "CREATE INDEX _lv_sneaky ON item_totals (total)",
        ),
        (
            "a column of the engine",
            "CREATE INDEX i_lv ON item_totals (_lv_row)",
        ),
    ] {
        let def = script("item_totals", SEL_TOTALS, &[decl]);
        let err = refusal(&def);
        assert!(
            !err.is_empty(),
            "(c): {what} (`{decl}`) must be refused, it was accepted"
        );
        assert!(
            mentions(&err, "_lv"),
            "(c): the error must name the reserved `{what}` namespace `_lv_*`; it says: {err}"
        );
    }
}

/// E9 (c): one decided, named refusal per (index kind, target product) pair that cannot be built
/// — FULLTEXT is refused by name on a PostgreSQL target and accepted on MySQL/Percona/MariaDB.
/// The table the plan names is `Mysql | Percona | MariaDb | Postgres`; the refusal must be answerable
/// per product, not per family.
///
/// RED today for the reason the plan's table gives: `src/target/mod.rs:23-26` — the target's
/// `Kind` is [`crate::dialect::Family`] (`Mysql | Postgres`) alone, so `mariadb://` answers
/// `Mysql` exactly like `mysql://` and no seam can tell the products (still less Percona) apart to
/// answer "can this flavor build this kind of index?". The behaviour half of E9 is asserted
/// end-to-end by `idx_fulltext_refused_on_pg` / `idx_fulltext_where_supported` in tests/indexes.rs.
#[test]
fn index_kind_supported_per_flavor() {
    use crate::target::Kind;
    let mysql = Kind::from_dsn("mysql://u@h/db").expect("mysql:// is a target");
    let maria =
        Kind::from_dsn("mariadb://u@h/db").expect("mariadb:// is a target (D5: MariaDB IN)");
    assert_ne!(
        mysql, maria,
        "E9 (c): a per-flavor capability gate needs the target to name its product: `mariadb://` \
         and `mysql://` both answer {mysql:?} today (src/target/mod.rs:23-26), so FULLTEXT (which \
         MariaDB and MySQL answer differently, online-ddl-research.md items 1 and 6) cannot be \
         decided per product"
    );
    // D2 + the D5 flag: ClickHouse is last on the roadmap and no target exists for it
    // (src/target/mod.rs:18-19) — no expectation below is written for it, so it must stay refused.
    assert!(
        Kind::from_dsn("clickhouse://u@h/db").is_err(),
        "D2 (c): a clickhouse:// target must not answer as a flavor while no target module exists"
    );
}

/// H8 (a)→(c), D4: a virtual (and stored) generated column declared the same way as an index —
/// an explicit follow-up statement, "appended alter statement with create virtual or stored
/// column" — parses, and VIRTUAL/STORED is recorded (nothing in `src/` renders or parses a
/// generated column today, src/target/mysql.rs:100-111). The verb's exact spelling is Q1b/OPEN;
/// the shape below is D4's own wording, and the column is NOT a projection column of the view.
///
/// RED today: `SINGLE_SELECT` refuses the script outright.
#[test]
fn generated_column_declaration_parses() {
    for (kind, verb) in [("virtual", "VIRTUAL"), ("stored", "STORED")] {
        let decl = format!(
            "ALTER TABLE entity_status ADD COLUMN sig VARCHAR(32) AS (CONCAT(status, '-', grp)) {verb}"
        );
        let def = script(
            "entity_status",
            SEL_STATUS,
            &[&decl, "CREATE INDEX i_sig ON entity_status (sig)"],
        );
        let kept = kept(&def);
        assert!(
            !kept.starts_with("PARSE ERROR"),
            "H8 (a): the generated-column statement `{decl}` is refused: {kept}"
        );
        assert!(
            mentions(&kept, "sig"),
            "H8 (c): the declared column `sig` is not kept: {kept}"
        );
        assert!(
            mentions(&kept, verb),
            "H8 (c): the generated-column kind {kind} is not recorded: {kept}"
        );
        assert!(
            mentions(&kept, "i_sig"),
            "H8 (c): the index over the declared column is not kept: {kept}"
        );
    }
}
