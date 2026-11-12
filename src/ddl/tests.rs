use super::{statement_effects, statement_tables};

fn t(q: &str) -> Vec<(String, String)> {
    statement_tables(q, "d")
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
        assert!(!statement_tables(q, "d").is_empty(), "{q}");
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
    assert_eq!(statement_tables("DROP TABLE a", ""), names(&[("", "a")]));
}
