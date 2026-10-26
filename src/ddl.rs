//! Which tables a non-row binlog statement (DDL) may affect.
//!
//! With `binlog_format=ROW` every data change arrives as row events; QUERY events only carry DDL
//! and administrative statements. [`statement_tables`] tells the engine which tables such a
//! statement may have changed (structure or content) so their catalog entries can be invalidated
//! and dependent views rebuilt. It is deliberately cheap (sqlparser *tokenizer* only, no parsing)
//! and conservative: when a statement form is recognised every table it names is reported, even
//! when the change turns out to be a no-op.

use sqlparser::dialect::MySqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};

/// Tables possibly affected by the non-row statement `query`, executed with default database
/// `default_schema`. Unqualified names resolve to `default_schema`; names keep their spelling.
///
/// * `ALTER TABLE t …` → `t`, plus rename / exchange targets (`RENAME [TO|AS] t2`,
///   `EXCHANGE PARTITION p WITH TABLE t2`, MariaDB `CONVERT TABLE t2 TO PARTITION …`,
///   `CONVERT PARTITION p TO TABLE t2`)
/// * `DROP [TEMPORARY] TABLE [IF EXISTS] a, b` → `a`, `b`
/// * `RENAME TABLE a TO b, c TO d` → `a`, `b`, `c`, `d`
/// * `TRUNCATE [TABLE] t`, `REPAIR TABLE a, b` (may drop corrupt rows) → those tables
/// * `CREATE [OR REPLACE] [TEMPORARY] TABLE t …` (incl. `LIKE` / `AS SELECT`) → `t` only
/// * `CREATE … INDEX i ON t`, `DROP INDEX i ON t` → `t`
/// * `CREATE [OR REPLACE] / ALTER / DROP VIEW v` → `v` (views may be view sources)
/// * `DROP DATABASE x` / `CREATE OR REPLACE DATABASE x` → `("x", "*")` (every table of `x`)
///
/// Empty for DML and for statements that cannot change table data or structure: `CREATE
/// DATABASE`, `ALTER DATABASE`, `OPTIMIZE` / `ANALYZE` / `CHECK TABLE` (physical only), users,
/// grants, routines, triggers, events, `SET`, `FLUSH`, ….
///
/// Comments are ignored, except executable comments (`/*! … */`, `/*!50001 … */`, MariaDB
/// `/*M! … */`) whose content is treated as SQL. A statement the tokenizer rejects yields
/// `(default_schema, "*")` when it starts like DDL (it cannot be analysed, so assume the worst).
pub fn statement_tables(query: &str, default_schema: &str) -> Vec<(String, String)> {
    statement_effects(query, default_schema).tables
}

/// What a non-row statement may affect.
#[derive(Debug, Default, PartialEq)]
pub struct StatementEffects {
    /// The tables it may have changed, as [`statement_tables`].
    pub tables: Vec<(String, String)>,
    /// The base tables it removes: those of `DROP TABLE[S] [IF EXISTS] a, b`. Not `DROP TEMPORARY
    /// TABLE` (it never removes a base table), `DROP INDEX i ON t` or `DROP VIEW`.
    pub dropped: Vec<(String, String)>,
}

/// [`statement_tables`] plus the tables the statement removes (see [`StatementEffects`]).
pub fn statement_effects(query: &str, default_schema: &str) -> StatementEffects {
    let toks = match lex(query) {
        Some(t) => t,
        None => {
            return StatementEffects {
                tables: fallback(query, default_schema),
                dropped: Vec::new(),
            }
        }
    };
    let mut p = Parser {
        t: &toks,
        i: 0,
        schema: default_schema,
        out: Vec::new(),
        dropped: Vec::new(),
    };
    loop {
        let start = p.i;
        let more = p.statement();
        if !more {
            break;
        }
        // Next `;`-separated statement (QUERY events normally hold exactly one).
        p.i = start.max(p.i);
        let mut depth = 0i32;
        while let Some(t) = p.t.get(p.i) {
            match t {
                Tok::LParen => depth += 1,
                Tok::RParen => depth -= 1,
                Tok::Semi if depth <= 0 => break,
                _ => {}
            }
            p.i += 1;
        }
        if p.i >= p.t.len() {
            break;
        }
        p.i += 1; // skip `;`
    }
    let dedup = |v: Vec<(String, String)>| {
        let mut out: Vec<(String, String)> = Vec::with_capacity(v.len());
        for t in v {
            if !out.contains(&t) {
                out.push(t);
            }
        }
        out
    };
    StatementEffects {
        tables: dedup(p.out),
        dropped: dedup(p.dropped),
    }
}

/// Significant tokens: whitespace and comments dropped, executable comments expanded.
#[derive(Debug, Clone, PartialEq)]
enum Tok {
    /// Unquoted word (keyword or identifier).
    Word(String),
    /// Backtick- or double-quoted identifier.
    Quoted(String),
    Dot,
    Comma,
    LParen,
    RParen,
    Semi,
    Eq,
    Other,
}

fn lex(sql: &str) -> Option<Vec<Tok>> {
    let mut out = Vec::new();
    lex_into(sql, &mut out, 0)?;
    Some(out)
}

fn lex_into(sql: &str, out: &mut Vec<Tok>, depth: u8) -> Option<()> {
    let dialect = MySqlDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize().ok()?;
    for t in tokens {
        let tok = match t {
            Token::Whitespace(Whitespace::MultiLineComment(c)) => {
                // MySQL `/*!…*/` hints are expanded by the tokenizer already; MariaDB's
                // `/*M!…*/` (and `!` in case that ever changes) are expanded here.
                let body = c.strip_prefix("M!").or_else(|| c.strip_prefix('!'));
                if let (Some(body), true) = (body, depth < 4) {
                    lex_into(
                        body.trim_start_matches(|ch: char| ch.is_ascii_digit()),
                        out,
                        depth + 1,
                    )?;
                }
                continue;
            }
            Token::Whitespace(_) | Token::EOF => continue,
            Token::Word(w) if w.quote_style.is_some() => Tok::Quoted(w.value),
            Token::Word(w) => Tok::Word(w.value),
            Token::DoubleQuotedString(s) => Tok::Quoted(s), // ANSI_QUOTES identifiers
            Token::Period => Tok::Dot,
            Token::Comma => Tok::Comma,
            Token::LParen => Tok::LParen,
            Token::RParen => Tok::RParen,
            Token::SemiColon => Tok::Semi,
            Token::Eq => Tok::Eq,
            _ => Tok::Other,
        };
        out.push(tok);
    }
    Some(())
}

/// The tokenizer failed (should not happen for SQL the server executed): if it looks like DDL,
/// report the whole default schema.
fn fallback(query: &str, default_schema: &str) -> Vec<(String, String)> {
    let mut rest = query.trim_start();
    // Skip leading plain comments.
    while let Some(r) = rest.strip_prefix("/*") {
        if r.starts_with('!') || r.starts_with("M!") {
            rest = r
                .trim_start_matches(['!', 'M'])
                .trim_start_matches(|c: char| c.is_ascii_digit());
            break;
        }
        rest = r
            .split_once("*/")
            .map(|(_, r)| r)
            .unwrap_or("")
            .trim_start();
    }
    let first: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphabetic())
        .collect();
    let ddl = ["ALTER", "CREATE", "DROP", "RENAME", "TRUNCATE", "REPAIR"];
    if ddl.iter().any(|k| first.eq_ignore_ascii_case(k)) {
        vec![(default_schema.to_string(), "*".to_string())]
    } else {
        Vec::new()
    }
}

struct Parser<'a> {
    t: &'a [Tok],
    i: usize,
    schema: &'a str,
    out: Vec<(String, String)>,
    /// Base tables removed (`DROP TABLE`), also in `out`.
    dropped: Vec<(String, String)>,
}

impl Parser<'_> {
    fn peek_kw(&self, off: usize, kw: &str) -> bool {
        matches!(self.t.get(self.i + off), Some(Tok::Word(w)) if w.eq_ignore_ascii_case(kw))
    }

    fn at_kw(&self, kw: &str) -> bool {
        self.peek_kw(0, kw)
    }

    fn eat_kw(&mut self, kw: &str) -> bool {
        let hit = self.at_kw(kw);
        if hit {
            self.i += 1;
        }
        hit
    }

    fn eat_any(&mut self, kws: &[&str]) -> bool {
        kws.iter().any(|k| self.eat_kw(k))
    }

    /// `IF EXISTS` / `IF NOT EXISTS`.
    fn eat_if_exists(&mut self) {
        if self.at_kw("IF") {
            if self.peek_kw(1, "EXISTS") {
                self.i += 2;
            } else if self.peek_kw(1, "NOT") && self.peek_kw(2, "EXISTS") {
                self.i += 3;
            }
        }
    }

    fn ident(&mut self) -> Option<String> {
        let s = match self.t.get(self.i)? {
            Tok::Word(w) | Tok::Quoted(w) => w.clone(),
            _ => return None,
        };
        self.i += 1;
        Some(s)
    }

    /// `name` or `schema.name`.
    fn name(&mut self) -> Option<(String, String)> {
        let first = self.ident()?;
        if self.t.get(self.i) == Some(&Tok::Dot) {
            self.i += 1;
            let second = self.ident()?;
            return Some((first, second));
        }
        Some((self.schema.to_string(), first))
    }

    fn push_name(&mut self) -> bool {
        match self.name() {
            Some(n) => {
                self.out.push(n);
                true
            }
            None => false,
        }
    }

    /// Comma separated table list (`DROP TABLE a, b`, `REPAIR TABLE a, b`).
    fn push_name_list(&mut self) {
        while self.push_name() {
            if self.t.get(self.i) == Some(&Tok::Comma) {
                self.i += 1;
            } else {
                break;
            }
        }
    }

    /// Skips `ALGORITHM = x`, `DEFINER = user`, `SQL SECURITY x` and similar prefixes of
    /// `CREATE` / `ALTER`; returns once none matches.
    fn skip_object_attributes(&mut self) {
        loop {
            if self.at_kw("ALGORITHM") || self.at_kw("DEFINER") {
                let definer = self.at_kw("DEFINER");
                self.i += 1;
                if self.t.get(self.i) == Some(&Tok::Eq) {
                    self.i += 1;
                }
                if definer {
                    self.skip_user();
                } else {
                    self.i += 1;
                }
            } else if self.at_kw("SQL") && self.peek_kw(1, "SECURITY") {
                self.i += 3;
            } else if self.eat_any(&[
                "TEMPORARY",
                "UNIQUE",
                "FULLTEXT",
                "SPATIAL",
                "ONLINE",
                "OFFLINE",
                "IGNORE",
                "AGGREGATE",
            ]) {
            } else if self.at_kw("OR") && self.peek_kw(1, "REPLACE") {
                self.i += 2;
            } else {
                return;
            }
        }
    }

    /// `user`, `'user'@'host'`, `` `u`@`h` ``, `u@h`, `CURRENT_USER[()]`.
    fn skip_user(&mut self) {
        if self.i < self.t.len() {
            self.i += 1;
        }
        if self.t.get(self.i) == Some(&Tok::LParen) && self.t.get(self.i + 1) == Some(&Tok::RParen)
        {
            self.i += 2;
            return;
        }
        // `@` is an `Other` token; the host part follows it.
        if self.t.get(self.i) == Some(&Tok::Other) {
            self.i += 2;
        }
    }

    /// Parses one statement starting at `self.i`; returns whether later `;`-separated statements
    /// should be looked at (false for routine / trigger / event definitions whose bodies hold
    /// statements that are not executed now).
    fn statement(&mut self) -> bool {
        let Some(Tok::Word(first)) = self.t.get(self.i) else {
            return true;
        };
        let first = first.to_ascii_uppercase();
        self.i += 1;
        match first.as_str() {
            "ALTER" => self.alter(),
            "CREATE" => return self.create(),
            "DROP" => self.drop(),
            "RENAME" => self.rename(),
            "TRUNCATE" => {
                self.eat_kw("TABLE");
                self.push_name();
            }
            "REPAIR" => {
                self.eat_any(&["NO_WRITE_TO_BINLOG", "LOCAL"]);
                if self.eat_any(&["TABLE", "TABLES"]) {
                    self.push_name_list();
                }
            }
            _ => {}
        }
        true
    }

    fn alter(&mut self) {
        self.skip_object_attributes();
        if self.eat_kw("VIEW") {
            self.push_name();
            return;
        }
        if !self.eat_kw("TABLE") {
            return; // DATABASE, USER, EVENT, PROCEDURE, SERVER, TABLESPACE, SEQUENCE, …
        }
        self.eat_if_exists();
        if !self.push_name() {
            return;
        }
        // Scan the alter specifications (outside parentheses) for other tables.
        let mut depth = 0i32;
        while let Some(t) = self.t.get(self.i) {
            match t {
                Tok::LParen => depth += 1,
                Tok::RParen => depth -= 1,
                Tok::Semi if depth <= 0 => return,
                Tok::Word(_) if depth == 0 => {
                    if self.eat_kw("RENAME") {
                        if self.at_kw("COLUMN")
                            || self.at_kw("INDEX")
                            || self.at_kw("KEY")
                            || self.at_kw("CONSTRAINT")
                        {
                            continue;
                        }
                        self.eat_any(&["TO", "AS"]);
                        self.push_name();
                        continue;
                    }
                    if (self.at_kw("WITH") || self.at_kw("CONVERT") || self.at_kw("TO"))
                        && self.peek_kw(1, "TABLE")
                    {
                        self.i += 2;
                        self.push_name();
                        continue;
                    }
                }
                _ => {}
            }
            self.i += 1;
        }
    }

    fn create(&mut self) -> bool {
        let replace = self.at_kw("OR") && self.peek_kw(1, "REPLACE");
        self.skip_object_attributes();
        if self.eat_kw("TABLE") || self.eat_kw("VIEW") {
            self.eat_if_exists();
            self.push_name();
        } else if self.eat_kw("INDEX") {
            self.index_target();
        } else if self.at_kw("DATABASE") || self.at_kw("SCHEMA") {
            self.i += 1;
            if replace {
                self.eat_if_exists();
                if let Some(db) = self.ident() {
                    self.out.push((db, "*".into()));
                }
            }
        } else if self.at_kw("PROCEDURE")
            || self.at_kw("FUNCTION")
            || self.at_kw("TRIGGER")
            || self.at_kw("EVENT")
            || self.at_kw("PACKAGE")
        {
            return false;
        }
        true
    }

    fn drop(&mut self) {
        let temporary = self.eat_kw("TEMPORARY");
        if self.eat_any(&["TABLE", "TABLES"]) {
            self.eat_if_exists();
            let from = self.out.len();
            self.push_name_list();
            if !temporary {
                let names = self.out[from..].to_vec();
                self.dropped.extend(names);
            }
        } else if self.eat_kw("VIEW") {
            self.eat_if_exists();
            self.push_name_list();
        } else if self.eat_any(&["DATABASE", "SCHEMA"]) {
            self.eat_if_exists();
            if let Some(db) = self.ident() {
                self.out.push((db, "*".into()));
            }
        } else if self.eat_kw("INDEX") {
            self.index_target();
        }
    }

    /// `… INDEX [IF [NOT] EXISTS] idx [USING …] ON t …`: the table after the first top-level `ON`.
    fn index_target(&mut self) {
        while let Some(t) = self.t.get(self.i) {
            match t {
                Tok::LParen | Tok::Semi => return,
                Tok::Word(w) if w.eq_ignore_ascii_case("ON") => {
                    self.i += 1;
                    self.push_name();
                    return;
                }
                _ => self.i += 1,
            }
        }
    }

    /// `RENAME TABLE[S] [IF EXISTS] a [WAIT n | NOWAIT] TO b [, c TO d] …`
    fn rename(&mut self) {
        if !self.eat_any(&["TABLE", "TABLES"]) {
            return; // RENAME USER
        }
        self.eat_if_exists();
        loop {
            if !self.push_name() {
                return;
            }
            // old name … TO new name
            while let Some(t) = self.t.get(self.i) {
                match t {
                    Tok::Word(w) if w.eq_ignore_ascii_case("TO") => break,
                    Tok::Comma | Tok::Semi => return,
                    _ => self.i += 1,
                }
            }
            if !self.eat_kw("TO") || !self.push_name() {
                return;
            }
            match self.t.get(self.i) {
                Some(Tok::Comma) => self.i += 1,
                _ => return,
            }
        }
    }
}

#[cfg(test)]
mod tests {
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
}
