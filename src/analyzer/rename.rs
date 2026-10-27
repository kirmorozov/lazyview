//! The monkey-wrench renaming: one consistent, seeded renaming of every schema, table, column,
//! view, alias, CTE, output column, index and constraint name in SQL text, and the way back for
//! texts that carry such names (result sets, messages, plans).
//!
//! Names are found with sqlparser's AST visitor (never by string replacement): a statement is
//! parsed, every identifier is visited with its source span, and only the identifier tokens are
//! rewritten in place, so everything else (literals, comments, spacing) stays as written. Names
//! are registered where they are defined (`CREATE TABLE` columns, aliases, CTEs, ...) and renamed
//! wherever they occur; function names, data types, character sets, collations and table options
//! are never names. lazyview's reserved names (`_lv_*`, `__lv_*`, `_lazyview*`, the `__lv_new` /
//! `__lv_old` suffixes) and the system schemas are never renamed.
//!
//! Besides opaque random names the renaming hands out names that look meaningful or need care: a
//! non-key column named `id`, a key column named `zz9`, a plain column named `x_id` (or `*_id`), a
//! table without foreign keys named `*_link`, reserved words and names with spaces, dots, dashes
//! or backticks (quoted everywhere), mixed-case names and, for columns, a different letter case
//! at each occurrence (columns are case-insensitive).
//!
//! Test-only. Shared by the analyzer's unit tests (`tests_rename.rs`) and the integration
//! harness (`tests/common/mod.rs`), both through `#[path]`: it depends on sqlparser and std only.

#![cfg(test)]
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    AlterTableOperation, ColumnOption, CreateTableOptions, Expr, Ident, Insert, ObjectName,
    ObjectNamePart, RenameTableNameKind, Select, SelectItem, SetExpr, Statement, TableConstraint,
    TableFactor, TableObject, Value, ValueWithSpan, Visit, Visitor,
};
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Location, Span, Token, Tokenizer};

/// What a name is, where it is defined: decides which adversarial names it may get.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Schema,
    /// `plain`: the table has no foreign keys.
    Table {
        plain: bool,
    },
    /// Part of the primary key or a unique key.
    KeyColumn,
    /// `fk`: part of a foreign key.
    Column {
        fk: bool,
    },
    View,
    /// Table alias, CTE or named window.
    Alias,
    /// Output column alias, derived-table / CTE / view column list.
    OutputAlias,
    /// Index or constraint name.
    Index,
}

/// Reserved words (in MySQL and MariaDB) handed out as names: they must be quoted everywhere.
const RESERVED_WORDS: &[&str] = &["trigger", "zerofill", "accessible", "enclosed", "undo"];

/// Identifiers the parser keeps as plain identifiers that are not names.
const PSEUDO_IDENTS: &[&str] = &[
    "DEFAULT",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATE",
    "CURRENT_TIME",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "CURRENT_USER",
    "UTC_TIMESTAMP",
    "UTC_DATE",
    "UTC_TIME",
];

/// Columns of lazyview's metadata tables (never renamed).
const METADATA_COLUMNS: &[&str] = &[
    "id",
    "name",
    "definition",
    "enabled",
    "targets",
    "target",
    "rebuild",
    "state",
    "mode",
    "detail",
    "last_error",
    "definition_id",
    "definition_hash",
    "managed",
    "refreshed_at",
    "created_at",
    "updated_at",
];

const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "mysql", "performance_schema", "sys"];

/// Suffixes lazyview appends to view names for its own tables.
const RESERVED_SUFFIXES: &[&str] = &["__lv_new", "__lv_old"];

/// Never renamed: lazyview's own names and the system schemas.
pub fn is_reserved(name: &str) -> bool {
    let l = name.to_ascii_lowercase();
    l.starts_with("_lv_")
        || l.starts_with("__lv_")
        || l.starts_with("_lazyview")
        || RESERVED_SUFFIXES.iter().any(|s| l.ends_with(s))
        || SYSTEM_SCHEMAS.contains(&l.as_str())
}

fn is_keyword(s: &str) -> bool {
    let u = s.to_ascii_uppercase();
    sqlparser::keywords::ALL_KEYWORDS.iter().any(|k| *k == u)
}

/// Whether `s` must be backtick-quoted to be read as an identifier.
pub fn needs_quote(s: &str) -> bool {
    s.is_empty()
        || !s
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        || s.chars().next().is_some_and(|c| c.is_ascii_digit())
        || is_keyword(s)
        || RESERVED_WORDS.contains(&s.to_ascii_lowercase().as_str())
}

pub fn quote(s: &str) -> String {
    format!("`{}`", s.replace('`', "``"))
}

/// `s` as an identifier: bare where it can be, else quoted.
pub fn ident_sql(s: &str) -> String {
    if needs_quote(s) {
        quote(s)
    } else {
        s.to_string()
    }
}

/// A single-quoted SQL string literal (backslash escapes, the server default).
pub fn literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('\'');
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\'' => out.push_str("\\'"),
            '\0' => out.push_str("\\0"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\x1a' => out.push_str("\\Z"),
            c => out.push(c),
        }
    }
    out.push('\'');
    out
}

fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn hash(seed: u64, s: &str, k: usize) -> u64 {
    let mut h = seed ^ (k as u64).wrapping_mul(0x2545_f491_4f6c_dd1d);
    for b in s.bytes() {
        h = splitmix(&mut h) ^ b as u64;
    }
    splitmix(&mut h)
}

/// The result of renaming a statement.
#[derive(Debug, Clone)]
pub struct Renamed {
    pub sql: String,
    /// The statement reads lazyview's metadata tables or a system schema: its results carry
    /// names (to be mapped back with [`Names::unrename_text`]).
    pub metadata: bool,
}

/// One consistent renaming (original name ↔ new name, case-insensitive on both sides).
#[derive(Debug, Clone)]
pub struct Names {
    pub seed: u64,
    state: u64,
    /// Lower-case original → new name.
    fwd: HashMap<String, String>,
    /// Lower-case new name → original as first seen.
    back: HashMap<String, String>,
    /// (original, new, role) in assignment order.
    order: Vec<(String, String, Role)>,
    used: HashSet<String>,
    /// Spellings handed out for names written in another letter case than first seen (exact new
    /// spelling → the original as written there).
    variants: HashMap<String, String>,
    /// Columns are written in a different letter case at each occurrence.
    pub vary_case: bool,
    /// With `vary_case`: alternately lower and upper case (successive occurrences always differ)
    /// instead of a random case per occurrence.
    pub alternate_case: bool,
    /// Tables may get names with spaces or dashes. (MariaDB stores the name of a CTE unquoted in
    /// a view's definition: a CTE named like such a table makes the view unreadable, ERROR 1064.)
    pub special_tables: bool,
}

impl Names {
    pub fn new(seed: u64) -> Names {
        Names {
            seed,
            state: seed ^ 0x6d6f_6e6b_6579,
            fwd: HashMap::new(),
            back: HashMap::new(),
            order: Vec::new(),
            used: HashSet::new(),
            variants: HashMap::new(),
            vary_case: seed % 2 == 1,
            alternate_case: false,
            special_tables: true,
        }
    }

    fn next(&mut self) -> u64 {
        splitmix(&mut self.state)
    }

    /// The new name of `orig` (names derived from a view name with a reserved suffix map with
    /// their view), if it has one.
    pub fn get(&self, orig: &str) -> Option<String> {
        if let Some(n) = self.fwd.get(&orig.to_ascii_lowercase()) {
            return Some(n.clone());
        }
        let l = orig.to_ascii_lowercase();
        RESERVED_SUFFIXES.iter().find_map(|suf| {
            let base = l.strip_suffix(suf)?;
            let n = self.fwd.get(base)?;
            Some(format!("{n}{}", &orig[orig.len() - suf.len()..]))
        })
    }

    /// The original of a new name, if it is one.
    pub fn original(&self, new: &str) -> Option<String> {
        if let Some(o) = self.variants.get(new) {
            return Some(o.clone());
        }
        let l = new.to_ascii_lowercase();
        if let Some(o) = self.back.get(&l) {
            return Some(o.clone());
        }
        RESERVED_SUFFIXES.iter().find_map(|suf| {
            let base = l.strip_suffix(suf)?;
            let o = self.back.get(base)?;
            Some(format!("{o}{}", &new[new.len() - suf.len()..]))
        })
    }

    /// Every (original, new, role), in assignment order.
    pub fn pairs(&self) -> &[(String, String, Role)] {
        &self.order
    }

    /// Whether `name` (case-insensitive) is an original name.
    pub fn is_original(&self, name: &str) -> bool {
        self.fwd.contains_key(&name.to_ascii_lowercase())
    }

    /// The renaming, one `original -> new (role)` per line, for failure messages.
    pub fn describe(&self) -> String {
        let mut s = format!(
            "renaming seed {} (case varied per occurrence: {}):\n",
            self.seed, self.vary_case
        );
        for (o, n, r) in &self.order {
            s.push_str(&format!("  {} -> {} ({r:?})\n", quote(o), quote(n)));
        }
        s
    }

    /// Gives `orig` a new name (the first time it is seen), chosen by `role`.
    pub fn assign(&mut self, orig: &str, role: Role) -> String {
        if is_reserved(orig) {
            return orig.to_string();
        }
        if let Some(n) = self.get(orig) {
            return n;
        }
        let mut new = self.pick(role);
        while !self.free(&new) {
            new = self.opaque(role);
        }
        self.fwd.insert(orig.to_ascii_lowercase(), new.clone());
        self.back.insert(new.to_ascii_lowercase(), orig.to_string());
        self.order.push((orig.to_string(), new.clone(), role));
        new
    }

    /// Binds `orig` to a given new name (a schema created under it); false if it is taken.
    pub fn bind(&mut self, orig: &str, new: &str, role: Role) -> bool {
        if self.get(orig).is_some() || !self.free(new) {
            return false;
        }
        self.fwd.insert(orig.to_ascii_lowercase(), new.to_string());
        self.back.insert(new.to_ascii_lowercase(), orig.to_string());
        self.order.push((orig.to_string(), new.to_string(), role));
        true
    }

    /// A fresh opaque name (lower case for schemas), not bound to anything.
    pub fn fresh(&mut self, role: Role) -> String {
        self.opaque(role)
    }

    fn free(&self, new: &str) -> bool {
        let l = new.to_ascii_lowercase();
        !is_reserved(new)
            && !self.back.contains_key(&l)
            && !self.back.keys().any(|b| l.starts_with(&format!("{b}_")))
            && !self.back.keys().any(|b| b.starts_with(&format!("{l}_")))
    }

    /// A special name once per renaming (with some probability at each eligible name).
    fn special(&mut self, name: &str, one_in: u64) -> Option<String> {
        if self.used.contains(name) || !self.next().is_multiple_of(one_in) {
            return None;
        }
        self.used.insert(name.to_string());
        Some(name.to_string())
    }

    fn pick(&mut self, role: Role) -> String {
        match role {
            Role::KeyColumn => {
                if let Some(s) = self.special("zz9", 2) {
                    return s;
                }
            }
            Role::Column { fk: false } => {
                for (name, one_in) in [("id", 2), ("x_id", 2), ("trigger", 3), ("zerofill", 4)] {
                    if let Some(s) = self.special(name, one_in) {
                        return s;
                    }
                }
                let base = self.opaque(role);
                let r = self.next() % 12;
                return match r {
                    0 if !self.used.contains(" ") => {
                        self.used.insert(" ".into());
                        format!("{base} {}", self.opaque(role))
                    }
                    1 if !self.used.contains(".") => {
                        self.used.insert(".".into());
                        format!("{base}.{}", self.opaque(role))
                    }
                    2 if !self.used.contains("`") => {
                        self.used.insert("`".into());
                        format!("{base}`q")
                    }
                    3..=5 => format!("{base}_id"),
                    _ => base,
                };
            }
            Role::Table { plain } => {
                if let Some(s) = self.special("accessible", 4) {
                    return s;
                }
                let base = self.opaque(role);
                return match self.next() % 6 {
                    0 | 1 if !self.special_tables => base,
                    0 if !self.used.contains("t ") => {
                        self.used.insert("t ".into());
                        format!("{base} {}", self.opaque(role))
                    }
                    1 if !self.used.contains("t-") => {
                        self.used.insert("t-".into());
                        format!("{base}-x")
                    }
                    _ if plain && self.next().is_multiple_of(2) => format!("{base}_link"),
                    _ => base,
                };
            }
            Role::View => {
                if !self.used.contains("v ") && self.next().is_multiple_of(3) {
                    self.used.insert("v ".into());
                    let (a, b) = (self.opaque(role), self.opaque(role));
                    return format!("{a} {b}");
                }
            }
            Role::OutputAlias => {
                if let Some(s) = self.special("enclosed", 4) {
                    return s;
                }
            }
            _ => {}
        }
        self.opaque(role)
    }

    /// A random name with a digit (never a word or a keyword), sometimes in mixed case.
    fn opaque(&mut self, role: Role) -> String {
        const LETTERS: &[u8] = b"abcdefghijklmnopqrstuvwxyz";
        const ALNUM: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        loop {
            let len = 4 + (self.next() % 5) as usize;
            let mut s = String::new();
            s.push(LETTERS[(self.next() % 26) as usize] as char);
            for _ in 1..len {
                s.push(ALNUM[(self.next() % 36) as usize] as char);
            }
            if !s.chars().any(|c| c.is_ascii_digit()) {
                let at = 1 + (self.next() as usize % (len - 1));
                s.replace_range(at..at + 1, &((self.next() % 10).to_string()));
            }
            // Schemas are directories: keep them lower case.
            if role != Role::Schema && self.next().is_multiple_of(3) {
                s = s
                    .chars()
                    .enumerate()
                    .map(|(i, c)| {
                        if i % 2 == 0 {
                            c.to_ascii_uppercase()
                        } else {
                            c
                        }
                    })
                    .collect();
            }
            if !is_keyword(&s) && self.free(&s) {
                return s;
            }
        }
    }

    /// `new` in the letter case the original occurrence `orig` is written in, relative to the
    /// original's first spelling: the same spelling gives the new name as handed out, another
    /// one (`lz_case` after `Lz_Case`) a spelling of the new name that differs too (table and
    /// view names are case-sensitive on the server).
    fn case_like(&mut self, new: &str, orig: &str) -> String {
        let first = self.back.get(&new.to_ascii_lowercase());
        if first.is_none_or(|f| f == orig) {
            return new.to_string();
        }
        let swap = |s: &str| -> String {
            s.chars()
                .map(|c| {
                    if c.is_ascii_uppercase() {
                        c.to_ascii_lowercase()
                    } else {
                        c.to_ascii_uppercase()
                    }
                })
                .collect()
        };
        let lower = orig.chars().all(|c| !c.is_ascii_uppercase());
        let upper = orig.chars().all(|c| !c.is_ascii_lowercase());
        let candidates = if lower {
            [
                new.to_ascii_lowercase(),
                new.to_ascii_uppercase(),
                swap(new),
            ]
        } else if upper {
            [
                new.to_ascii_uppercase(),
                new.to_ascii_lowercase(),
                swap(new),
            ]
        } else {
            [
                swap(new),
                new.to_ascii_uppercase(),
                new.to_ascii_lowercase(),
            ]
        };
        let v = candidates
            .into_iter()
            .find(|c| c != new)
            .unwrap_or_else(|| new.to_string());
        self.variants.insert(v.clone(), orig.to_string());
        v
    }

    /// How occurrence `k` of `new` is written where the statement wrote `orig`: columns (case-
    /// insensitive) in a letter case of their own at each occurrence when `vary_case` is set,
    /// everything else in the case [`Self::case_like`] gives.
    fn spell(&mut self, new: &str, orig: &str, quoted: bool, column: bool, k: usize) -> String {
        let mut s = self.case_like(new, orig);
        if column && self.vary_case && s.chars().any(|c| c.is_ascii_alphabetic()) {
            let pick = if self.alternate_case {
                1 + (k as u64 % 2)
            } else {
                hash(self.seed, &new.to_ascii_lowercase(), k) % 4
            };
            s = match pick {
                0 => new.to_string(),
                1 => new.to_ascii_lowercase(),
                2 => new.to_ascii_uppercase(),
                _ => new
                    .chars()
                    .map(|c| {
                        if c.is_ascii_uppercase() {
                            c.to_ascii_lowercase()
                        } else {
                            c.to_ascii_uppercase()
                        }
                    })
                    .collect(),
            };
        }
        if quoted || needs_quote(&s) {
            quote(&s)
        } else {
            s
        }
    }

    /// Renames every name in `sql` (one or more statements).
    pub fn rename_sql(&mut self, sql: &str) -> Result<Renamed, String> {
        let stmts = match Parser::new(&MySqlDialect {})
            .try_with_sql(sql)
            .and_then(|mut p| p.parse_statements())
        {
            Ok(s) => s,
            Err(_) if check_option_tail(sql).is_some() => {
                // `CREATE VIEW … WITH [CASCADED | LOCAL] CHECK OPTION`: the parser does not read the
                // clause, which names nothing.
                let at = check_option_tail(sql).unwrap();
                let mut r = self.rename_sql(&sql[..at])?;
                r.sql.push_str(&sql[at..]);
                return Ok(r);
            }
            Err(e) => {
                // Statements the parser does not know (XA, SHOW variants, ...) pass unchanged when
                // they name nothing that was renamed.
                return match self.lexical_names(sql).first() {
                    None => Ok(Renamed {
                        sql: sql.to_string(),
                        metadata: false,
                    }),
                    Some(n) => Err(format!(
                        "cannot rename (sqlparser: {e}): the statement names `{n}`: {sql}"
                    )),
                };
            }
        };
        let mut scan = Scan::default();
        for s in &stmts {
            let _ = s.visit(&mut scan);
        }
        // Names defined here first (with their roles), then the names used before the statement
        // that defines them (a view over a view defined later).
        let used: &[(String, Role)] = if scan.on_tables && !scan.metadata {
            &scan.referenced
        } else {
            &[]
        };
        for (name, role) in scan.defining.iter().chain(used) {
            self.assign(name, *role);
        }
        let lines = LineIndex::new(sql);
        let mut edits: Vec<(usize, usize, String)> = Vec::new();
        let mut occurrences: HashMap<String, usize> = HashMap::new();
        let mut idents: Vec<&Ident> = scan.idents.iter().collect();
        idents.sort_by_key(|i| (i.span.start.line, i.span.start.column));
        let mut seen = HashSet::new();
        for i in idents {
            let k = key(&i.span);
            if (scan.excluded.contains(&k) && !scan.forced.contains(&k)) || !seen.insert(k) {
                continue;
            }
            if scan.metadata && !scan.relation.contains(&k) {
                continue;
            }
            let Some(new) = self.get(&i.value) else {
                continue;
            };
            if i.span.start.line == 0 {
                return Err(format!("no source span for `{}` in: {sql}", i.value));
            }
            let (a, b) = lines.range(&i.span);
            check_ident(&sql[a..b], &i.value)
                .map_err(|e| format!("{e} (span {:?}) in: {sql}", i.span))?;
            let n = occurrences.entry(new.to_ascii_lowercase()).or_default();
            *n += 1;
            let column = scan.columnish.contains(&k);
            edits.push((
                a,
                b,
                self.spell(&new, &i.value, i.quote_style.is_some(), column, *n),
            ));
        }
        if scan.metadata {
            for (val, span) in &scan.literals {
                let k = key(span);
                let new = if scan.sql_literals.contains(&k) {
                    Some(self.rename_sql(val)?.sql)
                } else if scan.name_literals.contains(&k) {
                    let new = self.assign(val, Role::View);
                    Some(self.case_like(&new, val))
                } else {
                    self.get(val).map(|new| self.case_like(&new, val))
                };
                if let Some(new) = new {
                    let (a, b) = lines.range(span);
                    edits.push((a, b, literal(&new)));
                }
            }
        }
        edits.sort_by_key(|e| e.0);
        let mut out = String::with_capacity(sql.len() + 16);
        let mut at = 0;
        for (a, b, text) in edits {
            if a < at {
                return Err(format!("overlapping renames at byte {a} in: {sql}"));
            }
            out.push_str(&sql[at..a]);
            out.push_str(&text);
            at = b;
        }
        out.push_str(&sql[at..]);
        Ok(Renamed {
            sql: out,
            metadata: scan.metadata,
        })
    }

    /// Original names that occur as words of `sql` (outside literals and comments).
    fn lexical_names(&self, sql: &str) -> Vec<String> {
        let Ok(tokens) = Tokenizer::new(&MySqlDialect {}, sql).tokenize() else {
            return Vec::new();
        };
        tokens
            .into_iter()
            .filter_map(|t| match t {
                Token::Word(w) if self.fwd.contains_key(&w.value.to_ascii_lowercase()) => {
                    Some(w.value)
                }
                _ => None,
            })
            .collect()
    }

    /// Maps the new names in a text (SQL, a server or lazyview message, a plan dump) back to
    /// their originals. Quoted names always map; bare words map unless the new name is a
    /// reserved word (`trigger` in a message is the word).
    pub fn unrename_text(&self, text: &str) -> String {
        map_words(text, &mut |w, q| self.unrename_word(w, q))
    }

    fn unrename_word(&self, w: &str, q: Quote) -> Option<String> {
        let o = self.original_name(w)?;
        if q == Quote::None && RESERVED_WORDS.contains(&w.to_ascii_lowercase().as_str()) {
            return None;
        }
        Some(match q {
            Quote::None => ident_sql(&o),
            Quote::Backtick => quote(&o),
            Quote::Single => format!("'{}'", o.replace('\'', "''")),
            Quote::Double => format!("\"{}\"", o.replace('"', "\"\"")),
        })
    }

    /// [`Self::unrename_text`] for messages, which quote names as `` `name` `` without doubling a
    /// backtick inside the name.
    pub fn unrename_message(&self, text: &str) -> String {
        let mut t = text.to_string();
        for (_, new, _) in &self.order {
            if new.contains('`') {
                t = t.replace(&format!("`{new}`"), &quote(new));
            }
        }
        // A message about lazyview's metadata names its columns (`WHERE id = 2`): they are not
        // renamed, even where a new name is spelled alike.
        if t.contains("_lazyview") {
            return map_words(&t, &mut |w, q| {
                if METADATA_COLUMNS.contains(&w.to_ascii_lowercase().as_str()) {
                    return None;
                }
                self.unrename_word(w, q)
            });
        }
        self.unrename_text(&t)
    }

    /// [`Self::unrename_message`] for texts that print names as they are, unquoted (log
    /// fields): new names that need quotes are quoted first, so they map back as one name.
    pub fn unrename_raw(&self, text: &str) -> String {
        let mut specials: Vec<&str> = self
            .order
            .iter()
            .map(|(_, n, _)| n.as_str())
            .filter(|n| needs_quote(n))
            .collect();
        specials.sort_by_key(|n| std::cmp::Reverse(n.len()));
        let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
        let mut out = String::with_capacity(text.len());
        let mut i = 0;
        while i < text.len() {
            let rest = &text[i..];
            let before = text[..i].chars().next_back();
            let hit = specials.iter().find(|n| {
                rest.starts_with(**n)
                    && !before.is_some_and(|c| word(c) || c == '`')
                    && !rest[n.len()..].starts_with(|c: char| word(c) || c == '`')
            });
            match hit {
                Some(n) => {
                    out.push_str(&quote(n));
                    i += n.len();
                }
                None => {
                    let c = rest.chars().next().unwrap();
                    out.push(c);
                    i += c.len_utf8();
                }
            }
        }
        self.unrename_message(&out)
    }

    /// Every original name of a text in one spelling (lower case, quoted only where it has to
    /// be): texts of a plain and of a renamed run (mapped back) compare equal when they differ
    /// only in the letter case or the quoting of names.
    pub fn canonical(&self, text: &str) -> String {
        map_words(text, &mut |w, q| {
            let l = w.to_ascii_lowercase();
            // Names derived from a name by a suffix (`<alias>__m1`) are names too.
            let derived = || {
                l.char_indices().any(|(i, c)| {
                    c == '_' && i > 0 && !l[..i].ends_with('_') && self.fwd.contains_key(&l[..i])
                })
            };
            (matches!(q, Quote::None | Quote::Backtick) && (self.fwd.contains_key(&l) || derived()))
                .then(|| ident_sql(&l))
        })
    }

    /// The original of a new name or of a name derived from new names (`<alias>_2`,
    /// `<table>_keys`, `<alias>__<table>`), if it is one.
    pub fn original_name(&self, w: &str) -> Option<String> {
        self.original(w).or_else(|| self.original_prefix(w))
    }

    /// A name derived from new names by a suffix (`<alias>__m1`, `<table>_keys`) or joined by
    /// `__` (`<alias>__<table>`).
    fn original_prefix(&self, w: &str) -> Option<String> {
        if w.contains("__") {
            let parts: Vec<&str> = w.split("__").collect();
            let mapped: Vec<Option<String>> = parts
                .iter()
                .map(|p| self.original(p).or_else(|| self.original_suffixed(p)))
                .collect();
            if mapped.iter().any(Option::is_some) {
                return Some(
                    parts
                        .iter()
                        .zip(mapped)
                        .map(|(p, m)| m.unwrap_or_else(|| p.to_string()))
                        .collect::<Vec<_>>()
                        .join("__"),
                );
            }
        }
        self.original_suffixed(w)
    }

    fn original_suffixed(&self, w: &str) -> Option<String> {
        let l = w.to_ascii_lowercase();
        let (n, o) = self
            .back
            .iter()
            .filter(|(n, _)| n.len() >= 3 && l.len() > n.len() && l.starts_with(n.as_str()))
            .filter(|(n, _)| l.as_bytes()[n.len()] == b'_')
            .max_by_key(|(n, _)| n.len())?;
        Some(format!("{o}{}", &w[n.len()..]))
    }
}

/// Where a trailing `WITH [CASCADED | LOCAL] CHECK OPTION` starts, if `sql` ends with one.
fn check_option_tail(sql: &str) -> Option<usize> {
    let tokens = Tokenizer::new(&MySqlDialect {}, sql)
        .tokenize_with_location()
        .ok()?;
    let words: Vec<(String, Location)> = tokens
        .iter()
        .filter_map(|t| match &t.token {
            Token::Word(w) if w.quote_style.is_none() => {
                Some((w.value.to_ascii_uppercase(), t.span.start))
            }
            Token::Whitespace(_) | Token::SemiColon => None,
            _ => Some((String::new(), t.span.start)),
        })
        .collect();
    let n = words.len();
    let tail: Vec<&str> = words[n.saturating_sub(4)..]
        .iter()
        .map(|(w, _)| w.as_str())
        .collect();
    let with = match tail.as_slice() {
        [.., "WITH", "CHECK", "OPTION"] => n - 3,
        [.., "WITH", "CASCADED" | "LOCAL", "CHECK", "OPTION"] => n - 4,
        _ => return None,
    };
    Some(LineIndex::new(sql).offset(&words[with].1))
}

/// How a word was quoted in a text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Quote {
    None,
    Backtick,
    Single,
    Double,
}

/// Rewrites the words of `text`: bare words, backtick-quoted identifiers and quoted strings are
/// offered to `f` (unquoted, unescaped value); `None` keeps them, except that quoted text that is
/// not one word is mapped word by word inside its quotes. Apostrophes inside words are text.
pub fn map_words(text: &str, f: &mut dyn FnMut(&str, Quote) -> Option<String>) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let word = |c: char| c.is_alphanumeric() || c == '_' || c == '$';
    while i < chars.len() {
        let c = chars[i];
        let q = match c {
            '`' => Some(Quote::Backtick),
            '\'' if i == 0 || !word(chars[i - 1]) => Some(Quote::Single),
            '"' => Some(Quote::Double),
            _ => None,
        };
        if let Some(q) = q {
            // Find the closing quote (doubled quotes and, in strings, backslashes escape).
            let mut j = i + 1;
            let mut value = String::new();
            let mut closed = false;
            while j < chars.len() {
                if chars[j] == '\\' && q != Quote::Backtick && j + 1 < chars.len() {
                    value.push(chars[j]);
                    value.push(chars[j + 1]);
                    j += 2;
                    continue;
                }
                if chars[j] == c {
                    if chars.get(j + 1) == Some(&c) {
                        value.push(c);
                        j += 2;
                        continue;
                    }
                    closed = true;
                    break;
                }
                value.push(chars[j]);
                j += 1;
            }
            if !closed {
                out.push(c);
                i += 1;
                continue;
            }
            match f(&value, q) {
                Some(r) => out.push_str(&r),
                None if value.chars().any(word) => {
                    let inner = map_words(&value, f);
                    let esc = c.to_string().repeat(2);
                    out.push(c);
                    out.push_str(&if inner == value {
                        chars[i + 1..j].iter().collect::<String>()
                    } else {
                        inner.replace(c, &esc)
                    });
                    out.push(c);
                }
                None => out.extend(&chars[i..=j]),
            }
            i = j + 1;
            continue;
        }
        if word(c) {
            let mut j = i;
            while j < chars.len() && word(chars[j]) {
                j += 1;
            }
            let w: String = chars[i..j].iter().collect();
            match f(&w, Quote::None) {
                Some(r) => out.push_str(&r),
                None => out.push_str(&w),
            }
            i = j;
            continue;
        }
        out.push(c);
        i += 1;
    }
    out
}

type SpanKey = (u64, u64, u64, u64);

fn key(s: &Span) -> SpanKey {
    (s.start.line, s.start.column, s.end.line, s.end.column)
}

/// Byte offsets of sqlparser locations (1-based lines and character columns).
struct LineIndex<'a> {
    text: &'a str,
    starts: Vec<usize>,
}

impl<'a> LineIndex<'a> {
    fn new(text: &'a str) -> Self {
        let mut starts = vec![0];
        starts.extend(text.match_indices('\n').map(|(i, _)| i + 1));
        LineIndex { text, starts }
    }

    fn offset(&self, l: &Location) -> usize {
        let line = self.starts[(l.line as usize - 1).min(self.starts.len() - 1)];
        self.text[line..]
            .char_indices()
            .nth(l.column as usize - 1)
            .map(|(i, _)| line + i)
            .unwrap_or(self.text.len())
    }

    fn range(&self, s: &Span) -> (usize, usize) {
        (self.offset(&s.start), self.offset(&s.end))
    }
}

/// The source text at an identifier's span must spell it.
fn check_ident(src: &str, value: &str) -> Result<(), String> {
    let inner = match src.chars().next() {
        Some(q @ ('`' | '"')) if src.len() >= 2 && src.ends_with(q) => {
            src[1..src.len() - 1].replace(&q.to_string().repeat(2), &q.to_string())
        }
        _ => src.to_string(),
    };
    if inner.eq_ignore_ascii_case(value) {
        Ok(())
    } else {
        Err(format!("the source text `{src}` does not spell `{value}`"))
    }
}

fn spans_in<T: Visit>(node: &T) -> Vec<SpanKey> {
    struct Idents(Vec<SpanKey>);
    impl Visitor for Idents {
        type Break = ();
        fn pre_visit_ident(&mut self, i: &Ident) -> ControlFlow<()> {
            self.0.push(key(&i.span));
            ControlFlow::Continue(())
        }
    }
    let mut v = Idents(Vec::new());
    let _ = node.visit(&mut v);
    v.0
}

fn last(n: &ObjectName) -> Option<&Ident> {
    n.0.last().and_then(ObjectNamePart::as_ident)
}

fn plain_column(e: &Expr) -> Option<&Ident> {
    match e {
        Expr::Identifier(i) => Some(i),
        Expr::CompoundIdentifier(p) => p.last(),
        _ => None,
    }
}

/// What one pass over a statement's AST finds.
#[derive(Default)]
struct Scan {
    idents: Vec<Ident>,
    /// Identifiers that are not names (functions, types, character sets, options, variables).
    excluded: HashSet<SpanKey>,
    /// Column references (case-insensitive: written in varying case).
    columnish: HashSet<SpanKey>,
    /// Parts of table references.
    relation: HashSet<SpanKey>,
    /// Renamed although they look excluded (an index column with a prefix length, `c(10)`,
    /// parses as a function call).
    forced: HashSet<SpanKey>,
    /// Names defined by the statement, with their roles, in order.
    defining: Vec<(String, Role)>,
    /// Names the statement uses as tables or columns (registered when first seen, here).
    referenced: Vec<(String, Role)>,
    /// A statement on tables (queries, DML, DDL): the identifiers it uses are names.
    on_tables: bool,
    /// Reads lazyview's metadata or a system schema: only table names and the string literals
    /// that hold names are renamed.
    metadata: bool,
    literals: Vec<(String, Span)>,
    /// `_lazyview` values: a view name (defined here), a definition (renamed as SQL).
    name_literals: HashSet<SpanKey>,
    sql_literals: HashSet<SpanKey>,
}

impl Scan {
    fn exclude<T: Visit>(&mut self, node: &T) {
        self.excluded.extend(spans_in(node));
    }

    fn define(&mut self, i: Option<&Ident>, role: Role) {
        if let Some(i) = i {
            self.defining.push((i.value.clone(), role));
        }
    }

    fn index_columns(&mut self, cols: &[sqlparser::ast::IndexColumn]) {
        for c in cols {
            if let Expr::Function(f) = &c.column.expr {
                if let [ObjectNamePart::Identifier(i)] = f.name.0.as_slice() {
                    self.forced.insert(key(&i.span));
                    self.columnish.insert(key(&i.span));
                }
            }
        }
    }

    fn constraint(&mut self, c: &TableConstraint) {
        match c {
            TableConstraint::PrimaryKey(k) => self.index_columns(&k.columns),
            TableConstraint::Unique(k) => self.index_columns(&k.columns),
            TableConstraint::Index(k) => self.index_columns(&k.columns),
            TableConstraint::FulltextOrSpatial(k) => self.index_columns(&k.columns),
            _ => {}
        }
        match c {
            TableConstraint::PrimaryKey(k) => {
                self.define(k.name.as_ref(), Role::Index);
                self.define(k.index_name.as_ref(), Role::Index);
                self.exclude(&k.index_type);
                self.exclude(&k.index_options);
            }
            TableConstraint::Unique(k) => {
                self.define(k.name.as_ref(), Role::Index);
                self.define(k.index_name.as_ref(), Role::Index);
                self.exclude(&k.index_type);
                self.exclude(&k.index_options);
            }
            TableConstraint::ForeignKey(k) => {
                self.define(k.name.as_ref(), Role::Index);
                self.define(k.index_name.as_ref(), Role::Index);
            }
            TableConstraint::Check(k) => self.define(k.name.as_ref(), Role::Index),
            TableConstraint::Index(k) => {
                self.define(k.name.as_ref(), Role::Index);
                self.exclude(&k.index_type);
                self.exclude(&k.index_options);
            }
            TableConstraint::FulltextOrSpatial(k) => {
                self.define(k.opt_index_name.as_ref(), Role::Index)
            }
            _ => {}
        }
    }

    fn column_options(&mut self, opts: &[ColumnOption]) {
        for o in opts {
            match o {
                ColumnOption::CharacterSet(n) | ColumnOption::Collation(n) => self.exclude(n),
                ColumnOption::Options(o) => self.exclude(o),
                ColumnOption::PrimaryKey(k) => self.define(k.name.as_ref(), Role::Index),
                ColumnOption::Unique(k) => self.define(k.name.as_ref(), Role::Index),
                ColumnOption::ForeignKey(k) => self.define(k.name.as_ref(), Role::Index),
                ColumnOption::Check(k) => self.define(k.name.as_ref(), Role::Index),
                _ => {}
            }
        }
    }

    fn table_options(&mut self, o: &CreateTableOptions) {
        self.exclude(o);
    }

    /// The literals of an INSERT / UPDATE of `_lazyview` that hold view names and definitions.
    fn lazyview_insert(&mut self, ins: &Insert) {
        let cols: Vec<String> = ins
            .columns
            .iter()
            .filter_map(|c| last(c).map(|i| i.value.to_ascii_lowercase()))
            .collect();
        let Some(src) = &ins.source else { return };
        let SetExpr::Values(v) = src.body.as_ref() else {
            return;
        };
        for row in &v.rows {
            for (c, e) in cols.iter().zip(&row.content) {
                self.lazyview_value(c, e);
            }
        }
    }

    fn lazyview_value(&mut self, column: &str, e: &Expr) {
        if let Expr::Value(v) = e {
            if matches!(
                v.value,
                Value::SingleQuotedString(_) | Value::DoubleQuotedString(_)
            ) {
                match column {
                    "definition" => {
                        self.sql_literals.insert(key(&v.span));
                    }
                    "name" => {
                        self.name_literals.insert(key(&v.span));
                    }
                    _ => {}
                }
            }
        }
    }
}

fn names_lazyview(n: &ObjectName) -> bool {
    last(n).is_some_and(|i| i.value.eq_ignore_ascii_case("_lazyview"))
}

impl Visitor for Scan {
    type Break = ();

    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<()> {
        self.on_tables |= matches!(
            s,
            Statement::Query(_)
                | Statement::Insert(_)
                | Statement::Update(_)
                | Statement::Delete(_)
                | Statement::CreateView(_)
                | Statement::CreateTable(_)
                | Statement::AlterTable(_)
                | Statement::CreateIndex(_)
                | Statement::Drop { .. }
                | Statement::Truncate(_)
                | Statement::RenameTable(_)
        );
        match s {
            Statement::CreateTable(ct) if last(&ct.name).is_some_and(|n| is_reserved(&n.value)) => {
            }
            Statement::CreateTable(ct) => {
                let mut keys: HashSet<String> = HashSet::new();
                let mut fks: HashSet<String> = HashSet::new();
                for c in &ct.constraints {
                    let (cols, set) = match c {
                        TableConstraint::PrimaryKey(k) => (&k.columns, &mut keys),
                        TableConstraint::Unique(k) => (&k.columns, &mut keys),
                        TableConstraint::ForeignKey(k) => {
                            fks.extend(k.columns.iter().map(|i| i.value.to_ascii_lowercase()));
                            continue;
                        }
                        _ => continue,
                    };
                    set.extend(
                        cols.iter()
                            .filter_map(|c| plain_column(&c.column.expr))
                            .map(|i| i.value.to_ascii_lowercase()),
                    );
                }
                for c in &ct.columns {
                    let l = c.name.value.to_ascii_lowercase();
                    for o in &c.options {
                        match o.option {
                            ColumnOption::PrimaryKey(_) | ColumnOption::Unique(_) => {
                                keys.insert(l.clone());
                            }
                            ColumnOption::ForeignKey(_) => {
                                fks.insert(l.clone());
                            }
                            _ => {}
                        }
                    }
                }
                self.define(
                    last(&ct.name),
                    Role::Table {
                        plain: fks.is_empty(),
                    },
                );
                for c in &ct.columns {
                    let l = c.name.value.to_ascii_lowercase();
                    let role = if keys.contains(&l) {
                        Role::KeyColumn
                    } else {
                        Role::Column {
                            fk: fks.contains(&l),
                        }
                    };
                    self.define(Some(&c.name), role);
                    self.exclude(&c.data_type);
                    let opts: Vec<ColumnOption> =
                        c.options.iter().map(|o| o.option.clone()).collect();
                    self.column_options(&opts);
                    for o in &c.options {
                        self.define(o.name.as_ref(), Role::Index);
                    }
                }
                for c in &ct.constraints {
                    self.constraint(c);
                }
                self.table_options(&ct.table_options);
            }
            Statement::AlterTable(at) => {
                for op in &at.operations {
                    match op {
                        AlterTableOperation::AddColumn { column_def, .. } => {
                            self.define(Some(&column_def.name), Role::Column { fk: false });
                            self.exclude(&column_def.data_type);
                            let opts: Vec<ColumnOption> = column_def
                                .options
                                .iter()
                                .map(|o| o.option.clone())
                                .collect();
                            self.column_options(&opts);
                        }
                        AlterTableOperation::ChangeColumn {
                            new_name,
                            data_type,
                            options,
                            ..
                        } => {
                            self.define(Some(new_name), Role::Column { fk: false });
                            self.exclude(data_type);
                            self.column_options(options);
                        }
                        AlterTableOperation::ModifyColumn {
                            data_type, options, ..
                        } => {
                            self.exclude(data_type);
                            self.column_options(options);
                        }
                        AlterTableOperation::RenameColumn {
                            new_column_name, ..
                        } => self.define(Some(new_column_name), Role::Column { fk: false }),
                        AlterTableOperation::AlterColumn { op, .. } => self.exclude(op),
                        AlterTableOperation::AddConstraint { constraint, .. } => {
                            self.constraint(constraint)
                        }
                        AlterTableOperation::RenameTable { table_name } => {
                            let (RenameTableNameKind::As(n) | RenameTableNameKind::To(n)) =
                                table_name;
                            self.define(last(n), Role::Table { plain: true });
                        }
                        AlterTableOperation::RenameConstraint { new_name, .. } => {
                            self.define(Some(new_name), Role::Index)
                        }
                        AlterTableOperation::SetTblProperties { table_properties } => {
                            self.exclude(table_properties)
                        }
                        _ => {}
                    }
                }
            }
            Statement::CreateView(cv) => {
                self.define(last(&cv.name), Role::View);
                for c in &cv.columns {
                    self.define(Some(&c.name), Role::OutputAlias);
                    self.exclude(&c.data_type);
                }
                self.exclude(&cv.options);
                self.exclude(&cv.params);
            }
            Statement::CreateIndex(ci) => {
                if let Some(n) = &ci.name {
                    self.define(last(n), Role::Index);
                }
                self.index_columns(&ci.columns);
                self.exclude(&ci.using);
                self.exclude(&ci.index_options);
            }
            Statement::RenameTable(rs) => {
                for r in rs {
                    self.define(last(&r.new_name), Role::Table { plain: true });
                }
            }
            Statement::Insert(ins) => {
                if let Some(a) = &ins.table_alias {
                    self.define(Some(&a.alias), Role::Alias);
                }
                if let Some(a) = &ins.insert_alias {
                    self.define(last(&a.row_alias), Role::Alias);
                    for c in a.col_aliases.iter().flatten() {
                        self.define(Some(c), Role::OutputAlias);
                    }
                }
                if matches!(&ins.table, TableObject::TableName(n) if names_lazyview(n)) {
                    self.lazyview_insert(ins);
                }
            }
            Statement::Update(up) => {
                if matches!(&up.table.relation, TableFactor::Table { name, .. } if names_lazyview(name))
                {
                    for a in &up.assignments {
                        if let sqlparser::ast::AssignmentTarget::ColumnName(c) = &a.target {
                            if let Some(c) = last(c) {
                                self.lazyview_value(&c.value.to_ascii_lowercase(), &a.value);
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_query(&mut self, q: &sqlparser::ast::Query) -> ControlFlow<()> {
        if let Some(w) = &q.with {
            for c in &w.cte_tables {
                self.define(Some(&c.alias.name), Role::Alias);
                for col in &c.alias.columns {
                    self.define(Some(&col.name), Role::OutputAlias);
                    self.exclude(&col.data_type);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        for item in &s.projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    self.define(Some(alias), Role::OutputAlias)
                }
                SelectItem::ExprWithAliases { aliases, .. } => {
                    for a in aliases {
                        self.define(Some(a), Role::OutputAlias);
                    }
                }
                _ => {}
            }
        }
        for w in &s.named_window {
            self.define(Some(&w.0), Role::Alias);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, t: &TableFactor) -> ControlFlow<()> {
        let alias = match t {
            TableFactor::Table { alias, .. }
            | TableFactor::Derived { alias, .. }
            | TableFactor::TableFunction { alias, .. }
            | TableFactor::Function { alias, .. }
            | TableFactor::UNNEST { alias, .. }
            | TableFactor::JsonTable { alias, .. }
            | TableFactor::NestedJoin { alias, .. } => alias.as_ref(),
            _ => None,
        };
        if let Some(a) = alias {
            self.define(Some(&a.name), Role::Alias);
            for c in &a.columns {
                self.define(Some(&c.name), Role::OutputAlias);
                self.exclude(&c.data_type);
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_relation(&mut self, n: &ObjectName) -> ControlFlow<()> {
        let parts: Vec<&Ident> = n.0.iter().filter_map(ObjectNamePart::as_ident).collect();
        let system = parts.len() >= 2
            && SYSTEM_SCHEMAS.contains(&parts[0].value.to_ascii_lowercase().as_str());
        if system || parts.iter().any(|p| is_reserved(&p.value)) {
            self.metadata = true;
        }
        let dual = parts.len() == 1
            && parts[0].quote_style.is_none()
            && parts[0].value.eq_ignore_ascii_case("dual");
        for p in parts {
            if system || dual {
                self.excluded.insert(key(&p.span));
            } else {
                self.relation.insert(key(&p.span));
                self.referenced
                    .push((p.value.clone(), Role::Table { plain: true }));
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        match e {
            Expr::Function(f) => self.exclude(&f.name),
            Expr::Cast { data_type, .. } => self.exclude(data_type),
            Expr::Convert {
                data_type, charset, ..
            } => {
                self.exclude(data_type);
                self.exclude(charset);
            }
            Expr::Collate { collation, .. } => self.exclude(collation),
            Expr::TypedString(t) => self.exclude(&t.data_type),
            Expr::Identifier(i) => {
                if i.value.starts_with('@')
                    || (i.quote_style.is_none()
                        && PSEUDO_IDENTS.contains(&i.value.to_ascii_uppercase().as_str()))
                {
                    self.excluded.insert(key(&i.span));
                } else {
                    self.columnish.insert(key(&i.span));
                    self.referenced
                        .push((i.value.clone(), Role::Column { fk: false }));
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.first().is_some_and(|p| p.value.starts_with('@')) {
                    for p in parts {
                        self.excluded.insert(key(&p.span));
                    }
                } else if let Some((l, qualifiers)) = parts.split_last() {
                    self.columnish.insert(key(&l.span));
                    self.referenced
                        .push((l.value.clone(), Role::Column { fk: false }));
                    for q in qualifiers {
                        self.referenced.push((q.value.clone(), Role::Alias));
                    }
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, v: &ValueWithSpan) -> ControlFlow<()> {
        if let Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) = &v.value {
            self.literals.push((s.clone(), v.span));
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_ident(&mut self, i: &Ident) -> ControlFlow<()> {
        self.idents.push(i.clone());
        ControlFlow::Continue(())
    }
}

/// The string literals of a Rust source (plain and raw strings, `\` line continuations): the
/// SQL statements tests write.
pub fn rust_literals(src: &str) -> Vec<String> {
    let b = src.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'/' if b.get(i + 1) == Some(&b'/') => {
                while i < b.len() && b[i] != b'\n' {
                    i += 1;
                }
            }
            b'\'' => {
                // A char literal or a lifetime.
                i += if b.get(i + 2) == Some(&b'\'') {
                    3
                } else if b.get(i + 1) == Some(&b'\\') {
                    4
                } else {
                    1
                };
            }
            b'r' if matches!(b.get(i + 1), Some(b'"' | b'#'))
                && (i == 0 || !b[i - 1].is_ascii_alphanumeric()) =>
            {
                let hashes = b[i + 1..].iter().take_while(|&&c| c == b'#').count();
                let start = i + 2 + hashes;
                let close = format!("\"{}", "#".repeat(hashes));
                let Some(end) = src[start..].find(&close).map(|e| start + e) else {
                    break;
                };
                out.push(src[start..end].to_string());
                i = end + close.len();
            }
            b'"' => {
                let mut s = String::new();
                let mut j = i + 1;
                while j < b.len() && b[j] != b'"' {
                    if b[j] == b'\\' {
                        j += 1;
                        match b.get(j) {
                            Some(b'n') => s.push('\n'),
                            Some(b't') => s.push('\t'),
                            Some(b'\\') => s.push('\\'),
                            Some(b'"') => s.push('"'),
                            Some(b'\'') => s.push('\''),
                            Some(b'0') => s.push('\0'),
                            Some(b'\n') => {
                                while b.get(j + 1).is_some_and(|c| c.is_ascii_whitespace()) {
                                    j += 1;
                                }
                            }
                            Some(_) => {
                                s.push('\\');
                                let c = src[j..].chars().next().unwrap();
                                s.push(c);
                                j += c.len_utf8() - 1;
                            }
                            None => {}
                        }
                        j += 1;
                        continue;
                    }
                    let c = src[j..].chars().next().unwrap();
                    s.push(c);
                    j += c.len_utf8();
                }
                out.push(s);
                i = j + 1;
            }
            _ => i += 1,
        }
    }
    out
}
