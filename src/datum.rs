//! Hashable, owned SQL values used as de-duplication keys and as query parameters.

use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;

use crate::catalog::{ColKind, Column};

/// One column value. Unlike `mysql_async::Value` it is `Eq + Hash`, so it can key a `HashSet`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Datum {
    Null,
    Int(i64),
    UInt(u64),
    /// Floats are keyed by bit pattern.
    Float(u64),
    Bytes(Vec<u8>),
    Date(u16, u8, u8, u8, u8, u8, u32),
    Time(bool, u32, u8, u8, u8, u32),
}

/// A tuple of datums (a unit key, a primary key or a projected row image).
pub type Key = Box<[Datum]>;

impl Datum {
    /// Unsigned values that fit are stored as `Int` so binlog- and query-sourced keys compare equal.
    pub fn uint(v: u64) -> Datum {
        match i64::try_from(v) {
            Ok(i) => Datum::Int(i),
            Err(_) => Datum::UInt(v),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Datum::Null)
    }

    pub fn to_value(&self) -> Value {
        match self {
            Datum::Null => Value::NULL,
            Datum::Int(v) => Value::Int(*v),
            Datum::UInt(v) => Value::UInt(*v),
            Datum::Float(b) => Value::Double(f64::from_bits(*b)),
            Datum::Bytes(b) => Value::Bytes(b.clone()),
            Datum::Date(y, m, d, h, i, s, us) => Value::Date(*y, *m, *d, *h, *i, *s, *us),
            Datum::Time(neg, d, h, i, s, us) => Value::Time(*neg, *d, *h, *i, *s, *us),
        }
    }

    /// Converts a value read with a regular query.
    pub fn from_value(v: Value) -> Datum {
        match v {
            Value::NULL => Datum::Null,
            Value::Int(v) => Datum::Int(v),
            Value::UInt(v) => Datum::uint(v),
            Value::Float(f) => Datum::Float((f as f64).to_bits()),
            Value::Double(f) => Datum::Float(f.to_bits()),
            Value::Bytes(b) => Datum::Bytes(b),
            Value::Date(y, m, d, h, i, s, us) => Datum::Date(y, m, d, h, i, s, us),
            Value::Time(neg, d, h, i, s, us) => Datum::Time(neg, d, h, i, s, us),
        }
    }

    /// Converts a binlog row value into the representation the server would accept back as a
    /// parameter for the same column (see `Column::param_sql` for the matching SQL wrapper).
    pub fn from_binlog(v: Option<&BinlogValue<'_>>, col: &Column) -> Datum {
        let v = match v {
            None => return Datum::Null,
            Some(BinlogValue::Value(v)) => v,
            Some(BinlogValue::Jsonb(j)) => {
                return match serde_json_text(j) {
                    Some(s) => Datum::Bytes(s.into_bytes()),
                    None => Datum::Null,
                }
            }
            // Partial JSON updates carry no full value; callers treat the column as changed.
            Some(BinlogValue::JsonDiff(_)) => return Datum::Null,
        };
        match (&col.kind, v) {
            (_, Value::NULL) => Datum::Null,
            (ColKind::Enum(labels), Value::Int(i)) => match usize::try_from(*i) {
                Ok(0) => Datum::Bytes(Vec::new()),
                Ok(i) if i <= labels.len() => Datum::Bytes(labels[i - 1].clone().into_bytes()),
                _ => Datum::Int(*i),
            },
            (ColKind::Set(labels), Value::Bytes(b)) => {
                let mut mask = 0u64;
                for (i, byte) in b.iter().enumerate().take(8) {
                    mask |= (*byte as u64) << (8 * i);
                }
                let parts: Vec<&str> = labels
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| mask & (1u64 << i) != 0)
                    .map(|(_, l)| l.as_str())
                    .collect();
                Datum::Bytes(parts.join(",").into_bytes())
            }
            (ColKind::Unsigned(bits), Value::Int(i)) => {
                let mask = if *bits >= 64 { u64::MAX } else { (1u64 << bits) - 1 };
                Datum::uint((*i as u64) & mask)
            }
            // TIMESTAMP2 arrives as "<epoch>[.<usec>]"; the param wrapper is FROM_UNIXTIME(?).
            (ColKind::Timestamp, Value::Int(i)) => Datum::Bytes(i.to_string().into_bytes()),
            (_, other) => Datum::from_value(other.clone()),
        }
    }
}

fn serde_json_text(j: &mysql_async::binlog::jsonb::Value<'_>) -> Option<String> {
    let v: serde_json::Value = j.clone().try_into().ok()?;
    Some(v.to_string())
}

pub fn key_has_null(k: &[Datum]) -> bool {
    k.iter().any(Datum::is_null)
}
