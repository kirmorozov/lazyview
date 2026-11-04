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

/// A TIME value as the server accepts it as a parameter. MySQL >= 9.0 (26.x included) rejects a
/// binary-protocol TIME parameter in the last hour of the range when it has a fraction ("TIME
/// value is out of range in 'set_parameter_value'" for 838:00:00.5 up to 838:59:59.999999,
/// either sign), although the server stores and returns such values (8.4 and MariaDB accept the
/// parameter). Such a value is sent as text instead, which every server converts like a literal
/// (lookups wrap it in `CAST(? AS TIME(6))`, see `Column::param_sql`; a TIME column takes it as
/// is). Other values pass through unchanged.
pub fn time_param(v: Value) -> Value {
    match v {
        Value::Time(neg, d, h, i, s, us) if us != 0 && d * 24 + u32::from(h) >= 838 => {
            Value::Bytes(
                format!(
                    "{}{}:{i:02}:{s:02}.{us:06}",
                    if neg { "-" } else { "" },
                    d * 24 + u32::from(h)
                )
                .into_bytes(),
            )
        }
        v => v,
    }
}

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
            Datum::Time(neg, d, h, i, s, us) => time_param(Value::Time(*neg, *d, *h, *i, *s, *us)),
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
                let mask = if *bits >= 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                };
                Datum::uint((*i as u64) & mask)
            }
            // TIMESTAMP2 arrives as "<epoch>[.<usec>]"; the param wrapper is FROM_UNIXTIME(?).
            (ColKind::Timestamp, Value::Int(i)) => Datum::Bytes(i.to_string().into_bytes()),
            // BIT(n): big-endian bytes; bound as CAST(? AS UNSIGNED).
            (ColKind::Bit(_), Value::Bytes(b)) => {
                Datum::uint(b.iter().take(8).fold(0u64, |acc, x| (acc << 8) | *x as u64))
            }
            // BINARY(n): the binlog drops trailing 0x00 padding that the stored value has.
            (ColKind::Binary, Value::Bytes(b)) if col.data_type == "binary" => {
                let mut b = b.clone();
                if let Some(n) = col.octet_len.and_then(|n| usize::try_from(n).ok()) {
                    if b.len() < n {
                        b.resize(n, 0);
                    }
                }
                Datum::Bytes(b)
            }
            (_, other) => Datum::from_value(other.clone()),
        }
    }

    /// A binlog value that [`Self::from_binlog`] turns back into this datum for `col`: the
    /// cascade map (`crate::cascade`) keeps datums and replays them as row images.
    pub fn to_binlog(&self, col: &Column) -> Value {
        match (&col.kind, self) {
            (_, Datum::Null) => Value::NULL,
            (ColKind::Enum(labels), Datum::Bytes(b)) => {
                let i = labels.iter().position(|l| l.as_bytes() == b.as_slice());
                Value::Int(i.map_or(0, |i| i as i64 + 1))
            }
            (ColKind::Set(labels), Datum::Bytes(b)) => {
                let mut mask = 0u64;
                for part in b.split(|&c| c == b',') {
                    if let Some(i) = labels.iter().position(|l| l.as_bytes() == part) {
                        mask |= 1u64 << i;
                    }
                }
                Value::Bytes(mask.to_le_bytes()[..labels.len().div_ceil(8).max(1)].to_vec())
            }
            (ColKind::Unsigned(_), Datum::UInt(u)) => Value::Int(*u as i64),
            (ColKind::Timestamp, Datum::Bytes(b)) => {
                match std::str::from_utf8(b)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(i) => Value::Int(i),
                    None => Value::Bytes(b.clone()),
                }
            }
            (ColKind::Bit(_), Datum::Int(i)) => Value::Bytes((*i as u64).to_be_bytes().to_vec()),
            (ColKind::Bit(_), Datum::UInt(u)) => Value::Bytes(u.to_be_bytes().to_vec()),
            (_, Datum::Int(i)) => Value::Int(*i),
            (_, Datum::UInt(u)) => Value::UInt(*u),
            (_, Datum::Float(b)) => Value::Double(f64::from_bits(*b)),
            (_, Datum::Bytes(b)) => Value::Bytes(b.clone()),
            (_, Datum::Date(y, m, d, h, i, s, us)) => Value::Date(*y, *m, *d, *h, *i, *s, *us),
            (_, Datum::Time(neg, d, h, i, s, us)) => Value::Time(*neg, *d, *h, *i, *s, *us),
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

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::binlog::value::BinlogValue;

    fn bv(v: Value) -> BinlogValue<'static> {
        BinlogValue::Value(v)
    }

    #[test]
    fn time_param_in_the_last_hour_with_a_fraction_is_text() {
        // Accepted as binary TIME by every server.
        let plain = Datum::Time(true, 34, 21, 59, 59, 990_000); // -837:59:59.99
        assert_eq!(plain.to_value(), Value::Time(true, 34, 21, 59, 59, 990_000));
        let whole = Datum::Time(true, 34, 22, 59, 59, 0); // -838:59:59
        assert_eq!(whole.to_value(), Value::Time(true, 34, 22, 59, 59, 0));
        // Rejected by MySQL >= 9.0 as a binary parameter: sent as text instead.
        let edge = Datum::Time(true, 34, 22, 59, 58, 990_000); // -838:59:58.99
        assert_eq!(edge.to_value(), Value::Bytes(b"-838:59:58.990000".to_vec()));
        let pos = Datum::Time(false, 34, 22, 0, 0, 500_000); // 838:00:00.5
        assert_eq!(pos.to_value(), Value::Bytes(b"838:00:00.500000".to_vec()));
        // Rows read back from a table go through the same conversion.
        assert_eq!(
            time_param(Value::Time(true, 34, 22, 59, 58, 990_000)),
            Value::Bytes(b"-838:59:58.990000".to_vec())
        );
        assert_eq!(time_param(Value::Int(5)), Value::Int(5));
        assert_eq!(
            time_param(Value::Time(false, 0, 1, 2, 3, 4)),
            Value::Time(false, 0, 1, 2, 3, 4)
        );
    }

    #[test]
    fn binary_padding_and_bit() {
        let bin = Column::new("b", "binary", "binary(4)", None, None, Some(4), false);
        let d = Datum::from_binlog(Some(&bv(Value::Bytes(vec![1, 2]))), &bin);
        assert_eq!(d, Datum::Bytes(vec![1, 2, 0, 0]));
        let varbin = Column::new("b", "varbinary", "varbinary(4)", None, None, Some(4), false);
        let d = Datum::from_binlog(Some(&bv(Value::Bytes(vec![1, 2]))), &varbin);
        assert_eq!(d, Datum::Bytes(vec![1, 2]));
        let bit = Column::new("f", "bit", "bit(12)", None, None, None, false);
        let d = Datum::from_binlog(Some(&bv(Value::Bytes(vec![0x0a, 0xbc]))), &bit);
        assert_eq!(d, Datum::Int(0x0abc));
        let wide = Column::new("f", "bit", "bit(64)", None, None, None, false);
        let d = Datum::from_binlog(Some(&bv(Value::Bytes(vec![0xff; 8]))), &wide);
        assert_eq!(d, Datum::UInt(u64::MAX));
    }

    /// Every binlog value of every column kind survives `from_binlog` -> `to_binlog` ->
    /// `from_binlog` unchanged (the cascade map replays kept datums as row images).
    #[test]
    fn binlog_values_round_trip() {
        let col = |dt: &str, ct: &str, octets: Option<u64>| {
            Column::new("c", dt, ct, None, None, octets, false)
        };
        let cases: Vec<(Column, Vec<Value>)> = vec![
            (
                col("int", "int", None),
                vec![Value::Int(-7), Value::Int(0), Value::NULL],
            ),
            (
                col("bigint", "bigint unsigned", None),
                vec![Value::Int(5), Value::Int(-1), Value::Int(i64::MIN)],
            ),
            (
                col("tinyint", "tinyint unsigned", None),
                vec![Value::Int(-1)],
            ),
            (
                col("enum", "enum('a','b','c')", None),
                vec![Value::Int(0), Value::Int(1), Value::Int(3)],
            ),
            (
                col("set", "set('a','b','c','d','e','f','g','h','i')", None),
                vec![Value::Bytes(vec![0, 0]), Value::Bytes(vec![0b101, 1])],
            ),
            (
                col("timestamp", "timestamp", None),
                vec![Value::Int(1_700_000_000)],
            ),
            (
                col("timestamp", "timestamp(3)", None),
                vec![Value::Bytes(b"1700000000.125".to_vec())],
            ),
            (
                col("bit", "bit(12)", None),
                vec![Value::Bytes(vec![0x0a, 0xbc])],
            ),
            (
                col("bit", "bit(64)", None),
                vec![Value::Bytes(vec![0xff; 8])],
            ),
            (
                col("binary", "binary(4)", Some(4)),
                vec![Value::Bytes(vec![1, 2])],
            ),
            (
                col("varchar", "varchar(10)", Some(40)),
                vec![Value::Bytes(b"x y".to_vec())],
            ),
            (
                col("json", "json", None),
                vec![Value::Bytes(br#"{"a":1}"#.to_vec())],
            ),
            (col("float", "float", None), vec![Value::Float(1.5)]),
            (col("double", "double", None), vec![Value::Double(-2.25)]),
            (
                col("datetime", "datetime(6)", None),
                vec![Value::Date(2024, 2, 29, 23, 59, 58, 1)],
            ),
            (
                col("time", "time(6)", None),
                vec![Value::Time(true, 34, 22, 59, 58, 990_000)],
            ),
            (
                col("decimal", "decimal(10,2)", None),
                vec![Value::Bytes(b"-12.50".to_vec())],
            ),
        ];
        for (c, values) in cases {
            for v in values {
                let d = Datum::from_binlog(Some(&bv(v.clone())), &c);
                let back = Datum::from_binlog(Some(&bv(d.to_binlog(&c))), &c);
                assert_eq!(back, d, "{} {v:?}", c.data_type);
            }
        }
    }
}
