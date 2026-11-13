//! Wire-format decoder for PostgreSQL logical replication (the `pgoutput` output plugin,
//! protocol version 1).
//!
//! It is what `src/binlog` is for MySQL on the future PostgreSQL reader side: bytes in,
//! borrowed typed messages out. Nothing calls it yet (issue #57); the reader and the
//! replication client arrive in later PRs.
//!
//! # Scope
//!
//! [`parse`] decodes the pgoutput payload of one XLogData message. The transport around it
//! (START_REPLICATION, the CopyData `'w'`/`'k'` framing, keepalives, standby status updates)
//! belongs to the pgwire-replication client crate, not here. Only protocol version 1 is
//! decoded; message kinds that version 1 never sends (stream start/end/abort, two-phase
//! prepare/commit-prepared/rollback-prepared, parameter status) are reported as
//! [`Error::Unsupported`] carrying the kind byte.
//!
//! # Guarantees
//!
//! * Decoding allocates nothing: strings, relation attributes and tuple cells borrow from the
//!   input buffer; a relation's attributes and a tuple's cells are lazy iterators.
//! * No input panics: every read is bounds-checked ([`Error::Truncated`]), structurally
//!   invalid input is [`Error::Invalid`], and [`parse`] validates the whole message eagerly
//!   (including the column records the lazy iterators walk), so the iterators only surface
//!   `Result` as a second belt.
//! * Strings are NUL-terminated UTF-8; invalid UTF-8 is an [`Error::Invalid`], never a lossy
//!   copy.
//!
//! The layouts match the PostgreSQL 16 publisher (`src/backend/replication/logical/proto.c`,
//! `logicalrep_write_*`), which is ground truth over the 16.x doc prose on three points
//! verified against real bytes: `Commit` carries a one-byte flags field (not 8), `Origin`
//! carries an 8-byte LSN and the origin name (not an Int32 node id), `Truncate` is
//! count + one options byte + the relation OIDs, and `Message` always carries its LSN with
//! the transactional flag sent as a 0/1 byte.

use std::fmt;

/// One pgoutput protocol-1 message, borrowing from the input buffer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message<'a> {
    /// `'B'` — first message of a transaction.
    Begin {
        /// Final LSN of the transaction commit.
        final_lsn: u64,
        /// Commit time in microseconds since 2000-01-01 00:00:00 UTC.
        commit_time_us: i64,
        /// Transaction id.
        xid: u32,
    },
    /// `'C'` — last message of a transaction.
    Commit {
        /// Flags byte (always 0 in protocol version 1).
        flags: u8,
        /// LSN of the commit record.
        commit_lsn: u64,
        /// LSN just after the commit record.
        end_lsn: u64,
        /// Commit time in microseconds since 2000-01-01 00:00:00 UTC.
        commit_time_us: i64,
    },
    /// `'O'` — the transaction was replayed from this origin; sent before any DML of the
    /// transaction, only in cascaded setups.
    Origin {
        /// LSN of the remote commit record.
        origin_lsn: u64,
        /// Origin (remote node) name.
        name: &'a str,
    },
    /// `'R'` — describes a relation; sent before the first DML message for a relation id.
    Relation(Relation<'a>),
    /// `'Y'` — names a non-built-in column type by its OID; sent before the Relation
    /// message that uses it.
    Type {
        /// Type OID (the base type, for domains).
        type_oid: u32,
        /// Type namespace.
        namespace: &'a str,
        /// Type name.
        name: &'a str,
    },
    /// `'I'` — one inserted row.
    Insert {
        /// Publisher-side relation OID (as seen in the Relation message).
        rel_id: u32,
        /// The new row.
        new: Tuple<'a>,
    },
    /// `'U'` — one updated row.
    Update {
        /// Publisher-side relation OID.
        rel_id: u32,
        /// Old tuple, only sent when the replica identity requires it: `'K'` key columns
        /// only, `'O'` the whole old row (REPLICA IDENTITY FULL).
        old: Option<(OldTuple, Tuple<'a>)>,
        /// The new row.
        new: Tuple<'a>,
    },
    /// `'D'` — one deleted row; the old tuple is always present.
    Delete {
        /// Publisher-side relation OID.
        rel_id: u32,
        /// Old tuple: `'K'` key columns only, `'O'` the whole old row.
        old: (OldTuple, Tuple<'a>),
    },
    /// `'T'` — table(s) truncated, in its own transaction.
    Truncate {
        /// Options bit mask (1 = CASCADE, 2 = RESTART IDENTITY).
        options: u8,
        /// OID of each truncated relation, in statement order.
        rel_ids: Oids<'a>,
    },
    /// `'M'` — a logical decoding message from `pg_logical_emit_message`.
    Message {
        /// True for transactional messages (delivered on commit, dropped on rollback).
        transactional: bool,
        /// LSN of the message record (always sent by a PG16 publisher).
        lsn: u64,
        /// Message prefix.
        prefix: &'a str,
        /// Message content, raw bytes.
        content: &'a [u8],
    },
}

/// Which old tuple an update/delete message carried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTuple {
    /// `'K'` — only the replica key columns.
    Key,
    /// `'O'` — the whole old row (REPLICA IDENTITY FULL).
    Full,
}

/// One column descriptor of a Relation message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attr<'a> {
    /// Column name.
    pub name: &'a str,
    /// Column type OID (see Message::Type for non-built-in types).
    pub type_oid: u32,
    /// Type modifier (-1 when the type has none).
    pub type_mod: i32,
    /// True when the column is part of the replica identity.
    pub key: bool,
}

/// One cell of a tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column<'a> {
    /// SQL NULL.
    Null,
    /// TOASTed value unchanged by this statement (its real value is not sent).
    Unchanged,
    /// Value in text format, raw (already through the type's output function).
    Text(&'a [u8]),
    /// Value in binary format (only with the publisher-side `binary` option).
    Binary(&'a [u8]),
}

/// Replica identity values of the `relreplident` byte in a Relation message.
pub mod replica_identity {
    /// Default: primary key (or first unique index).
    pub const DEFAULT: u8 = b'd';
    /// No rows can be updated or deleted.
    pub const NOTHING: u8 = b'n';
    /// Whole old row sent for updates/deletes.
    pub const FULL: u8 = b'f';
    /// Named index (its columns are flagged `key` on the attributes).
    pub const INDEX: u8 = b'i';
}

/// A Relation message: everything except the column descriptors, which the [`Attrs`]
/// iterator walks lazily.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation<'a> {
    /// Publisher-side relation OID.
    pub rel_id: u32,
    /// Namespace (schema) name.
    pub namespace: &'a str,
    /// Relation name.
    pub name: &'a str,
    /// Replica identity, one of [`replica_identity`].
    pub replica_identity: u8,
    ncols: u16,
    attrs: &'a [u8],
}

impl<'a> Relation<'a> {
    /// Lazy iterator over the column descriptors (bounds were checked at parse time).
    pub fn columns(&self) -> Attrs<'a> {
        Attrs {
            left: self.ncols,
            r: Reader::new(self.attrs),
        }
    }
}

/// Iterator over the column descriptors of a [`Relation`].
pub struct Attrs<'a> {
    left: u16,
    r: Reader<'a>,
}

impl<'a> Iterator for Attrs<'a> {
    type Item = Result<Attr<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(read_attr(&mut self.r))
    }
}

/// A tuple: the column count plus the raw column records the [`Columns`] iterator walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tuple<'a> {
    ncols: u16,
    body: &'a [u8],
}

impl<'a> Tuple<'a> {
    /// Number of cells, as declared in the tuple header.
    pub fn len(&self) -> usize {
        self.ncols as usize
    }
    /// True when the tuple has no cells.
    pub fn is_empty(&self) -> bool {
        self.ncols == 0
    }
    /// Lazy iterator over the cells (bounds were checked at parse time).
    pub fn columns(&self) -> Columns<'a> {
        Columns {
            left: self.ncols,
            r: Reader::new(self.body),
        }
    }
}

/// Iterator over the cells of a [`Tuple`].
pub struct Columns<'a> {
    left: u16,
    r: Reader<'a>,
}

impl<'a> Iterator for Columns<'a> {
    type Item = Result<Column<'a>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(read_column(&mut self.r))
    }
}

/// Iterator over the relation OIDs of a [`Message::Truncate`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Oids<'a> {
    left: u32,
    r: Reader<'a>,
}

impl<'a> Iterator for Oids<'a> {
    type Item = Result<u32, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(self.r.u32())
    }
}

/// Decoder failures. The decoder never panics on any input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// Input ended in the middle of a field.
    Truncated,
    /// Structurally invalid input; the text says what.
    Invalid(&'static str),
    /// A message kind protocol version 1 never sends (stream and two-phase messages,
    /// parameter status, and any unknown kind). Carries the kind byte.
    Unsupported(u8),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Truncated => f.write_str("input ends in the middle of a field"),
            Error::Invalid(what) => write!(f, "malformed message: {what}"),
            Error::Unsupported(kind) => {
                write!(
                    f,
                    "message kind 0x{kind:02x} is not part of protocol version 1"
                )
            }
        }
    }
}

impl std::error::Error for Error {}

/// Decode the pgoutput payload of one XLogData message.
///
/// The whole message (including the column records that [`Attrs`] and [`Columns`] walk) is
/// validated here, so the iterators only surface `Result` as a second belt; nothing
/// allocates, everything borrows from `buf`.
pub fn parse(buf: &[u8]) -> Result<Message<'_>, Error> {
    let mut r = Reader::new(buf);
    let kind = r.byte()?;
    let msg = match kind {
        b'B' => Message::Begin {
            final_lsn: r.u64()?,
            commit_time_us: r.i64()?,
            xid: r.u32()?,
        },
        b'C' => Message::Commit {
            flags: r.byte()?,
            commit_lsn: r.u64()?,
            end_lsn: r.u64()?,
            commit_time_us: r.i64()?,
        },
        b'O' => Message::Origin {
            origin_lsn: r.u64()?,
            name: r.cstr()?,
        },
        b'R' => Message::Relation(read_relation(&mut r)?),
        b'Y' => Message::Type {
            type_oid: r.u32()?,
            namespace: r.cstr()?,
            name: r.cstr()?,
        },
        b'I' => {
            let rel_id = r.u32()?;
            r.tag(b'N', "insert: expected 'N' before the new tuple")?;
            Message::Insert {
                rel_id,
                new: read_tuple(&mut r)?,
            }
        }
        b'U' => {
            let rel_id = r.u32()?;
            let mut tag = r.byte()?;
            let old = match tag {
                b'K' => {
                    let t = read_tuple(&mut r)?;
                    tag = r.byte()?;
                    Some((OldTuple::Key, t))
                }
                b'O' => {
                    let t = read_tuple(&mut r)?;
                    tag = r.byte()?;
                    Some((OldTuple::Full, t))
                }
                _ => None,
            };
            if tag != b'N' {
                return Err(Error::Invalid(
                    "update: expected 'N' after the optional old tuple",
                ));
            }
            Message::Update {
                rel_id,
                old,
                new: read_tuple(&mut r)?,
            }
        }
        b'D' => {
            let rel_id = r.u32()?;
            let old = match r.byte()? {
                b'K' => (OldTuple::Key, read_tuple(&mut r)?),
                b'O' => (OldTuple::Full, read_tuple(&mut r)?),
                _ => {
                    return Err(Error::Invalid(
                        "delete: expected 'K' or 'O' before the old tuple",
                    ))
                }
            };
            Message::Delete { rel_id, old }
        }
        b'T' => {
            let count = r.u32()?;
            let options = r.byte()?;
            let n = (count as usize).checked_mul(4).ok_or(Error::Truncated)?;
            Message::Truncate {
                options,
                rel_ids: Oids {
                    left: count,
                    r: Reader::new(r.take(n)?),
                },
            }
        }
        b'M' => {
            let flags = r.byte()?;
            let lsn = r.u64()?;
            let prefix = r.cstr()?;
            let content = r.length_bytes()?;
            Message::Message {
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content,
            }
        }
        other => return Err(Error::Unsupported(other)),
    };
    r.done()?;
    Ok(msg)
}

/// Bounds-checked big-endian reader over the borrowed input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], Error> {
        let end = self.pos.checked_add(n).ok_or(Error::Truncated)?;
        if end > self.buf.len() {
            return Err(Error::Truncated);
        }
        let s = &self.buf[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn byte(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    fn i16(&mut self) -> Result<i16, Error> {
        let b = self.take(2)?;
        Ok(i16::from_be_bytes([b[0], b[1]]))
    }

    fn u32(&mut self) -> Result<u32, Error> {
        let b = self.take(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    fn i32(&mut self) -> Result<i32, Error> {
        Ok(self.u32()? as i32)
    }

    fn u64(&mut self) -> Result<u64, Error> {
        let b = self.take(8)?;
        Ok(u64::from_be_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }

    fn i64(&mut self) -> Result<i64, Error> {
        Ok(self.u64()? as i64)
    }

    /// NUL-terminated string; invalid UTF-8 is an error, never a lossy copy.
    fn cstr(&mut self) -> Result<&'a str, Error> {
        let rest = &self.buf[self.pos..];
        let end = rest.iter().position(|&b| b == 0).ok_or(Error::Truncated)?;
        let s = std::str::from_utf8(&rest[..end])
            .map_err(|_| Error::Invalid("string is not valid UTF-8"))?;
        self.pos += end + 1;
        Ok(s)
    }

    fn tag(&mut self, want: u8, what: &'static str) -> Result<(), Error> {
        if self.byte()? == want {
            Ok(())
        } else {
            Err(Error::Invalid(what))
        }
    }

    fn length_bytes(&mut self) -> Result<&'a [u8], Error> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    fn done(&self) -> Result<(), Error> {
        if self.pos == self.buf.len() {
            Ok(())
        } else {
            Err(Error::Invalid("trailing bytes after the message"))
        }
    }
}

fn read_attr<'a>(r: &mut Reader<'a>) -> Result<Attr<'a>, Error> {
    let key = r.byte()? == 1;
    let name = r.cstr()?;
    let type_oid = r.u32()?;
    let type_mod = r.i32()?;
    Ok(Attr {
        name,
        type_oid,
        type_mod,
        key,
    })
}

fn read_column<'a>(r: &mut Reader<'a>) -> Result<Column<'a>, Error> {
    match r.byte()? {
        b'n' => Ok(Column::Null),
        b'u' => Ok(Column::Unchanged),
        b't' => Ok(Column::Text(r.length_bytes()?)),
        b'b' => Ok(Column::Binary(r.length_bytes()?)),
        _ => Err(Error::Invalid("unknown tuple column kind")),
    }
}

fn read_tuple<'a>(r: &mut Reader<'a>) -> Result<Tuple<'a>, Error> {
    let ncols = r.i16()?;
    if ncols < 0 {
        return Err(Error::Invalid("negative tuple column count"));
    }
    let start = r.pos;
    for _ in 0..ncols {
        read_column(r)?;
    }
    Ok(Tuple {
        ncols: ncols as u16,
        body: &r.buf[start..r.pos],
    })
}

fn read_relation<'a>(r: &mut Reader<'a>) -> Result<Relation<'a>, Error> {
    let rel_id = r.u32()?;
    let namespace = r.cstr()?;
    let name = r.cstr()?;
    let replica_identity = r.byte()?;
    let ncols = r.i16()?;
    if ncols < 0 {
        return Err(Error::Invalid("negative relation column count"));
    }
    let start = r.pos;
    for _ in 0..ncols {
        read_attr(r)?;
    }
    Ok(Relation {
        rel_id,
        namespace,
        name,
        replica_identity,
        ncols: ncols as u16,
        attrs: &r.buf[start..r.pos],
    })
}

#[cfg(test)]
mod tests;
