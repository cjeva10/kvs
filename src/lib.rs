#[deny(missing_docs)]

use crate::ser::SerializeResp;
use std::fmt;

mod de;
mod error;
mod ser;

pub use error::{Error, Result};

/// Representation of a Redis serialized value
///
/// see [https://redis.io/docs/reference/protocol-spec](https://redis.io/docs/reference/protocol-spec)
#[derive(Debug, PartialEq)]
pub enum Resp {
    /// A simple string has the format `+hello\r\n`
    SimpleString(String),
    /// A bulk string has the format `$5\r\nhello\r\n`
    BulkString(String),
    /// An integer has the format `:10\r\n`
    Integer(i64),
    /// An error has the format `-error message\r\n`
    Error(String),
    /// A Resp array has the format `*2\r\n+hello\r\n+world\r\n`
    Array(Vec<Resp>),
    /// The null value is represented by the literal `$-1\r\n`
    Null,
    /// The null array is represented by the literal `*-1\r\n`
    NullArray,
}

impl fmt::Display for Resp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.serialize())
    }
}
