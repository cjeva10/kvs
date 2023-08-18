//! # Resp
//!
//! This crate implements the Redis Serialization Protocol (RESP)
//!
//! The `Resp` enum provides the basic types for Redis serialized messages.
//!
//! `Resp` can be serialized to strings, and can be parsed from `&[u8]`, `String`
//! and readers
//!
//! # Examples
//!
//! ## Printing `Resp` to a `String`
//!
//! ```rust
//! use resp::Resp;
//!
//! let hello = Resp::SimpleString("hello world".to_owned());
//! assert_eq!(format!("{}", hello), "hello world".to_string());
//! ```
//!
//! ## Parse `Resp` from a `&[u8]` and `&str`
//!
//! ```rust
//! use resp::Resp;
//!
//! let bytes = b"$11\r\nhello world\r\n";
//! let res = Resp::from_bytes(bytes).unwrap();
//!
//! assert_eq!(res, Resp::BulkString("hello world".to_owned()));
//!
//! let string = "$11\r\nhello world\r\n";
//! let res = Resp::from_str(string).unwrap();
//!
//! assert_eq!(res, Resp::BulkString("hello world".to_owned()));
//! ```
//!
//! ## Parse `Resp` from an reader and an async reader 
//! ```rust
//! use std::io::Cursor;
//! use resp::Resp;
//!
//! let reader = Cursor::new(b"$11\r\nhello world\r\n".to_owned());
//! let res = Resp::from_reader(reader).unwrap();
//!
//! assert_eq!(res, Resp::BulkString("hello world".to_owned()));
//! ```
//!
//! ```rust
//! # tokio_test::block_on(async {
//! use std::io::Cursor;
//! use resp::Resp;
//!
//! let reader = Cursor::new(b"$11\r\nhello world\r\n".to_owned());
//! let res = Resp::from_reader_async(reader).await.unwrap();
//!
//! assert_eq!(res, Resp::BulkString("hello world".to_owned()));
//! # })
//! ```
//!

#[deny(missing_docs)]

use std::fmt;

mod de;
mod error;
mod ser;

pub use ser::SerializeResp;
pub use de::ReaderParser;
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
        write!(f, "{}", self.stringify())
    }
}

impl Resp {
    fn stringify(&self) -> String {
        match self {
            Resp::SimpleString(s) => s.to_string(),
            Resp::BulkString(s) => s.to_string(),
            Resp::Integer(int) => int.to_string(),
            Resp::Error(s) => s.to_string(),
            Resp::Array(list) => Self::stringify_list(list),
            Resp::Null => "Null".to_string(),
            Resp::NullArray => "NullArray".to_string(),
        }
    }

    fn stringify_list(list: &Vec<Resp>) -> String {
        let mut out = String::new();

        match list.len() {
            0 => return "[]".to_string(),
            _ => (),
        }

        out.push('[');
        for l in list {
            out.push_str(&l.stringify());
            out.push(',');
        }
        out.pop();
        out.push(']');

        out
    }
}
