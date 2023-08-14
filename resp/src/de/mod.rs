use crate::error::{Error, Result};
use crate::Resp;
use std::io::BufRead;

mod byte;
mod reader;

use byte::ByteParser;
use reader::ReaderParser;

impl Resp {
    /// Convert a string into a `Resp` object
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crate::resp::Resp;
    ///
    /// let input = "*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_str(input).unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn from_str(s: &str) -> Result<Self> {
        let mut deserializer = ByteParser::from_str(s);

        let res = deserializer.parse_any()?;

        if deserializer.is_empty() {
            Ok(res)
        } else {
            Err(Error::TrailingCharacters)
        }
    }

    /// Use a reader to build a `Resp` object
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crate::resp::Resp;
    ///
    /// let input = b"*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_reader(&input[..]).unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn from_reader(reader: impl BufRead) -> Result<Self> {
        let mut deserializer = ReaderParser::from_reader(reader);

        let res = deserializer.parse_any()?;

        if deserializer.is_empty()? {
            Ok(res)
        } else {
            Err(Error::TrailingCharacters)
        }
    }

    /// Convert potentially multiple `Resp`s from a string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crate::resp::Resp;
    ///
    /// let input = "*2\r\n+hello\r\n+world\r\n*2\r\n+hello\r\n+world\r\n";
    /// let resp: Vec<Resp> = Resp::vec_from_str(input).unwrap();
    /// let expected = vec![
    ///     Resp::Array(vec![
    ///         Resp::SimpleString("hello".to_owned()),
    ///         Resp::SimpleString("world".to_owned()),
    ///     ]),
    ///     Resp::Array(vec![
    ///         Resp::SimpleString("hello".to_owned()),
    ///         Resp::SimpleString("world".to_owned()),
    ///     ]),
    /// ];
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn vec_from_str(s: &str) -> Result<Vec<Self>> {
        let mut out = Vec::new();

        let mut deserializer = ByteParser::from_str(s);

        while !deserializer.is_empty() {
            let res = deserializer.parse_any()?;

            out.push(res);
        }

        if out.len() == 0 {
            return Err(Error::Eof);
        }

        Ok(out)
    }

    /// Convert a byte array into a `Resp` object
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crate::resp::Resp;
    ///
    /// let input = b"*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_bytes(input).unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn from_bytes(b: &[u8]) -> Result<Self> {
        let mut deserializer = ByteParser::from_bytes(b);

        let res = deserializer.parse_any()?;

        if deserializer.is_empty() {
            Ok(res)
        } else {
            Err(Error::TrailingCharacters)
        }
    }
}

/// This trait defines required methods for parsing a `Resp` object
trait ParseResp {
    fn parse_any(&mut self) -> Result<Resp>;
    fn parse_simple_string(&mut self) -> Result<Resp>;
    fn parse_bulk_string(&mut self) -> Result<Resp>;
    fn parse_integer(&mut self) -> Result<Resp>;
    fn parse_array(&mut self) -> Result<Resp>;
    fn parse_null(&mut self) -> Result<Resp>;
    fn parse_error(&mut self) -> Result<Resp>;
    fn parse_null_array(&mut self) -> Result<Resp>;
}
