use crate::{Error, Resp, Result};
use async_trait::async_trait;
use log::trace;
use std::{collections::VecDeque, io::Read};
use tokio::io::AsyncReadExt;

mod async_reader;
mod byte;
mod reader;

use async_reader::AsyncReaderParser;
use byte::ByteParser;

use log::debug;

pub use reader::ReaderParser;

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

    /// Use an async reader to build a `Resp` object
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    /// use crate::resp::Resp;
    ///
    /// let input = b"*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_reader_async(&input[..]).await.unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// # })
    /// ```
    pub async fn from_reader_async<R: AsyncReadExt + Send + Unpin + Sync>(
        reader: R,
    ) -> Result<Self> {
        let mut deserializer = AsyncReaderParser::from_reader(reader);

        let res = deserializer.parse_any().await?;

        debug!("Parsed result = {:?}", res);

        // check just that the buffer is empty because a read could block the thread indefinitely
        if deserializer.is_buf_empty() {
            debug!("Deserializer buffer is empty, returning");
            Ok(res)
        } else {
            debug!("Deserializer buffer is not empty, failing");
            Err(Error::TrailingCharacters)
        }
    }

    /// Parse `Resp` from a synchronous reader
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
    pub fn from_reader<R: Read>(reader: R) -> Result<Self> {
        let mut deserializer = ReaderParser::from_reader(reader);

        let res = deserializer.parse_any()?;

        debug!("Parsed result = {:?}", res);

        if deserializer.is_buf_empty() {
            debug!("Deserializer buffer is empty, returning");
            Ok(res)
        } else {
            debug!("Deserializer buffer is not empty, failing");
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
#[async_trait]
trait AsyncParseResp {
    async fn parse_any(&mut self) -> Result<Resp>;
    async fn parse_simple_string(&mut self) -> Result<Resp>;
    async fn parse_bulk_string(&mut self) -> Result<Resp>;
    async fn parse_integer(&mut self) -> Result<Resp>;
    async fn parse_array(&mut self) -> Result<Resp>;
    async fn parse_null(&mut self) -> Result<Resp>;
    async fn parse_error(&mut self) -> Result<Resp>;
    async fn parse_null_array(&mut self) -> Result<Resp>;
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

trait StartsWith {
    fn starts_with(&self, needle: &[u8]) -> bool;
}

impl StartsWith for VecDeque<u8> {
    fn starts_with(&self, needle: &[u8]) -> bool {
        let n = needle.len();
        if n != self.len() {
            return false;
        }
        trace!("vec = {:?}, n = {}", self.iter().map(|x| *x as char).collect::<Vec<char>>(), n);
        let start: Vec<u8> = self.range(..n).copied().collect();
        self.len() >= n && needle == &start
    }
}

trait StartsWithMut {
    fn starts_with_mut(&mut self, needle: &[u8]) -> Result<bool>;
}
