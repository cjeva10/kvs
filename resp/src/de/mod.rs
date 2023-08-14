use crate::error::{Error, Result};
use crate::Resp;
use std::io::BufRead;

mod reader;
mod byte;

use reader::ReaderDeserializer;
use byte::ByteParser;

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
    /// ```
    pub fn from_reader(reader: impl BufRead) -> Result<Self> {
        let mut deserializer = ReaderDeserializer::from_reader(reader);

        let res = deserializer.parse_any()?;

        if deserializer.is_empty() {
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

/// Defines required methods for parsing a `Resp` object
trait RespParser {
    fn parse_any(&mut self) -> Result<Resp>;
    fn parse_simple_string(&mut self) -> Result<Resp>;
    fn parse_bulk_string(&mut self) -> Result<Resp>;
    fn parse_integer(&mut self) -> Result<Resp>;
    fn parse_list(&mut self) -> Result<Resp>;
    fn parse_null(&mut self) -> Result<Resp>;
    fn parse_error(&mut self) -> Result<Resp>;
    fn parse_null_array(&mut self) -> Result<Resp>;
}

#[cfg(test)]
mod tests {
    use crate::{Error, Resp};

    #[test]
    fn test_parse_bulk_string() {
        let s = "$5\r\nhello\r\n";
        let res = Resp::from_str(s).unwrap();
        let expected = Resp::BulkString("hello".to_owned());

        assert_eq!(res, expected);

        let s = b"$5\r\nhello\r\n";
        let res = Resp::from_bytes(s).unwrap();
        let expected = Resp::BulkString("hello".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_null() {
        let input = "$-1\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Null;

        assert_eq!(res, expected);

        let input = b"$-1\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::Null;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_null_array() {
        let input = "*-1\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::NullArray;

        assert_eq!(res, expected);

        let input = b"*-1\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::NullArray;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_simple_string() {
        let input = "+hello\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::SimpleString("hello".to_owned());

        assert_eq!(res, expected);

        let input = b"+hello\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::SimpleString("hello".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_integer() {
        let pos = ":10\r\n";
        let res = Resp::from_str(pos).unwrap();
        let expected = Resp::Integer(10);

        assert_eq!(res, expected);

        let neg = ":-10\r\n";
        let res = Resp::from_str(neg).unwrap();
        let expected = Resp::Integer(-10);

        assert_eq!(res, expected);

        let pos = b":10\r\n";
        let res = Resp::from_bytes(pos).unwrap();
        let expected = Resp::Integer(10);

        assert_eq!(res, expected);

        let neg = b":-10\r\n";
        let res = Resp::from_bytes(neg).unwrap();
        let expected = Resp::Integer(-10);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_error() {
        let input = "-ERR error\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Error("ERR error".to_owned());

        assert_eq!(res, expected);

        let input = b"-ERR error\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::Error("ERR error".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_list() {
        let input = "*2\r\n+hello\r\n+world\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Array(vec![
            Resp::SimpleString("hello".to_owned()),
            Resp::SimpleString("world".to_owned()),
        ]);

        assert_eq!(res, expected);

        let input = b"*2\r\n+hello\r\n+world\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::Array(vec![
            Resp::SimpleString("hello".to_owned()),
            Resp::SimpleString("world".to_owned()),
        ]);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_nested_list() {
        let input = "*2\r\n*2\r\n+hello\r\n+world\r\n:10\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Array(vec![
            Resp::Array(vec![
                Resp::SimpleString("hello".to_owned()),
                Resp::SimpleString("world".to_owned()),
            ]),
            Resp::Integer(10),
        ]);

        assert_eq!(res, expected);

        let input = b"*2\r\n*2\r\n+hello\r\n+world\r\n:10\r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::Array(vec![
            Resp::Array(vec![
                Resp::SimpleString("hello".to_owned()),
                Resp::SimpleString("world".to_owned()),
            ]),
            Resp::Integer(10),
        ]);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_weird_characters() {
        let input = "++//$$-+*\n\t\n !@#$%^&*()_\\  \r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::SimpleString("+//$$-+*\n\t\n !@#$%^&*()_\\  ".to_owned());

        assert_eq!(res, expected);
        
        let input = b"++//$$-+*\n\t\n !@#$%^&*()_\\  \r\n";
        let res = Resp::from_bytes(input).unwrap();
        let expected = Resp::SimpleString("+//$$-+*\n\t\n !@#$%^&*()_\\  ".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_invalid_prefix() {
        let input = "bad";
        let res = Resp::from_str(input).err().unwrap();
        let expected = Error::InvalidPrefix;

        assert_eq!(res, expected);

        let input = b"bad";
        let res = Resp::from_bytes(input).err().unwrap();
        let expected = Error::InvalidPrefix;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_bytes() {
        let bytes = b"+hello world\r\n";
        let res = Resp::from_bytes(bytes).unwrap();
        let expected = Resp::SimpleString("hello world".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_bad_bytes() {
        let shift_jis = b"\x82\xe6\x82\xa8\x82\xb1\x82\xbb";
        let res = Resp::from_bytes(shift_jis).err().unwrap();
        let expected = "encountered an invalid prefix".to_owned();

        assert_eq!(res.to_string(), expected);
    }
}
