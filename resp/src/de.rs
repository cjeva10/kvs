use crate::error::{Error, Result};
use crate::Resp;
use bstr::{ByteSlice, ByteVec};
use std::collections::VecDeque;
use std::io::BufRead;
use std::str::from_utf8;

trait StartsWith {
    fn starts_with(&self, needle: &[u8]) -> bool;
}

impl StartsWith for VecDeque<u8> {
    fn starts_with(&self, needle: &[u8]) -> bool {
        let n = needle.len();
        let start: Vec<u8> = self.range(..n).copied().collect();
        self.len() >= n && needle == &start
    }
}

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
        let mut deserializer = Deserializer::from_str(s);

        let res = deserializer.deserialize_any()?;

        if deserializer.input.is_empty() {
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
    /// let input = "*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_str(input).unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn from_reader(reader: impl BufRead) -> Result<Self> {
        let mut deserializer = ReaderDeserializer::from_reader(reader);

        let res = deserializer.deserialize_any()?;

        if deserializer.input.is_empty() {
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

        let mut deserializer = Deserializer::from_str(s);

        while !deserializer.input.is_empty() {
            let res = deserializer.deserialize_any()?;

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
        let mut deserializer = Deserializer::from_bytes(b);

        let res = deserializer.deserialize_any()?;

        if deserializer.input.is_empty() {
            Ok(res)
        } else {
            Err(Error::TrailingCharacters)
        }
    }
}

struct Deserializer<'de> {
    input: &'de [u8],
}

struct ReaderDeserializer<R: BufRead> {
    reader: R,
    input: VecDeque<u8>,
}

impl<R: BufRead> ReaderDeserializer<R> {
    pub fn from_reader(reader: R) -> Self {
        ReaderDeserializer {
            reader,
            input: VecDeque::new(),
        }
    }

    fn peek_char(&mut self, count: usize) -> Result<Vec<u8>> {
        while self.input.len() < count {
            let mut buf = [0; 128];
            let n = match self.reader.read(&mut buf) {
                Ok(n) => n,
                Err(_) => return Err(Error::ReaderFailed),
            };
            if n == 0 {
                return Err(Error::Eof);
            }
            for i in 0..n {
                self.input.push_back(buf[i]);
            }
        }

        Ok(self.input.range(..count).copied().collect())
    }

    fn next_char(&mut self, count: usize) -> Result<Vec<u8>> {
        while self.input.len() < count {
            let mut buf = [0; 128];
            let n = match self.reader.read(&mut buf) {
                Ok(n) => n,
                Err(_) => return Err(Error::ReaderFailed),
            };
            if n == 0 {
                return Err(Error::Eof)
            }
            for i in 0..n {
                self.input.push_back(buf[i]);
            }
        }

        Ok(self.input.drain(..count).collect())
    }
}

impl<'de> Deserializer<'de> {
    pub fn from_str(input: &'de str) -> Self {
        Deserializer {
            input: input.as_bytes(),
        }
    }

    pub fn from_bytes(input: &'de [u8]) -> Self {
        Deserializer { input }
    }

    fn peek_char(&self) -> Result<u8> {
        self.input.get(0).copied().ok_or(Error::Eof)
    }

    fn next_char(&mut self) -> Result<u8> {
        let ch = self.peek_char()?;
        self.input = &self.input[1..];
        Ok(ch)
    }

    fn consume_crlf(&mut self) -> Result<()> {
        if self.next_char()? != b'\r' {
            return Err(Error::ExpectedCRLF);
        }
        if self.next_char()? != b'\n' {
            return Err(Error::ExpectedCRLF);
        }

        Ok(())
    }

    fn parse_length(&mut self) -> Result<usize> {
        let mut int: usize = match self.next_char()? {
            ch @ b'0'..=b'9' => (ch - b'0') as usize,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.input.get(0).copied() {
                Some(ch @ b'0'..=b'9') => {
                    self.input = &self.input[1..];
                    int *= 10;
                    int += (ch - b'0') as usize;
                }
                _ => {
                    return Ok(int);
                }
            }
        }
    }
}

/// Defines methods for deserializing into a `Resp` object
trait DeserializeResp {
    fn deserialize_any(&mut self) -> Result<Resp>;
    fn deserialize_simple_string(&mut self) -> Result<Resp>;
    fn deserialize_bulk_string(&mut self) -> Result<Resp>;
    fn deserialize_integer(&mut self) -> Result<Resp>;
    fn deserialize_list(&mut self) -> Result<Resp>;
    fn deserialize_null(&mut self) -> Result<Resp>;
    fn deserialize_error(&mut self) -> Result<Resp>;
    fn deserialize_null_array(&mut self) -> Result<Resp>;
}

impl<R: BufRead> DeserializeResp for ReaderDeserializer<R> {
    fn deserialize_any(&mut self) -> Result<Resp> {
        match self.peek_char(1)?.get(0).ok_or(Error::Eof)? {
            b'$' => self.deserialize_bulk_string(),
            b'+' => self.deserialize_simple_string(),
            b'-' => self.deserialize_error(),
            b'*' => self.deserialize_list(),
            b':' => self.deserialize_integer(),
            _ => Err(Error::InvalidPrefix),
        }
    }

    fn deserialize_null(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"$-1\r\n") {
            self.next_char(b"$-1\r\n".len())?;
            return Ok(Resp::Null);
        }
        Err(Error::ExpectedNull)
    }

    fn deserialize_error(&mut self) -> Result<Resp> {
        todo!()
    }
    fn deserialize_simple_string(&mut self) -> Result<Resp> {
        todo!()
    }

    fn deserialize_integer(&mut self) -> Result<Resp> {
        todo!()
    }

    fn deserialize_bulk_string(&mut self) -> Result<Resp> {
        todo!()
    }

    fn deserialize_list(&mut self) -> Result<Resp> {
        todo!()
    }

    fn deserialize_null_array(&mut self) -> Result<Resp> {
        todo!()
    }
}

impl<'de> DeserializeResp for Deserializer<'de> {
    fn deserialize_any(&mut self) -> Result<Resp> {
        match self.peek_char()? {
            b'$' => self.deserialize_bulk_string(),
            b'+' => self.deserialize_simple_string(),
            b'-' => self.deserialize_error(),
            b'*' => self.deserialize_list(),
            b':' => self.deserialize_integer(),
            _ => Err(Error::InvalidPrefix),
        }
    }

    fn deserialize_null(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"$-1\r\n") {
            self.input = &self.input[b"$-1\r\n".len()..];
            return Ok(Resp::Null);
        }
        Err(Error::ExpectedNull)
    }

    fn deserialize_error(&mut self) -> Result<Resp> {
        if self.next_char()? != b'-' {
            return Err(Error::ExpectedError);
        }

        match self.input.find("\r\n") {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len..];
                self.consume_crlf()?;
                Ok(Resp::Error(from_utf8(s)?.to_string()))
            }
            None => Err(Error::Eof),
        }
    }
    fn deserialize_simple_string(&mut self) -> Result<Resp> {
        if self.next_char()? != b'+' {
            return Err(Error::ExpectedSimpleString);
        }

        match self.input.find("\r\n") {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len..];
                self.consume_crlf()?;
                Ok(Resp::SimpleString(from_utf8(s)?.to_string()))
            }
            None => Err(Error::Eof),
        }
    }

    fn deserialize_integer(&mut self) -> Result<Resp> {
        if self.next_char()? != b':' {
            return Err(Error::ExpectedInteger);
        }

        let mut neg = false;
        if self.peek_char()? == b'-' {
            self.next_char()?;
            neg = true;
        }

        let mut int: i64 = match self.next_char()? {
            ch @ b'0'..=b'9' => (ch - b'0') as i64,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.input.get(0).copied() {
                Some(ch @ b'0'..=b'9') => {
                    self.input = &self.input[1..];
                    int *= 10;
                    int += (ch as u8 - b'0') as i64;
                }
                _ => {
                    self.consume_crlf()?;
                    if neg {
                        return Ok(Resp::Integer(-int));
                    } else {
                        return Ok(Resp::Integer(int));
                    }
                }
            }
        }
    }

    fn deserialize_bulk_string(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"$-1\r\n") {
            return self.deserialize_null();
        }

        if self.next_char()? != b'$' {
            return Err(Error::ExpectedBulkString);
        }

        let len = self.parse_length()?;

        self.consume_crlf()?;

        let s = &self.input[..len];
        self.input = &self.input[len..];

        self.consume_crlf()?;

        Ok(Resp::BulkString(from_utf8(s)?.to_string()))
    }

    fn deserialize_list(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"*-1\r\n") {
            return self.deserialize_null_array();
        }

        if self.next_char()? != b'*' {
            return Err(Error::ExpectedArray);
        }

        let len = self.parse_length()?;

        self.consume_crlf()?;

        let mut out: Vec<Resp> = Vec::new();

        for _ in 0..len {
            let next = self.deserialize_any()?;
            out.push(next);
        }

        Ok(Resp::Array(out))
    }

    fn deserialize_null_array(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"*-1\r\n") {
            self.input = &self.input["*-1\r\n".len()..];
            return Ok(Resp::NullArray);
        }
        Err(Error::ExpectedNullArray)
    }
}

#[cfg(test)]
mod tests {
    use std::{io::Cursor, str::from_utf8};

    use crate::{Error, Resp};

    use super::ReaderDeserializer;

    #[test]
    fn test_deserialize_bulk_string() {
        let s = "$5\r\nhello\r\n";
        let res = Resp::from_str(s).unwrap();
        let expected = Resp::BulkString("hello".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_null() {
        let input = "$-1\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Null;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_null_array() {
        let input = "*-1\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::NullArray;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_simple_string() {
        let input = "+hello\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::SimpleString("hello".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_integer() {
        let pos = ":10\r\n";
        let res = Resp::from_str(pos).unwrap();
        let expected = Resp::Integer(10);

        assert_eq!(res, expected);

        let neg = ":-10\r\n";
        let res = Resp::from_str(neg).unwrap();
        let expected = Resp::Integer(-10);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_error() {
        let input = "-ERR error\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Error("ERR error".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_list() {
        let input = "*2\r\n+hello\r\n+world\r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::Array(vec![
            Resp::SimpleString("hello".to_owned()),
            Resp::SimpleString("world".to_owned()),
        ]);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_nested_list() {
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
    }

    #[test]
    fn test_weird_characters() {
        let input = "++//$$-+*\n\t\n !@#$%^&*()_\\  \r\n";
        let res = Resp::from_str(input).unwrap();
        let expected = Resp::SimpleString("+//$$-+*\n\t\n !@#$%^&*()_\\  ".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_invalid_prefix() {
        let input = "bad";
        let res = Resp::from_str(input).err().unwrap();
        let expected = Error::InvalidPrefix;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_deserialize_bytes() {
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

    #[test]
    fn test_reader_peek_char() {
        let mut vec: Vec<u8> = Vec::from(b"hello world\n".to_owned());
        let reader = Cursor::new(&mut vec);
        
        let mut deserializer = ReaderDeserializer::from_reader(reader);

        let hello: Vec<u8> = deserializer.peek_char(5).unwrap();
        assert_eq!(vec, b"hello world\n".to_owned());
        assert_eq!(hello, Vec::from(b"hello".to_owned()));
    }

    #[test]
    fn test_reader_next_char() {
        let vec: Vec<u8> = Vec::from(b"hello world\n".to_owned());
        let reader = Cursor::new(vec.clone());
        
        let mut deserializer = ReaderDeserializer::from_reader(reader);

        let hello: Vec<u8> = deserializer.next_char(5).unwrap();

        println!("{:?}", deserializer.input);
        assert_eq!(deserializer.input, b" world\n".to_owned());
        assert_eq!(hello, Vec::from(b"hello".to_owned()));
    }
}
