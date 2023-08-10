use crate::error::{Error, Result};
use crate::Resp;

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

    /// Convert a byte array into a `Resp` object
    ///
    /// Under the hood this just calls `Resp::from_str`, because
    /// valid RESP must be a valid string
    ///
    /// # Examples
    ///
    /// ```rust
    /// use crate::resp::Resp;
    ///
    /// let input = "*2\r\n+hello\r\n+world\r\n";
    /// let resp = Resp::from_bytes(input).unwrap();
    /// let expected = Resp::Array(vec![
    ///     Resp::SimpleString("hello".to_owned()),
    ///     Resp::SimpleString("world".to_owned()),
    /// ]);
    ///
    /// assert_eq!(resp, expected);
    /// ```
    pub fn from_bytes(b: impl AsRef<[u8]>) -> Result<Self> {
        let s = std::str::from_utf8(b.as_ref())?;

        Resp::from_str(s)
    }
}

struct Deserializer<'de> {
    input: &'de str,
}

impl<'de> Deserializer<'de> {
    pub fn from_str(input: &'de str) -> Self {
        Deserializer { input }
    }

    fn peek_char(&self) -> Result<char> {
        self.input.chars().next().ok_or(Error::Eof)
    }

    fn next_char(&mut self) -> Result<char> {
        let ch = self.peek_char()?;
        self.input = &self.input[ch.len_utf8()..];
        Ok(ch)
    }

    fn consume_crlf(&mut self) -> Result<()> {
        if self.next_char()? != '\r' {
            return Err(Error::ExpectedCRLF);
        }
        if self.next_char()? != '\n' {
            return Err(Error::ExpectedCRLF);
        }

        Ok(())
    }

    fn parse_length(&mut self) -> Result<usize> {
        let mut int: usize = match self.next_char()? {
            ch @ '0'..='9' => (ch as u8 - b'0') as usize,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    int *= 10;
                    int += (ch as u8 - b'0') as usize;
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

impl<'de> DeserializeResp for Deserializer<'de> {
    fn deserialize_any(&mut self) -> Result<Resp> {
        match self.peek_char()? {
            '$' => self.deserialize_bulk_string(),
            '+' => self.deserialize_simple_string(),
            '-' => self.deserialize_error(),
            '*' => self.deserialize_list(),
            ':' => self.deserialize_integer(),
            _ => Err(Error::InvalidPrefix),
        }
    }

    fn deserialize_null(&mut self) -> Result<Resp> {
        if self.input.starts_with("$-1\r\n") {
            self.input = &self.input["$-1\r\n".len()..];
            println!("{}", self.input);
            return Ok(Resp::Null);
        }
        Err(Error::ExpectedNull)
    }

    fn deserialize_error(&mut self) -> Result<Resp> {
        if self.next_char()? != '-' {
            return Err(Error::ExpectedError);
        }

        match self.input.find("\r\n") {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len..];
                self.consume_crlf()?;
                Ok(Resp::Error(s.to_string()))
            }
            None => Err(Error::Eof),
        }
    }
    fn deserialize_simple_string(&mut self) -> Result<Resp> {
        if self.next_char()? != '+' {
            return Err(Error::ExpectedSimpleString);
        }

        match self.input.find("\r\n") {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len..];
                self.consume_crlf()?;
                Ok(Resp::SimpleString(s.to_string()))
            }
            None => Err(Error::Eof),
        }
    }

    fn deserialize_integer(&mut self) -> Result<Resp> {
        if self.next_char()? != ':' {
            return Err(Error::ExpectedInteger);
        }

        let mut neg = false;
        if self.peek_char()? == '-' {
            self.next_char()?;
            neg = true;
        }

        let mut int: i64 = match self.next_char()? {
            ch @ '0'..='9' => (ch as u8 - b'0') as i64,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
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
        if self.input.starts_with("$-1\r\n") {
            return self.deserialize_null();
        }

        if self.next_char()? != '$' {
            return Err(Error::ExpectedBulkString);
        }

        let len = self.parse_length()?;

        self.consume_crlf()?;

        let s = &self.input[..len];
        self.input = &self.input[len..];

        self.consume_crlf()?;

        Ok(Resp::BulkString(s.to_string()))
    }

    fn deserialize_list(&mut self) -> Result<Resp> {
        if self.input.starts_with("*-1\r\n") {
            return self.deserialize_null_array();
        }

        if self.next_char()? != '*' {
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
        if self.input.starts_with("*-1\r\n") {
            self.input = &self.input["*-1\r\n".len()..];
            println!("{}", self.input);
            return Ok(Resp::NullArray);
        }
        Err(Error::ExpectedNullArray)
    }
}

#[cfg(test)]
mod tests {
    use crate::{Resp, Error};

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
        let expected = "Couldn't parse bytes to string".to_owned();

        assert_eq!(res.to_string(), expected);
    }
}
