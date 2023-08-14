use crate::{de::ParseResp, Error, Resp, Result};
use bstr::ByteSlice;
use std::str::from_utf8;

pub struct ByteParser<'de> {
    input: &'de [u8],
}

impl<'de> ByteParser<'de> {
    pub fn from_str(input: &'de str) -> Self {
        ByteParser {
            input: input.as_bytes(),
        }
    }

    pub fn from_bytes(input: &'de [u8]) -> Self {
        ByteParser { input }
    }

    pub fn is_empty(&self) -> bool {
        self.input.is_empty()
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

impl<'de> ParseResp for ByteParser<'de> {
    fn parse_any(&mut self) -> Result<Resp> {
        match self.peek_char()? {
            b'$' => self.parse_bulk_string(),
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b'*' => self.parse_array(),
            b':' => self.parse_integer(),
            _ => Err(Error::InvalidPrefix),
        }
    }

    fn parse_null(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"$-1\r\n") {
            self.input = &self.input[b"$-1\r\n".len()..];
            return Ok(Resp::Null);
        }
        Err(Error::ExpectedNull)
    }

    fn parse_error(&mut self) -> Result<Resp> {
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
    fn parse_simple_string(&mut self) -> Result<Resp> {
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

    fn parse_integer(&mut self) -> Result<Resp> {
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

    fn parse_bulk_string(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"$-1\r\n") {
            return self.parse_null();
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

    fn parse_array(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"*-1\r\n") {
            return self.parse_null_array();
        }

        if self.next_char()? != b'*' {
            return Err(Error::ExpectedArray);
        }

        let len = self.parse_length()?;

        self.consume_crlf()?;

        let mut out: Vec<Resp> = Vec::new();

        for _ in 0..len {
            let next = self.parse_any()?;
            out.push(next);
        }

        Ok(Resp::Array(out))
    }

    fn parse_null_array(&mut self) -> Result<Resp> {
        if self.input.starts_with(b"*-1\r\n") {
            self.input = &self.input["*-1\r\n".len()..];
            return Ok(Resp::NullArray);
        }
        Err(Error::ExpectedNullArray)
    }
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
