use crate::{de::RespParser, Error, Resp, Result};
use bstr::ByteSlice;
use std::str::from_utf8;

pub struct Deserializer<'de> {
    input: &'de [u8],
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

impl<'de> RespParser for Deserializer<'de> {
    fn parse_any(&mut self) -> Result<Resp> {
        match self.peek_char()? {
            b'$' => self.parse_bulk_string(),
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b'*' => self.parse_list(),
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

    fn parse_list(&mut self) -> Result<Resp> {
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
