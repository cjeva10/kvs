use crate::{
    de::{ParseResp, StartsWith, StartsWithMut},
    Error, Resp, Result,
};
use log::trace;
use std::{collections::VecDeque, io::Read, str::from_utf8};

/// Synchronous parser for types that implement `std::io::Read`
///
/// `ReaderParser` implements `IntoIter`, which makes it easy to pull multiple
/// `Resp` values iteratively from a byte stream, for example when parsing a file.
///
/// To parse a single value, it is better to use `Resp::from_reader` directly
///
/// ## Examples
/// 
/// ```rust
/// use resp::{ReaderParser, Resp};
///
/// let input = b"+hello\r\n+world\r\n:10\r\n";
///
/// let mut iterator = ReaderParser::from_reader(&input[..]).into_iter();
///
/// let hello = iterator.next().unwrap();
/// assert_eq!(hello, Resp::SimpleString("hello".to_owned()));
///
/// let world = iterator.next().unwrap();
/// assert_eq!(world, Resp::SimpleString("world".to_owned()));
///
/// let ten = iterator.next().unwrap();
/// assert_eq!(ten, Resp::Integer(10));
/// ```
pub struct ReaderParser<R: Read> {
    reader: R,
    buf: VecDeque<u8>,
    offset: usize,
}

pub struct ReaderParserIter<R: Read> {
    inner: ReaderParser<R>,
}

impl<R: Read> ReaderParserIter<R> {
    pub fn byte_offset(&self) -> usize {
        self.inner.offset
    }
}

impl<R: Read> IntoIterator for ReaderParser<R> {
    type Item = Resp;
    type IntoIter = ReaderParserIter<R>;

    fn into_iter(self) -> Self::IntoIter {
        ReaderParserIter { inner: self }
    }
}

impl<R: Read> Iterator for ReaderParserIter<R> {
    type Item = Resp;

    fn next(&mut self) -> Option<Resp> {
        match self.inner.parse_any() {
            Ok(resp) => Some(resp),
            Err(_) => None,
        }
    }
}

impl<R: Read> StartsWithMut for ReaderParser<R> {
    fn starts_with_mut(&mut self, needle: &[u8]) -> Result<bool> {
        trace!("starts_with_mut({})", from_utf8(needle)?);
        if self.buf.len() < needle.len() {
            self.fill_buf()?
        }

        Ok(self.buf.starts_with(needle))
    }
}

impl<R: Read> ReaderParser<R> {
    pub fn from_reader(reader: R) -> Self {
        ReaderParser {
            reader,
            buf: VecDeque::new(),
            offset: 0,
        }
    }

    pub fn is_buf_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn peek_char(&mut self) -> Result<u8> {
        if self.buf.len() == 0 {
            self.fill_buf()?;
        }

        Ok(self.buf.front().copied().unwrap())
    }

    fn next_char(&mut self) -> Result<u8> {
        if self.buf.len() == 0 {
            self.fill_buf()?;
        }

        self.offset += 1;

        Ok(self.buf.pop_front().unwrap())
    }

    fn fill_buf(&mut self) -> Result<()> {
        let mut tmp = [0; 128];
        let n = match self.reader.read(&mut tmp) {
            Ok(n) => n,
            Err(_) => return Err(Error::ReaderFailed),
        };
        if n == 0 {
            return Err(Error::Eof);
        }
        for i in 0..n {
            self.buf.push_back(tmp[i]);
        }

        Ok(())
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
            match self.peek_char()? {
                ch @ b'0'..=b'9' => {
                    self.next_char()?;
                    int *= 10;
                    int += (ch - b'0') as usize;
                }
                b'\r' => {
                    return Ok(int);
                }
                _ => return Err(Error::ExpectedLength),
            }
        }
    }
}

impl<R: Read> ParseResp for ReaderParser<R> {
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
        let expected = b"$-1\r\n";

        for b in expected {
            if self.next_char()? != *b {
                return Err(Error::ExpectedNull);
            }
        }

        Ok(Resp::Null)
    }

    fn parse_error(&mut self) -> Result<Resp> {
        if self.next_char()? != b'-' {
            return Err(Error::ExpectedError);
        }

        let mut b: Vec<u8> = Vec::new();
        loop {
            match self.next_char()? {
                b'\r' => match self.next_char()? {
                    b'\n' => return Ok(Resp::Error(from_utf8(&b)?.to_string())),
                    _ => return Err(Error::ExpectedCRLF),
                },
                x => {
                    b.push(x);
                }
            }
        }
    }

    fn parse_simple_string(&mut self) -> Result<Resp> {
        if self.next_char()? != b'+' {
            return Err(Error::ExpectedError);
        }

        let mut b: Vec<u8> = Vec::new();
        loop {
            match self.next_char()? {
                b'\r' => match self.next_char()? {
                    b'\n' => return Ok(Resp::SimpleString(from_utf8(&b)?.to_string())),
                    _ => return Err(Error::ExpectedCRLF),
                },
                x => {
                    b.push(x);
                }
            }
        }
    }

    fn parse_integer(&mut self) -> Result<Resp> {
        if self.next_char()? != b':' {
            return Err(Error::ExpectedError);
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
            match self.next_char()? {
                ch @ b'0'..=b'9' => {
                    int *= 10;
                    int += (ch as u8 - b'0') as i64;
                }
                b'\r' => {
                    if self.next_char()? != b'\n' {
                        return Err(Error::ExpectedCRLF);
                    }

                    if neg {
                        return Ok(Resp::Integer(-int));
                    } else {
                        return Ok(Resp::Integer(int));
                    }
                }
                _ => return Err(Error::ExpectedInteger),
            }
        }
    }

    fn parse_bulk_string(&mut self) -> Result<Resp> {
        if self.starts_with_mut(b"$-1\r\n")? {
            return self.parse_null();
        }

        if self.next_char()? != b'$' {
            return Err(Error::ExpectedBulkString);
        }

        let len = self.parse_length()?;

        self.consume_crlf()?;

        trace!("parse bulk string: self = {:?}, len = {}", self.buf.iter().map(|x| *x as char).collect::<Vec<char>>(), len);
        // Note this doesn't work because if len > buf.len() we are out of bounds
        // let b: Vec<u8> = self.buf.drain(..len).collect();
        let mut b = Vec::new();
        for _ in 0..len {
            b.push(self.next_char()?);
        }

        self.consume_crlf()?;

        Ok(Resp::BulkString(from_utf8(&b)?.to_string()))
    }

    fn parse_array(&mut self) -> Result<Resp> {
        if self.starts_with_mut(b"*-1\r\n")? {
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
        if self.starts_with_mut(b"*-1\r\n")? {
            for _ in 0..b"*-1\r\n".len() {
                self.next_char()?;
            }
            return Ok(Resp::NullArray);
        }
        Err(Error::ExpectedNullArray)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_bad_crlf() {
        let mut vec: Vec<u8> = Vec::from(b"\rabc\n".to_owned());
        let reader = Cursor::new(&mut vec);

        let mut deserializer = ReaderParser::from_reader(reader);

        let h = deserializer.consume_crlf().err().unwrap();

        assert_eq!(h, Error::ExpectedCRLF);
    }

    #[test]
    fn test_reader_peek_char() {
        let mut vec: Vec<u8> = Vec::from(b"hello world\n".to_owned());
        let reader = Cursor::new(&mut vec);

        let mut deserializer = ReaderParser::from_reader(reader);

        let h = deserializer.peek_char().unwrap();

        assert_eq!(h, b'h');
    }

    #[test]
    fn test_reader_next_char() {
        let input = b"hello world\n".to_owned();
        let reader = Cursor::new(input);

        let mut deserializer = ReaderParser::from_reader(reader);

        for i in 0..b"hello world\n".len() {
            let h: u8 = deserializer.next_char().unwrap();

            assert_eq!(deserializer.buf, input[i + 1..].to_owned());
            assert_eq!(h, input[i]);
        }

        assert!(deserializer.is_buf_empty());
    }

    #[test]
    fn test_parse_simple_string() {
        let input = b"+hello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_simple_string().unwrap();

        assert_eq!(res, Resp::SimpleString("hello world".to_owned()));
    }

    #[test]
    fn test_parse_error() {
        let input = b"-hello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_error().unwrap();

        assert_eq!(res, Resp::Error("hello world".to_owned()));
    }

    #[test]
    fn test_parse_integer() {
        let input = b":10\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_integer().unwrap();

        assert_eq!(res, Resp::Integer(10));
    }

    #[test]
    fn test_parse_bad_integer() {
        let pos = b":10abc\r\n";
        let err = Resp::from_reader(&pos[..]).err().unwrap();
        assert_eq!(err, Error::ExpectedInteger)
    }

    #[test]
    fn test_parse_bulk_string() {
        let input = b"$11\r\nhello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().unwrap();

        let res = deserializer.parse_bulk_string().unwrap();

        assert_eq!(res, Resp::BulkString("hello world".to_owned()));
    }

    #[test]
    fn test_parse_bad_length() {
        let s = b"$5abc\r\nhello\r\n";
        let err = Resp::from_reader(&s[..]).err().unwrap();
        assert_eq!(err, Error::ExpectedLength)
    }

    #[test]
    fn test_parse_array() {
        let input = b"*2\r\n+hello\r\n+world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().unwrap();

        let res = deserializer.parse_array().unwrap();

        let expected = Resp::Array(vec![
            Resp::SimpleString("hello".to_owned()),
            Resp::SimpleString("world".to_owned()),
        ]);

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_nested_array() {
        let input = b"*2\r\n*2\r\n+hello\r\n+world\r\n:10\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().unwrap();
        let res = deserializer.parse_array().unwrap();

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
        let input = b"++//$$-+*\n\t\n !@#$%^&*()_\\  \r\n";
        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_simple_string().unwrap();

        let expected = Resp::SimpleString("+//$$-+*\n\t\n !@#$%^&*()_\\  ".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_invalid_prefix() {
        let input = b"bad";
        let res = Resp::from_reader(&input[..]).err().unwrap();
        let expected = Error::InvalidPrefix;

        assert_eq!(res, expected);
    }

    #[test]
    fn test_parse_bytes() {
        let bytes = b"+hello world\r\n";
        let res = Resp::from_reader(&bytes[..]).unwrap();
        let expected = Resp::SimpleString("hello world".to_owned());

        assert_eq!(res, expected);
    }

    #[test]
    fn test_bad_bytes() {
        let shift_jis = b"\x82\xe6\x82\xa8\x82\xb1\x82\xbb";
        let res = Resp::from_reader(&shift_jis[..]).err().unwrap();
        let expected = "encountered an invalid prefix".to_owned();

        assert_eq!(res.to_string(), expected);
    }

    #[test]
    fn test_iterators() {
        let input = b"+hello\r\n+hello\r\n+hello\r\n";
        let mut res = ReaderParser::from_reader(&input[..]).into_iter();

        assert_eq!(res.next().unwrap(), Resp::SimpleString("hello".to_owned()));
        assert_eq!(res.next().unwrap(), Resp::SimpleString("hello".to_owned()));
        assert_eq!(res.next().unwrap(), Resp::SimpleString("hello".to_owned()));

        let input = b"+hello\r\n:10\r\n$5\r\nhello\r\n*2\r\n:10\r\n:10\r\n";
        let mut res = ReaderParser::from_reader(&input[..]).into_iter();

        assert_eq!(res.next().unwrap(), Resp::SimpleString("hello".to_owned()));
        assert_eq!(res.next().unwrap(), Resp::Integer(10));
        assert_eq!(res.next().unwrap(), Resp::BulkString("hello".to_owned()));
        assert_eq!(
            res.next().unwrap(),
            Resp::Array(vec![Resp::Integer(10), Resp::Integer(10),])
        );
    }

    #[test]
    fn test_offset() {
        let input = b"+hello\r\n:10\r\n*2\r\n$2\r\nRM\r\n$5\r\nhello\r\n";
        let mut res = ReaderParser::from_reader(&input[..]).into_iter();

        let mut expected = 0;

        assert_eq!(res.byte_offset(), expected);

        let _ = res.next().unwrap();
        expected += b"+hello\r\n".len();

        assert_eq!(res.byte_offset(), expected);

        let _ = res.next().unwrap();
        expected += b":10\r\n".len();

        assert_eq!(res.byte_offset(), expected);

        let _ = res.next().unwrap();
        expected = input.len();

        assert_eq!(res.byte_offset(), expected);
    }
}
