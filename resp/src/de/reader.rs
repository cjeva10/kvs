use crate::{de::ParseResp, Error, Resp, Result};
use async_trait::async_trait;
use std::{collections::VecDeque, str::from_utf8};
use tokio::io::AsyncReadExt;

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

pub struct ReaderParser<R: AsyncReadExt + Unpin + Sync + Send> {
    reader: R,
    buf: VecDeque<u8>,
}

impl<R: AsyncReadExt + Unpin + Sync + Send> ReaderParser<R> {
    pub fn from_reader(reader: R) -> Self {
        ReaderParser {
            reader,
            buf: VecDeque::new(),
        }
    }

    pub fn is_buf_empty(&self) -> bool {
        self.buf.is_empty()
    }

    async fn peek_char(&mut self) -> Result<u8> {
        self.fill_buf().await?;

        Ok(self.buf.front().copied().unwrap())
    }

    async fn next_char(&mut self) -> Result<u8> {
        self.fill_buf().await?;

        Ok(self.buf.pop_front().unwrap())
    }

    async fn fill_buf(&mut self) -> Result<()> {
        if self.buf.len() == 0 {
            let mut tmp = [0; 128];
            let n = match self.reader.read(&mut tmp).await {
                Ok(n) => n,
                Err(_) => return Err(Error::ReaderFailed),
            };
            if n == 0 {
                return Err(Error::Eof);
            }
            for i in 0..n {
                self.buf.push_back(tmp[i]);
            }
        }

        Ok(())
    }

    async fn consume_crlf(&mut self) -> Result<()> {
        if self.next_char().await? != b'\r' {
            return Err(Error::ExpectedCRLF);
        }
        if self.next_char().await? != b'\n' {
            return Err(Error::ExpectedCRLF);
        }

        Ok(())
    }

    async fn parse_length(&mut self) -> Result<usize> {
        let mut int: usize = match self.next_char().await? {
            ch @ b'0'..=b'9' => (ch - b'0') as usize,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.peek_char().await? {
                ch @ b'0'..=b'9' => {
                    self.next_char().await?;
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

#[async_trait]
impl<R: AsyncReadExt + Send + Unpin + Sync> ParseResp for ReaderParser<R> {
    async fn parse_any(&mut self) -> Result<Resp> {
        match self.peek_char().await? {
            b'$' => self.parse_bulk_string().await,
            b'+' => self.parse_simple_string().await,
            b'-' => self.parse_error().await,
            b'*' => self.parse_array().await,
            b':' => self.parse_integer().await,
            _ => Err(Error::InvalidPrefix),
        }
    }

    async fn parse_null(&mut self) -> Result<Resp> {
        let expected = b"$-1\r\n";

        for b in expected {
            if self.next_char().await? != *b {
                return Err(Error::ExpectedNull);
            }
        }

        Ok(Resp::Null)
    }

    async fn parse_error(&mut self) -> Result<Resp> {
        if self.next_char().await? != b'-' {
            return Err(Error::ExpectedError);
        }

        let mut b: Vec<u8> = Vec::new();
        loop {
            match self.next_char().await? {
                b'\r' => match self.next_char().await? {
                    b'\n' => return Ok(Resp::Error(from_utf8(&b)?.to_string())),
                    _ => return Err(Error::ExpectedCRLF),
                },
                x => {
                    b.push(x);
                }
            }
        }
    }

    async fn parse_simple_string(&mut self) -> Result<Resp> {
        if self.next_char().await? != b'+' {
            return Err(Error::ExpectedError);
        }

        let mut b: Vec<u8> = Vec::new();
        loop {
            match self.next_char().await? {
                b'\r' => match self.next_char().await? {
                    b'\n' => return Ok(Resp::SimpleString(from_utf8(&b)?.to_string())),
                    _ => return Err(Error::ExpectedCRLF),
                },
                x => {
                    b.push(x);
                }
            }
        }
    }

    async fn parse_integer(&mut self) -> Result<Resp> {
        if self.next_char().await? != b':' {
            return Err(Error::ExpectedError);
        }

        let mut neg = false;
        if self.peek_char().await? == b'-' {
            self.next_char().await?;
            neg = true;
        }

        let mut int: i64 = match self.next_char().await? {
            ch @ b'0'..=b'9' => (ch - b'0') as i64,
            _ => {
                return Err(Error::ExpectedInteger);
            }
        };

        loop {
            match self.next_char().await? {
                ch @ b'0'..=b'9' => {
                    int *= 10;
                    int += (ch as u8 - b'0') as i64;
                }
                b'\r' => {
                    if self.next_char().await? != b'\n' {
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

    async fn parse_bulk_string(&mut self) -> Result<Resp> {
        self.peek_char().await?;

        if self.buf.starts_with(b"$-1\r\n") {
            return self.parse_null().await;
        }

        if self.next_char().await? != b'$' {
            return Err(Error::ExpectedBulkString);
        }

        let len = self.parse_length().await?;

        self.consume_crlf().await?;

        let b: Vec<u8> = self.buf.drain(..len).collect();

        self.consume_crlf().await?;

        Ok(Resp::BulkString(from_utf8(&b)?.to_string()))
    }

    async fn parse_array(&mut self) -> Result<Resp> {
        if self.buf.starts_with(b"*-1\r\n") {
            return self.parse_null_array().await;
        }

        if self.next_char().await? != b'*' {
            return Err(Error::ExpectedArray);
        }

        let len = self.parse_length().await?;

        self.consume_crlf().await?;

        let mut out: Vec<Resp> = Vec::new();

        for _ in 0..len {
            let next = self.parse_any().await?;
            out.push(next);
        }

        Ok(Resp::Array(out))
    }

    async fn parse_null_array(&mut self) -> Result<Resp> {
        if self.buf.starts_with(b"*-1\r\n") {
            for _ in 0..b"*-1\r\n".len() {
                self.next_char().await?;
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

    #[tokio::test]
    async fn test_bad_crlf() {
        let mut vec: Vec<u8> = Vec::from(b"\rabc\n".to_owned());
        let reader = Cursor::new(&mut vec);

        let mut deserializer = ReaderParser::from_reader(reader);

        let h = deserializer.consume_crlf().await.err().unwrap();

        assert_eq!(h, Error::ExpectedCRLF);
    }

    #[tokio::test]
    async fn test_reader_peek_char() {
        let mut vec: Vec<u8> = Vec::from(b"hello world\n".to_owned());
        let reader = Cursor::new(&mut vec);

        let mut deserializer = ReaderParser::from_reader(reader);

        let h = deserializer.peek_char().await.unwrap();

        assert_eq!(h, b'h');
    }

    #[tokio::test]
    async fn test_reader_next_char() {
        let input = b"hello world\n".to_owned();
        let reader = Cursor::new(input);

        let mut deserializer = ReaderParser::from_reader(reader);

        for i in 0..b"hello world\n".len() {
            let h: u8 = deserializer.next_char().await.unwrap();

            assert_eq!(deserializer.buf, input[i + 1..].to_owned());
            assert_eq!(h, input[i]);
        }

        assert!(deserializer.is_buf_empty());
    }

    #[tokio::test]
    async fn test_parse_simple_string() {
        let input = b"+hello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_simple_string().await.unwrap();

        assert_eq!(res, Resp::SimpleString("hello world".to_owned()));
    }

    #[tokio::test]
    async fn test_parse_error() {
        let input = b"-hello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_error().await.unwrap();

        assert_eq!(res, Resp::Error("hello world".to_owned()));
    }

    #[tokio::test]
    async fn test_parse_integer() {
        let input = b":10\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_integer().await.unwrap();

        assert_eq!(res, Resp::Integer(10));
    }

    #[tokio::test]
    async fn test_parse_bad_integer() {
        let pos = b":10abc\r\n";
        let err = Resp::from_reader(&pos[..]).await.err().unwrap();
        assert_eq!(err, Error::ExpectedInteger)
    }

    #[tokio::test]
    async fn test_parse_bulk_string() {
        let input = b"$11\r\nhello world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().await.unwrap();

        let res = deserializer.parse_bulk_string().await.unwrap();

        assert_eq!(res, Resp::BulkString("hello world".to_owned()));
    }

    #[tokio::test]
    async fn test_parse_bad_length() {
        let s = b"$5abc\r\nhello\r\n";
        let err = Resp::from_reader(&s[..]).await.err().unwrap();
        assert_eq!(err, Error::ExpectedLength)
    }

    #[tokio::test]
    async fn test_parse_array() {
        let input = b"*2\r\n+hello\r\n+world\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().await.unwrap();

        let res = deserializer.parse_array().await.unwrap();

        let expected = Resp::Array(vec![
            Resp::SimpleString("hello".to_owned()),
            Resp::SimpleString("world".to_owned()),
        ]);

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_parse_nested_array() {
        let input = b"*2\r\n*2\r\n+hello\r\n+world\r\n:10\r\n";

        let mut deserializer = ReaderParser::from_reader(&input[..]);

        // peek first to fill the buffer
        deserializer.peek_char().await.unwrap();
        let res = deserializer.parse_array().await.unwrap();

        let expected = Resp::Array(vec![
            Resp::Array(vec![
                Resp::SimpleString("hello".to_owned()),
                Resp::SimpleString("world".to_owned()),
            ]),
            Resp::Integer(10),
        ]);

        assert_eq!(res, expected);
    }
    #[tokio::test]
    async fn test_weird_characters() {
        let input = b"++//$$-+*\n\t\n !@#$%^&*()_\\  \r\n";
        let mut deserializer = ReaderParser::from_reader(&input[..]);

        let res = deserializer.parse_simple_string().await.unwrap();

        let expected = Resp::SimpleString("+//$$-+*\n\t\n !@#$%^&*()_\\  ".to_owned());

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_invalid_prefix() {
        let input = b"bad";
        let res = Resp::from_reader(&input[..]).await.err().unwrap();
        let expected = Error::InvalidPrefix;

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_parse_bytes() {
        let bytes = b"+hello world\r\n";
        let res = Resp::from_reader(&bytes[..]).await.unwrap();
        let expected = Resp::SimpleString("hello world".to_owned());

        assert_eq!(res, expected);
    }

    #[tokio::test]
    async fn test_bad_bytes() {
        let shift_jis = b"\x82\xe6\x82\xa8\x82\xb1\x82\xbb";
        let res = Resp::from_reader(&shift_jis[..]).await.err().unwrap();
        let expected = "encountered an invalid prefix".to_owned();

        assert_eq!(res.to_string(), expected);
    }
}
