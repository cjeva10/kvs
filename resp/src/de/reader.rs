use crate::de::RespParser;
use crate::{Error, Result};
use crate::Resp;
use std::collections::VecDeque;
use std::io::Read;

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

pub struct ReaderDeserializer<R: Read> {
    reader: R,
    input: VecDeque<u8>,
}

impl<R: Read> ReaderDeserializer<R> {
    pub fn from_reader(reader: R) -> Self {
        ReaderDeserializer {
            reader,
            input: VecDeque::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.input.is_empty()
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
                return Err(Error::Eof);
            }
            for i in 0..n {
                self.input.push_back(buf[i]);
            }
        }

        Ok(self.input.drain(..count).collect())
    }
}

impl<R: Read> RespParser for ReaderDeserializer<R> {
    fn parse_any(&mut self) -> Result<Resp> {
        match self.peek_char(1)?.get(0).ok_or(Error::Eof)? {
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
            self.next_char(b"$-1\r\n".len())?;
            return Ok(Resp::Null);
        }
        Err(Error::ExpectedNull)
    }

    fn parse_error(&mut self) -> Result<Resp> {
        todo!()
    }
    fn parse_simple_string(&mut self) -> Result<Resp> {
        todo!()
    }

    fn parse_integer(&mut self) -> Result<Resp> {
        todo!()
    }

    fn parse_bulk_string(&mut self) -> Result<Resp> {
        todo!()
    }

    fn parse_list(&mut self) -> Result<Resp> {
        todo!()
    }

    fn parse_null_array(&mut self) -> Result<Resp> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;
    use super::*;

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
