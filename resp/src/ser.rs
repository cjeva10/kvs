use crate::Resp;

impl Resp {
    fn serialize_simple_string(s: &str) -> String {
        let mut out: String = "+".to_string();
        out.push_str(s);
        out.push_str("\r\n");
        out
    }

    fn serialize_integer(int: i64) -> String {
        let mut out: String = ":".to_string();
        out.push_str(&int.to_string());
        out.push_str("\r\n");
        out
    }

    fn serialize_error(s: &str) -> String {
        let mut out: String = "-".to_string();
        out.push_str(s);
        out.push_str("\r\n");
        out
    }

    fn serialize_bulk_string(s: &str) -> String {
        let mut out: String = "$".to_string();
        let len = s.len();

        out.push_str(&len.to_string());
        out.push_str("\r\n");

        out.push_str(s);
        out.push_str("\r\n");
        out
    }

    fn serialize_list(list: &Vec<Resp>) -> String {
        let mut out = "*".to_string();
        let len = list.len();

        out.push_str(&len.to_string());
        out.push_str("\r\n");

        for l in list {
            out.push_str(&l.serialize())
        }

        out
    }
}

pub trait SerializeResp {
    fn serialize(&self) -> String;
}

impl SerializeResp for Resp {
    fn serialize(&self) -> String {
        match self {
            Resp::SimpleString(s) => Self::serialize_simple_string(s),
            Resp::BulkString(s) => Self::serialize_bulk_string(s),
            Resp::Error(s) => Self::serialize_error(s),
            Resp::Integer(int) => Self::serialize_integer(*int),
            Resp::Array(list) => Self::serialize_list(list),
            Resp::Null => "$-1\r\n".to_string(),
            Resp::NullArray => "*-1\r\n".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{Resp, ser::SerializeResp};

    #[test]
    fn test_printing() {
        let null = Resp::Null;
        let err = Resp::Error("error".to_string());
        let int = Resp::Integer(10);
        let simple = Resp::SimpleString("hello".to_string());
        let bulk = Resp::BulkString("hello".to_string());
        let list = Resp::Array(vec![
            Resp::Integer(10),
            Resp::BulkString("hello".to_string()),
        ]);

        println!("{}", null);
        println!("{}", err);
        println!("{}", int);
        println!("{}", simple);
        println!("{}", bulk);
        println!("{}", list);
    }

    #[test]
    fn test_serialize_null() {
        let null = Resp::Null;

        let s = null.serialize();

        assert_eq!(s, "$-1\r\n".to_string());
    }

    #[test]
    fn test_serialize_integer() {
        let int = Resp::Integer(10);

        let s = int.serialize();

        assert_eq!(s, ":10\r\n");
    }

    #[test]
    fn test_serialize_simple_string() {
        let s = Resp::SimpleString("hello".to_owned());

        let s = s.serialize();

        assert_eq!(s, "+hello\r\n");
    }

    #[test]
    fn test_serialize_bulk_string() {
        let s = Resp::BulkString("hello".to_owned());

        let s = s.serialize();

        assert_eq!(s, "$5\r\nhello\r\n");
    }

    #[test]
    fn test_serialize_error() {
        let s = Resp::Error("hello".to_owned());

        let s = s.serialize();

        assert_eq!(s, "-hello\r\n");
    }

    #[test]
    fn test_serialize_list() {
        let list = Resp::Array(vec![
            Resp::Integer(10),
            Resp::BulkString("hello".to_string()),
            Resp::Array(vec![
                Resp::Integer(5),
                Resp::SimpleString("world".to_string()),
            ]),
        ]);

        let s = list.serialize();

        let expected = "*3\r\n:10\r\n$5\r\nhello\r\n*2\r\n:5\r\n+world\r\n";

        assert_eq!(s, expected.to_string());
    }

    #[test]
    fn test_serialize_null_list() {
        let list = Resp::NullArray;

        let s = list.serialize();

        let expected = "*-1\r\n";

        assert_eq!(s, expected.to_string());
    }
}
