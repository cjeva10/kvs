use resp::{Resp, Result};

fn main() -> Result<()> {
    let s = "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";

    let resp = Resp::from_str(s)?;

    assert_eq!(s.to_string(), resp.to_string());

    Ok(())
}
