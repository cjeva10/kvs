use resp::{Resp, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let s = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
    let resp = Resp::from_reader(&s[..]).await?;

    assert_eq!(s, resp.to_string().as_bytes());

    Ok(())
}
