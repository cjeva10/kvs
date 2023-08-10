use std::{
    io::{BufReader, Read},
    net::{TcpListener, TcpStream},
};
use resp::Resp; 
use eyre::Result;

fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        handle_connection(stream)?;
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) -> Result<()>{
    let mut reader = BufReader::new(&mut stream);
    let mut buf: String = String::new();

    let _ = reader.read_to_string(&mut buf)?;

    let resp = Resp::from_str(&buf)?;

    println!("{}", resp);

    Ok(())
}
