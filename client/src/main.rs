use clap::{Parser, Subcommand};
use eyre::Result;
use log::debug;
use resp::Resp;
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
    str::from_utf8,
};

#[derive(Parser)]
#[command(name = "kvs-client", rename_all = "snake_case")]
#[command(
    about = "A client for sending Redis messages over the wire",
    bin_name = "kvs",
    author,
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, Clone)]
enum Command {
    #[command(arg_required_else_help = true)]
    Get {
        #[arg(value_name = "key")]
        key: String,
    },
    #[command(arg_required_else_help = true)]
    Set {
        #[arg(value_name = "key")]
        key: String,
        #[arg(value_name = "value")]
        value: String,
    },
    #[command(arg_required_else_help = true)]
    Remove {
        #[arg(value_name = "key")]
        key: String,
    },
    Ping,
}

fn main() -> Result<()> {
    env_logger::init();

    debug!("parsing cli commands");
    let args = Cli::parse();

    let host = "localhost:6379";
    debug!("Connecting to server on {}", host);

    let reader = TcpStream::connect(host).expect("Failed to connect to Tcp host");
    let writer = reader.try_clone()?;

    let writer = BufWriter::new(writer);
    let reader = BufReader::new(reader);

    send_command(args.command, writer)?;

    read_response(reader)?;

    Ok(())
}

fn send_command(command: Command, mut writer: BufWriter<TcpStream>) -> Result<()> {
    debug!("Sending command {:?}", command);
    match command {
        Command::Set { key, value } => {
            let set = Resp::Array(vec![
                Resp::BulkString("SET".to_string()),
                Resp::BulkString(key.clone()),
                Resp::BulkString(value.clone()),
            ]);

            debug!("Sending SET {} {}", key, value);
            writer.write_all(set.to_string().as_bytes())?;
            debug!("Sent SET {} {}", key, value);
        }
        Command::Get { key } => {
            let get = Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending GET {}", key);
            writer.write_all(get.to_string().as_bytes())?;
            debug!("Sent GET {}", key);
        }
        Command::Remove { key } => {
            let rm = Resp::Array(vec![
                Resp::BulkString("REMOVE".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending REMOVE {}", key);
            writer.write_all(rm.to_string().as_bytes())?;
            debug!("Sent REMOVE {}", key);
        }
        Command::Ping => {
            let ping = Resp::SimpleString("PING".to_owned());
            debug!("Sending PING");
            writer.write_all(ping.to_string().as_bytes())?;
            debug!("Sent PING");
        }
    }

    writer.flush()?;

    Ok(())
}

fn read_response(mut reader: BufReader<TcpStream>) -> Result<()> {
    let mut response = [0; 1024];

    debug!("Reading reply");

    let n = reader.read(&mut response)?;

    debug!("Read response into buffer: {}", from_utf8(&response)?);

    let resp: Resp = Resp::from_bytes(&response[..n])?;

    debug!("Converted response into RESP");

    debug!("Response = {}", resp);

    Ok(())
}
