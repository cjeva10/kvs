use clap::{Parser, Subcommand};
use eyre::Result;
use resp::Resp;
use log::debug;
use std::{
    io::{BufReader, BufWriter, Read, Write},
    net::TcpStream,
    process::exit,
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
    let mut stream = TcpStream::connect(host).expect("Failed to connect to Tcp host");
    let mut writer = BufWriter::new(&mut stream);

    debug!("Sending command {:?}", args.command);
    match args.command {
        Command::Set { key, value } => {
            let set = Resp::Array(vec![
                Resp::BulkString("SET".to_string()),
                Resp::BulkString(key.clone()),
                Resp::BulkString(value.clone()),
            ]);

            debug!("Sending SET {} {}", key, value);
            writer.write(set.to_string().as_bytes()).unwrap();
        }
        Command::Get { key } => {
            let get = Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending GET {}", key);
            writer.write(get.to_string().as_bytes()).unwrap();
        }
        Command::Remove { key } => {
            let rm = Resp::Array(vec![
                Resp::BulkString("REMOVE".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending REMOVE {}", key);
            writer.write(rm.to_string().as_bytes()).unwrap();
        }
        Command::Ping => {
            let ping = Resp::SimpleString("PING".to_owned());
            debug!("Sending PING");
            writer.write(ping.to_string().as_bytes()).unwrap();
        }
    }
    writer.flush()?;
    drop(writer);

    let mut reader = BufReader::new(&mut stream);
    let mut buf: String = String::new();

    let _ = reader.read_to_string(&mut buf)?;

    let resp: Resp = Resp::from_str(&buf)?;

    debug!("Got response {}", resp);

    Ok(())
}
