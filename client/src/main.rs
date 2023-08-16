use clap::{Parser, Subcommand};
use eyre::Result;
use log::debug;
use resp::{Resp, SerializeResp};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, BufReader, BufWriter},
    net::TcpStream,
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

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    debug!("parsing cli commands");
    let args = Cli::parse();

    let host = "localhost:6379";
    debug!("Connecting to server on {}", host);

    let mut stream = TcpStream::connect(host)
        .await
        .expect("Failed to connect to Tcp host");

    let writer = BufWriter::new(&mut stream);

    send_command(args.command, writer).await?;

    let reader = BufReader::new(&mut stream);

    read_response(reader).await?;

    Ok(())
}

async fn send_command(command: Command, mut writer: impl AsyncWriteExt + Unpin) -> Result<()> {
    debug!("Sending command {:?}", command);
    match command {
        Command::Set { key, value } => {
            let set = Resp::Array(vec![
                Resp::BulkString("SET".to_string()),
                Resp::BulkString(key.clone()),
                Resp::BulkString(value.clone()),
            ]);

            debug!("Sending SET {} {}", key, value);
            writer.write_all(set.serialize().as_bytes()).await?;
            debug!("Sent SET {} {}", key, value);
        }
        Command::Get { key } => {
            let get = Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending GET {}", key);
            writer.write_all(get.serialize().as_bytes()).await?;
            debug!("Sent GET {}", key);
        }
        Command::Remove { key } => {
            let rm = Resp::Array(vec![
                Resp::BulkString("REMOVE".to_string()),
                Resp::BulkString(key.clone()),
            ]);

            debug!("Sending REMOVE {}", key);
            writer.write_all(rm.serialize().as_bytes()).await?;
            debug!("Sent REMOVE {}", key);
        }
        Command::Ping => {
            let ping = Resp::SimpleString("PING".to_owned());
            debug!("Sending PING");

            writer.write_all(ping.serialize().as_bytes()).await?;
            debug!("Sent PING");
        }
    }

    writer.flush().await?;

    Ok(())
}

async fn read_response(reader: impl AsyncReadExt + Send + Sync + Unpin) -> Result<()> {
    debug!("Reading reply");

    let resp: Resp = Resp::from_reader(reader).await?;

    debug!("Converted response into RESP");

    debug!("Response = {}", resp);
    println!("{}", resp);

    Ok(())
}
