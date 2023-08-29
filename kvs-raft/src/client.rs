use clap::{Parser, Subcommand};
use eyre::Result;
use log::debug;
use log::error;
use raft::rpc::{raft_client::RaftClient, ClientRequestArgs};
use resp::{Resp, SerializeResp};
use std::net::SocketAddr;

#[derive(Parser)]
#[command(
    about = "A CLI client for sending commands to a kvs server running on Raft",
    bin_name = "kvs",
    version,
    name = "kvs-client",
    rename_all = "snake_case",
    author = "Chris E. <cjevanko@gmail.com>"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// The address + port of the db server
    #[arg(long, default_value = "127.0.0.1:50051")]
    host: SocketAddr,
}

#[derive(Debug, Subcommand, Clone)]
enum Command {
    /// Get value of a key
    #[command(arg_required_else_help = true)]
    Get {
        #[arg(value_name = "key")]
        key: String,
    },
    /// Set the key to a given value
    #[command(arg_required_else_help = true)]
    Set {
        #[arg(value_name = "key")]
        key: String,
        #[arg(value_name = "value")]
        value: String,
    },
    /// Remove a key
    #[command(arg_required_else_help = true)]
    Remove {
        #[arg(value_name = "key")]
        key: String,
    },
    /// Ping the db server host
    Ping,
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    debug!("parsing cli commands");
    let args = Cli::parse();

    let mut host = "http://".to_owned();
    host.push_str(&args.host.to_string());

    debug!("Connecting to server on {}", host);

    let Ok(mut client) = RaftClient::connect(host.clone()).await else {
        error!("Failed to connect to host {}", &host);
        std::process::exit(1);
    };

    match args.command {
        Command::Get { key } => {
            let args = ClientRequestArgs {
                command: Resp::Array(vec![
                    Resp::BulkString("GET".to_string()),
                    Resp::BulkString(key),
                ])
                .serialize(),
            };
            let response = client.client_request(args).await.unwrap();
            println!("RESPONSE = {:?}", response);
        }
        Command::Set { key, value } => {
            let args = ClientRequestArgs {
                command: Resp::Array(vec![
                    Resp::BulkString("SET".to_string()),
                    Resp::BulkString(key),
                    Resp::BulkString(value),
                ])
                .serialize(),
            };
            let response = client.client_request(args).await.unwrap();
            println!("RESPONSE = {:?}", response);
        }
        Command::Remove { key } => {
            let args = ClientRequestArgs {
                command: Resp::Array(vec![
                    Resp::BulkString("RM".to_string()),
                    Resp::BulkString(key),
                ])
                .serialize(),
            };
            let response = client.client_request(args).await.unwrap();
            println!("RESPONSE = {:?}", response);
        }
        Command::Ping => {
            let args = ClientRequestArgs {
                command: Resp::SimpleString("PING".to_string()).serialize(),
            };
            let response = client.client_request(args).await.unwrap();
            println!("RESPONSE = {:?}", response);
        }
    };

    Ok(())
}
