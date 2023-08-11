use clap::{Parser, Subcommand};
use resp::Resp;
use std::{
    io::{BufWriter, Write},
    net::TcpStream, process::exit,
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
    #[command(arg_required_else_help = true)]
    GetMultiple {
        #[arg(value_name = "keys")]
        keys: Vec<String>,
    },
    #[command(arg_required_else_help = true, )]
    SetMultiple {
        #[arg(value_name = "keys")]
        keys: Vec<String>,
        #[arg(value_name = "values")]
        values: Vec<String>,
    },
}

fn main() {
    let args = Cli::parse();

    let stream = TcpStream::connect("localhost:6379").expect("Failed to connect to Tcp host");
    let mut writer = BufWriter::new(stream);

    match args.command {
        Command::Set { key, value } => {
            let set = Resp::Array(vec![
                Resp::BulkString("SET".to_string()),
                Resp::BulkString(key),
                Resp::BulkString(value),
            ]);

            writer.write(set.to_string().as_bytes()).unwrap();
        }
        Command::Get { key } => {
            let get = Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key),
            ]);

            writer.write(get.to_string().as_bytes()).unwrap();
        }
        Command::Remove { key } => {
            let rm = Resp::Array(vec![
                Resp::BulkString("REMOVE".to_string()),
                Resp::BulkString(key),
            ]);

            writer.write(rm.to_string().as_bytes()).unwrap();
        }
        Command::Ping => {
            let ping = Resp::SimpleString("PING".to_owned());
            writer.write(ping.to_string().as_bytes()).unwrap();
        }
        Command::GetMultiple { keys } => {
            for key in keys {
                let get = Resp::Array(vec![
                    Resp::BulkString("GET".to_string()),
                    Resp::BulkString(key),
                ]);

                writer.write(get.to_string().as_bytes()).unwrap();
            }
        }
        Command::SetMultiple { keys, values } => {
            if keys.len() != values.len() {
                eprintln!("Must have same number of keys and values!");
                exit(1);
            }

            for (i, key) in keys.iter().enumerate() {
                let set = Resp::Array(vec![
                    Resp::BulkString("SET".to_string()),
                    Resp::BulkString(key.to_string()),
                    Resp::BulkString(values[i].to_string()),
                ]);

                writer.write(set.to_string().as_bytes()).unwrap();
            }
        }
    }
}
