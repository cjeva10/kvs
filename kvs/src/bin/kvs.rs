use clap::{Parser, Subcommand};
use eyre::Result;
use kvs::KvStore;
use std::{env::current_dir, process::exit};

#[derive(Parser)] // requires `derive` feature
#[command(name = "kvs")]
#[command(about = "A simple key-value store", bin_name = "kvs", author, version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
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
    Rm {
        #[arg(value_name = "key")]
        key: String,
    },
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let mut store = KvStore::open(current_dir()?)?;

    match args.command {
        Commands::Set { key, value } => {
            store.set(key, value)?;
            Ok(())
        }
        Commands::Get { key } => {
            let value = store.get(key)?;

            match value {
                Some(value) => println!("{}", value),
                None => println!("Key not found"),
            }

            Ok(())
        }
        Commands::Rm { key } => {
            let value = store.get(key.clone())?;

            match value {
                Some(_) => store.remove(key)?,
                None => {
                    println!("Key not found");
                    exit(1)
                },
            }
            
            Ok(())
        }
    }
}
