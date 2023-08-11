use eyre::Result;
use kvs::KvStore;
use log::debug;
use resp::Resp;
use std::{
    env::current_dir,
    io::{BufReader, BufWriter, Read, Write},
    net::{TcpListener, TcpStream},
};

fn main() -> Result<()> {
    env_logger::init();

    let path = current_dir()?;
    debug!("Opening KvStore at {}", path.to_str().unwrap());
    let mut kvs = KvStore::open(current_dir()?)?;

    let addr = "127.0.0.1:6379";
    debug!("Starting TcpListener at {}", addr);
    let listener = TcpListener::bind(addr).unwrap();

    for stream in listener.incoming() {
        debug!("Received client request");
        let stream = stream.unwrap();

        handle_connection(stream, &mut kvs)?;
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream, kvs: &mut KvStore) -> Result<()> {
    let mut reader = BufReader::new(&mut stream);
    let mut buf: String = String::new();

    let _ = reader.read_to_string(&mut buf)?;

    let resp: Resp = Resp::from_str(&buf)?;

    let mut writer = BufWriter::new(&mut stream);

    match resp {
        Resp::Array(arr) => {
            debug!("Received array request {:?}", arr);
            let cmd = read_cmd(arr);
            exec_cmd(cmd, kvs, writer)?;
        }
        Resp::BulkString(str) => {
            debug!("Received BulkString request {:?}", str);
        }
        Resp::SimpleString(str) => {
            debug!("Received SimpleString request {:?}", str);
        }
        _ => {
            let reply = Resp::Error("Invalid command".to_string()).to_string();
            writer.write(reply.as_bytes())?;
            debug!("Received non-array, non=-string request, sent invalid command error");
        }
    }

    Ok(())
}

fn exec_cmd(cmd: Option<Command>, kvs: &mut KvStore, mut writer: impl Write) -> Result<()>{
    match cmd {
        Some(Command::Set { key, value }) => {
            debug!("Setting {} to {}", key, value);
            if let Ok(()) = kvs.set(key.clone(), value.clone()) {
                debug!("Set {} to {} successfully", key, value);
                let ok = Resp::SimpleString("OK".to_string()).to_string();
                writer.write(ok.as_bytes())?;
            } else {
                debug!("Failed to set {} to {}", key, value);
                let err = Resp::Error(format!("SET {} {} failed", key, value)).to_string();
                writer.write(err.as_bytes())?;
            }
            Ok(())
        }
        Some(Command::Get { key }) => {
            debug!("Getting {}", key);
            if let Ok(Some(value)) = kvs.get(key.clone()) {
                debug!("Got {} = {}", key, value);
                let resp = Resp::BulkString(value).to_string();
                writer.write(resp.as_bytes())?;
            } else {
                debug!("Failed to get {}", key);
                let err = Resp::Error(format!("GET {} failed", key)).to_string();
                writer.write(err.as_bytes())?;
            }
            Ok(())
        }
        Some(Command::Remove { key }) => {
            debug!("Removing {}", key);
            if let Ok(()) = kvs.remove(key.clone()) {
                debug!("Removed {} successfully", key);
                let ok = Resp::SimpleString("OK".to_string()).to_string();
                writer.write(ok.as_bytes())?;
            } else {
                debug!("Failed to remove {}", key);
                let err = Resp::Error(format!("REMOVE {} failed", key)).to_string();
                writer.write(err.as_bytes())?;
            }
            Ok(())
        }
        None => {
            debug!("Command was invalid");
            let err = Resp::Error("Invalid command".to_string()).to_string();
            writer.write(err.as_bytes())?;
            Ok(())
        }
    }
}

enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

fn read_cmd(arr: Vec<Resp>) -> Option<Command> {
    match arr.len() {
        2 => {
            let Resp::BulkString(name) = &arr[0] else {
                    debug!("Command value not a BulkString: got {:?}", arr[0]);
                    return None;
                };
            let Resp::BulkString(key) = &arr[1] else {
                    debug!("Command value not a BulkString: got {:?}", arr[1]);
                    return None;
                };
            if name == "GET" {
                debug!("Got GET {}", key);
                return Some(Command::Get {
                    key: key.to_string(),
                });
            }
            if name == "REMOVE" {
                debug!("Got REMOVE {}", key);
                return Some(Command::Remove {
                    key: key.to_string(),
                });
            }

            debug!("Invalid Command {} {}", name, key);

            None
        }
        3 => {
            let Resp::BulkString(name) = &arr[0] else {
                    debug!("Command value not a BulkString: got {:?}", arr[0]);
                    return None;
                };
            let Resp::BulkString(key) = &arr[1] else {
                    debug!("Command value not a BulkString: got {:?}", arr[1]);
                    return None;
                };
            let Resp::BulkString(value) = &arr[2] else {
                    debug!("Command value not a BulkString: got {:?}", arr[2]);
                    return None;
                };

            if name != "SET" {
                debug!("Invalid command name {}", name);
                return None;
            }

            return Some(Command::Set {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
        _ => {
            debug!("Received invalid length command");
            None
        }
    }
}
