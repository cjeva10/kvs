use eyre::Result;
use kvs::KvStore;
use log::{debug, error, info, trace};
use resp::Resp;
use std::{
    env::current_dir,
    io::{BufReader, BufWriter, Read, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
};

enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

fn main() -> Result<()> {
    env_logger::init();

    let path = current_dir()?;

    info!("Opening KvStore at {}", path.to_str().unwrap());
    let mut kvs = KvStore::open(current_dir()?)?;

    let addr = "127.0.0.1:6379";

    info!("Starting TcpListener at {}", addr);
    let listener = TcpListener::bind(addr).unwrap();

    for stream in listener.incoming() {
        debug!("Received client request");

        let stream = match stream {
            Ok(stream) => stream,
            Err(e) => {
                error!("Failed to open stream: {}", e);
                continue;
            }
        };

        let peer = stream
            .peer_addr()
            .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0000));

        info!("Received request from {}", peer);
        match handle_connection(stream, &mut kvs) {
            Ok(()) => (),
            Err(e) => {
                error!("Connection handler for {} failed: {}", peer, e);
            }
        };
    }

    Ok(())
}

fn handle_connection(stream: TcpStream, kvs: &mut KvStore) -> Result<()> {
    trace!("creating Reader and Writer");

    let writer = BufWriter::new(stream.try_clone()?);
    let reader = BufReader::new(stream);

    trace!("Reading stream into buffer");

    let resp = read_reader(reader)?;

    match resp {
        Resp::Array(arr) => {
            debug!("Received array request {:?}", arr);
            let cmd = read_cmd(arr);
            let resp = exec_cmd(cmd, kvs)?;
            send_reply(resp, writer)?;
        }
        Resp::BulkString(str) => {
            debug!("Received BulkString request {:?}", str);
            let reply = Resp::Error("Invalid: BulkStrings not acceptable as commands".to_string());
            send_reply(reply, writer)?;
        }
        Resp::SimpleString(str) => {
            trace!("Received SimpleString request {:?}", str);
            if str != "PING" {
                debug!("Command {} is invalid", Resp::SimpleString(str));
                let reply = Resp::Error("Invalid command".to_string());
                send_reply(reply, writer)?;
            } else {
                debug!("Received ping, ponging");
                let reply = Resp::SimpleString("PONG".to_string());
                send_reply(reply, writer)?;
            }
        }
        _ => {
            let reply = Resp::Error("Invalid command".to_string());
            send_reply(reply, writer)?;
            debug!("Received non-array, non-string request, sent invalid command error");
        }
    }

    Ok(())
}

fn send_reply(resp: Resp, mut writer: impl Write) -> Result<()> {
    writer.write(resp.to_string().as_bytes())?;
    writer.flush()?;
    trace!("Sent reply {}", resp);
    Ok(())
}

fn read_reader(reader: impl Read) -> Result<Resp> {
    trace!("Convert buffer into RESP");
    let resp: Resp = Resp::from_reader(reader)?;
    Ok(resp)
}

fn exec_cmd(cmd: Option<Command>, kvs: &mut KvStore) -> Result<Resp> {
    match cmd {
        Some(Command::Set { key, value }) => {
            debug!("Setting {} to {}", &key, &value);

            if let Ok(()) = kvs.set(key.clone(), value.clone()) {
                debug!("Set {} to {} successfully", key, value);
                let ok = Resp::SimpleString("OK".to_string());
                Ok(ok)
            } else {
                debug!("Failed to set {} to {}", key, value);
                let err = Resp::Error(format!("SET {} {} failed", key, value));
                Ok(err)
            }
        }
        Some(Command::Get { key }) => {
            debug!("Getting {}", key);
            if let Ok(Some(value)) = kvs.get(key.clone()) {
                debug!("Got {} = {}", key, value);
                let resp = Resp::BulkString(value);
                Ok(resp)
            } else {
                debug!("Failed to get {}", key);
                let err = Resp::Error(format!("GET {} failed", key));
                Ok(err)
            }
        }
        Some(Command::Remove { key }) => {
            debug!("Removing {}", key);
            if let Ok(()) = kvs.remove(key.clone()) {
                debug!("Removed {} successfully", key);
                let ok = Resp::SimpleString("OK".to_string());
                Ok(ok)
            } else {
                debug!("Failed to remove {}", key);
                let err = Resp::Error(format!("REMOVE {} failed", key));
                Ok(err)
            }
        }
        None => {
            debug!("Command was invalid");
            let err = Resp::Error("Invalid command".to_string());
            Ok(err)
        }
    }
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
