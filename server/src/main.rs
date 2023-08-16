use eyre::Result;
use kvs::KvStore;
use log::{debug, error, info, trace};
use resp::{Resp, SerializeResp};
use std::{
    env::current_dir,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncWriteExt, BufReader, BufWriter},
    net::{TcpListener, TcpStream},
};

enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let mut path = current_dir()?;
    path.push("data");

    info!("Opening KvStore at {}", path.to_str().unwrap());
    let kvs = Arc::new(Mutex::new(KvStore::open(path)?));

    let addr = "127.0.0.1:6379";

    info!("Starting TcpListener at {}", addr);
    let listener = TcpListener::bind(addr)
        .await
        .expect(format!("Failed to bind TcpListener to {}", addr).as_str());

    loop {
        let stream = match listener.accept().await {
            Ok((stream, addr)) => {
                debug!("Received client request from {}", addr);
                stream
            }
            Err(e) => {
                error!("Failed to open stream: {}", e);
                continue;
            }
        };

        process(stream, kvs.clone()).await?;
    }
}

async fn process(stream: TcpStream, kvs: Arc<Mutex<KvStore>>) -> Result<()> {
    tokio::spawn(async {
        let peer = stream
            .peer_addr()
            .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0000));
        info!("Received request from {}", peer);

        match handle_connection(stream, kvs).await {
            Ok(()) => (),
            Err(e) => {
                error!("Connection handler for {} failed: {}", peer, e);
            }
        };
    });

    Ok(())
}

async fn handle_connection(mut stream: TcpStream, kvs: Arc<Mutex<KvStore>>) -> Result<()> {
    trace!("creating Reader and Writer");

    let reader = BufReader::new(&mut stream);

    trace!("Reading stream into buffer");

    let resp = Resp::from_reader_async(reader).await?;

    let writer = BufWriter::new(&mut stream);

    match resp {
        Resp::Array(arr) => {
            debug!("Received array request {:?}", arr);
            let cmd = read_cmd(arr);
            let resp = exec_cmd(cmd, kvs)?;
            send_reply(resp, writer).await?;
        }
        Resp::BulkString(str) => {
            debug!("Received BulkString request {:?}", str);
            let reply = Resp::Error("Invalid: BulkStrings not acceptable as commands".to_string());
            send_reply(reply, writer).await?;
        }
        Resp::SimpleString(str) => {
            trace!("Received SimpleString request {:?}", str);
            if str != "PING" {
                debug!("Command {} is invalid", Resp::SimpleString(str));
                let reply = Resp::Error("Invalid command".to_string());
                send_reply(reply, writer).await?;
            } else {
                debug!("Received ping, ponging");
                let reply = Resp::SimpleString("PONG".to_string());
                send_reply(reply, writer).await?;
            }
        }
        _ => {
            let reply = Resp::Error("Invalid command".to_string());
            send_reply(reply, writer).await?;
            debug!("Received non-array, non-string request, sent invalid command error");
        }
    }

    Ok(())
}

async fn send_reply<W: AsyncWriteExt + Unpin>(resp: Resp, mut writer: W) -> Result<()> {
    writer.write(resp.serialize().as_bytes()).await?;
    writer.flush().await?;

    trace!("Sent reply {}", resp);
    Ok(())
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
            debug!("Command length not 2 or 3 invalid");
            None
        }
    }
}

fn exec_cmd(cmd: Option<Command>, kvs: Arc<Mutex<KvStore>>) -> Result<Resp> {
    match cmd {
        Some(Command::Set { key, value }) => {
            debug!("Setting {} to {}", &key, &value);

            if let Ok(()) = kvs.lock().unwrap().set(key.clone(), value.clone()) {
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
            if let Ok(Some(value)) = kvs.lock().unwrap().get(key.clone()) {
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
            if let Ok(()) = kvs.lock().unwrap().remove(key.clone()) {
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
