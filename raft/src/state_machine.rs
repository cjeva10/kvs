use crate::{Error, Result};
use kvs::KvEngine;
use kvs::KvStore;
use log::debug;
use resp::{Resp, SerializeResp};
use std::fmt::Display;

/// A state machine has an interior `Cmd` type that defines the type of commands that it receives
/// and implemennts `apply` which simply applies the given command to the machine
pub trait StateMachine<C: TryFrom<String>> {
    fn apply(&self, command: &C) -> Result<String>;
}

/// `DummyStateMachine` simply takes `String` commands and prints them to stdout.
pub struct DummyStateMachine {}

impl StateMachine<String> for DummyStateMachine {
    fn apply(&self, command: &String) -> Result<String> {
        println!("DummyStateMachine applying {}", command);

        Ok("".to_string())
    }
}

#[derive(Clone, Debug, Default)]
pub enum KvCommand {
    Set {
        key: String,
        value: String,
    },
    Get {
        key: String,
    },
    Remove {
        key: String,
    },
    #[default]
    Ping,
}

impl Display for KvCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Set { key, value } => write!(f, "[SET, {}, {}]", key, value),
            Self::Get { key } => write!(f, "[GET, {}]", key),
            Self::Remove { key } => write!(f, "[REMOVE, {}]", key),
            Self::Ping => write!(f, "PING"),
        }
    }
}

pub trait Serialize {
    fn serialize(&self) -> String;
}

impl Serialize for KvCommand {
    fn serialize(&self) -> String {
        match self {
            Self::Set { key, value } => Resp::Array(vec![
                Resp::BulkString("SET".to_string()),
                Resp::BulkString(key.to_string()),
                Resp::BulkString(value.to_string()),
            ])
            .serialize(),
            Self::Get { key } => Resp::Array(vec![
                Resp::BulkString("GET".to_string()),
                Resp::BulkString(key.to_string()),
            ])
            .serialize(),
            Self::Remove { key } => Resp::Array(vec![
                Resp::BulkString("RM".to_string()),
                Resp::BulkString(key.to_string()),
            ])
            .serialize(),
            Self::Ping => Resp::SimpleString("PING".to_string()).serialize(),
        }
    }
}

impl Serialize for String {
    fn serialize(&self) -> String {
        self.to_string()
    }
}

impl TryFrom<&str> for KvCommand {
    type Error = Error;

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let resp: Resp = Resp::try_from(value).map_err(|_| Error::ParseError)?;

        let command: KvCommand = KvCommand::try_from(resp)?;

        Ok(command)
    }
}

impl TryFrom<String> for KvCommand {
    type Error = Error;

    fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
        KvCommand::try_from(&value[..])
    }
}

impl TryFrom<Resp> for KvCommand {
    type Error = Error;

    fn try_from(resp: Resp) -> std::result::Result<Self, Self::Error> {
        match resp {
            Resp::Array(arr) => {
                debug!("Received array request {:?}", arr);
                read_cmd(arr)
            }
            Resp::SimpleString(str) => {
                if str == "PING" {
                    debug!("Received ping");
                    Ok(KvCommand::Ping)
                } else {
                    debug!("Command {} is invalid", Resp::SimpleString(str));
                    Err(Error::ParseError)
                }
            }
            _ => {
                debug!("Received non-array, non-string request {:?}", resp);
                Err(Error::ParseError)
            }
        }
    }
}

fn read_cmd(arr: Vec<Resp>) -> Result<KvCommand> {
    match arr.len() {
        2 => {
            let Resp::BulkString(name) = &arr[0] else {
                    debug!("Command value not a BulkString: got {:?}", arr[0]);
                    return Err(Error::ParseError);
                };
            let Resp::BulkString(key) = &arr[1] else {
                    debug!("Command value not a BulkString: got {:?}", arr[1]);
                    return Err(Error::ParseError);
                };
            if name == "GET" {
                debug!("Got GET {}", key);
                return Ok(KvCommand::Get {
                    key: key.to_string(),
                });
            }
            if name == "RM" {
                debug!("Got RM {}", key);
                return Ok(KvCommand::Remove {
                    key: key.to_string(),
                });
            }

            debug!("Invalid Command {} {}", name, key);

            Err(Error::ParseError)
        }
        3 => {
            let Resp::BulkString(name) = &arr[0] else {
                    debug!("Command value not a BulkString: got {:?}", arr[0]);
                    return Err(Error::ParseError);
                };
            let Resp::BulkString(key) = &arr[1] else {
                    debug!("Command value not a BulkString: got {:?}", arr[1]);
                    return Err(Error::ParseError);
                };
            let Resp::BulkString(value) = &arr[2] else {
                    debug!("Command value not a BulkString: got {:?}", arr[2]);
                    return Err(Error::ParseError);
                };

            if name != "SET" {
                debug!("Invalid command name {}", name);
                return Err(Error::ParseError);
            }

            return Ok(KvCommand::Set {
                key: key.to_string(),
                value: value.to_string(),
            });
        }
        _ => {
            debug!("Command length not 2 or 3 invalid");
            return Err(Error::ParseError);
        }
    }
}

pub struct KvStateMachine {
    store: KvStore,
}

impl KvStateMachine {
    pub fn new(store: KvStore) -> Self {
        Self { store }
    }
}

impl StateMachine<KvCommand> for KvStateMachine {
    fn apply(&self, command: &KvCommand) -> Result<String> {
        match command {
            KvCommand::Set { key, value } => {
                self.store.set(key.to_string(), value.to_string())?;
                Ok(Resp::SimpleString("OK".to_string()).to_string())
            }
            KvCommand::Get { key } => match self.store.get(key.to_string())? {
                Some(str) => Ok(Resp::BulkString(str).to_string()),
                None => Ok(Resp::Error(format!("{} not found", key)).to_string()),
            },
            KvCommand::Remove { key } => match self.store.remove(key.to_string()) {
                Ok(_) => Ok(Resp::SimpleString("OK".to_string()).to_string()),
                Err(_) => Ok(Resp::Error(format!("{} not found", key)).to_string()),
            },
            KvCommand::Ping => Ok(Resp::SimpleString("PONG".to_string()).to_string()),
        }
    }
}
