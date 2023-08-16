use crate::{KvEngine, KvsError, Result};
use log::info;
use resp::{Resp, SerializeResp};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

// compact when there is 1 MB compactable
const COMPACT_THRESHOLD: usize = 1024 * 1024;

/// A key-value store with sequential file log
#[derive(Clone)]
pub struct KvStore {
    inner: Arc<Mutex<InnerKvStore>>,
}

impl KvStore {
    /// Open the KvStore at the given file
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let inner = InnerKvStore::open(path)?;

        Ok(Self {
            inner: Arc::new(Mutex::new(inner)),
        })
    }
}

impl KvEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| KvsError::LockError)?
            .set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        self.inner.lock().map_err(|_| KvsError::LockError)?.get(key)
    }

    fn remove(&self, key: String) -> Result<()> {
        self.inner
            .lock()
            .map_err(|_| KvsError::LockError)?
            .remove(key)
    }
}

// Inner structure of a `KvStore`
//
// Note: Does not satisfy `KvsEngine`, therefore you should use `KvStore` which
// hides the inner state behind a `Arc<Mutex<InnerKvStore>>`
struct InnerKvStore {
    index: BTreeMap<String, LogPointer>,
    log_path: PathBuf,
    dir_path: PathBuf,
    compactable: usize,
    file_number: usize,
}

#[derive(Clone, Debug)]
struct LogPointer {
    offset: usize,
    size: usize,
}

enum LogCommand {
    Set { key: String, value: String },
    Rm { key: String },
}

impl TryFrom<Resp> for LogCommand {
    type Error = KvsError;

    fn try_from(resp: Resp) -> Result<Self> {
        match resp {
            Resp::Array(arr) => match arr.len() {
                2 => {
                    let Resp::BulkString(rm) = &arr[0] else { return Err(KvsError::ParseError)};
                    let Resp::BulkString(key) = &arr[1] else { return Err(KvsError::ParseError)};

                    if rm != "RM" {
                        return Err(KvsError::ParseError);
                    }

                    Ok(LogCommand::Rm { key: key.to_string() })
                }
                3 => {
                    let Resp::BulkString(set) = &arr[0] else { return Err(KvsError::ParseError)};
                    let Resp::BulkString(key) = &arr[1] else { return Err(KvsError::ParseError)};
                    let Resp::BulkString(value) = &arr[2] else { return Err(KvsError::ParseError)};

                    if set != "SET" {
                        return Err(KvsError::ParseError);
                    }

                    Ok(LogCommand::Set { key: key.to_string(), value: value.to_string() })
                }
                _ => Err(KvsError::ParseError)
            },
            _ => Err(KvsError::ParseError),
        }
    }
}

impl Into<Resp> for LogCommand {
    fn into(self) -> Resp {
        match self {
            LogCommand::Rm { key } => {
                Resp::Array(vec![
                    Resp::BulkString("RM".to_string()),
                    Resp::BulkString(key),
                ])
            }
            LogCommand::Set { key, value } => {
                Resp::Array(vec![
                    Resp::BulkString("SET".to_string()),
                    Resp::BulkString(key),
                    Resp::BulkString(value),
                ])
            }
        }
        
    }

}

impl InnerKvStore {
    fn open(path: impl Into<PathBuf>) -> Result<InnerKvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut this_path = path.clone();
        for entry in fs::read_dir(&path)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "log" {
                    this_path = path;
                    break;
                }
            }
        }

        if this_path.is_dir() {
            this_path = this_path.join(format!("{}.log", 0));
        }
        let log_path = this_path;

        // open the file path and read all the commands as an iterator
        let file = OpenOptions::new()
            .read(true)
            .create(true)
            .write(true)
            .open(&log_path)?;

        let mut store = InnerKvStore {
            log_path: log_path.clone(),
            index: BTreeMap::new(),
            compactable: 0,
            file_number: 0,
            dir_path: path.clone(),
        };

        let reader = BufReader::new(&file);

        let deserializer = resp::ReaderParser::from_reader(reader);
        let mut iterator = deserializer.into_iter();

        // loop over all the items and rebuild the index
        let mut offset = 0;
        while let Some(resp) = iterator.next() {
            let cmd = resp.try_into()?;

            let key = match cmd {
                LogCommand::Rm { key } => key,
                LogCommand::Set { key, .. } => key,
            };

            let new_offset = iterator.byte_offset();
            let ptr = LogPointer {
                offset,
                size: new_offset - offset,
            };

            // if the key is already stored, note that we can compact
            if let Some(ptr) = store.index.insert(key, ptr) {
                store.compactable += ptr.size;
            };

            offset = new_offset;
        }

        Ok(store)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        // check if the key is already in the index
        if let Some(ptr) = self.index.get(&key) {
            self.compactable += ptr.size;
        }

        let command = LogCommand::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let j: Resp = command.into();

        // open the log file with append-only permissions
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;

        // simply write the json encoded string to the end of the log file
        let offset = file.seek(std::io::SeekFrom::End(0))?;

        let _ = file.write(j.serialize().as_bytes())?;

        // insert the byte offset into the index
        let ptr = LogPointer {
            offset: offset as usize,
            size: j.serialize().len(),
        };

        if let Some(ptr) = self.index.insert(key, ptr) {
            self.compactable += ptr.size;
        }

        if self.compactable > COMPACT_THRESHOLD {
            info!("Compacting database: {} compactable", self.compactable);
            self.compact()?;
        }

        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        self.file_number += 1;
        let temp_path = self.dir_path.join(format!("{}.log", self.file_number));
        println!("temp_path = {:?}", temp_path);

        let new_log_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&temp_path)?;

        let mut writer = BufWriter::new(&new_log_file);

        let mut offset = 0;
        for value in self.index.values_mut() {
            let mut file = File::open(&self.log_path)?;

            file.seek(std::io::SeekFrom::Start(value.offset as u64))?;

            let mut reader = file.take(value.size as u64);
            let resp: Resp = Resp::from_reader(&mut reader)?;

            writer.write(resp.serialize().as_bytes())?;

            *value = LogPointer {
                offset,
                size: value.size,
            };

            offset += value.size;
        }

        drop(writer);

        fs::remove_file(&self.log_path)?;
        self.log_path = temp_path;
        self.compactable = 0;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.index.get(&key).cloned() {
            Some(ptr) => {
                let offset = ptr.offset;
                let size = ptr.size;

                let mut file = File::open(&self.log_path)?;

                file.seek(std::io::SeekFrom::Start(offset as u64))?;
                let reader = file.take(size as u64);

                let cmd: LogCommand = resp::Resp::from_reader(reader)?.try_into()?;

                let res = match cmd {
                    LogCommand::Rm { .. } => Ok(None),
                    LogCommand::Set { key: _, value } => Ok(Some(value)),
                };

                res
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let command = LogCommand::Rm { key: key.clone() };

        let resp: Resp = command.into();

        // open the log file with append-only permissions
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;

        // simply write the json encoded string to the end of the log file
        let _ = file.seek(std::io::SeekFrom::End(0))?;

        let _ = file.write(resp.serialize().as_bytes())?;

        match self.index.remove(&key) {
            Some(ptr) => {
                self.compactable += ptr.size;
                Ok(())
            }
            None => Err(KvsError::NotFound { key }),
        }
    }
}
