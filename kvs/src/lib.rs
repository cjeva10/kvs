#![deny(missing_docs)]
//! Implementation of an in-memory key-value store
//!
//! For now, this only supports storing keys and values as `String`

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufReader, BufWriter, Read, Seek, Write},
    path::PathBuf,
};
use thiserror::Error;

// compact when there is 1 MB compactable
const COMPACT_THRESHOLD: usize = 1024 * 1024;

/// KvStore error type
#[derive(Debug, Error)]
pub enum KvsError {
    /// An unknown error
    #[error("Unknown KvStore error")]
    Unknown,
    /// Could not open the log file
    #[error("data store failed to open")]
    CantOpen(#[from] std::io::Error),
    /// Failed to parse a command from json
    #[error("Could not parse command")]
    ParseError(#[from] serde_json::Error),
    /// Key not found in the index / log
    #[error("Could not find key {key:?}")]
    NotFound {
        /// the key that is missing
        key: String,
    },
}

/// Custom Result type for KvsStore
pub type Result<T> = std::result::Result<T, KvsError>;

/// An key-value store with sequential file log
pub struct KvStore {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
enum LogCommand {
    Set { key: String, value: String },
    Rm { key: String },
}

impl KvStore {
    /// Open the KvStore at the given file
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
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

        let mut store = KvStore {
            log_path: log_path.clone(),
            index: BTreeMap::new(),
            compactable: 0,
            file_number: 0,
            dir_path: path.clone(),
        };

        let reader = BufReader::new(&file);

        let deserializer = serde_json::de::Deserializer::from_reader(reader);
        let mut iterator = deserializer.into_iter::<LogCommand>();

        // loop over all the items and rebuild the index
        let mut offset = 0;
        while let Some(cmd) = iterator.next() {
            let key = match cmd? {
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

    /// set a key-value pair
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // check if the key is already in the index
        if let Some(ptr) = self.index.get(&key) {
            self.compactable += ptr.size;
        }

        let command = LogCommand::Set {
            key: key.clone(),
            value: value.clone(),
        };

        let j = serde_json::to_string(&command)?;

        // open the log file with append-only permissions
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;

        // simply write the json encoded string to the end of the log file
        let offset = file.seek(std::io::SeekFrom::End(0))?;

        let _ = file.write(j.as_bytes())?;

        // insert the byte offset into the index
        let ptr = LogPointer {
            offset: offset as usize,
            size: j.as_bytes().len(),
        };

        if let Some(ptr) = self.index.insert(key, ptr) {
            self.compactable += ptr.size;
        }

        if self.compactable > COMPACT_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    // iterate through the index, write all the entries at the end of the file, then simply delete
    // everything before the starting index
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
            let cmd: LogCommand = serde_json::from_reader(&mut reader)?;

            writer.write(serde_json::to_string(&cmd)?.as_bytes())?;

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

    /// get a key-value pair
    /// Takes a `String` and returns `Option<String>`
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.index.get(&key).cloned() {
            Some(ptr) => {
                let offset = ptr.offset;
                let size = ptr.size;

                let mut file = File::open(&self.log_path)?;

                file.seek(std::io::SeekFrom::Start(offset as u64))?;
                let reader = file.take(size as u64);

                let cmd: LogCommand = serde_json::from_reader(reader)?;

                let res = match cmd {
                    LogCommand::Rm { .. } => Ok(None),
                    LogCommand::Set { key: _, value } => Ok(Some(value)),
                };

                res
            }
            None => Ok(None),
        }
    }

    /// removes a key-value pair
    /// if the key does not exist, does nothing
    pub fn remove(&mut self, key: String) -> Result<()> {
        let command = LogCommand::Rm { key: key.clone() };

        let j = serde_json::to_string(&command)?;

        // open the log file with append-only permissions
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.log_path)?;

        // simply write the json encoded string to the end of the log file
        let _ = file.seek(std::io::SeekFrom::End(0))?;

        let _ = file.write(j.as_bytes())?;

        match self.index.remove(&key) {
            Some(ptr) => {
                self.compactable += ptr.size;
                Ok(())
            }
            None => Err(KvsError::NotFound { key }),
        }
    }
}
