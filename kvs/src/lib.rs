#![deny(missing_docs)]
//! Implementation of an in-memory key-value store
//!
//! For now, this only supports storing keys and values as `String`

pub use kvstore::KvStore;
use thiserror::Error;

mod kvstore;

/// Defines shared behavior for interacting with a key-value store 
///
/// Note, that all the methods receive shared references to the underlying type.
/// This allows sharing the state of the engine across threads. Implementors
/// should employ synchronization primitives such as `Mutex` in order to acquire
/// interior mutability across threads.
pub trait KvEngine: Clone + Send + 'static {
    /// set the given `key` to the given `value`
    fn set(&self, key: String, value: String) -> Result<()>;
    /// get the value of the given `key`
    ///
    /// Return `Err` if the `key` is missing
    fn get(&self, key: String) -> Result<Option<String>>;
    /// remove the `key` from the database
    ///
    /// Returns an `Err` if the `key` is missing
    fn remove(&self, key: String) -> Result<()>;

}

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
    ParseError,
    /// Key not found in the index / log
    #[error("Could not find key {key:?}")]
    NotFound {
        /// the key that is missing
        key: String,
    },
    /// failed to acquire a `Mutex` lock
    #[error("Failed to acquire lock")]
    LockError,
    /// failed to read the command from a `Resp` value
    #[error("Failed to read value")]
    ReaderError(#[from] resp::Error)
}

/// Custom Result type for KvsStore
pub type Result<T> = std::result::Result<T, KvsError>;

