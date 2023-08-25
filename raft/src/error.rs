use thiserror::Error;

use crate::common::Message;
use tokio::sync::mpsc::error::SendError;

/// Custom error type for dealing with Raft errors
#[derive(Debug, Error)]
pub enum Error {
    /// Channel handling incoming messages is broken / disconnected
    #[error("Inbox channel broken")]
    BrokenInbox,
    /// Failed to send message on channel to the network
    #[error("Failed to send message to peer")]
    FailedSend(#[from] SendError<Message>),
    #[error("Transport Error")]
    TransportError,
    #[error("Can't use address when running on local in-memory server")]
    DidntExpectSocketAddr,
}
pub type Result<T> = std::result::Result<T, Error>;
