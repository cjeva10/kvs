use thiserror::Error;

use crate::Message;
use std::sync::mpsc::SendError;

/// Custom error type for dealing with Raft errors
#[derive(Debug, Error)]
pub enum Error {
    /// Channel handling incoming messages is broken / disconnected
    #[error("Inbox channel broken")]
    BrokenInbox,
    /// Failed to send message on channel to the network
    #[error("Failed to send message to peer")]
    FailedSend(#[from] SendError<Message>),
    /// Node has been manually killed in a test
    #[error("Node manually killed")]
    Killed,
}
pub type Result<T> = std::result::Result<T, Error>;

