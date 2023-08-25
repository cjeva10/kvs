//! Implementation of the Raft distributed consensus algorithm

mod common;
mod error;
pub mod net;
mod node;

pub mod rpc {
    tonic::include_proto!("raft");
}
pub use common::{Callback, Message, OutboundMessage};
pub use error::{Error, Result};
pub use node::Node;
