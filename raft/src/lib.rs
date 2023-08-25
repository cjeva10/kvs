//! Implementation of the Raft distributed consensus algorithm

mod common;
mod error;
mod helpers;
mod node;

pub mod rpc {
    tonic::include_proto!("raft");
}
pub use common::{Callback, Message, OutboundMessage};
pub use error::{Error, Result};
pub use helpers::init_local_nodes;
pub use node::Node;
