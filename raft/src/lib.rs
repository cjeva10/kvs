mod common;
mod error;
mod node;
mod rpc {
    tonic::include_proto!("raft");
}

pub use error::{Error, Result};
pub use node::Node;
