#[deny(missing_docs)]

mod node;
mod rpc;
mod error;

pub use error::{Error, Result};
pub use rpc::RaftRPC;

#[cfg(test)]
mod tests {
}
