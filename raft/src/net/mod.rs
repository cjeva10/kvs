use crate::Result;
use async_trait::async_trait;
use std::net::SocketAddr;

mod local;
mod tcp;

pub use local::init_local_nodes;
pub use tcp::client::TcpRaftClient;
pub use tcp::server::TcpRaftServer;

/// A `Server` handles requests and forwards them to the inner node, returning replies to the
/// caller
#[async_trait]
pub trait Server {
    async fn serve(self, addr: Option<SocketAddr>) -> Result<()>;
}

/// A `Client` handles messages from the inner nodes outbox, sending requests to other nodes and
/// then returning the replies into the inner node
#[async_trait]
pub trait Client {
    async fn start(mut self);
}
