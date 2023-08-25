use crate::Result;
use std::net::SocketAddr;
use async_trait::async_trait;

mod tcp;
mod local;

pub use tcp::client::TcpRaftClient;
pub use tcp::server::TcpRaftServer;
pub use local::init_local_nodes;

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
