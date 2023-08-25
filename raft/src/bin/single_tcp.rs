use log::info;
use raft::net::{Client, Server, TcpRaftClient, TcpRaftServer};
use raft::Node;
use std::collections::HashMap;
use tokio::try_join;

const MIN_DELAY: u64 = 100;

// Start the node with no peers, should just become a leader and then wait for responses
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    // initialize the nodes id, address, inbox/outbox and peer list
    let id = 1;
    let addr = "[::1]:50051".parse().unwrap();

    let (to_inbox, inbox) = tokio::sync::mpsc::channel(64);
    let (to_outbox, outbox) = tokio::sync::mpsc::channel(64);

    let node = Node::new(
        id,
        inbox,
        to_inbox.clone(),
        to_outbox.clone(),
        Vec::new(),
    );

    // start the inner node
    let inner_handle =
        tokio::spawn(async { node.start(MIN_DELAY).await.expect("Inner Node has died") });
    info!("Spawned raft node");

    // start the client
    let client_handle = tokio::spawn(async move {
        let peers = HashMap::new();
        let client = TcpRaftClient::<String>::new(outbox, peers);

        client.start().await;
    });
    info!("Started client loop");

    // start the server
    let server_handle = tokio::spawn(async move {
        let server = TcpRaftServer::new(to_inbox.clone());
        server.serve(Some(addr)).await.expect("RPC server died");
    });
    info!("Started RPC server");

    try_join!(inner_handle, client_handle, server_handle).unwrap();

    Ok(())
}
