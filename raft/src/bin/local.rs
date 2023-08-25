use eyre::Result;
use raft::net::init_local_nodes;
use raft::net::{Client, Server};

const MIN_DELAY: u64 = 100;
const NUM_NODES: usize = 5;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let everything = init_local_nodes(NUM_NODES);

    let mut handles = Vec::new();

    for (node, client, server, _) in everything {
        handles.push(tokio::spawn(async move { node.start(MIN_DELAY).await }));
        tokio::spawn(async move { client.start().await });
        tokio::spawn(async move { server.serve(None).await });
    }

    tokio::join!(futures::future::join_all(handles));

    Ok(())
}
