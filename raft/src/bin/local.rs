use eyre::Result;
use raft::init_local_nodes;

const MIN_DELAY: u64 = 100;
const NUM_NODES: usize = 5;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let (nodes, _) = init_local_nodes(NUM_NODES);

    let mut handles = Vec::new();

    for node in nodes {
        handles.push(tokio::spawn(async move {
            node.start(MIN_DELAY).await;
        }));
    }

    tokio::join!(futures::future::join_all(handles));

    Ok(())
}
