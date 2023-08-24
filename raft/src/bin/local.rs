use eyre::Result;
use raft::init_nodes;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let num: usize = 3;

    let (nodes, _) = init_nodes(num);

    let mut handles = Vec::new();
    for node in nodes {
        handles.push(tokio::spawn( async {
            node.start().unwrap();
        }));
    }

    tokio::join!(futures::future::join_all(handles));

    Ok(())
}
