use eyre::Result;
use raft::net::init_local_nodes;
use raft::net::{Client, Server};
use raft::Message;

const MIN_DELAY: u64 = 100;
const NUM_NODES: usize = 5;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let everything = init_local_nodes(NUM_NODES);

    let mut handles = Vec::new();

    let mut senders = Vec::new();
    for (node, client, server, sender) in everything {
        handles.push(tokio::spawn(async move { node.start(MIN_DELAY).await }));
        tokio::spawn(async move { client.start().await });
        tokio::spawn(async move { server.serve(None).await });
        senders.push(sender);
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    let (tx, mut rx) = tokio::sync::mpsc::channel(5);

    for sender in senders {
        sender
            .send(Message::ClientRequest(
                "hello world".to_string(),
                raft::Callback::Mpsc(tx.clone()),
            ))
            .await?;
    }

    while let Some(res) = rx.recv().await {
        println!("{:?}", res);
    }

    tokio::join!(futures::future::join_all(handles));

    Ok(())
}
