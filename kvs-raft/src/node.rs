use clap::Parser;
use eyre::Result;
use kvs::KvStore;
use log::info;
use raft::{
    net::{Client, Server, TcpRaftClient, TcpRaftServer},
    state_machine::KvStateMachine,
    Node,
};
use std::{collections::HashMap, env::current_dir, net::SocketAddr};
use tokio::try_join;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, required = true)]
    id: u64,
    #[arg(short, long)]
    addr: SocketAddr,
    #[arg(long, required = true)]
    peer_ids: Vec<u64>,
    #[arg(long, required = true)]
    peer_addrs: Vec<SocketAddr>,
}

const MIN_DELAY: u64 = 100;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();

    if cli.peer_ids.len() != cli.peer_addrs.len() {
        eprintln!(
            "peer ids and addresses must be same length: got {} and {}",
            cli.peer_ids.len(),
            cli.peer_addrs.len()
        );
        std::process::exit(1);
    }

    if cli.peer_ids.contains(&cli.id) {
        eprintln!("Duplicate id {}", cli.id);
        std::process::exit(1);
    }

    let mut peers = Vec::new();
    for (id, addr) in cli.peer_ids.iter().zip(cli.peer_addrs.iter()) {
        let mut prefix = "http://".to_owned();
        prefix.push_str(&addr.to_string());
        peers.push((*id, prefix));
    }

    println!("Peers = {:?}", peers);

    let mut path = current_dir()?;
    path.push("data");

    info!("Opening KvStore at {}", path.to_str().unwrap());
    let kvs = KvStore::open(path)?;
    let state_machine = KvStateMachine::new(kvs);

    let (to_inbox, inbox) = tokio::sync::mpsc::channel(64);
    let (to_outbox, outbox) = tokio::sync::mpsc::channel(64);

    info!("Creating new node");
    let node = Node::new(
        cli.id,
        inbox,
        to_inbox.clone(),
        to_outbox.clone(),
        cli.peer_ids,
        state_machine,
    );

    let inner_handle =
        tokio::spawn(async { node.start(MIN_DELAY).await.expect("Inner Node has died") });
    info!("Spawned raft node");

    let addr = cli.addr;

    // start the client
    let client_handle = tokio::spawn(async move {
        let peers = HashMap::from_iter(peers.into_iter());

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
