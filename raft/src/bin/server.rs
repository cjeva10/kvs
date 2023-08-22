use raft::Node;
use raft::RaftServer;
use raft::RaftClient;
use raft::Peer;
use tonic::transport::Server;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let peers = vec![
        Peer { id: 2, client: RaftClient::connect("[::1]:50052").await? },
        Peer { id: 3, client: RaftClient::connect("[::1]:50053").await? },
        Peer { id: 4, client: RaftClient::connect("[::1]:50054").await? },
        Peer { id: 5, client: RaftClient::connect("[::1]:50055").await? },
    ];

    let raft = Node::new(
        1,
        peers,
    )?;

    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
