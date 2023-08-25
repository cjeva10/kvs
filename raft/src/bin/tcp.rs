use raft::rpc::{
    raft_server::{Raft, RaftServer},
    AppendEntriesArgs, AppendEntriesReply, ClientRequestArgs, ClientRequestReply, RequestVoteArgs,
    RequestVoteReply,
};
use raft::{Callback, Message, Node};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tonic::{transport::Server, Request, Response, Status};

pub struct MyRaft {
    inbox: Sender<Message>,
}

const MIN_DELAY: u64 = 100;

#[tonic::async_trait]
impl Raft for MyRaft {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
            peer: 0,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = self
            .inbox
            .send(Message::RequestVote(
                request.into_inner(),
                Callback::OneShot(tx),
            ))
            .await;

        match rx.await {
            Ok(Message::RequestVoteReply(reply)) => Ok(Response::new(reply)),
            _ => Ok(Response::new(reply)),
        }
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = AppendEntriesReply {
            term: 0,
            success: false,
            peer: 0,
            next_index: 0,
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = self
            .inbox
            .send(Message::AppendEntries(request.into_inner(), Callback::OneShot(tx)))
            .await;

        match rx.await {
            Ok(Message::AppendEntriesReply(reply)) => Ok(Response::new(reply)),
            _ => Ok(Response::new(reply)),
        }
    }

    async fn client_request(
        &self,
        request: Request<ClientRequestArgs>,
    ) -> Result<Response<ClientRequestReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = ClientRequestReply {
            success: false,
            leader: 0,
            message: String::new(),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = self
            .inbox
            .send(Message::ClientRequest(request.into_inner().command, Callback::OneShot(tx)))
            .await;

        match rx.await {
            Ok(reply) => Ok(Response::new(reply)),
            Err(_) => Ok(Response::new(reply)),
        }
    }
}

// Start the node with no peers, should just become a leader and then wait for responses
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr = "[::1]:50051".parse().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(64);

    let node = Node::new(1, rx, tx.clone(), HashMap::new());
    let rpc = MyRaft { inbox: tx.clone() };

    println!("Raft listening on {}", addr);

    tokio::spawn(async { node.start(MIN_DELAY).await });

    Server::builder()
        .add_service(RaftServer::new(rpc))
        .serve(addr)
        .await?;

    Ok(())
}
