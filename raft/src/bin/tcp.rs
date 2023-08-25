use raft::rpc::raft_server::{Raft, RaftServer};
use raft::rpc::{
    AppendEntriesArgs, AppendEntriesReply, ClientRequestArgs, ClientRequestReply, Empty,
    RequestVoteArgs, RequestVoteReply,
};
use raft::{Message, Node};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use tonic::{transport::Server, Request, Response, Status};

pub struct MyRaft {
    inbox: Sender<Message>,
}

#[tonic::async_trait]
impl Raft for MyRaft {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<Empty>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let empty = Empty {};

        Ok(Response::new(empty))
    }

    async fn reply_to_request_vote(
        &self,
        request: Request<RequestVoteReply>,
    ) -> Result<Response<Empty>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let empty = Empty {};

        Ok(Response::new(empty))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<Empty>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let empty = Empty {};

        Ok(Response::new(empty))
    }

    async fn reply_to_append_entries(
        &self,
        request: Request<AppendEntriesReply>,
    ) -> Result<Response<Empty>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let empty = Empty {};

        Ok(Response::new(empty))
    }

    async fn client_request(
        &self,
        request: Request<ClientRequestArgs>,
    ) -> Result<Response<ClientRequestReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = ClientRequestReply {
            success: false,
            leader: 0,
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let (tx, rx) = tokio::sync::mpsc::channel(64);

    let node = Node::new(1, rx, HashMap::new());
    let rpc = MyRaft { inbox: tx.clone() };

    println!("Raft listening on {}", addr);

    Server::builder()
        .add_service(RaftServer::new(rpc))
        .serve(addr)
        .await?;

    Ok(())
}
