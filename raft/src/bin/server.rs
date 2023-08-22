use raft_rpc::raft_server::{Raft, RaftServer};
use raft_rpc::{RequestVoteArgs, RequestVoteReply, AppendEntriesArgs, AppendEntriesReply};
use tonic::{transport::Server, Request, Response, Status};

pub mod raft_rpc {
    tonic::include_proto!("raft");
}

#[derive(Debug, Default)]
pub struct MyRaft {}

#[tonic::async_trait]
impl Raft for MyRaft {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = raft_rpc::RequestVoteReply {
            vote_granted: false,
            term: 0,
        };

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = raft_rpc::AppendEntriesReply {
            success: false,
            term: 0,
        };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let raft = MyRaft::default();

    Server::builder()
        .add_service(RaftServer::new(raft))
        .serve(addr)
        .await?;

    Ok(())
}
