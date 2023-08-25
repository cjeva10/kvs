use crate::{
    net::Server,
    rpc::{
        raft_server::{Raft, RaftServer},
        AppendEntriesArgs, AppendEntriesReply, ClientRequestArgs, ClientRequestReply,
        RequestVoteArgs, RequestVoteReply,
    },
    Error,
};
use crate::{Callback, Message, Result};

use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::sync::mpsc::Sender;
use tonic::{Request, Response, Status};

/// A server object that handles external RPC requests.
///
/// Communicates with the inner `Node` object through its inbox channel.
///
/// Implements `Raft` trait which allows it to act as an asynchronous gRPC server.
pub struct TcpRaftServer {
    inbox: Sender<Message>,
}

impl TcpRaftServer {
    /// Create a new `TcpRaftServer` from a `tokio::sync::mpsc::Sender`
    pub fn new(inbox: Sender<Message>) -> Self {
        Self { inbox }
    }
}

#[async_trait]
impl Server for TcpRaftServer {
    /// Start the RPC server at the given `SocketAddr`
    async fn serve(self, addr: Option<SocketAddr>) -> Result<()> {
        tonic::transport::Server::builder()
            .add_service(RaftServer::new(self))
            .serve(addr.expect("Missing Socket Address"))
            .await
            .map_err(|_| Error::TransportError)
    }
}

#[tonic::async_trait]
impl Raft for TcpRaftServer {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> std::result::Result<Response<RequestVoteReply>, Status> {
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
    ) -> std::result::Result<Response<AppendEntriesReply>, Status> {
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
            .send(Message::AppendEntries(
                request.into_inner(),
                Callback::OneShot(tx),
            ))
            .await;

        match rx.await {
            Ok(Message::AppendEntriesReply(reply)) => Ok(Response::new(reply)),
            _ => Ok(Response::new(reply)),
        }
    }

    async fn client_request(
        &self,
        request: Request<ClientRequestArgs>,
    ) -> std::result::Result<Response<ClientRequestReply>, Status> {
        println!("Got a request from {:?}", request.remote_addr());

        let reply = ClientRequestReply {
            success: false,
            leader: 0,
            message: String::new(),
        };

        let (tx, rx) = tokio::sync::oneshot::channel();

        let _ = self
            .inbox
            .send(Message::ClientRequest(
                request.into_inner().command,
                Callback::OneShot(tx),
            ))
            .await;

        match rx.await {
            Ok(reply) => Ok(Response::new(reply)),
            Err(_) => Ok(Response::new(reply)),
        }
    }
}
