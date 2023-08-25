use raft::rpc::{
    raft_client::RaftClient,
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

// Start the node with no peers, should just become a leader and then wait for responses
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let addr = "[::1]:50051".parse().unwrap();
    let (to_inbox, inbox) = tokio::sync::mpsc::channel(64);
    let (to_outbox, mut outbox) = tokio::sync::mpsc::channel(64);

    let node = Node::new(
        1,
        inbox,
        to_inbox.clone(),
        to_outbox.clone(),
        HashMap::new(),
    );

    let rpc = MyRaft {
        inbox: to_inbox.clone(),
    };

    println!("Raft listening on {}", addr);

    // start the node on a new thread
    tokio::spawn(async { node.start(MIN_DELAY).await });

    // start the client
    tokio::spawn(async move {
        loop {
            let request = outbox.recv().await.unwrap();
            match request.message {
                Message::AppendEntries(args, callback) => {
                    // TODO need a map from peer id to url
                    let mut client = RaftClient::connect("http://[::1]:50051").await.unwrap();
                    let response = client.append_entries(Request::new(args)).await.unwrap();
                    println!("RESPONSE={:?}", response);
                    match callback {
                        Callback::Mpsc(mpsc) => {
                            let _ = mpsc
                                .send(Message::AppendEntriesReply(response.into_inner()))
                                .await;
                        }
                        Callback::OneShot(once) => {
                            let _ = once.send(Message::AppendEntriesReply(response.into_inner()));
                        }
                    }
                }
                Message::RequestVote(args, callback) => {
                    let mut client = RaftClient::connect("http://[::1]:50051").await.unwrap();
                    let response = client.request_vote(Request::new(args)).await.unwrap();
                    println!("RESPONSE={:?}", response);
                    match callback {
                        Callback::Mpsc(mpsc) => {
                            let _ = mpsc
                                .send(Message::RequestVoteReply(response.into_inner()))
                                .await;
                        }
                        Callback::OneShot(once) => {
                            let _ = once.send(Message::RequestVoteReply(response.into_inner()));
                        }
                    }
                }
                _ => {}
            }
        }
    });

    Server::builder()
        .add_service(RaftServer::new(rpc))
        .serve(addr)
        .await?;

    Ok(())
}
