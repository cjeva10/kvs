use crate::rpc::raft_client::RaftClient;
use crate::{Callback, Message, OutboundMessage};
use log::{error, info, warn};
use std::{collections::HashMap, fmt::Debug};
use tokio::sync::mpsc::Receiver;
use tonic::codegen::StdError;
use tonic::Request;

/// A `TcpRaftClient` handles sending messages from the
pub struct TcpRaftClient<D>
where
    D: TryInto<tonic::transport::Endpoint> + Clone + Sync + Debug,
    D::Error: Into<StdError>,
{
    outbox: Receiver<OutboundMessage>,
    peers: HashMap<u64, D>,
}

impl<D> TcpRaftClient<D>
where
    D: TryInto<tonic::transport::Endpoint> + Sync + Clone + Debug,
    D::Error: Into<StdError>,
{
    pub async fn start(mut self) {
        loop {
            // wait to receive any messages on the outbox
            let Some(request) = self.outbox.recv().await else {
                info!("Node outbox has been closed, exiting client");
                break;
            };

            let to = request.to;

            let endpoint = self
                .peers
                .get(&to)
                .expect(format!("Did not find peer {}", to).as_str())
                .to_owned();

            let Ok(mut client) = RaftClient::connect(endpoint.clone()).await else {
                error!("Failed to connect to client endpoint {:?}", &endpoint);
                continue;
            };

            match request.message {
                Message::AppendEntries(args, callback) => {
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
                message => {
                    warn!(
                        "can only handle RequestVote and AppendEntries, got {:?}",
                        message
                    );
                }
            }
        }
    }
}
