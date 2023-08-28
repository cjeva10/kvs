use async_trait::async_trait;
use log::{error, info, warn};
use std::collections::HashMap;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::net::Client;
use crate::{Message, OutboundMessage};

pub struct LocalRaftClient {
    peers: HashMap<u64, Sender<OutboundMessage>>,
    outbox: Receiver<OutboundMessage>,
    id: u64,
}

impl LocalRaftClient {
    pub fn new(
        id: u64,
        outbox: Receiver<OutboundMessage>,
        peers: HashMap<u64, Sender<OutboundMessage>>,
    ) -> Self {
        Self { id, peers, outbox }
    }

    pub fn set_peers(&mut self, peers: HashMap<u64, Sender<OutboundMessage>>) {
        self.peers = peers;
    }
}

#[async_trait]
impl Client for LocalRaftClient {
    async fn start(mut self) {
        loop {
            // wait to receive any messages on the outbox
            let Some(request) = self.outbox.recv().await else {
                info!("Node outbox has been closed, exiting client");
                break;
            };

            let to = request.to;

            let Some(sender) = self.peers.get(&to) else {
                error!("Failed to find peer {}", to);
                continue;
            };

            match request.message {
                Message::AppendEntries(..) => {
                    let _ = sender.send(request).await;
                }
                Message::RequestVote(..) => {
                    let _ = sender.send(request).await;
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
