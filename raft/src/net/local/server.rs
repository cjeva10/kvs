use async_trait::async_trait;
use log::{error, info};
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::net::Server;
use crate::{Error, Message, OutboundMessage, Result};

pub struct LocalRaftServer {
    id: u64,
    inbox: Receiver<OutboundMessage>,
    to_inbox: Sender<Message>,
}

impl LocalRaftServer {
    pub fn new(id: u64, inbox: Receiver<OutboundMessage>, to_inbox: Sender<Message>) -> Self {
        Self {
            id,
            inbox,
            to_inbox,
        }
    }

    pub async fn send(&self, message: Message) -> Result<()> {
        self.to_inbox.send(message).await?;
        Ok(())
    }
}

#[async_trait]
impl Server for LocalRaftServer {
    async fn serve(mut self, addr: Option<SocketAddr>) -> Result<()> {
        if !addr.is_none() {
            return Err(Error::DidntExpectSocketAddr);
        }

        loop {
            let Some(request) = self.inbox.recv().await else {
                info!("Node inbox has been close, exiting server");
                break;
            };

            if request.to != self.id {
                error!(
                    "Message from {} sent to wrong id: got {}, have {}",
                    request.from, request.to, self.id
                );
                continue;
            }

            let _ = self.to_inbox.send(request.message).await;
        }

        Ok(())
    }
}
