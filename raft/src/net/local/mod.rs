use crate::{state_machine::DummyStateMachine, Message, Node, OutboundMessage};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

pub use self::{client::LocalRaftClient, server::LocalRaftServer};

mod client;
mod server;

pub fn init_local_nodes(
    num: usize,
) -> Vec<(
    Node<DummyStateMachine, String>,
    LocalRaftClient,
    LocalRaftServer,
    Sender<Message>,
)> {
    let mut everything = Vec::new();
    let mut server_inboxes = HashMap::<u64, Sender<OutboundMessage>>::new();
    let mut peers = Vec::new();

    for i in 0..num {
        // create all the channels for this node
        let (to_inbox, inner_inbox) = tokio::sync::mpsc::channel::<Message>(64);
        let (inner_outbox, outbox) = tokio::sync::mpsc::channel::<OutboundMessage>(64);
        let (to_server_inbox, server_inbox) = tokio::sync::mpsc::channel::<OutboundMessage>(64);
        let dummy = DummyStateMachine {};
        let id = i + 1;

        // initialize the node, server, and client
        let node = Node::new(
            id as u64,
            inner_inbox,
            to_inbox.clone(),
            inner_outbox.clone(),
            Vec::new(),
            dummy,
        );
        let server = LocalRaftServer::new(id as u64, server_inbox, to_inbox.clone());
        let client = LocalRaftClient::new(outbox, HashMap::new());

        // push into the vector
        everything.push((node, client, server, to_inbox.clone()));
        server_inboxes.insert(id as u64, to_server_inbox);
        peers.push(i as u64 + 1);
    }

    for i in 0..num {
        let mut client_outbox = server_inboxes.clone();
        let mut these_peers = peers.clone();
        these_peers.remove(i);
        client_outbox.remove(&(i as u64 + 1));
        everything[i as usize].1.set_peers(client_outbox);
        everything[i as usize].0.peers = these_peers;
    }

    everything
}

#[cfg(test)]
mod tests {
    use crate::{
        common::{Role, State},
        net::{init_local_nodes, Client, Server},
        node::Log,
        Message,
    };
    use log::debug;
    use std::collections::HashMap;
    use tokio::{
        sync::mpsc::{self, Receiver, Sender},
        time::Duration,
    };

    const MIN_DELAY: u64 = 50;

    fn make_state_channels(num: usize) -> (Vec<Sender<Message>>, Vec<Receiver<Message>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..num {
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            senders.push(tx);
            receivers.push(rx);
        }

        (senders, receivers)
    }

    // A single node should start an election and elect itself the leader
    #[tokio::test]
    async fn test_one_node_election() {
        let everything = init_local_nodes(1);

        let (state_tx, mut state_rx) = mpsc::channel(1);

        let mut senders = Vec::new();
        for (node, client, server, sender) in everything {
            tokio::spawn(async move {
                node.start(MIN_DELAY).await.unwrap();
            });
            tokio::spawn(async move {
                client.start().await;
            });
            tokio::spawn(async move {
                let _ = server.serve(None).await;
            });
            senders.push(sender);
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 3)).await;

        senders[0]
            .send(Message::CheckState(state_tx.clone()))
            .await
            .unwrap();

        let Message::State(state) = state_rx.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        let expected = State {
            id: 1,
            commit_index: 0,
            last_applied: 0,
            log: vec![Log {
                term: 0,
                command: "".to_string(),
            }],
            match_index: HashMap::new(),
            next_index: HashMap::new(),
            role: Role::Leader,
            term: 1,
            voted_for: Some(1),
            votes: 1,
            killed: false,
            leader_id: Some(1),
        };

        assert_eq!(expected, state);
    }

    #[tokio::test]
    async fn test_three_node_election() {
        let everything = init_local_nodes(3);
        let (state_tx_1, mut state_rx_1) = tokio::sync::mpsc::channel(4);
        let (state_tx_2, mut state_rx_2) = tokio::sync::mpsc::channel(4);
        let (state_tx_3, mut state_rx_3) = tokio::sync::mpsc::channel(4);

        let mut senders = Vec::new();
        for (node, client, server, sender) in everything {
            tokio::spawn(async {
                let _ = node.start(MIN_DELAY).await;
            });
            tokio::spawn(async {
                let _ = client.start().await;
            });
            tokio::spawn(async {
                let _ = server.serve(None).await;
            });
            senders.push(sender);
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        senders[0]
            .send(Message::CheckState(state_tx_1))
            .await
            .unwrap();
        senders[1]
            .send(Message::CheckState(state_tx_2))
            .await
            .unwrap();
        senders[2]
            .send(Message::CheckState(state_tx_3))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let Message::State(state_1) = state_rx_1.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_2) = state_rx_2.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_3) = state_rx_3.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        if state_1.role != Role::Leader
            && state_2.role != Role::Leader
            && state_3.role != Role::Leader
        {
            panic!("No leader elected!!");
        }
    }
    #[tokio::test]
    async fn test_three_nodes_kill_leader() {
        env_logger::init();

        let everything = init_local_nodes(3);
        let (state_tx, mut state_rx) = make_state_channels(3);
        let mut senders = Vec::new();

        for (node, client, server, sender) in everything {
            tokio::spawn(async {
                let _ = node.start(MIN_DELAY).await;
            });
            tokio::spawn(async {
                let _ = client.start().await;
            });
            tokio::spawn(async {
                let _ = server.serve(None).await;
            });
            senders.push(sender);
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        senders[0]
            .send(Message::CheckState(state_tx[0].clone()))
            .await
            .unwrap();
        senders[1]
            .send(Message::CheckState(state_tx[1].clone()))
            .await
            .unwrap();
        senders[2]
            .send(Message::CheckState(state_tx[2].clone()))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        #[allow(unused_assignments)]
        let mut killed = 0;

        if state_1.role == Role::Leader {
            debug!("Killed Node 1");
            senders[0].send(Message::Kill).await.unwrap();
            killed = 1;
        } else if state_2.role == Role::Leader {
            debug!("Killed Node 2");
            senders[1].send(Message::Kill).await.unwrap();
            killed = 2;
        } else if state_3.role == Role::Leader {
            debug!("Killed Node 3");
            senders[2].send(Message::Kill).await.unwrap();
            killed = 3;
        } else {
            panic!("No leader elected!");
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        let mut found_leader = 0;

        match killed {
            1 => {
                senders[1]
                    .send(Message::CheckState(state_tx[1].clone()))
                    .await
                    .unwrap();
                senders[2]
                    .send(Message::CheckState(state_tx[2].clone()))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_2.role == Role::Leader {
                    found_leader += 1;
                }
                if state_3.role == Role::Leader {
                    found_leader += 1;
                }
            }
            2 => {
                senders[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                senders[2]
                    .send(Message::CheckState(state_tx[2].clone()))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_1.role == Role::Leader {
                    found_leader += 1;
                }
                if state_3.role == Role::Leader {
                    found_leader += 1;
                }
            }
            3 => {
                senders[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                senders[1]
                    .send(Message::CheckState(state_tx[1].clone()))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_1.role == Role::Leader {
                    found_leader += 1;
                }
                if state_2.role == Role::Leader {
                    found_leader += 1;
                }
            }
            _ => panic!("No one was killed"),
        }

        assert_eq!(found_leader, 1);
    }
}
