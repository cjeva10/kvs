mod client;
mod server;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::rpc::Log;
    use crate::common::{State, Role};
    use crate::helpers::init_local_nodes;
    use crate::{Node, Message};
    use tokio::{
        sync::mpsc::{Receiver, Sender},
        time::Duration,
    };
    use log::debug;

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
        let (to_inbox, inbox) = tokio::sync::mpsc::channel(64);
        let (to_outbox, _) = tokio::sync::mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel(16);
        let node = Node::new(1, inbox, to_inbox.clone(), to_outbox.clone(), Vec::new());

        tokio::spawn(async move {
            node.start(MIN_DELAY).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 3 / 2)).await;

        to_inbox
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
        let (nodes, senders) = init_local_nodes(3);
        let (state_tx_1, mut state_rx_1) = tokio::sync::mpsc::channel(4);
        let (state_tx_2, mut state_rx_2) = tokio::sync::mpsc::channel(4);
        let (state_tx_3, mut state_rx_3) = tokio::sync::mpsc::channel(4);

        for node in nodes {
            tokio::spawn(async {
                let _ = node.start(MIN_DELAY).await;
            });
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
        let (nodes, tx) = init_local_nodes(3);
        let (state_tx, mut state_rx) = make_state_channels(3);

        for node in nodes {
            tokio::spawn(async {
                let _ = node.start(MIN_DELAY).await;
            });
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        tx[0]
            .send(Message::CheckState(state_tx[0].clone()))
            .await
            .unwrap();
        tx[1]
            .send(Message::CheckState(state_tx[1].clone()))
            .await
            .unwrap();
        tx[2]
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
            tx[0].send(Message::Kill).await.unwrap();
            killed = 1;
        } else if state_2.role == Role::Leader {
            debug!("Killed Node 2");
            tx[1].send(Message::Kill).await.unwrap();
            killed = 2;
        } else if state_3.role == Role::Leader {
            debug!("Killed Node 3");
            tx[2].send(Message::Kill).await.unwrap();
            killed = 3;
        } else {
            panic!("No leader elected!");
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        let mut found_leader = 0;

        match killed {
            1 => {
                tx[1]
                    .send(Message::CheckState(state_tx[1].clone()))
                    .await
                    .unwrap();
                tx[2]
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
                tx[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                tx[2]
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
                tx[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                tx[1]
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
