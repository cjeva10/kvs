use crate::{common::Message, Node};
use std::collections::HashMap;
use std::sync::mpsc::Sender;

/// Initialize `num` local nodes.
///
/// Returns a vector of the nodes as well as a clone of their inbox `Sender`s
/// for manually sending messages
pub fn init_local_nodes(num: usize) -> (Vec<Node>, Vec<Sender<Message>>) {
    let mut nodes = Vec::new();
    let mut senders = Vec::new();

    for i in 0..num {
        let (tx, rx) = std::sync::mpsc::channel::<Message>();
        nodes.push(Node::new(i as u64 + 1, rx, HashMap::new()));
        senders.push(tx);
    }

    for i in 0..num {
        for j in 0..num {
            if i != j {
                nodes[i].peers.insert(j as u64 + 1, senders[j].clone());
            }
        }
    }

    (nodes, senders.clone())
}
