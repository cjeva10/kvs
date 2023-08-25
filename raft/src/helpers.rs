use crate::{common::Message, Node};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

/// Initialize `num` local nodes.
///
/// Returns a vector of the nodes as well as a clone of their inbox `Sender`s
/// for manually sending messages
pub fn init_local_nodes(num: usize) -> (Vec<Node>, Vec<Sender<Message>>) {
    let mut nodes = Vec::new();
    let mut senders = Vec::new();

    for i in 0..num {
        let (to_inbox, inbox) = tokio::sync::mpsc::channel::<Message>(64);
        let (to_outbox, _) = tokio::sync::mpsc::channel(64);
        nodes.push(Node::new(i as u64 + 1, inbox, to_inbox.clone(), to_outbox.clone(), HashMap::new()));
        senders.push(to_inbox);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_one_node() {
        let (nodes, senders) = init_local_nodes(1);

        assert_eq!(nodes.len(), 1);
        assert_eq!(senders.len(), 1);

        assert_eq!(nodes[0].peers.len(), 0);
    }

    #[test]
    fn test_init_three_nodes() {
        let (nodes, senders) = init_local_nodes(3);

        assert_eq!(nodes.len(), 3);
        assert_eq!(senders.len(), 3);

        assert_eq!(nodes[0].peers.len(), 2);
        assert_eq!(nodes[1].peers.len(), 2);
        assert_eq!(nodes[2].peers.len(), 2);
    }
}
