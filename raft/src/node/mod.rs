use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::RaftRPC;
use crate::{Error, Result};
use async_trait::async_trait;
use log::debug;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub enum NodeStatus {
    Follower,
    Candidate,
    Leader,
}

pub struct Log {
    term: usize,
    command: String,
}

struct State {
    current_term: usize,
    voted_for: usize,
    log: Vec<Log>,

    commit_index: usize,
    last_applied: usize,

    next_index: HashMap<usize, usize>,
    match_index: HashMap<usize, usize>,

    status: NodeStatus,

    pulses: usize,
}

impl State {
    fn new() -> Self {
        Self {
            current_term: 0,
            voted_for: 0,
            log: vec![Log {
                term: 0,
                command: "".to_string(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            status: NodeStatus::Follower,
            pulses: 0,
        }
    }
}

pub struct Node {
    state: Arc<Mutex<State>>,

    peers: Vec<usize>,
    id: usize,
}

impl Node {
    fn new(id: usize, peer_ids: Vec<usize>) -> Result<Self> {
        if peer_ids.contains(&id) {
            return Err(Error::BadIds);
        }

        Ok(Node {
            state: Arc::new(Mutex::new(State::new())),
            peers: peer_ids,
            id,
        })
    }
}

impl RaftRPC for Node {
    fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };

        let mut state = self.state.lock().unwrap();
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = 0;
            state.status = NodeStatus::Follower;
        }

        reply.term = state.current_term;

        if args.term < state.current_term {
            debug!(
                "{}: outdated term, rejected vote: got {}, have {}",
                self.id, args.term, state.current_term
            );
            reply.vote_granted = false;
            return Ok(reply);
        }

        if state.voted_for == 0 || state.voted_for == args.candidate_id {
            let our_last_index = state.log.len() - 1;
            let our_last_term = state.log[our_last_index].term;

            if args.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, args.candidate_id);
                state.voted_for = args.candidate_id;
                reply.vote_granted = true;
                // reset the timer here
            } else if args.last_log_term == our_last_term {
                if args.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, args.candidate_id);
                    state.voted_for = args.candidate_id;
                    reply.vote_granted = true;
                    // reset the timer here
                } else {
                    debug!("{}: rejected vote, log outdated: got term {}, index {}, have term {}, index {}", self.id, args.last_log_term, args.last_log_index, our_last_term, our_last_index);
                }
            }
        } else {
            reply.vote_granted = false;
            debug!(
                "{}: rejected vote from {}, already voted for {}",
                self.id, args.candidate_id, state.voted_for
            );
        }

        Ok(reply)
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        Ok(AppendEntriesReply {
            term: 0,
            success: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{Log, Node};
    use crate::rpc::{RequestVoteArgs, RequestVoteReply};
    use crate::RaftRPC;

    fn setup_follower() -> Node {
        let node: Node = Node::new(1, vec![2, 3, 4, 5]).unwrap();
        let mut state = node.state.lock().unwrap();
        state.log.push(Log {
            term: 1,
            command: "1".to_string(),
        });
        state.log.push(Log {
            term: 2,
            command: "2".to_string(),
        });
        state.current_term = 5;
        state.voted_for = 0;

        drop(state);
        node
    }

    #[test]
    fn request_vote_on_follower_test() {
        let tests: Vec<(RequestVoteArgs, RequestVoteReply)> = vec![
            (
                RequestVoteArgs {
                    term: 4,
                    candidate_id: 2,
                    last_log_index: 1,
                    last_log_term: 1,
                },
                RequestVoteReply {
                    term: 5,
                    vote_granted: false,
                },
            ),
            (
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 2,
                    last_log_index: 3,
                    last_log_term: 3,
                },
                RequestVoteReply {
                    term: 5,
                    vote_granted: true,
                },
            ),
            (
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 2,
                    last_log_index: 1,
                    last_log_term: 2,
                },
                RequestVoteReply {
                    term: 5,
                    vote_granted: false,
                },
            ),
            (
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 2,
                    last_log_index: 2,
                    last_log_term: 2,
                },
                RequestVoteReply {
                    term: 5,
                    vote_granted: true,
                },
            ),
        ];

        for (args, expected) in tests {
            let follower = setup_follower();

            let reply = follower.request_vote(args).unwrap();

            assert_eq!(reply, expected);
        }
    }
}
