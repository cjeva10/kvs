use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
use crate::Rpc;
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

#[derive(Clone, Debug)]
pub struct Log {
    term: usize,
    command: String,
}

struct State {
    current_term: usize,
    voted_for: Option<usize>,
    leader_id: Option<usize>,
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
            voted_for: None,
            leader_id: None,
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

impl Rpc for Node {
    fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply> {
        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };

        let mut state = self.state.lock().unwrap();
        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
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

        if state.voted_for == None || state.voted_for == Some(args.candidate_id) {
            let our_last_index = state.log.len() - 1;
            let our_last_term = state.log[our_last_index].term;

            if args.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, args.candidate_id);
                state.voted_for = Some(args.candidate_id);
                reply.vote_granted = true;
                // TODO: reset election timer here
            } else if args.last_log_term == our_last_term {
                if args.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, args.candidate_id);
                    state.voted_for = Some(args.candidate_id);
                    reply.vote_granted = true;
                    // TODO: reset election timer here
                } else {
                    debug!("{}: rejected vote, log outdated: got term {}, index {}, have term {}, index {}", self.id, args.last_log_term, args.last_log_index, our_last_term, our_last_index);
                }
            }
        } else {
            reply.vote_granted = false;
            debug!(
                "{}: rejected vote from {}, already voted for {:?}",
                self.id, args.candidate_id, state.voted_for
            );
        }

        Ok(reply)
    }

    fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply> {
        let mut reply = AppendEntriesReply {
            term: 0,
            success: false,
        };

        let mut state = self.state.lock().map_err(|_| Error::FailedLock)?;

        debug!(
            "{}: AppendEntries received from {}",
            self.id, args.leader_id
        );

        if args.term > state.current_term {
            state.current_term = args.term;
            state.voted_for = None;
            state.status = NodeStatus::Follower;
        }

        reply.term = state.current_term;

        if args.term < state.current_term {
            debug!(
                "{}: AppendEntries leader term less than ours: got {}, have {}",
                self.id, args.term, state.current_term
            );
            reply.success = false;
            return Ok(reply);
        }

        if state.log.len() - 1 < args.prev_log_index {
            debug!(
                "{}: AppendEntries from {}: inconsistent log: our index {}, args.prev_log_index {}",
                self.id,
                args.leader_id,
                state.log.len() - 1,
                args.prev_log_index
            );
            reply.success = false;

            return Ok(reply);
        }

        if state.log[args.prev_log_index].term != args.prev_log_term {
            debug!(
                "{}: AppendEntries from {}: inconsistent log: our last term {}, args.prev_log_term {}",
                self.id,
                args.leader_id,
                state.log[args.prev_log_term].term,
                args.prev_log_term,
            );
            reply.success = false;

            return Ok(reply);
        }

        let mut idx = args.prev_log_index + 1;
        for entry in &args.entries {
            if idx > state.log.len() - 1 {
                break;
            }

            if entry.term != state.log[idx].term {
                state.log = state.log[..idx].to_vec();
                break;
            }

            idx += 1;
        }

        idx = args.prev_log_index + 1;

        for i in 0..args.entries.len() {
            if idx + i > state.log.len() - 1 {
                state.log.append(&mut args.entries[i..].to_vec());
                break;
            }
        }

        if args.leader_commit > state.commit_index {
            let idx_last_new_entry = state.log.len() - 1;

            if idx_last_new_entry < args.leader_commit {
                state.commit_index = idx_last_new_entry;
            } else {
                state.commit_index = args.leader_commit;
            }

            // TODO: check if we need to apply new entries
        }

        state.voted_for = Some(args.leader_id);
        state.leader_id = Some(args.leader_id);
        reply.success = true;
        debug!(
            "{}: AppendEntries successful: Current Log: {:?}, commit_index {}, current leader {:?}",
            self.id, state.log, state.commit_index, state.leader_id
        );

        // TODO: reset election timer

        Ok(reply)
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{Log, Node};
    use crate::rpc::{AppendEntriesArgs, AppendEntriesReply, RequestVoteArgs, RequestVoteReply};
    use crate::Rpc;

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
        state.voted_for = None;

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

    #[test]
    fn append_entries_term_update() {
        let tests: Vec<(AppendEntriesArgs, AppendEntriesReply)> = vec![
            (
                AppendEntriesArgs {
                    term: 4,
                    leader_commit: 0,
                    leader_id: 2,
                    entries: Vec::new(),
                    prev_log_index: 0,
                    prev_log_term: 0,
                },
                AppendEntriesReply {
                    term: 5,
                    success: false,
                },
            ),
            ( // our log is too short
                AppendEntriesArgs {
                    term: 5,
                    leader_commit: 0,
                    leader_id: 2,
                    entries: Vec::new(),
                    prev_log_index: 3,
                    prev_log_term: 3,
                },
                AppendEntriesReply {
                    term: 5,
                    success: false,
                },
            ),
            ( // inconsistent term at prev_log_index
                AppendEntriesArgs {
                    term: 5,
                    leader_commit: 0,
                    leader_id: 2,
                    entries: Vec::new(),
                    prev_log_index: 2,
                    prev_log_term: 1,
                },
                AppendEntriesReply {
                    term: 5,
                    success: false,
                },
            ),
            ( // higher term, make sure we update
                AppendEntriesArgs {
                    term: 6,
                    leader_commit: 0,
                    leader_id: 2,
                    entries: Vec::new(),
                    prev_log_index: 2,
                    prev_log_term: 1,
                },
                AppendEntriesReply {
                    term: 6,
                    success: false,
                },
            ),
            ( // higher term, make sure we update
                AppendEntriesArgs {
                    term: 6,
                    leader_commit: 0,
                    leader_id: 2,
                    entries: Vec::new(),
                    prev_log_index: 2,
                    prev_log_term: 2,
                },
                AppendEntriesReply {
                    term: 6,
                    success: true,
                },
            ),
        ];

        for (args, expected) in tests {
            let follower = setup_follower();

            let reply = follower.append_entries(args).unwrap();

            assert_eq!(reply, expected);
        }
    }
}
