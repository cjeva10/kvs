use crate::{Error, Result};
use log::debug;
use raft_rpc::raft_server::{Raft, RaftServer};
use raft_rpc::{AppendEntriesArgs, AppendEntriesReply, Log, RequestVoteArgs, RequestVoteReply};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tonic::{transport::Server, Request, Response, Status};

pub mod raft_rpc {
    tonic::include_proto!("raft");
}

pub enum NodeStatus {
    Follower,
    Candidate,
    Leader,
}

struct State {
    current_term: u64,
    voted_for: Option<u64>,
    leader_id: Option<u64>,
    log: Vec<Log>,

    commit_index: u64,
    last_applied: u64,

    next_index: HashMap<u64, u64>,
    match_index: HashMap<u64, u64>,

    status: NodeStatus,

    pulses: u64,
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

#[tonic::async_trait]
impl Raft for Node {
    async fn request_vote(
        &self,
        request: Request<RequestVoteArgs>,
    ) -> std::result::Result<Response<RequestVoteReply>, Status> {
        let request = request.into_inner();

        let mut reply = RequestVoteReply {
            term: 0,
            vote_granted: false,
        };

        let mut state = self.state.lock().map_err(|_| Status::internal("Failed to get state lock"))?;

        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            state.status = NodeStatus::Follower;
        }

        reply.term = state.current_term;

        if request.term < state.current_term {
            debug!(
                "{}: outdated term, rejected vote: got {}, have {}",
                self.id, request.term, state.current_term
            );
            reply.vote_granted = false;
            return Ok(Response::new(reply));
        }

        if state.voted_for == None || state.voted_for == Some(request.candidate_id) {
            let our_last_index = (state.log.len() - 1) as u64;
            let our_last_term = (state.log[our_last_index as usize].term) as u64;

            if request.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, request.candidate_id);
                state.voted_for = Some(request.candidate_id);
                reply.vote_granted = true;
                // TODO: reset election timer here
            } else if request.last_log_term == our_last_term {
                if request.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, request.candidate_id);
                    state.voted_for = Some(request.candidate_id);
                    reply.vote_granted = true;
                    // TODO: reset election timer here
                } else {
                    debug!("{}: rejected vote, log outdated: got term {}, index {}, have term {}, index {}", self.id, request.last_log_term, request.last_log_index, our_last_term, our_last_index);
                }
            }
        } else {
            reply.vote_granted = false;
            debug!(
                "{}: rejected vote from {}, already voted for {:?}",
                self.id, request.candidate_id, state.voted_for
            );
        }

        Ok(Response::new(reply))
    }

    async fn append_entries(
        &self,
        request: Request<AppendEntriesArgs>,
    ) -> std::result::Result<Response<AppendEntriesReply>, Status> {
        let request = request.into_inner();
        let mut reply = AppendEntriesReply {
            term: 0,
            success: false,
        };

        let mut state = self.state.lock().map_err(|_| Status::internal("Failed to get state lock"))?;

        debug!(
            "{}: AppendEntries received from {}",
            self.id, request.leader_id
        );

        if request.term > state.current_term {
            state.current_term = request.term;
            state.voted_for = None;
            state.status = NodeStatus::Follower;
        }

        reply.term = state.current_term;

        if request.term < state.current_term {
            debug!(
                "{}: AppendEntries leader term less than ours: got {}, have {}",
                self.id, request.term, state.current_term
            );
            reply.success = false;
            return Ok(Response::new(reply));
        }

        if state.log.len() - 1 < request.prev_log_index as usize {
            debug!(
                "{}: AppendEntries from {}: inconsistent log: our index {}, request.prev_log_index {}",
                self.id,
                request.leader_id,
                state.log.len() - 1,
                request.prev_log_index
            );
            reply.success = false;

            return Ok(Response::new(reply));
        }

        if state.log[request.prev_log_index as usize].term != request.prev_log_term {
            debug!(
                "{}: AppendEntries from {}: inconsistent log: our last term {}, request.prev_log_term {}",
                self.id,
                request.leader_id,
                state.log[request.prev_log_term as usize].term,
                request.prev_log_term,
            );
            reply.success = false;

            return Ok(Response::new(reply));
        }

        let mut idx: usize = request.prev_log_index as usize + 1;
        for entry in &request.entries {
            if idx as usize > state.log.len() - 1 {
                break;
            }

            if entry.term != state.log[idx].term as u64 {
                state.log = state.log[..idx].to_vec();
                break;
            }

            idx += 1;
        }

        idx = request.prev_log_index as usize + 1;

        for i in 0..request.entries.len() {
            if idx + i > state.log.len() - 1 {
                state.log.append(&mut request.entries[i..].to_vec());
                break;
            }
        }

        if request.leader_commit > state.commit_index {
            let idx_last_new_entry = state.log.len() - 1;

            if idx_last_new_entry < request.leader_commit as usize {
                state.commit_index = idx_last_new_entry as u64;
            } else {
                state.commit_index = request.leader_commit;
            }

            // TODO: check if we need to apply new entries
        }

        state.voted_for = Some(request.leader_id);
        state.leader_id = Some(request.leader_id);
        reply.success = true;
        debug!(
            "{}: AppendEntries successful: Current Log: {:?}, commit_index {}, current leader {:?}",
            self.id, state.log, state.commit_index, state.leader_id
        );

        // TODO: reset election timer

        Ok(Response::new(reply))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    async fn request_vote_on_follower_test() {
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

            let reply = follower.request_vote(Request::new(args)).await.unwrap();

            assert_eq!(reply.into_inner(), expected);
        }
    }

    #[tokio::test]
    async fn append_entries_term_update() {
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
            (
                // our log is too short
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
            (
                // inconsistent term at prev_log_index
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
            (
                // higher term, make sure we update
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
            (
                // higher term, make sure we update
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

            let reply = follower.append_entries(Request::new(args)).await.unwrap();

            assert_eq!(reply.into_inner(), expected);
        }
    }
}
