use crate::{Error, Result, RaftClient};
use log::debug;
use tokio::net::ToSocketAddrs;
use tonic::transport::Channel;
use crate::raft_rpc::raft_server::Raft;
use crate::raft_rpc::{AppendEntriesArgs, AppendEntriesReply, Log, RequestVoteArgs, RequestVoteReply};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tonic::{Request, Response, Status};

mod rpc;
mod helpers;
mod client;

#[derive(Debug, Default)]
enum NodeStatus {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
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

impl Default for State {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
pub struct Node {
    state: Arc<Mutex<State>>,

    peers: Vec<Peer>,
    id: usize,
}

#[derive(Debug)]
pub struct Peer {
    pub client: RaftClient<Channel>,
    pub id: usize,
}

impl Node {
    pub fn new(id: usize, peers: Vec<Peer>) -> Result<Self> {
        let peer_ids: Vec<usize> = peers.iter().map(|peer| peer.id).collect();
        if peer_ids.contains(&id) {
            return Err(Error::BadIds);
        }

        Ok(Node {
            state: Arc::new(Mutex::new(State::new())),
            peers,
            id,
        })
    }
}


