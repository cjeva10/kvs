use crate::rpc::{ClientRequestReply, AppendEntriesArgs, AppendEntriesReply, Log, RequestVoteArgs, RequestVoteReply};
use std::collections::HashMap;
use tokio::sync::{
    mpsc::Sender,
    oneshot::Sender as OneShotSender,
};

// for checking what the current state of the node is
#[derive(Debug, PartialEq, Clone)]
pub struct State {
    pub id: u64,
    pub role: Role,
    pub votes: u64,
    pub term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<Log>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: HashMap<u64, usize>,
    pub match_index: HashMap<u64, usize>,
    pub killed: bool,
    pub leader_id: Option<u64>,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug)]
pub enum Message {
    AppendEntries(AppendEntriesArgs, Callback<Message>),
    AppendEntriesReply(AppendEntriesReply),
    RequestVote(RequestVoteArgs, Callback<Message>),
    RequestVoteReply(RequestVoteReply),
    CheckState(Sender<Message>),
    State(State),
    ClientRequest(String, Callback<ClientRequestReply>),
    ClientRequestReply(ClientRequestReply),
    Kill,
}

#[derive(Debug)]
pub enum Callback<T> {
    Mpsc(Sender<T>),
    OneShot(OneShotSender<T>),
}

