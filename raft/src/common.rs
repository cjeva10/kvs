use crate::rpc::{
    AppendEntriesArgs, AppendEntriesReply, ClientRequestReply, RequestVoteArgs,
    RequestVoteReply,
};
use crate::node::Log;
use std::{collections::HashMap, fmt::Display};
use tokio::sync::{mpsc::Sender, oneshot::Sender as OneShotSender};

// for checking what the current state of the node is
#[derive(Debug, PartialEq, Clone)]
pub struct State {
    pub id: u64,
    pub role: Role,
    pub votes: u64,
    pub term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<Log<String>>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub next_index: HashMap<u64, u64>,
    pub match_index: HashMap<u64, u64>,
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

impl Display for Log<String> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[term: {}, command: {}]", self.term, self.command)
    }
}

#[derive(Debug)]
pub struct OutboundMessage {
    pub message: Message,
    pub to: u64,
    pub from: u64,
}

#[derive(Debug)]
pub enum Callback<T> {
    Mpsc(Sender<T>),
    OneShot(OneShotSender<T>),
}
