use crate::node::Log;
use crate::Result;

pub struct RequestVoteArgs {
    pub term: usize,
    pub candidate_id: usize,
    pub last_log_index: usize,
    pub last_log_term: usize,
}

#[derive(Debug, PartialEq)]
pub struct RequestVoteReply {
    pub term: usize,
    pub vote_granted: bool,
}

pub struct AppendEntriesArgs {
    pub term: usize,
    pub leader_id: usize,
    pub prev_log_index: usize,
    pub prev_log_term: usize,
    pub entries: Vec<Log>,
    pub leader_commit: usize,
}

#[derive(Debug, PartialEq)]
pub struct AppendEntriesReply {
    pub term: usize,
    pub success: bool,
}

pub trait Rpc {
    fn request_vote(&self, args: RequestVoteArgs) -> Result<RequestVoteReply>;
    fn append_entries(&self, args: AppendEntriesArgs) -> Result<AppendEntriesReply>;
}
