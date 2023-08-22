mod error;
mod node;

pub use error::{Error, Result};
pub use node::{Node, Peer};

mod raft_rpc {
    tonic::include_proto!("raft");
}

pub use raft_rpc::{
    raft_client::RaftClient,
    raft_server::{Raft, RaftServer},
};
pub use raft_rpc::{AppendEntriesArgs, AppendEntriesReply, Log, RequestVoteArgs, RequestVoteReply};
