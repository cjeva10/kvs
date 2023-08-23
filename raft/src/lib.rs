use log::debug;
use rand::{thread_rng, Rng};
use std::{
    cmp::min,
    collections::HashMap,
    sync::mpsc::{Receiver, SendError, Sender, TryRecvError},
    time::{Duration, Instant},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Inbox channel broken")]
    BrokenInbox,
    #[error("Failed to send message to peer")]
    FailedSend(#[from] SendError<Message>),
}
pub type Result<T> = std::result::Result<T, Error>;

/// An instance of a Raft Node
pub struct Node {
    /// This `Node`s id
    pub id: usize,
    /// Either `Follower`, `Candidate` or `Leader`
    pub role: Role,
    /// The number of votes we received this term
    pub votes: usize,

    /// The current term
    pub term: usize,
    /// If we've voted in this term and who for
    pub voted_for: Option<usize>,
    /// The current log
    pub log: Vec<Log>,

    /// index of the highest log entry known to be committed
    pub commit_index: usize,
    /// index of highest log entry applied to state machine
    pub last_applied: usize,

    /// for each server, index of the next log entry to send to that server
    pub next_index: HashMap<usize, usize>,
    /// for each server, index of highest log entry known to be replicated on that server
    pub match_index: HashMap<usize, usize>,

    /// Receive all messages on a single channel
    ///
    /// TODO: Create a network thread to send RPC messages on this channel
    pub inbox: Receiver<Message>,
    /// Store network peers as a `HashMap` from id's to a `Sender`
    ///
    /// TODO: Create a network thread that simply receives on these channels
    pub peers: HashMap<usize, Sender<Message>>,
}

#[derive(Debug, Default)]
pub enum Role {
    #[default]
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
pub struct RequestVoteArgs {
    term: usize,
    candidate_id: usize,
    last_log_index: usize,
    last_log_term: usize,
}

#[derive(Debug)]
pub struct RequestVoteReply {
    term: usize,
    vote_granted: bool,
}

#[derive(Debug)]
pub struct AppendEntriesArgs {
    term: usize,
    leader_id: usize,
    prev_log_index: usize,
    prev_log_term: usize,
    entries: Vec<Log>,
    leader_commit: usize,
}

#[derive(Debug, Clone)]
pub struct Log {
    pub term: usize,
    pub command: String,
}

#[derive(Debug)]
pub struct AppendEntriesReply {
    term: usize,
    success: bool,
    peer: usize,
}

#[derive(Debug)]
pub enum Message {
    AppendEntries(AppendEntriesArgs),
    AppendEntriesReply(AppendEntriesReply),
    RequestVote(RequestVoteArgs),
    RequestVoteReply(RequestVoteReply),
}

impl Node {
    pub fn start(mut self) -> Result<()> {
        let mut t = Instant::now();
        let mut rng = thread_rng();
        loop {
            // handle all the messages in our inbox
            loop {
                match self.inbox.try_recv() {
                    Ok(m) => {
                        // if a read returns true, that means we should restart our tick timer
                        if self.handle_message(m)? {
                            t = Instant::now();
                        };
                    }
                    Err(TryRecvError::Empty) => break,
                    // This shouldn't happen because the network should live as long as the node
                    Err(TryRecvError::Disconnected) => return Err(Error::BrokenInbox),
                }
            }

            // TODO: implement different timer based on whether Node is leader
            if t.elapsed() >= Duration::from_millis(100 + rng.gen_range(0..50)) {
                self.tick()?;
                t = Instant::now();
            }
        }
    }

    /// handles the message on the state machine
    ///
    /// if the message results in needing to reset the timer, we return `true`
    fn handle_message(&mut self, m: Message) -> Result<bool> {
        match m {
            Message::RequestVote(args) => self.handle_request_vote(args),
            Message::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Message::AppendEntries(args) => self.handle_append_entries(args),
            Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply),
        }
    }

    fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Result<bool> {
        let mut reply = RequestVoteReply {
            term: self.term,
            vote_granted: false,
        };

        if args.term > self.term {
            self.become_follower(args.term);
        }

        reply.term = self.term;

        debug!(
            "{}: Received vote request from {}, term = {}",
            self.id, args.candidate_id, self.term
        );

        if args.term < self.term {
            debug!(
                "{}: Rejected vote request from {}, outdated term: got {}, have {}",
                self.id, args.candidate_id, args.term, self.term
            );

            reply.vote_granted = false;

            self.send_message(args.candidate_id, Message::RequestVoteReply(reply))?;

            return Ok(false);
        }

        if self.voted_for == None || self.voted_for == Some(args.candidate_id) {
            let our_last_index = self.log.len() - 1;
            let our_last_term = self.log[our_last_index].term;

            if args.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, args.candidate_id);

                self.voted_for = Some(args.candidate_id);
                reply.vote_granted = true;

                self.send_message(args.candidate_id, Message::RequestVoteReply(reply))?;
                return Ok(true);
            } else if args.last_log_term == our_last_term {
                if args.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, args.candidate_id);

                    self.voted_for = Some(args.candidate_id);
                    reply.vote_granted = true;
                    self.send_message(args.candidate_id, Message::RequestVoteReply(reply))?;
                    return Ok(true);
                } else {
                    debug!("{}: Rejected vote request from {}, log outdated: got term {}, have term {}", self.id, args.candidate_id, our_last_term, args.last_log_term);
                }
            } else {
                debug!(
                    "{}: Rejected vote request from {}, log outdated: got index {}, have index {}",
                    self.id, args.candidate_id, our_last_index, args.last_log_index
                );
            }
        }
        debug!(
            "{}: Rejected vote request from {}, already voted for {}",
            self.id,
            args.candidate_id,
            self.voted_for.unwrap()
        );

        self.send_message(args.candidate_id, Message::RequestVoteReply(reply))?;
        Ok(false)
    }

    /// It is assumed that replies to `requestVote` don't go to the wrong node, they either
    /// get sent or they fail to reach destination for whatever reason.
    ///
    /// Therefore, if we receive a reply, it's either to our current term, or it's a stale reply
    /// In the first case, we handle it as specified exactly in the raft paper, else we ignore
    fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) -> Result<bool> {
        if reply.term < self.term {
            debug!(
                "{}: Stale request vote reply from term {}",
                self.id, reply.term
            );
            return Ok(false);
        } else if reply.term > self.term {
            debug!(
                "{}: request vote reply with greater term {}",
                self.id, reply.term
            );
            self.become_follower(reply.term);
        } else {
            if reply.vote_granted {
                self.votes += 1;
            }
        }

        Ok(false)
    }

    fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> Result<bool> {
        debug!(
            "{}: AppendEntries received from {}",
            self.id, args.leader_id
        );

        let mut reply = AppendEntriesReply {
            term: self.term,
            success: false,
            peer: self.id,
        };

        if args.term > self.term {
            self.become_follower(args.term);
        }

        reply.term = self.term;

        if args.term < self.term {
            debug!(
                "{}: AppendEntries from {}, term less than ours: got {}, have {}",
                self.id, args.leader_id, args.term, self.term
            );

            reply.success = false;

            self.send_message(args.leader_id, Message::AppendEntriesReply(reply))?;
            return Ok(false);
        }

        if self.log.len() - 1 < args.prev_log_index {
            debug!(
                "{}: AppendEntries from {}: inconsistent log index: got {}, have {}",
                self.id,
                args.leader_id,
                args.prev_log_index,
                self.log.len() - 1
            );

            reply.success = false;

            self.send_message(args.leader_id, Message::AppendEntriesReply(reply))?;
            return Ok(false);
        }

        if self.log[args.prev_log_index].term != args.prev_log_term {
            debug!(
                "{}: AppendEntries from {}: inconsistent log term: got {}, have {}",
                self.id, args.leader_id, args.prev_log_term, self.log[args.prev_log_index].term
            );

            reply.success = false;

            self.send_message(args.leader_id, Message::AppendEntriesReply(reply))?;
            return Ok(false);
        }

        // if an existing entry conflicts with a new one (same index but different terms)
        // delete the existing entry and all that follow it
        let mut idx = args.prev_log_index + 1;
        for entry in &args.entries {
            if idx > self.log.len() - 1 {
                break;
            }
            if entry.term != self.log[idx].term {
                self.log = self.log[..idx].to_vec();
                break;
            }
            idx += 1;
        }

        // append any new entries not in the log
        let idx = args.prev_log_index + 1;
        for i in 0..args.entries.len() {
            if idx + 1 > self.log.len() - 1 {
                self.log.append(&mut args.entries[i..].to_vec());
                break;
            }
        }

        // receiver implementation 5 from Raft paper
        if args.leader_commit > self.commit_index {
            let idx_last_new_entry = self.log.len() - 1;

            self.commit_index = min(idx_last_new_entry, args.leader_commit);

            self.check_last_applied();
        }

        self.voted_for = Some(args.leader_id);
        reply.success = true;
        self.send_message(args.leader_id, Message::AppendEntriesReply(reply))?;
        debug!(
            "{}: AppendEntries from {} successful: Current log {:?}, commit_index {}",
            self.id, args.leader_id, self.log, self.commit_index
        );

        // reset the timer
        Ok(true)
    }

    fn check_last_applied(&mut self) {
        todo!()
    }

    fn become_follower(&mut self, term: usize) {
        self.term = term;
        self.voted_for = None;
        self.votes = 0;
        self.role = Role::Follower;
    }

    fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) -> Result<bool> {
        if reply.term > self.term {
            self.become_follower(reply.term);
            return Ok(true);
        } else if reply.term < self.term {
            return Ok(false);
        } else {
            if reply.success {
                // WARNING: This might create a race condition if there is a log appended
                // before this reply gets to us, therefore causing us to think this peer is
                // more up to date than he is.
                self.next_index.insert(reply.peer, self.log.len());
                self.match_index.insert(reply.peer, self.log.len() - 1);
            } else {
                let next_index = self.next_index.get(&reply.peer).unwrap();
                self.next_index.insert(reply.peer, next_index - 1);
            }
        }
        Ok(false)
    }

    /// send a message to a given peer
    pub fn send_message(&self, peer: usize, m: Message) -> Result<()> {
        let sender = self.peers.get(&peer).unwrap();
        sender.send(m)?;
        Ok(())
    }

    /// either call an election if we are a candidate or a follower, else send heartbeats
    /// to all the followers
    pub fn tick(&mut self) -> Result<()> {
        match self.role {
            Role::Follower | Role::Candidate => self.start_election()?,
            Role::Leader => self.send_heartbeats()?,
        }

        Ok(())
    }

    fn start_election(&mut self) -> Result<()> {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes = 1;

        let request = RequestVoteArgs {
            term: self.term,
            candidate_id: self.id,
            last_log_index: self.log.len() - 1,
            last_log_term: self.log[self.log.len() - 1].term,
        };
        for peer in self.peers.keys() {
            self.send_message(*peer, Message::RequestVote(request.clone()))?;
        }

        Ok(())
    }

    fn send_heartbeats(&mut self) -> Result<()> {
        todo!()
    }
}
