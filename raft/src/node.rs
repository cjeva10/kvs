use crate::{
    common::{ClientRequestReply, Message, Role, State},
    rpc::{AppendEntriesArgs, AppendEntriesReply, Log, RequestVoteArgs, RequestVoteReply},
    Error, Result,
};
use log::{debug, warn};
use rand::Rng;
use std::{cmp::min, collections::HashMap, path::PathBuf};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{timeout, Duration, Instant},
};

/// An instance of a Raft Node
#[derive(Debug)]
pub struct Node {
    /// This `Node`s id
    pub id: u64,
    /// Either `Follower`, `Candidate` or `Leader`
    pub role: Role,
    /// The number of votes we received this term
    pub votes: u64,

    /// The current term
    pub term: u64,
    /// If we've voted in this term and who for
    pub voted_for: Option<u64>,
    /// The current log
    pub log: Vec<Log>,

    /// index of the highest log entry known to be committed
    pub commit_index: u64,
    /// index of highest log entry applied to state machine
    pub last_applied: u64,

    /// for each server, index of the next log entry to send to that server
    pub next_index: HashMap<u64, usize>,
    /// for each server, index of highest log entry known to be replicated on that server
    pub match_index: HashMap<u64, usize>,

    /// Receive all messages on a single channel
    ///
    /// TODO: Create a network thread to send RPC messages on this channel
    pub inbox: Receiver<Message>,
    /// Store network peers as a `HashMap` from id's to a `Sender`
    ///
    /// TODO: Create a network thread that simply receives on these channels
    pub peers: HashMap<u64, Sender<Message>>,

    /// For manually shutting down the node in testing
    pub killed: bool,
    /// The current leader
    pub leader_id: Option<u64>,
}

impl Node {
    /// Generate a new `Node` with a blank slate (i.e. a "Follower", with empty log)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raft::Node;
    /// use std::collections::HashMap;
    /// use tokio::sync::mpsc;
    ///
    ///
    /// let (_, rx) = mpsc::channel(1);
    /// let node = Node::new(1, rx, HashMap::new());
    ///
    /// assert_eq!(node.term, 0);
    /// ```
    pub fn new(id: u64, inbox: Receiver<Message>, peers: HashMap<u64, Sender<Message>>) -> Self {
        assert_ne!(id, 0);
        Self {
            id,
            role: Role::Follower,
            votes: 0,
            term: 0,
            voted_for: None,
            log: vec![Log {
                term: 0,
                command: "".to_string(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            inbox,
            peers,
            killed: false,
            leader_id: None,
        }
    }

    /// Recover a `Node` from a persistent state file
    /// TODO
    pub fn from_file(
        id: u64,
        inbox: Receiver<Message>,
        path: impl Into<PathBuf>,
        peers: HashMap<u64, Sender<Message>>,
    ) -> Result<Self> {
        todo!()
    }

    /// Start the `Node`s main event loop. Reads messages and "tick" the current state whenever the
    /// timeout elapses, calling an election or sending heartbeats.
    ///
    /// Since the loop is infinite it is recommended to spawn this function on a new task
    ///
    /// # Examples
    ///
    /// ```rust
    /// # tokio_test::block_on(async {
    ///
    /// use raft::{Message, Node};
    /// use std::{
    ///     collections::HashMap,
    /// };
    /// use tokio::{
    ///     sync::mpsc,
    ///     time::Duration,
    /// };
    ///
    /// let (tx, rx) = mpsc::channel(32);
    ///
    /// let node = Node::new(1, rx, HashMap::new());
    ///
    /// tokio::spawn(async {
    ///     node.start(100).await.unwrap();
    /// });
    ///
    /// let (state_tx, mut state_rx) = mpsc::channel(32);
    /// tx.send(Message::CheckState(state_tx)).await.unwrap();
    ///
    /// tokio::time::sleep(Duration::from_millis(10)).await;
    ///
    /// let Message::State(state) = state_rx.recv().await.unwrap() else {
    ///     panic!("Expected Message::State");
    /// };
    ///
    /// assert_eq!(state.term, 0);
    /// # })
    /// ```
    pub async fn start(mut self, min_delay: u64) -> Result<()> {
        if min_delay < 50 {
            warn!("Minimum timeout of {}ms might be too low", min_delay);
        }

        // initialize the timer
        let mut t = Instant::now();
        let mut time_left = match self.role {
            Role::Follower | Role::Candidate => {
                Duration::from_millis(min_delay + rand::thread_rng().gen_range(0..50))
            }
            Role::Leader => Duration::from_millis(min_delay / 2),
        };
        debug!("Setting initial timeout to {}", time_left.as_millis());

        // infinite loop where we check for messages
        //
        // If the inbox is empty, the thread blocks until the timeout is reached. Then the Node
        // will `tick` either calling an election or triggering heartbeats.
        loop {
            match timeout(time_left, self.inbox.recv()).await {
                Ok(Some(m)) => {
                    debug!("Got a message {:?}", m);
                    // if a read returns true, that means we should restart our tick timer
                    if self.handle_message(m).await? {
                        t = Instant::now();
                        time_left = match self.role {
                            Role::Follower | Role::Candidate => Duration::from_millis(
                                min_delay + rand::thread_rng().gen_range(0..50),
                            ),
                            Role::Leader => Duration::from_millis(min_delay / 2),
                        }
                    // if read returns false, then simply decrement the time elapsed
                    } else {
                        time_left -= t.elapsed();
                        t = Instant::now();
                    };
                }
                Err(_) => {
                    debug!(
                        "{}: Timer timeout: term {}, role {:?}",
                        self.id, self.term, self.role
                    );
                    self.tick().await?;
                    t = Instant::now();
                    time_left = match self.role {
                        Role::Leader => Duration::from_millis(50),
                        Role::Follower | Role::Candidate => {
                            Duration::from_millis(min_delay + rand::thread_rng().gen_range(0..50))
                        }
                    };
                }
                Ok(None) => {
                    debug!("Broken Inbox: inbox channel disconnected");
                    return Err(Error::BrokenInbox);
                }
            }
        }
    }

    async fn handle_client_request(&mut self, s: String, tx: Sender<Message>) -> Result<bool> {
        debug!("{}: Received ClientRequest: command = {}", self.id, s);

        if self.role != Role::Leader {
            debug!(
                "{}: ClientRequest failed, not leader: alleged leader {:?}",
                self.id, self.leader_id
            );
            let reply = ClientRequestReply {
                success: false,
                leader_id: self.leader_id,
            };

            tx.send(Message::ClientRequestReply(reply)).await?;
        } else {
        }
        Ok(false)
    }

    async fn handle_message(&mut self, m: Message) -> Result<bool> {
        if self.killed {
            return Ok(true);
        }

        match m {
            Message::RequestVote(args) => self.handle_request_vote(args).await,
            Message::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Message::AppendEntries(args) => self.handle_append_entries(args).await,
            Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply),
            Message::CheckState(tx) => self.handle_check_state(tx).await,
            Message::ClientRequest(str, tx) => self.handle_client_request(str, tx).await,
            Message::State(_) | Message::ClientRequestReply(_) => Ok(false),
            Message::Kill => {
                self.killed = true;
                Ok(true)
            }
        }
    }

    async fn handle_request_vote(&mut self, args: RequestVoteArgs) -> Result<bool> {
        let mut reply = RequestVoteReply {
            term: self.term,
            vote_granted: false,
            peer: self.id,
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

            self.send_message(args.candidate_id, Message::RequestVoteReply(reply))
                .await?;

            return Ok(false);
        }

        if self.voted_for == None || self.voted_for == Some(args.candidate_id) {
            let our_last_index = self.log.len() as u64 - 1;
            let our_last_term = self.log[our_last_index as usize].term;

            if args.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, args.candidate_id);

                self.voted_for = Some(args.candidate_id);
                reply.vote_granted = true;

                self.send_message(args.candidate_id, Message::RequestVoteReply(reply))
                    .await?;
                return Ok(true);
            } else if args.last_log_term == our_last_term {
                if args.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, args.candidate_id);

                    self.voted_for = Some(args.candidate_id);
                    reply.vote_granted = true;
                    self.send_message(args.candidate_id, Message::RequestVoteReply(reply))
                        .await?;
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

        self.send_message(args.candidate_id, Message::RequestVoteReply(reply))
            .await?;
        Ok(false)
    }

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
                if self.votes >= self.peers.len() as u64 / 2 + 1 {
                    debug!(
                        "{}: won the election: votes {}, needed {}",
                        self.id,
                        self.votes,
                        self.peers.len() / 2 + 1
                    );

                    self.become_leader();
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    async fn handle_append_entries(&mut self, args: AppendEntriesArgs) -> Result<bool> {
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

            self.send_message(args.leader_id, Message::AppendEntriesReply(reply))
                .await?;
            return Ok(false);
        }

        if self.log.len() as u64 - 1 < args.prev_log_index {
            debug!(
                "{}: AppendEntries from {}: inconsistent log index: got {}, have {}",
                self.id,
                args.leader_id,
                args.prev_log_index,
                self.log.len() - 1
            );

            reply.success = false;

            // ignore any errors when sending a message
            let _ = self
                .send_message(args.leader_id, Message::AppendEntriesReply(reply))
                .await;
            return Ok(false);
        }

        if self.log[args.prev_log_index as usize].term != args.prev_log_term {
            debug!(
                "{}: AppendEntries from {}: inconsistent log term: got {}, have {}",
                self.id,
                args.leader_id,
                args.prev_log_term,
                self.log[args.prev_log_index as usize].term
            );

            reply.success = false;

            self.send_message(args.leader_id, Message::AppendEntriesReply(reply))
                .await?;
            return Ok(false);
        }

        // if an existing entry conflicts with a new one (same index but different terms)
        // delete the existing entry and all that follow it
        let mut idx = args.prev_log_index as usize + 1;
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
        let idx = args.prev_log_index as usize + 1;
        for i in 0..args.entries.len() {
            if idx + 1 > self.log.len() - 1 {
                self.log.append(&mut args.entries[i..].to_vec());
                break;
            }
        }

        // receiver implementation 5 from Raft paper
        if args.leader_commit > self.commit_index {
            let idx_last_new_entry = self.log.len() - 1;

            self.commit_index = min(idx_last_new_entry as u64, args.leader_commit);

            self.check_last_applied();
        }

        self.voted_for = Some(args.leader_id);
        reply.success = true;
        self.send_message(args.leader_id, Message::AppendEntriesReply(reply))
            .await?;
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

    async fn handle_check_state(&self, tx: Sender<Message>) -> Result<bool> {
        tx.send(Message::State(self.state())).await.unwrap();
        Ok(false)
    }

    fn state(&self) -> State {
        State {
            id: self.id,
            role: self.role.clone(),
            votes: self.votes,
            term: self.term,
            voted_for: self.voted_for,
            log: self.log.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            next_index: self.next_index.clone(),
            match_index: self.match_index.clone(),
            killed: self.killed,
            leader_id: self.leader_id,
        }
    }

    /// either call an election if we are a candidate or a follower, else send heartbeats
    /// to all the followers
    async fn tick(&mut self) -> Result<()> {
        if self.killed {
            return Ok(());
        }

        match self.role {
            Role::Follower | Role::Candidate => self.start_election().await?,
            Role::Leader => self.send_heartbeats().await?,
        }

        Ok(())
    }

    async fn start_election(&mut self) -> Result<()> {
        self.term += 1;
        self.voted_for = Some(self.id);
        self.votes = 1;

        debug!(
            "{}: calling an election: term {}, votes {}",
            self.id, self.term, self.votes
        );
        if self.votes >= self.peers.len() as u64 / 2 + 1 {
            debug!(
                "{}: won the election: votes {}, needed {}",
                self.id,
                self.votes,
                self.peers.len() / 2 + 1
            );

            self.become_leader();
            return Ok(());
        }

        let request = RequestVoteArgs {
            term: self.term,
            candidate_id: self.id,
            last_log_index: self.log.len() as u64 - 1,
            last_log_term: self.log[self.log.len() - 1].term,
        };

        for peer in self.peers.keys() {
            debug!("{}: sending request vote to {}", self.id, peer);
            self.send_message(*peer, Message::RequestVote(request.clone()))
                .await?;
        }

        Ok(())
    }

    async fn send_heartbeats(&mut self) -> Result<()> {
        debug!(
            "{}: Sending heartbeats to peers: term {}",
            self.id, self.term
        );

        let request = AppendEntriesArgs {
            entries: Vec::new(),
            leader_commit: self.commit_index as u64,
            leader_id: self.id,
            prev_log_index: 0,
            prev_log_term: 0,
            term: self.term,
        };

        for peer in self.peers.keys() {
            debug!("{}: sending heartbeat to {}", self.id, peer);
            self.send_message(*peer, Message::AppendEntries(request.clone()))
                .await?;
        }
        Ok(())
    }

    fn become_follower(&mut self, term: u64) {
        self.term = term;
        self.voted_for = None;
        self.votes = 0;
        self.role = Role::Follower;
    }

    fn become_leader(&mut self) {
        self.role = Role::Leader;
        self.leader_id = Some(self.id);
    }

    /// send a message to a given peer
    async fn send_message(&self, peer: u64, m: Message) -> Result<()> {
        let sender = self
            .peers
            .get(&peer)
            .expect(format!("{}: Looked for peer {}, and missed", self.id, peer).as_str());
        let _ = sender.send(m).await;
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::init_local_nodes;

    const MIN_DELAY: u64 = 100;

    fn make_state_channels(num: usize) -> (Vec<Sender<Message>>, Vec<Receiver<Message>>) {
        let mut senders = Vec::new();
        let mut receivers = Vec::new();
        for _ in 0..num {
            let (tx, rx) = tokio::sync::mpsc::channel(64);
            senders.push(tx);
            receivers.push(rx);
        }

        (senders, receivers)
    }

    // A single node should start an election and elect itself the leader
    #[tokio::test]
    async fn test_one_node_election() {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::mpsc::channel(16);
        let node = Node::new(1, rx, HashMap::new());

        tokio::spawn(async move {
            node.start(MIN_DELAY).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 3 / 2)).await;

        tx.send(Message::CheckState(state_tx.clone()))
            .await
            .unwrap();

        let Message::State(state) = state_rx.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        let expected = State {
            id: 1,
            commit_index: 0,
            last_applied: 0,
            log: vec![Log {
                term: 0,
                command: "".to_string(),
            }],
            match_index: HashMap::new(),
            next_index: HashMap::new(),
            role: Role::Leader,
            term: 1,
            voted_for: Some(1),
            votes: 1,
            killed: false,
            leader_id: Some(1),
        };

        assert_eq!(expected, state);
    }

    #[tokio::test]
    async fn test_three_node_election() {
        let (tx_1, rx_1) = tokio::sync::mpsc::channel(64);
        let (tx_2, rx_2) = tokio::sync::mpsc::channel(64);
        let (tx_3, rx_3) = tokio::sync::mpsc::channel(64);

        let (state_tx_1, mut state_rx_1) = tokio::sync::mpsc::channel(4);
        let (state_tx_2, mut state_rx_2) = tokio::sync::mpsc::channel(4);
        let (state_tx_3, mut state_rx_3) = tokio::sync::mpsc::channel(4);

        let node_1 = Node::new(
            1,
            rx_1,
            HashMap::from([(2, tx_2.clone()), (3, tx_3.clone())]),
        );
        let node_2 = Node::new(
            2,
            rx_2,
            HashMap::from([(1, tx_1.clone()), (3, tx_3.clone())]),
        );
        let node_3 = Node::new(
            3,
            rx_3,
            HashMap::from([(1, tx_1.clone()), (2, tx_2.clone())]),
        );

        tokio::spawn(async move {
            node_1.start(MIN_DELAY).await.unwrap();
        });
        tokio::spawn(async move {
            node_2.start(MIN_DELAY).await.unwrap();
        });
        tokio::spawn(async move {
            node_3.start(MIN_DELAY).await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        tx_1.send(Message::CheckState(state_tx_1)).await.unwrap();
        tx_2.send(Message::CheckState(state_tx_2)).await.unwrap();
        tx_3.send(Message::CheckState(state_tx_3)).await.unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let Message::State(state_1) = state_rx_1.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_2) = state_rx_2.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_3) = state_rx_3.recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        if state_1.role != Role::Leader
            && state_2.role != Role::Leader
            && state_3.role != Role::Leader
        {
            panic!("No leader elected!!");
        }
    }
    #[tokio::test]
    async fn test_three_nodes_kill_leader() {
        let (nodes, tx) = init_local_nodes(3);
        let (state_tx, mut state_rx) = make_state_channels(3);

        for node in nodes {
            tokio::spawn(async {
                let _ = node.start(MIN_DELAY).await;
            });
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        tx[0]
            .send(Message::CheckState(state_tx[0].clone()))
            .await
            .unwrap();
        tx[1]
            .send(Message::CheckState(state_tx[1].clone()))
            .await
            .unwrap();
        tx[2]
            .send(Message::CheckState(state_tx[2].clone()))
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(100)).await;

        let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };
        let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
            panic!("Expected Message::State");
        };

        #[allow(unused_assignments)]
        let mut killed = 0;

        if state_1.role == Role::Leader {
            debug!("Killed Node 1");
            tx[0].send(Message::Kill).await.unwrap();
            killed = 1;
        } else if state_2.role == Role::Leader {
            debug!("Killed Node 2");
            tx[1].send(Message::Kill).await.unwrap();
            killed = 2;
        } else if state_3.role == Role::Leader {
            debug!("Killed Node 3");
            tx[2].send(Message::Kill).await.unwrap();
            killed = 3;
        } else {
            panic!("No leader elected!");
        }

        tokio::time::sleep(Duration::from_millis(MIN_DELAY * 2)).await;

        let mut found_leader = 0;

        match killed {
            1 => {
                tx[1]
                    .send(Message::CheckState(state_tx[1].clone()))
                    .await
                    .unwrap();
                tx[2]
                    .send(Message::CheckState(state_tx[2].clone()))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_2.role == Role::Leader {
                    found_leader += 1;
                }
                if state_3.role == Role::Leader {
                    found_leader += 1;
                }
            }
            2 => {
                tx[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                tx[2]
                    .send(Message::CheckState(state_tx[2].clone()))
                    .await
                    .unwrap();

                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_3) = state_rx[2].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_1.role == Role::Leader {
                    found_leader += 1;
                }
                if state_3.role == Role::Leader {
                    found_leader += 1;
                }
            }
            3 => {
                tx[0]
                    .send(Message::CheckState(state_tx[0].clone()))
                    .await
                    .unwrap();
                tx[1]
                    .send(Message::CheckState(state_tx[1].clone()))
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_millis(MIN_DELAY / 4)).await;

                let Message::State(state_1) = state_rx[0].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };
                let Message::State(state_2) = state_rx[1].recv().await.unwrap() else {
                    panic!("Expected Message::State");
                };

                if state_1.role == Role::Leader {
                    found_leader += 1;
                }
                if state_2.role == Role::Leader {
                    found_leader += 1;
                }
            }
            _ => panic!("No one was killed"),
        }

        assert_eq!(found_leader, 1);
    }
}
