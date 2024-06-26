use crate::{
    common::{Callback, Message, OutboundMessage, Role, State},
    rpc::{
        self, AppendEntriesArgs, AppendEntriesReply, ClientRequestReply, RequestVoteArgs,
        RequestVoteReply,
    },
    state_machine::{Serialize, StateMachine},
    Error, Result,
};
use log::{debug, error, trace, warn};
use rand::Rng;
use std::{cmp::min, collections::HashMap, fmt::Debug, fmt::Display, path::PathBuf};
use tokio::{
    sync::mpsc::{Receiver, Sender},
    time::{timeout, Duration, Instant},
};

#[derive(Debug, PartialEq, Clone)]
pub struct Log<C>
where
    C: TryFrom<String> + Clone + Default + Debug + Display,
{
    pub command: C,
    pub term: u64,
}

/// An instance of a Raft Node
#[derive(Debug)]
pub struct Node<SM, C>
where
    C: TryFrom<String> + Clone + Display + Serialize + Debug + Default,
    SM: StateMachine<C>,
{
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
    pub log: Vec<Log<C>>,

    /// index of the highest log entry known to be committed
    pub commit_index: u64,
    /// index of highest log entry applied to state machine
    pub last_applied: u64,

    /// for each server, index of the next log entry to send to that server
    pub next_index: HashMap<u64, u64>,
    /// for each server, index of highest log entry known to be replicated on that server
    pub match_index: HashMap<u64, u64>,

    /// Receive all messages on a single channel
    pub inbox: Receiver<Message>,
    /// Store a reference to our inbox `Sender` half so that we can use it as a callback
    pub to_inbox: Sender<Message>,
    /// For outgoing AppendEntries and RequestVote RPC messages
    pub outbox: Sender<OutboundMessage>,

    /// A list of peer ids. The network layer will determine how to route messages to the correct
    /// peer depending on what type of network connection we're using
    pub peers: Vec<u64>,

    /// TODO: Store callback channels for client requests. When the last_applied is greater than the
    /// index stored here, we can

    /// For manually shutting down the node in testing
    pub killed: bool,
    /// The current leader
    pub leader_id: Option<u64>,

    /// The state machine for this node
    pub machine: SM,

    /// Client callbacks for replying to client requests
    ///
    /// When we apply new logs to our machine, if there is a pending client request we can send the
    /// reply across these channels
    ///
    /// Note that sometimes there is no callback (i.e. when logs are applied when we are a follower)
    pub callbacks: Vec<Option<Callback<ClientRequestReply>>>,
}

impl<SM, C> Node<SM, C>
where
    C: TryFrom<String> + Serialize + Clone + Display + Default + Debug,
    SM: StateMachine<C>,
{
    /// Generate a new `Node` with a blank slate (i.e. a "Follower", with empty log)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use raft::{Node, state_machine::DummyStateMachine};
    /// use std::collections::HashMap;
    /// use tokio::sync::mpsc;
    ///
    ///
    /// let (to_inbox, inbox) = mpsc::channel(1);
    /// let (to_outbox, outbox) = mpsc::channel(1);
    /// let node = Node::new(1, inbox, to_inbox.clone(), to_outbox.clone(), Vec::new(), DummyStateMachine {});
    ///
    /// assert_eq!(node.term, 0);
    /// ```
    pub fn new(
        id: u64,
        inbox: Receiver<Message>,
        to_inbox: Sender<Message>,
        outbox: Sender<OutboundMessage>,
        peers: Vec<u64>,
        machine: SM,
    ) -> Self {
        assert_ne!(id, 0);
        Self {
            id,
            role: Role::Follower,
            votes: 0,
            term: 0,
            voted_for: None,
            log: vec![Log {
                term: 0,
                command: C::default(),
            }],
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            inbox,
            to_inbox,
            outbox,
            peers,
            killed: false,
            leader_id: None,
            machine,
            callbacks: vec![None],
        }
    }

    /// Recover a `Node` from a persistent state file
    /// TODO
    #[allow(unused_variables)]
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
    /// use raft::{Message, Node, state_machine::DummyStateMachine};
    /// use std::{
    ///     collections::HashMap,
    /// };
    /// use tokio::{
    ///     sync::mpsc,
    ///     time::Duration,
    /// };
    ///
    /// let (to_inbox, inbox) = mpsc::channel(32);
    /// let (tx, _) = mpsc::channel(32);
    ///
    /// let node = Node::new(1, inbox, to_inbox.clone(), tx.clone(), Vec::new(), DummyStateMachine {});
    ///
    /// tokio::spawn(async {
    ///     node.start(100).await.unwrap();
    /// });
    ///
    /// let (state_tx, mut state_rx) = mpsc::channel(32);
    /// to_inbox.send(Message::CheckState(state_tx)).await.unwrap();
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
        debug!(
            "{}: Setting initial timeout to {}",
            self.id,
            time_left.as_millis()
        );

        // infinite loop where we check for messages
        //
        // If the inbox is empty, the thread blocks until the timeout is reached. Then the Node
        // will `tick` either calling an election or triggering heartbeats.
        loop {
            match timeout(time_left, self.inbox.recv()).await {
                Ok(Some(m)) => {
                    // if a read returns true, that means we should restart our tick timer
                    match self.handle_message(m).await {
                        Ok(true) => {
                            t = Instant::now();
                            time_left = match self.role {
                                Role::Follower | Role::Candidate => Duration::from_millis(
                                    min_delay + rand::thread_rng().gen_range(0..50),
                                ),
                                Role::Leader => Duration::from_millis(min_delay / 2),
                            }
                        }
                        // if read returns false, then simply decrement the time elapsed
                        Ok(false) => {
                            time_left =
                                time_left.checked_sub(t.elapsed()).unwrap_or(Duration::ZERO);
                            t = Instant::now();
                        }
                        Err(e) => {
                            time_left =
                                time_left.checked_sub(t.elapsed()).unwrap_or(Duration::ZERO);
                            t = Instant::now();
                            error!("{}", e);
                        }
                    }
                }
                Err(_) => {
                    debug!(
                        "{}: Timer timeout: term {}, role {:?}",
                        self.id, self.term, self.role
                    );
                    // TODO handle error case
                    self.tick().await?;
                    t = Instant::now();
                    time_left = match self.role {
                        Role::Leader => Duration::from_millis(min_delay / 2),
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

    async fn handle_message(&mut self, m: Message) -> Result<bool> {
        if self.killed {
            return Ok(true);
        }

        match m {
            Message::RequestVote(args, tx) => self.handle_request_vote(args, tx).await,
            Message::RequestVoteReply(reply) => self.handle_request_vote_reply(reply),
            Message::AppendEntries(args, tx) => self.handle_append_entries(args, tx).await,
            Message::AppendEntriesReply(reply) => self.handle_append_entries_reply(reply).await,
            Message::CheckState(tx) => self.handle_check_state(tx).await,
            Message::ClientRequest(str, tx) => self.handle_client_request(str, tx).await,
            Message::State(_) | Message::ClientRequestReply(_) => Ok(false),
            Message::Kill => {
                self.killed = true;
                self.role = Role::Follower;
                Ok(true)
            }
        }
    }

    // takes the command as a string and a callback channel for the reply
    //
    // If we are the leader, this function should store the callback in a data structure inside the
    // Node so that when this command is committed, we can look up the callback and send the reply
    async fn handle_client_request(
        &mut self,
        s: String,
        callback: Callback<ClientRequestReply>,
    ) -> Result<bool> {
        debug!("{}: Received ClientRequest: command = {}", self.id, s);

        if self.role != Role::Leader {
            debug!(
                "{}: ClientRequest failed, not leader: alleged leader {:?}",
                self.id, self.leader_id
            );
            let reply = ClientRequestReply {
                success: false,
                leader: self.leader_id.unwrap_or(0),
                message: String::new(),
            };

            self.send_reply(reply, callback).await;
        } else {
            debug!("{}: We are leader, appending to log", self.id);
            self.log.push(Log {
                term: self.term,
                command: s.try_into().map_err(|_| Error::ParseError)?,
            });
            trace!("{}: pushing callback {:?}", self.id, callback);
            self.callbacks.push(Some(callback));
            self.check_commit_index().await?;
        }
        Ok(false)
    }

    async fn handle_request_vote(
        &mut self,
        args: RequestVoteArgs,
        callback: Callback<Message>,
    ) -> Result<bool> {
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

            self.send_reply(Message::RequestVoteReply(reply), callback)
                .await;
            return Ok(false);
        }

        if self.voted_for == None || self.voted_for == Some(args.candidate_id) {
            let our_last_index = self.log.len() as u64 - 1;
            let our_last_term = self.log[our_last_index as usize].term;

            if args.last_log_term > our_last_term {
                debug!("{}: granting vote to {}", self.id, args.candidate_id);

                self.voted_for = Some(args.candidate_id);
                reply.vote_granted = true;

                self.send_reply(Message::RequestVoteReply(reply), callback)
                    .await;
                return Ok(true);
            } else if args.last_log_term == our_last_term {
                if args.last_log_index >= our_last_index {
                    debug!("{}: granting vote to {}", self.id, args.candidate_id);

                    self.voted_for = Some(args.candidate_id);
                    reply.vote_granted = true;

                    self.send_reply(Message::RequestVoteReply(reply), callback)
                        .await;
                    return Ok(true);
                } else {
                    debug!("{}: Rejected vote request from {}, log outdated: got term {}, have term {}", self.id, args.candidate_id, our_last_term, args.last_log_term);
                    reply.vote_granted = false;
                }
            } else {
                debug!(
                    "{}: Rejected vote request from {}, log outdated: got index {}, have index {}",
                    self.id, args.candidate_id, our_last_index, args.last_log_index
                );
                reply.vote_granted = false;
            }
        }
        debug!(
            "{}: Rejected vote request from {}, already voted for {}",
            self.id,
            args.candidate_id,
            self.voted_for.unwrap()
        );

        self.send_reply(Message::RequestVoteReply(reply), callback)
            .await;
        Ok(false)
    }

    async fn send_reply<T>(&self, reply: T, callback: Callback<T>) {
        match callback {
            Callback::Mpsc(tx) => {
                let _ = tx.send(reply).await;
            }
            Callback::OneShot(tx) => {
                let _ = tx.send(reply);
            }
        }
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
                if self.votes >= self.majority() {
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

    async fn handle_append_entries(
        &mut self,
        args: AppendEntriesArgs,
        callback: Callback<Message>,
    ) -> Result<bool> {
        debug!(
            "{}: AppendEntries received from {}",
            self.id, args.leader_id
        );

        let mut reply = AppendEntriesReply {
            term: self.term,
            success: false,
            peer: self.id,
            next_index: self.log.len() as u64,
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

            self.send_reply(Message::AppendEntriesReply(reply), callback)
                .await;
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

            self.send_reply(Message::AppendEntriesReply(reply), callback)
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

            self.send_reply(Message::AppendEntriesReply(reply), callback)
                .await;
            return Ok(false);
        }

        let mut args_entries = Vec::new();
        for entry in args.entries {
            args_entries.push(Log {
                command: entry.command.try_into().map_err(|_| Error::ParseError)?,
                term: entry.term,
            });
        }

        // if an existing entry conflicts with a new one (same index but different terms)
        // delete the existing entry and all that follow it
        let mut idx = args.prev_log_index as usize + 1;
        for entry in &args_entries {
            if idx > self.log.len() - 1 {
                break;
            }
            if entry.term != self.log[idx].term {
                self.log.drain(idx..);
                self.callbacks.drain(idx..);

                assert_eq!(self.callbacks.len(), self.log.len());
                break;
            }
            idx += 1;
        }

        // append any new entries not in the log
        let idx = args.prev_log_index as usize + 1;
        for i in 0..args_entries.len() {
            if idx + 1 > self.log.len() - 1 {
                self.log.append(&mut args_entries[i..].to_vec());

                for _ in i..args_entries.len() {
                    self.callbacks.push(None);
                }

                assert_eq!(self.callbacks.len(), self.log.len());
                break;
            }
        }

        // receiver implementation 5 from Raft paper
        if args.leader_commit > self.commit_index {
            let idx_last_new_entry = self.log.len() - 1;

            let old_commit_index = self.commit_index;

            self.commit_index = min(idx_last_new_entry as u64, args.leader_commit);

            self.check_last_applied(old_commit_index).await?;
        }

        self.voted_for = Some(args.leader_id);
        self.leader_id = Some(args.leader_id);
        reply.success = true;

        // make sure to tell the leader what our next index is in case this message beomces stale
        // so that the leader can ensure that next index increases monotoonically
        reply.next_index = self.log.len() as u64;
        debug!(
            "{}: AppendEntries from {} successful: Current log {:?}, commit_index {}",
            self.id, args.leader_id, self.log, self.commit_index
        );
        self.send_reply(Message::AppendEntriesReply(reply), callback)
            .await;

        // reset the timer
        Ok(true)
    }

    async fn check_last_applied(&mut self, old_commit_index: u64) -> Result<()> {
        trace!(
            "{}: check_last_applied @ old_commit_index {} to commit_index {}",
            self.id,
            old_commit_index,
            self.commit_index
        );
        for i in old_commit_index..self.commit_index + 1 {
            debug!(
                "{}: applying {} to state machine",
                self.id, self.log[i as usize].command
            );
            let cmd = &self.log[i as usize].command;

            let message = self.apply(cmd)?;

            // get the callback and return the value to the server
            trace!("{}: searching for callback at {}", self.id, i);
            if let Some(callback) = std::mem::replace(&mut self.callbacks[i as usize], None) {
                trace!("{}: found callback {:?}", self.id, callback);
                let reply = ClientRequestReply {
                    message,
                    leader: self.leader_id.unwrap_or(0),
                    success: true,
                };
                self.send_reply(reply, callback).await;
            };
        }

        Ok(())
    }

    fn apply(&self, command: &C) -> Result<String> {
        Ok(self.machine.apply(command)?)
    }

    async fn handle_append_entries_reply(&mut self, reply: AppendEntriesReply) -> Result<bool> {
        if reply.term > self.term {
            self.become_follower(reply.term);
            return Ok(true);
        // stale reply
        } else if reply.term < self.term {
            return Ok(false);
        } else {
            if reply.success {
                let have_index = *self.next_index.get(&reply.peer).ok_or(Error::MissedPeer)?;
                if reply.next_index > have_index {
                    self.next_index.insert(reply.peer, reply.next_index);
                    self.match_index.insert(reply.peer, reply.next_index - 1);

                    self.check_commit_index().await?;
                } else if reply.next_index < have_index {
                    warn!(
                        "{}: Stale Append Entries Reply from {}: got next_index {}, have {}",
                        self.id,
                        reply.peer,
                        reply.next_index,
                        self.next_index.get(&reply.peer).unwrap()
                    );
                }
            } else {
                // decrement next index
                let next_index = self.next_index.get(&reply.peer).unwrap();
                self.next_index.insert(reply.peer, next_index - 1);
                self.send_heartbeat_to_peer(reply.peer).await?;
            }
        }
        Ok(false)
    }

    async fn check_commit_index(&mut self) -> Result<()> {
        trace!(
            "{}: checking commit_index; commit_index = {}",
            self.id,
            self.commit_index
        );
        let commit_index = self.commit_index;
        let mut n = commit_index + 1;

        loop {
            let mut match_count = 1;
            for peer in &self.peers {
                if *self.match_index.get(peer).ok_or(Error::MissedPeer)? >= n {
                    trace!(
                        "{}: found good match index {} for {}",
                        self.id,
                        *self.match_index.get(peer).ok_or(Error::MissedPeer)?,
                        peer
                    );
                    match_count += 1;
                }
            }

            if match_count < self.majority() {
                trace!(
                    "{}: not enough large match counts: have {}, need {}",
                    self.id,
                    match_count,
                    self.peers.len() / 2
                );
                break;
            }

            if self.log.get(n as usize).is_none() {
                trace!(
                    "{}: missing log at N {}; current log {:?}",
                    self.id,
                    n,
                    self.log
                );
                break;
            }

            if self.log[n as usize].term != self.term {
                trace!(
                    "{}: {} = log[{}].term != self.term = {}",
                    self.id,
                    self.log[n as usize].term,
                    n,
                    self.term
                );
                break;
            }

            trace!(
                "{}: setting commit_index = {}, checking n+1 {}",
                self.id,
                n,
                n + 1
            );

            self.commit_index += 1;
            n += 1;

            self.check_last_applied(n - 1).await?;
        }

        Ok(())
    }

    async fn send_heartbeat_to_peer(&self, peer: u64) -> Result<()> {
        trace!("{}: Sending heartbeat to {}", self.id, peer);
        let last_log_index = self.log.len();
        let next_index = *self.next_index.get(&peer).ok_or(Error::MissedPeer)? as usize;

        let mut request = AppendEntriesArgs {
            entries: Vec::new(),
            leader_commit: self.commit_index as u64,
            leader_id: self.id,
            prev_log_index: 0,
            prev_log_term: 0,
            term: self.term,
        };

        if last_log_index >= next_index {
            let entries = self.log[next_index..last_log_index].to_vec();
            for entry in entries {
                request.entries.push(rpc::Log {
                    command: entry.command.serialize(),
                    term: entry.term,
                });
            }
            trace!("{}: to {}, next_index {}", self.id, peer, next_index,);
            request.prev_log_index = next_index as u64 - 1;
            request.prev_log_term = self.log[next_index - 1].term;
        }

        debug!(
            "{}: sending heartbeat {:?} to {}, term {}",
            self.id, request, peer, self.term
        );

        self.send_message(
            peer,
            Message::AppendEntries(request.clone(), Callback::Mpsc(self.to_inbox.clone())),
        )
        .await;

        Ok(())
    }

    async fn handle_check_state(&self, tx: Sender<Message>) -> Result<bool> {
        tx.send(Message::State(self.state())).await.unwrap();
        Ok(false)
    }

    fn state(&self) -> State {
        let mut log = Vec::new();
        for entry in &self.log {
            log.push(Log {
                term: entry.term,
                command: entry.command.to_string(),
            });
        }
        State {
            id: self.id,
            role: self.role.clone(),
            votes: self.votes,
            term: self.term,
            voted_for: self.voted_for,
            log,
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
        if self.votes >= self.majority() {
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

        for peer in &self.peers {
            debug!(
                "{}: sending request vote to {}, term {}",
                self.id, peer, self.term
            );

            self.send_message(
                *peer,
                Message::RequestVote(request.clone(), Callback::Mpsc(self.to_inbox.clone())),
            )
            .await;
        }

        Ok(())
    }

    async fn send_heartbeats(&mut self) -> Result<()> {
        trace!(
            "{}: Sending heartbeats to peers: term {}",
            self.id,
            self.term
        );

        for peer in &self.peers {
            self.send_heartbeat_to_peer(*peer).await?;
        }
        Ok(())
    }

    fn become_follower(&mut self, term: u64) {
        trace!("{}: becoming a follower, term {}", self.id, self.term);
        self.term = term;
        self.voted_for = None;
        self.votes = 0;
        self.role = Role::Follower;
    }

    fn become_leader(&mut self) {
        trace!("{}: Becoming leader", self.id);
        self.role = Role::Leader;
        self.leader_id = Some(self.id);

        // initialize next index and match index
        for peer in &self.peers {
            self.next_index.insert(*peer, self.log.len() as u64);
            self.match_index.insert(*peer, 0);
        }
    }

    /// send a message to a given peer
    async fn send_message(&self, peer: u64, m: Message) {
        let _ = self
            .outbox
            .send(OutboundMessage {
                message: m,
                from: self.id,
                to: peer,
            })
            .await;
    }

    #[inline(always)]
    fn majority(&self) -> u64 {
        (self.peers.len() as u64 + 1) / 2 + 1
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        node::Log,
        rpc::{
            AppendEntriesArgs, AppendEntriesReply, Log as RpcLog, RequestVoteArgs, RequestVoteReply,
        },
        state_machine::DummyStateMachine,
        Message, Node,
    };

    #[tokio::test]
    async fn test_handle_append_entries() {
        let tests = Vec::<(AppendEntriesArgs, AppendEntriesReply, Vec<Log<String>>)>::from([
            (
                AppendEntriesArgs {
                    term: 5,
                    leader_id: 5,
                    prev_log_index: 2,
                    prev_log_term: 2,
                    entries: vec![RpcLog {
                        term: 5,
                        command: "3".to_string(),
                    }],
                    leader_commit: 2,
                },
                AppendEntriesReply {
                    success: true,
                    term: 5,
                    peer: 1,
                    next_index: 4,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                    Log {
                        term: 5,
                        command: "3".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 2,
                    prev_log_term: 2,
                    entries: vec![RpcLog {
                        term: 6,
                        command: "3".to_string(),
                    }],
                    leader_commit: 2,
                },
                AppendEntriesReply {
                    success: true,
                    term: 6,
                    peer: 1,
                    next_index: 4,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 4,
                    leader_id: 5,
                    prev_log_index: 2,
                    prev_log_term: 2,
                    entries: vec![RpcLog {
                        term: 4,
                        command: "3".to_string(),
                    }],
                    leader_commit: 2,
                },
                AppendEntriesReply {
                    success: false,
                    term: 5,
                    peer: 1,
                    next_index: 3,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 1,
                    prev_log_term: 1,
                    entries: vec![RpcLog {
                        term: 6,
                        command: "2".to_string(),
                    }],
                    leader_commit: 1,
                },
                AppendEntriesReply {
                    success: true,
                    term: 6,
                    peer: 1,
                    next_index: 3,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "2".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 0,
                    prev_log_term: 0,
                    entries: vec![RpcLog {
                        term: 3,
                        command: "3".to_string(),
                    }],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: true,
                    term: 6,
                    peer: 1,
                    next_index: 2,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 3,
                        command: "3".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 4,
                    leader_id: 5,
                    prev_log_index: 3,
                    prev_log_term: 3,
                    entries: vec![RpcLog {
                        term: 4,
                        command: "3".to_string(),
                    }],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: false,
                    term: 5,
                    peer: 1,
                    next_index: 3,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 3,
                    prev_log_term: 3,
                    entries: vec![RpcLog {
                        term: 3,
                        command: "3".to_string(),
                    }],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: false,
                    term: 6,
                    peer: 1,
                    next_index: 3,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 2,
                    prev_log_term: 5,
                    entries: vec![RpcLog {
                        term: 6,
                        command: "3".to_string(),
                    }],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: false,
                    term: 6,
                    peer: 1,
                    next_index: 3,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 2,
                    prev_log_term: 2,
                    entries: vec![
                        RpcLog {
                            term: 6,
                            command: "3".to_string(),
                        },
                        RpcLog {
                            term: 6,
                            command: "3".to_string(),
                        },
                        RpcLog {
                            term: 6,
                            command: "3".to_string(),
                        },
                    ],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: true,
                    term: 6,
                    peer: 1,
                    next_index: 6,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 2,
                        command: "2".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                ],
            ),
            (
                AppendEntriesArgs {
                    term: 6,
                    leader_id: 5,
                    prev_log_index: 1,
                    prev_log_term: 1,
                    entries: vec![
                        RpcLog {
                            term: 6,
                            command: "3".to_string(),
                        },
                        RpcLog {
                            term: 6,
                            command: "3".to_string(),
                        },
                    ],
                    leader_commit: 0,
                },
                AppendEntriesReply {
                    success: true,
                    term: 6,
                    peer: 1,
                    next_index: 4,
                },
                vec![
                    Log {
                        term: 0,
                        command: "".to_string(),
                    },
                    Log {
                        term: 1,
                        command: "1".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                    Log {
                        term: 6,
                        command: "3".to_string(),
                    },
                ],
            ),
        ]);

        for (args, expected_reply, expected_log) in tests {
            let (to_inbox, inbox) = tokio::sync::mpsc::channel(2);
            let (to_outbox, _) = tokio::sync::mpsc::channel(2);
            let (callback, mut outbox) = tokio::sync::oneshot::channel();
            let dummy = DummyStateMachine {};
            let mut node = Node::new(1, inbox, to_inbox, to_outbox, Vec::new(), dummy);

            node.log.push(Log {
                term: 1,
                command: "1".to_string(),
            });
            node.log.push(Log {
                term: 2,
                command: "2".to_string(),
            });
            node.callbacks.push(None);
            node.callbacks.push(None);

            node.term = 5;
            node.voted_for = None;

            let callback = crate::Callback::OneShot(callback);

            node.handle_append_entries(args, callback).await.unwrap();

            let Message::AppendEntriesReply(reply) = outbox.try_recv().unwrap() else {
                panic!();
            };

            assert_eq!(reply, expected_reply);
            assert_eq!(node.log, expected_log);
        }
    }

    #[tokio::test]
    async fn test_handle_request_vote() {
        let tests = Vec::<(RequestVoteArgs, RequestVoteReply)>::from([
            (
                // simple yes
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 5,
                    last_log_index: 2,
                    last_log_term: 2,
                },
                RequestVoteReply {
                    vote_granted: true,
                    term: 5,
                    peer: 1,
                },
            ),
            (
                // higher last log term
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 5,
                    last_log_index: 2,
                    last_log_term: 3,
                },
                RequestVoteReply {
                    vote_granted: true,
                    term: 5,
                    peer: 1,
                },
            ),
            (
                // higher last log index
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 5,
                    last_log_index: 3,
                    last_log_term: 2,
                },
                RequestVoteReply {
                    vote_granted: true,
                    term: 5,
                    peer: 1,
                },
            ),
            (
                // lower term
                RequestVoteArgs {
                    term: 4,
                    candidate_id: 5,
                    last_log_index: 2,
                    last_log_term: 2,
                },
                RequestVoteReply {
                    vote_granted: false,
                    term: 5,
                    peer: 1,
                },
            ),
            (
                // lower last log term
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 5,
                    last_log_index: 2,
                    last_log_term: 1,
                },
                RequestVoteReply {
                    vote_granted: false,
                    term: 5,
                    peer: 1,
                },
            ),
            (
                // lower last log index
                RequestVoteArgs {
                    term: 5,
                    candidate_id: 5,
                    last_log_index: 1,
                    last_log_term: 1,
                },
                RequestVoteReply {
                    vote_granted: false,
                    term: 5,
                    peer: 1,
                },
            ),
        ]);

        for (args, expected_reply) in tests {
            let (to_inbox, inbox) = tokio::sync::mpsc::channel(2);
            let (to_outbox, _) = tokio::sync::mpsc::channel(2);
            let (callback, mut outbox) = tokio::sync::oneshot::channel();
            let dummy = DummyStateMachine {};
            let mut node = Node::new(1, inbox, to_inbox, to_outbox, Vec::new(), dummy);

            node.term = 5;
            node.voted_for = None;
            node.log.push(Log {
                term: 1,
                command: "1".to_string(),
            });
            node.log.push(Log {
                term: 2,
                command: "2".to_string(),
            });

            let callback = crate::Callback::OneShot(callback);

            node.handle_request_vote(args, callback).await.unwrap();

            let Message::RequestVoteReply(reply) = outbox.try_recv().unwrap() else {
                panic!();
            };

            assert_eq!(reply, expected_reply);
        }
    }
}
