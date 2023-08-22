use std::time::Duration;
use rand::{thread_rng, Rng};

use crate::{Node, Result, Error};

use super::NodeStatus;

const PULSETIME: u64 = 100; 

impl Node {
    async fn reset_timer(&'static self) -> Result<()>{
        let mut state = self.state.lock().map_err(|_| Error::FailedLock)?;

        state.pulses += 1;
        let curr_pulses = state.pulses;

        let mut rng = thread_rng();
        let wild = rng.gen_range(0..50);
        let delay = Duration::from_millis(PULSETIME + wild);

        tokio::time::sleep(delay).await;

        let state = self.state.lock().map_err(|_| Error::FailedLock)?;

        if curr_pulses == state.pulses {
            drop(state);
            let _ = tokio::spawn(async {
                Box::new(self.call_election().await)
            });
        }

        Ok(())
    }

    async fn call_election(&self) -> Result<()> {
        let votes_needed = self.peers.len() / 2 + 1;
        let mut votes_received = 0;
        let mut votes_finished = 0;

        let mut state = self.state.lock().map_err(|_| Error::FailedLock)?;

        state.current_term += 1;
        state.voted_for = Some(self.id as u64);
        state.status = NodeStatus::Candidate;
        state.leader_id = None;

        let this_term = state.current_term;

        // figure out how to reset the timer again

        Ok(())
    }
}
