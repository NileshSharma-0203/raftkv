//! One replica: Raft + state machine + tracking of in-flight client requests.
//!
//! Both the real server and the simulator drive this type, so the simulator
//! tests the same request-handling logic that ships.

use crate::kv::*;
use crate::raft::*;
use crate::storage::Storage;
use crate::types::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// (index, term, data) of one applied entry.
pub type AppliedEntry = (Index, Term, Arc<[u8]>);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    Applied(Applied),
    Value(Option<Vec<u8>>),
    NotLeader(Option<NodeId>),
    /// Outcome unknown (for example leadership changed). Safe to retry.
    Retry,
    Busy,
}

#[derive(Default)]
pub struct Output {
    pub messages: Vec<Envelope>,
    pub replies: Vec<(u64, Reply)>,
}

#[derive(Clone, Debug)]
pub struct ReplicaConfig {
    pub snapshot_threshold: u64,
    pub snapshot_trailing: u64,
    pub max_sessions: usize,
    pub max_pending: usize,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        ReplicaConfig {
            snapshot_threshold: 100_000,
            snapshot_trailing: 5_000,
            max_sessions: 100_000,
            max_pending: 20_000,
        }
    }
}

pub struct Replica<S: Storage> {
    pub raft: Raft<S>,
    pub kv: KvState,
    cfg: ReplicaConfig,
    applied: Index,
    pending: BTreeMap<Index, (Term, u64)>,
    read_keys: BTreeMap<u64, Vec<u8>>,
    waiting_reads: VecDeque<(Index, u64)>,
    replies: Vec<(u64, Reply)>,
    served_term: Term,
    snapshot_in_progress: bool,
    /// (index, term, data) of each applied entry, when the simulator asks.
    pub apply_trace: Option<Vec<AppliedEntry>>,
}

impl<S: Storage> Replica<S> {
    pub fn new(raft_cfg: Config, cfg: ReplicaConfig, mut store: S) -> Result<Self, String> {
        let bugs = raft_cfg.bugs;
        let meta = store.snapshot_meta();
        let mut kv = KvState::new(cfg.max_sessions, bugs);
        if meta.last_index > 0 {
            kv.restore(&store.snapshot_data()).map_err(|e| format!("snapshot restore: {e}"))?;
        }
        Ok(Replica {
            raft: Raft::new(raft_cfg, store),
            kv,
            cfg,
            applied: meta.last_index,
            pending: BTreeMap::new(),
            read_keys: BTreeMap::new(),
            waiting_reads: VecDeque::new(),
            replies: Vec::new(),
            served_term: 0,
            snapshot_in_progress: false,
            apply_trace: None,
        })
    }

    pub fn applied(&self) -> Index {
        self.applied
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn tick(&mut self) {
        self.raft.tick();
    }

    pub fn step(&mut self, env: Envelope) {
        self.raft.step(env);
    }

    pub fn submit(&mut self, token: u64, cmd: Command) {
        if !self.raft.is_leader() {
            self.replies.push((token, Reply::NotLeader(self.raft.leader())));
            return;
        }
        if self.pending.len() >= self.cfg.max_pending {
            self.replies.push((token, Reply::Busy));
            return;
        }
        match self.raft.propose(cmd.encode()) {
            Some((index, term)) => {
                if self.raft.cfg.bugs.ack_before_commit {
                    let reply = match cmd {
                        Command::Register => Applied::Registered(index),
                        Command::Write { .. } => Applied::Ok,
                    };
                    self.replies.push((token, Reply::Applied(reply)));
                } else {
                    self.pending.insert(index, (term, token));
                }
            }
            None => self.replies.push((token, Reply::NotLeader(self.raft.leader()))),
        }
    }

    pub fn read(&mut self, token: u64, key: Vec<u8>) {
        if !self.raft.is_leader() {
            self.replies.push((token, Reply::NotLeader(self.raft.leader())));
            return;
        }
        if self.raft.cfg.bugs.stale_read {
            self.replies.push((token, Reply::Value(self.kv.get(&key))));
            return;
        }
        if self.raft.read_index(token) {
            self.read_keys.insert(token, key);
        } else {
            self.replies.push((token, Reply::Retry));
        }
    }

    /// End of one event-loop pass. The order here is what makes acknowledgements
    /// honest: persist first, then let anything leave this process.
    pub fn flush(&mut self) -> Output {
        self.raft.flush();
        self.raft.store.sync();
        let messages = self.raft.take_outbox();

        if let Some((meta, data)) = self.raft.take_restored() {
            if let Err(e) = self.kv.restore(&data) {
                self.raft.safety_violation = Some(format!("bad snapshot from leader: {e}"));
            }
            self.applied = meta.last_index;
        }

        self.apply_committed();

        for (token, index) in self.raft.take_ready_reads() {
            self.waiting_reads.push_back((index, token));
        }
        let mut still_waiting = VecDeque::new();
        while let Some((index, token)) = self.waiting_reads.pop_front() {
            if index <= self.applied {
                if let Some(key) = self.read_keys.remove(&token) {
                    self.replies.push((token, Reply::Value(self.kv.get(&key))));
                }
            } else {
                still_waiting.push_back((index, token));
            }
        }
        self.waiting_reads = still_waiting;

        // Requests from a term we no longer lead have an unknown fate.
        let current = if self.raft.is_leader() { self.raft.term() } else { 0 };
        if self.served_term != current {
            self.served_term = current;
            self.pending.retain(|_, (term, token)| {
                let keep = *term == current;
                if !keep {
                    self.replies.push((*token, Reply::Retry));
                }
                keep
            });
            for (token, _) in std::mem::take(&mut self.read_keys) {
                self.replies.push((token, Reply::Retry));
            }
            self.waiting_reads.clear();
        }

        Output { messages, replies: std::mem::take(&mut self.replies) }
    }

    fn apply_committed(&mut self) {
        let commit = self.raft.commit();
        while self.applied < commit {
            let batch = self.raft.store.entries(self.applied + 1, commit, 4 << 20);
            if batch.is_empty() {
                let msg = format!(
                    "committed entry {} is missing (first={}, last={})",
                    self.applied + 1,
                    self.raft.store.first_index(),
                    self.raft.store.last_index()
                );
                self.raft.safety_violation = Some(msg);
                return;
            }
            for e in batch {
                if e.index != self.applied + 1 {
                    let msg = format!("log gap: expected entry {}, found {}", self.applied + 1, e.index);
                    self.raft.safety_violation = Some(msg);
                    return;
                }
                self.apply_entry(e);
            }
        }
    }

    fn apply_entry(&mut self, e: Entry) {
        if let Some(t) = &mut self.apply_trace {
            t.push((e.index, e.term, e.data.clone()));
        }
        self.applied = e.index;
        let reply = if e.data.is_empty() {
            None
        } else {
            match Command::decode(&e.data) {
                Ok(cmd) => Some(self.kv.apply(e.index, cmd)),
                Err(err) => {
                    self.raft.safety_violation = Some(format!("undecodable entry {}: {err}", e.index));
                    None
                }
            }
        };
        if let Some((term, token)) = self.pending.remove(&e.index) {
            let r = match reply {
                Some(a) if term == e.term => Reply::Applied(a),
                _ => Reply::Retry,
            };
            self.replies.push((token, r));
        }
    }

    /// If enough log has accumulated, serialise the state for a snapshot. The
    /// caller persists it (on another thread, in the server) and then calls
    /// `snapshot_done`.
    pub fn maybe_snapshot(&mut self) -> Option<(SnapshotMeta, Vec<u8>)> {
        if self.snapshot_in_progress {
            return None;
        }
        let current = self.raft.store.snapshot_meta().last_index;
        if self.applied.saturating_sub(current) < self.cfg.snapshot_threshold {
            return None;
        }
        let last_term = self.raft.store.term_at(self.applied)?;
        self.snapshot_in_progress = true;
        Some((SnapshotMeta { last_index: self.applied, last_term }, self.kv.encode()))
    }

    pub fn snapshot_done(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>) {
        self.snapshot_in_progress = false;
        self.raft.store.adopt_snapshot(meta, data);
        let upto = meta.last_index.saturating_sub(self.cfg.snapshot_trailing);
        self.raft.store.compact(upto);
    }
}
