//! Types shared by every layer.

use std::sync::Arc;

pub type NodeId = u64;
pub type Term = u64;
pub type Index = u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub term: Term,
    pub index: Index,
    /// Encoded state-machine command. Empty means a leader's no-op.
    pub data: Arc<[u8]>,
}

impl Entry {
    pub fn new(term: Term, index: Index, data: Vec<u8>) -> Self {
        Entry { term, index, data: data.into() }
    }
    pub fn size(&self) -> usize {
        self.data.len() + 16
    }
}

/// The state Raft requires on stable storage before answering any RPC.
/// (The log is persisted separately.)
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HardState {
    pub term: Term,
    pub voted_for: Option<NodeId>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SnapshotMeta {
    pub last_index: Index,
    pub last_term: Term,
}

/// Deliberate protocol bugs. Everything is `false` in production; the server
/// can only set these when compiled with the `fault-injection` feature.
///
/// A test suite that has never caught a bug has proven nothing, so each flag
/// reintroduces a classic Raft mistake and the verification suite must detect it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Bugs {
    /// Apply retried writes again instead of de-duplicating them.
    pub no_dedup: bool,
    /// Acknowledge a write before it is committed.
    pub ack_before_commit: bool,
    /// Grant votes without the election restriction (§5.4.1).
    pub vote_ignores_log: bool,
    /// Commit entries from earlier terms by counting replicas (§5.4.2, Figure 8).
    pub commit_old_term: bool,
    /// Serve reads from the local leader without confirming leadership.
    pub stale_read: bool,
    /// Do not persist who we voted for.
    pub forget_vote: bool,
    /// Treat fsync as a no-op.
    pub skip_fsync: bool,
    /// Keep conflicting log entries instead of truncating them.
    pub no_truncate: bool,
    /// Omit the client session table from snapshots.
    pub snapshot_drops_sessions: bool,
}

impl Bugs {
    pub const NAMES: [&'static str; 9] = [
        "no_dedup",
        "ack_before_commit",
        "vote_ignores_log",
        "commit_old_term",
        "stale_read",
        "forget_vote",
        "skip_fsync",
        "no_truncate",
        "snapshot_drops_sessions",
    ];

    pub fn single(name: &str) -> Option<Bugs> {
        let mut b = Bugs::default();
        match name {
            "no_dedup" => b.no_dedup = true,
            "ack_before_commit" => b.ack_before_commit = true,
            "vote_ignores_log" => b.vote_ignores_log = true,
            "commit_old_term" => b.commit_old_term = true,
            "stale_read" => b.stale_read = true,
            "forget_vote" => b.forget_vote = true,
            "skip_fsync" => b.skip_fsync = true,
            "no_truncate" => b.no_truncate = true,
            "snapshot_drops_sessions" => b.snapshot_drops_sessions = true,
            _ => return None,
        }
        Some(b)
    }
}
