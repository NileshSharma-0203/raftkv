//! The persistence boundary.
//!
//! Durability contract, which the Raft core relies on:
//! - `save_hard_state`, `install_snapshot` are durable when they return.
//! - `append` and `truncate_from` may be buffered, and are durable only after
//!   `sync` returns. The driver always calls `sync` before releasing any
//!   outgoing message or client reply.
//!
//! This trait is the seam where a different engine (for example NucleusDB's
//! storage layer) can be substituted.
//!
//! An I/O failure is fatal. After a failed fsync the kernel may have dropped
//! the dirty pages, so retrying can falsely report success. Implementations
//! must stop the process rather than return.

pub mod disk;
pub mod mem;

use crate::types::*;
use std::sync::Arc;

pub trait Storage {
    fn hard_state(&self) -> HardState;
    fn save_hard_state(&mut self, hs: HardState);

    fn snapshot_meta(&self) -> SnapshotMeta;
    fn snapshot_data(&mut self) -> Arc<Vec<u8>>;

    /// First index still held in the log. Equals `last_index() + 1` when empty.
    fn first_index(&self) -> Index;
    fn last_index(&self) -> Index;
    /// Term at `index`; also answers for the snapshot's last index and for 0.
    fn term_at(&self, index: Index) -> Option<Term>;
    /// Entries in `lo..=hi`, stopping once `max_bytes` is exceeded (at least one
    /// entry is returned if the range is non-empty).
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry>;

    fn append(&mut self, entries: &[Entry]);
    fn truncate_from(&mut self, index: Index);
    fn sync(&mut self);

    /// Replace state with a snapshot received from the leader (Figure 13).
    /// Keeps the log after the snapshot if it holds the snapshot's last entry
    /// with a matching term; otherwise discards the whole log.
    fn install_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>);

    /// Record a locally produced snapshot that is already durable. Ignored if a
    /// newer snapshot has arrived in the meantime.
    fn adopt_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>);

    /// Discard log entries at or below `upto`. Never passes the snapshot.
    fn compact(&mut self, upto: Index);
}
