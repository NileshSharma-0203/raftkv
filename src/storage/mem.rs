//! In-memory storage with crash semantics, for the deterministic simulator.
//!
//! Writes that have not been synced are held separately. A simulated crash
//! keeps a random prefix of them, which models a disk that persisted only part
//! of a buffered write. `kill -9` cannot produce this: the page cache survives
//! process death, so only a simulated disk can test that fsync is called.

use super::Storage;
use crate::types::*;
use crate::util::Rng;
use std::sync::Arc;

#[derive(Clone, Debug)]
enum PendingOp {
    Append(Entry),
    Truncate(Index),
}

#[derive(Clone, Default)]
struct Log {
    entries: Vec<Entry>,
}

impl Log {
    fn first(&self, snap: SnapshotMeta) -> Index {
        self.entries.first().map(|e| e.index).unwrap_or(snap.last_index + 1)
    }
    fn last(&self, snap: SnapshotMeta) -> Index {
        self.entries.last().map(|e| e.index).unwrap_or(snap.last_index)
    }
    fn truncate(&mut self, index: Index) {
        if let Some(first) = self.entries.first().map(|e| e.index) {
            if index <= first {
                self.entries.clear();
            } else {
                self.entries.truncate((index - first) as usize);
            }
        }
    }
    fn compact(&mut self, upto: Index) {
        if let Some(first) = self.entries.first().map(|e| e.index) {
            if upto >= first {
                let n = ((upto - first + 1) as usize).min(self.entries.len());
                self.entries.drain(..n);
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct MemStorage {
    hs: HardState,
    snap: SnapshotMeta,
    snap_data: Arc<Vec<u8>>,
    durable: Log,
    log: Log,
    pending: Vec<PendingOp>,
    pub skip_fsync: bool,
}

impl MemStorage {
    pub fn new(skip_fsync: bool) -> Self {
        MemStorage { skip_fsync, ..Default::default() }
    }

    fn apply_durable(&mut self, op: PendingOp) {
        match op {
            PendingOp::Append(e) => {
                // A snapshot installed after this write was buffered may have
                // moved the log on; only contiguous writes can land.
                if e.index == self.durable.last(self.snap) + 1 {
                    self.durable.entries.push(e);
                }
            }
            PendingOp::Truncate(i) => self.durable.truncate(i),
        }
    }

    /// Simulate power loss. Keeps a random prefix of the unsynced writes.
    pub fn crash(&mut self, rng: &mut Rng) {
        let keep = if self.pending.is_empty() { 0 } else { rng.below(self.pending.len() + 1) };
        let ops: Vec<PendingOp> = self.pending.drain(..).take(keep).collect();
        for op in ops {
            self.apply_durable(op);
        }
        // Same rule as disk recovery: a log that ends before the snapshot is stale.
        if self.durable.entries.last().map(|e| e.index < self.snap.last_index).unwrap_or(false) {
            self.durable.entries.clear();
        }
        self.log = self.durable.clone();
    }

    /// Model the OS writing back dirty pages on its own schedule.
    pub fn background_writeback(&mut self) {
        let ops: Vec<PendingOp> = self.pending.drain(..).collect();
        for op in ops {
            self.apply_durable(op);
        }
    }
}

impl Storage for MemStorage {
    fn hard_state(&self) -> HardState {
        self.hs
    }
    fn save_hard_state(&mut self, hs: HardState) {
        self.hs = hs;
    }
    fn snapshot_meta(&self) -> SnapshotMeta {
        self.snap
    }
    fn snapshot_data(&mut self) -> Arc<Vec<u8>> {
        self.snap_data.clone()
    }
    fn first_index(&self) -> Index {
        self.log.first(self.snap)
    }
    fn last_index(&self) -> Index {
        self.log.last(self.snap)
    }
    fn term_at(&self, index: Index) -> Option<Term> {
        if index == self.snap.last_index {
            return Some(self.snap.last_term);
        }
        let first = self.log.entries.first()?.index;
        if index < first {
            return None;
        }
        self.log.entries.get((index - first) as usize).map(|e| e.term)
    }
    fn entries(&self, lo: Index, hi: Index, max_bytes: usize) -> Vec<Entry> {
        let mut out = Vec::new();
        let Some(first) = self.log.entries.first().map(|e| e.index) else { return out };
        if lo < first {
            return out;
        }
        let mut total = 0;
        for e in self.log.entries.iter().skip((lo - first) as usize) {
            if e.index > hi || (!out.is_empty() && total + e.size() > max_bytes) {
                break;
            }
            total += e.size();
            out.push(e.clone());
        }
        out
    }
    fn append(&mut self, entries: &[Entry]) {
        for e in entries {
            self.log.entries.push(e.clone());
            self.pending.push(PendingOp::Append(e.clone()));
        }
    }
    fn truncate_from(&mut self, index: Index) {
        self.log.truncate(index);
        self.pending.push(PendingOp::Truncate(index));
    }
    fn sync(&mut self) {
        if !self.skip_fsync {
            self.background_writeback();
        }
    }
    fn install_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>) {
        self.sync();
        let keep =
            self.term_at(meta.last_index) == Some(meta.last_term) && self.log.first(self.snap) <= meta.last_index + 1;
        self.snap = meta;
        self.snap_data = data;
        if !keep {
            self.log.entries.clear();
            self.durable.entries.clear();
            self.pending.clear();
        }
    }
    fn adopt_snapshot(&mut self, meta: SnapshotMeta, data: Arc<Vec<u8>>) {
        if meta.last_index > self.snap.last_index {
            self.snap = meta;
            self.snap_data = data;
        }
    }
    fn compact(&mut self, upto: Index) {
        let upto = upto.min(self.snap.last_index);
        self.log.compact(upto);
        self.durable.compact(upto);
    }
}
