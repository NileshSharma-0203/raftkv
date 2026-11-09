//! The Raft consensus algorithm (Ongaro & Ousterhout, 2014) with no I/O.
//!
//! The module only reacts to inputs (`tick`, `step`, `propose`, `read_index`)
//! and queues outgoing messages. That makes it deterministic, so the simulator
//! can replay any failure from a seed.
//!
//! Beyond the core paper: PreVote and CheckQuorum (dissertation §9.6, §6.2),
//! ReadIndex linearizable reads (§6.4), and leadership transfer (§3.10).

use crate::storage::Storage;
use crate::types::*;
use crate::util::Rng;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Msg {
    Vote {
        pre: bool,
        force: bool,
        term: Term,
        last_index: Index,
        last_term: Term,
    },
    VoteResp {
        pre: bool,
        term: Term,
        granted: bool,
    },
    Append {
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        commit: Index,
        read_ctx: u64,
    },
    AppendResp {
        term: Term,
        success: bool,
        match_index: Index,
        rejected_prev: Index,
        conflict_index: Index,
        conflict_term: Term,
        read_ctx: u64,
    },
    Snapshot {
        term: Term,
        meta: SnapshotMeta,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    SnapshotResp {
        term: Term,
        last_index: Index,
        next_offset: u64,
        done: bool,
    },
    TimeoutNow {
        term: Term,
    },
}

impl Msg {
    pub fn term(&self) -> Term {
        match self {
            Msg::Vote { term, .. }
            | Msg::VoteResp { term, .. }
            | Msg::Append { term, .. }
            | Msg::AppendResp { term, .. }
            | Msg::Snapshot { term, .. }
            | Msg::SnapshotResp { term, .. }
            | Msg::TimeoutNow { term } => *term,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Msg,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub id: NodeId,
    /// Every voter, including this node.
    pub voters: Vec<NodeId>,
    pub election_min: u32,
    pub election_max: u32,
    pub heartbeat: u32,
    pub max_append_bytes: usize,
    pub max_inflight: usize,
    pub snapshot_chunk: usize,
    pub pre_vote: bool,
    pub check_quorum: bool,
    pub seed: u64,
    pub bugs: Bugs,
}

impl Config {
    pub fn new(id: NodeId, voters: Vec<NodeId>) -> Self {
        Config {
            id,
            voters,
            election_min: 30,
            election_max: 60,
            heartbeat: 5,
            max_append_bytes: 1 << 20,
            max_inflight: 64,
            snapshot_chunk: 256 * 1024,
            pre_vote: true,
            check_quorum: true,
            seed: id,
            bugs: Bugs::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

impl Role {
    pub fn as_u8(self) -> u8 {
        match self {
            Role::Follower => 0,
            Role::PreCandidate => 1,
            Role::Candidate => 2,
            Role::Leader => 3,
        }
    }
    pub fn name(code: u8) -> &'static str {
        ["follower", "pre-candidate", "candidate", "leader"].get(code as usize).copied().unwrap_or("?")
    }
}

#[derive(Clone)]
struct OutSnapshot {
    meta: SnapshotMeta,
    data: Arc<Vec<u8>>,
    offset: usize,
}

#[derive(Clone)]
enum PeerState {
    /// Unknown position: one message at a time until a reply arrives.
    Probe,
    /// Known position: pipeline optimistically.
    Replicate,
    Snapshot(OutSnapshot),
}

#[derive(Clone)]
struct Progress {
    next: Index,
    matched: Index,
    state: PeerState,
    probe_sent: bool,
    inflight: VecDeque<Index>,
    /// Heard from since the last CheckQuorum round.
    active: bool,
    ack_ctx: u64,
    idle_ticks: u32,
    /// Ticks with unacknowledged entries and no replication progress. This is
    /// separate from `idle_ticks` because heartbeat replies keep a peer looking
    /// alive even when an Append carrying entries was lost.
    stalled_ticks: u32,
}

struct ReadRound {
    seq: u64,
    index: Index,
    tokens: Vec<u64>,
}

struct InSnapshot {
    from: NodeId,
    meta: SnapshotMeta,
    buf: Vec<u8>,
}

#[derive(Default, Clone, Copy, Debug)]
pub struct RaftStats {
    pub elections_started: u64,
    pub became_leader: u64,
    pub snapshots_sent: u64,
    pub snapshots_installed: u64,
    pub truncations: u64,
}

pub struct Raft<S: Storage> {
    pub cfg: Config,
    pub store: S,
    role: Role,
    term: Term,
    voted_for: Option<NodeId>,
    leader: Option<NodeId>,
    commit: Index,
    election_elapsed: u32,
    election_timeout: u32,
    heartbeat_elapsed: u32,
    rng: Rng,
    votes: BTreeMap<NodeId, bool>,
    progress: BTreeMap<NodeId, Progress>,
    outbox: Vec<Envelope>,
    unsent_reads: Vec<u64>,
    read_rounds: VecDeque<ReadRound>,
    read_seq: u64,
    ready_reads: Vec<(u64, Index)>,
    transfer_to: Option<NodeId>,
    snap_in: Option<InSnapshot>,
    restored: Option<(SnapshotMeta, Arc<Vec<u8>>)>,
    replicate_dirty: bool,
    /// Set if this replica sees a state that Raft says cannot exist. The server
    /// halts on it; the simulator reports it as a failed run.
    pub safety_violation: Option<String>,
    pub stats: RaftStats,
}

impl<S: Storage> Raft<S> {
    pub fn new(cfg: Config, store: S) -> Self {
        let hs = store.hard_state();
        let commit = store.snapshot_meta().last_index;
        let rng = Rng::new(cfg.seed);
        let mut r = Raft {
            role: Role::Follower,
            term: hs.term,
            voted_for: hs.voted_for,
            leader: None,
            commit,
            election_elapsed: 0,
            election_timeout: cfg.election_min,
            heartbeat_elapsed: 0,
            rng,
            votes: BTreeMap::new(),
            progress: BTreeMap::new(),
            outbox: Vec::new(),
            unsent_reads: Vec::new(),
            read_rounds: VecDeque::new(),
            read_seq: 0,
            ready_reads: Vec::new(),
            transfer_to: None,
            snap_in: None,
            restored: None,
            replicate_dirty: false,
            safety_violation: None,
            stats: RaftStats::default(),
            cfg,
            store,
        };
        r.reset_election_timer();
        r
    }

    pub fn id(&self) -> NodeId {
        self.cfg.id
    }
    pub fn role(&self) -> Role {
        self.role
    }
    pub fn term(&self) -> Term {
        self.term
    }
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }
    pub fn commit(&self) -> Index {
        self.commit
    }
    pub fn is_leader(&self) -> bool {
        self.role == Role::Leader
    }
    pub fn take_outbox(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }
    pub fn take_restored(&mut self) -> Option<(SnapshotMeta, Arc<Vec<u8>>)> {
        self.restored.take()
    }
    pub fn take_ready_reads(&mut self) -> Vec<(u64, Index)> {
        std::mem::take(&mut self.ready_reads)
    }

    fn quorum(&self) -> usize {
        self.cfg.voters.len() / 2 + 1
    }

    fn peers(&self) -> Vec<NodeId> {
        self.cfg.voters.iter().copied().filter(|p| *p != self.cfg.id).collect()
    }

    fn violation(&mut self, what: String) {
        if self.safety_violation.is_none() {
            self.safety_violation = Some(format!("node {}: {}", self.cfg.id, what));
        }
    }

    fn send(&mut self, to: NodeId, msg: Msg) {
        self.outbox.push(Envelope { from: self.cfg.id, to, msg });
    }

    fn reset_election_timer(&mut self) {
        self.election_elapsed = 0;
        self.election_timeout = self.rng.range(self.cfg.election_min as u64, self.cfg.election_max as u64) as u32;
    }

    fn persist(&mut self) {
        let voted_for = if self.cfg.bugs.forget_vote { None } else { self.voted_for };
        self.store.save_hard_state(HardState { term: self.term, voted_for });
    }

    fn last_log(&self) -> (Index, Term) {
        let i = self.store.last_index();
        (i, self.store.term_at(i).unwrap_or(0))
    }

    fn clear_leader_state(&mut self) {
        self.progress.clear();
        self.unsent_reads.clear();
        self.read_rounds.clear();
        self.transfer_to = None;
        self.replicate_dirty = false;
    }

    fn become_follower(&mut self, term: Term, leader: Option<NodeId>) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
            self.persist();
        }
        self.role = Role::Follower;
        self.leader = leader;
        self.votes.clear();
        self.clear_leader_state();
        self.reset_election_timer();
    }

    fn campaign(&mut self, pre: bool, force: bool) {
        self.stats.elections_started += 1;
        self.leader = None;
        self.clear_leader_state();
        self.votes.clear();
        self.votes.insert(self.cfg.id, true);
        let term = if pre {
            self.role = Role::PreCandidate;
            self.term + 1
        } else {
            self.role = Role::Candidate;
            self.term += 1;
            self.voted_for = Some(self.cfg.id);
            self.persist();
            self.term
        };
        self.reset_election_timer();
        if self.quorum() == 1 {
            self.win_round(pre);
            return;
        }
        let (last_index, last_term) = self.last_log();
        for p in self.peers() {
            self.send(p, Msg::Vote { pre, force, term, last_index, last_term });
        }
    }

    fn win_round(&mut self, pre: bool) {
        if pre {
            self.campaign(false, false);
        } else {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.stats.became_leader += 1;
        self.role = Role::Leader;
        self.leader = Some(self.cfg.id);
        self.votes.clear();
        self.clear_leader_state();
        self.reset_election_timer();
        self.heartbeat_elapsed = 0;
        let last = self.store.last_index();
        for p in self.peers() {
            self.progress.insert(
                p,
                Progress {
                    next: last + 1,
                    matched: 0,
                    state: PeerState::Probe,
                    probe_sent: false,
                    inflight: VecDeque::new(),
                    active: true,
                    ack_ctx: 0,
                    idle_ticks: 0,
                    stalled_ticks: 0,
                },
            );
        }
        // A leader may only commit entries from its own term (§5.4.2), so a
        // no-op immediately makes earlier entries committable.
        let e = Entry::new(self.term, last + 1, Vec::new());
        self.store.append(&[e]);
        self.advance_commit();
        self.replicate_dirty = true;
    }

    // ---------------------------------------------------------------- timers

    pub fn tick(&mut self) {
        if self.role == Role::Leader {
            self.tick_leader();
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                self.campaign(self.cfg.pre_vote, false);
            }
        }
    }

    fn tick_leader(&mut self) {
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        for p in self.progress.values_mut() {
            p.idle_ticks = p.idle_ticks.saturating_add(1);
            if p.inflight.is_empty() {
                p.stalled_ticks = 0;
            } else {
                p.stalled_ticks = p.stalled_ticks.saturating_add(1);
            }
        }
        if self.election_elapsed >= self.cfg.election_min {
            self.election_elapsed = 0;
            self.transfer_to = None;
            if self.cfg.check_quorum {
                let active = 1 + self.progress.values().filter(|p| p.active).count();
                if active < self.quorum() {
                    // Cut off from the majority: stop accepting work.
                    self.become_follower(self.term, None);
                    return;
                }
                for p in self.progress.values_mut() {
                    p.active = false;
                }
            }
        }
        if self.heartbeat_elapsed >= self.cfg.heartbeat {
            self.heartbeat_elapsed = 0;
            self.broadcast_heartbeat();
        }
    }

    fn stale_after(&self) -> u32 {
        self.cfg.heartbeat * 2
    }

    fn broadcast_heartbeat(&mut self) {
        for p in self.peers() {
            self.heartbeat_peer(p);
        }
    }

    fn heartbeat_peer(&mut self, peer: NodeId) {
        let stale = self.stale_after();
        let pr = self.progress.get_mut(&peer).unwrap();
        match pr.state {
            PeerState::Snapshot(_) => {
                if pr.idle_ticks >= stale {
                    self.send_snapshot_chunk(peer);
                }
                return;
            }
            PeerState::Probe => {
                pr.probe_sent = false;
                self.send_append(peer);
                return;
            }
            PeerState::Replicate => {
                if !pr.inflight.is_empty() && pr.stalled_ticks >= stale {
                    pr.state = PeerState::Probe;
                    pr.next = pr.matched + 1;
                    pr.inflight.clear();
                    pr.probe_sent = false;
                    pr.stalled_ticks = 0;
                    self.send_append(peer);
                    return;
                }
            }
        }
        let prev_index = if pr.matched > 0 { pr.matched } else { pr.next - 1 };
        match self.store.term_at(prev_index) {
            Some(prev_term) => {
                let msg = Msg::Append {
                    term: self.term,
                    prev_index,
                    prev_term,
                    entries: Vec::new(),
                    commit: self.commit,
                    read_ctx: self.read_seq,
                };
                self.send(peer, msg);
            }
            None => self.start_snapshot(peer),
        }
    }

    // --------------------------------------------------------------- clients

    /// Append a command to the leader's log. Durable only after the next `sync`.
    pub fn propose(&mut self, data: Vec<u8>) -> Option<(Index, Term)> {
        if self.role != Role::Leader || self.transfer_to.is_some() {
            return None;
        }
        let index = self.store.last_index() + 1;
        self.store.append(&[Entry::new(self.term, index, data)]);
        self.replicate_dirty = true;
        if self.quorum() == 1 {
            self.advance_commit();
        }
        Some((index, self.term))
    }

    /// Start a linearizable read (dissertation §6.4). Fails if this node is not
    /// a leader that has committed an entry in its own term.
    pub fn read_index(&mut self, token: u64) -> bool {
        if self.role != Role::Leader || self.store.term_at(self.commit) != Some(self.term) {
            return false;
        }
        self.unsent_reads.push(token);
        true
    }

    /// Hand leadership to the most caught-up peer, for graceful shutdown.
    pub fn transfer_leadership(&mut self) -> bool {
        if self.role != Role::Leader {
            return false;
        }
        let best = self.progress.iter().max_by_key(|(_, p)| p.matched).map(|(id, _)| *id);
        let Some(target) = best else { return false };
        self.transfer_to = Some(target);
        self.election_elapsed = 0;
        self.maybe_send_timeout_now(target);
        true
    }

    fn maybe_send_timeout_now(&mut self, peer: NodeId) {
        if self.transfer_to == Some(peer)
            && self.progress.get(&peer).map(|p| p.matched) == Some(self.store.last_index())
        {
            let term = self.term;
            self.send(peer, Msg::TimeoutNow { term });
        }
    }

    /// Release batched work. The driver calls this once per event-loop pass.
    pub fn flush(&mut self) {
        if self.role != Role::Leader {
            return;
        }
        if !self.unsent_reads.is_empty() {
            self.read_seq += 1;
            let round =
                ReadRound { seq: self.read_seq, index: self.commit, tokens: std::mem::take(&mut self.unsent_reads) };
            self.read_rounds.push_back(round);
            if self.quorum() == 1 {
                self.confirm_reads();
            } else {
                self.heartbeat_elapsed = 0;
                self.broadcast_heartbeat();
            }
        }
        if self.replicate_dirty {
            self.replicate_dirty = false;
            for p in self.peers() {
                self.pump(p);
            }
        }
    }

    fn confirm_reads(&mut self) {
        let mut acks: Vec<u64> = self.progress.values().map(|p| p.ack_ctx).collect();
        acks.push(u64::MAX);
        acks.sort_unstable_by(|a, b| b.cmp(a));
        let confirmed = acks[self.quorum() - 1];
        while self.read_rounds.front().map(|r| r.seq <= confirmed).unwrap_or(false) {
            let r = self.read_rounds.pop_front().unwrap();
            for t in r.tokens {
                self.ready_reads.push((t, r.index));
            }
        }
    }

    // ----------------------------------------------------------- replication

    fn pump(&mut self, peer: NodeId) {
        while self.send_append(peer) {}
    }

    /// Returns true if more could usefully be sent straight away.
    fn send_append(&mut self, peer: NodeId) -> bool {
        let first = self.store.first_index();
        let last = self.store.last_index();
        let max_inflight = self.cfg.max_inflight;
        let pr = self.progress.get_mut(&peer).unwrap();
        match pr.state {
            PeerState::Snapshot(_) => return false,
            PeerState::Probe if pr.probe_sent => return false,
            PeerState::Replicate if pr.inflight.len() >= max_inflight => return false,
            _ => {}
        }
        let next = pr.next;
        let prev_index = next - 1;
        let is_replicate = matches!(pr.state, PeerState::Replicate);
        if is_replicate && next > last {
            return false;
        }
        let Some(prev_term) = self.store.term_at(prev_index) else {
            self.start_snapshot(peer);
            return false;
        };
        if next < first {
            self.start_snapshot(peer);
            return false;
        }
        let entries = if next <= last { self.store.entries(next, last, self.cfg.max_append_bytes) } else { Vec::new() };
        let pr = self.progress.get_mut(&peer).unwrap();
        if is_replicate {
            let l = entries.last().unwrap().index;
            pr.next = l + 1;
            pr.inflight.push_back(l);
        } else {
            pr.probe_sent = true;
        }
        let msg = Msg::Append {
            term: self.term,
            prev_index,
            prev_term,
            entries,
            commit: self.commit,
            read_ctx: self.read_seq,
        };
        self.send(peer, msg);
        is_replicate
    }

    fn start_snapshot(&mut self, peer: NodeId) {
        let meta = self.store.snapshot_meta();
        if meta.last_index == 0 {
            self.violation(format!("peer {peer} needs entries this leader has already lost"));
            return;
        }
        let data = self.store.snapshot_data();
        self.stats.snapshots_sent += 1;
        let pr = self.progress.get_mut(&peer).unwrap();
        pr.inflight.clear();
        pr.state = PeerState::Snapshot(OutSnapshot { meta, data, offset: 0 });
        self.send_snapshot_chunk(peer);
    }

    fn send_snapshot_chunk(&mut self, peer: NodeId) {
        let chunk_size = self.cfg.snapshot_chunk;
        let term = self.term;
        let pr = self.progress.get_mut(&peer).unwrap();
        pr.idle_ticks = 0;
        let PeerState::Snapshot(s) = &pr.state else { return };
        let end = (s.offset + chunk_size).min(s.data.len());
        let msg = Msg::Snapshot {
            term,
            meta: s.meta,
            offset: s.offset as u64,
            data: s.data[s.offset..end].to_vec(),
            done: end == s.data.len(),
        };
        self.send(peer, msg);
    }

    fn advance_commit(&mut self) {
        let mut m: Vec<Index> = self.progress.values().map(|p| p.matched).collect();
        m.push(self.store.last_index());
        m.sort_unstable_by(|a, b| b.cmp(a));
        let n = m[self.quorum() - 1];
        if n > self.commit {
            let current_term = self.store.term_at(n) == Some(self.term);
            if current_term || self.cfg.bugs.commit_old_term {
                self.commit = n;
                self.replicate_dirty = true;
            }
        }
    }

    // ------------------------------------------------------------- messages

    pub fn step(&mut self, env: Envelope) {
        let from = env.from;
        match env.msg {
            Msg::Vote { pre, force, term, last_index, last_term } => {
                self.on_vote(from, pre, force, term, last_index, last_term)
            }
            Msg::VoteResp { pre, term, granted } => self.on_vote_resp(from, pre, term, granted),
            Msg::Append { term, prev_index, prev_term, entries, commit, read_ctx } => {
                self.on_append(from, term, prev_index, prev_term, entries, commit, read_ctx)
            }
            Msg::AppendResp { term, success, match_index, rejected_prev, conflict_index, conflict_term, read_ctx } => {
                self.on_append_resp(
                    from,
                    term,
                    success,
                    match_index,
                    rejected_prev,
                    conflict_index,
                    conflict_term,
                    read_ctx,
                )
            }
            Msg::Snapshot { term, meta, offset, data, done } => self.on_snapshot(from, term, meta, offset, data, done),
            Msg::SnapshotResp { term, last_index, next_offset, done } => {
                self.on_snapshot_resp(from, term, last_index, next_offset, done)
            }
            Msg::TimeoutNow { term } => {
                if term == self.term && self.role != Role::Leader {
                    self.campaign(false, true);
                }
            }
        }
    }

    fn on_vote(&mut self, from: NodeId, pre: bool, force: bool, term: Term, li: Index, lt: Term) {
        // A node that recently heard from a live leader ignores vote requests,
        // so a replica rejoining after a partition cannot depose a healthy one.
        let in_lease =
            self.cfg.check_quorum && self.leader.is_some() && self.election_elapsed < self.cfg.election_min && !force;
        if term > self.term && in_lease {
            return;
        }
        if term < self.term {
            let t = self.term;
            self.send(from, Msg::VoteResp { pre, term: t, granted: false });
            return;
        }
        if !pre && term > self.term {
            self.become_follower(term, None);
        }
        let can_vote = self.voted_for == Some(from)
            || (self.voted_for.is_none() && self.leader.is_none())
            || (pre && term > self.term);
        let (my_i, my_t) = self.last_log();
        let up_to_date = lt > my_t || (lt == my_t && li >= my_i);
        let log_ok = up_to_date || self.cfg.bugs.vote_ignores_log;
        if can_vote && log_ok {
            if pre {
                self.send(from, Msg::VoteResp { pre, term, granted: true });
            } else {
                self.voted_for = Some(from);
                self.persist();
                self.reset_election_timer();
                self.send(from, Msg::VoteResp { pre, term, granted: true });
            }
        } else {
            let t = self.term;
            self.send(from, Msg::VoteResp { pre, term: t, granted: false });
        }
    }

    fn on_vote_resp(&mut self, from: NodeId, pre: bool, term: Term, granted: bool) {
        if term > self.term && (!pre || !granted) {
            self.become_follower(term, None);
            return;
        }
        let expected = match (self.role, pre) {
            (Role::PreCandidate, true) => term == self.term + 1 || !granted,
            (Role::Candidate, false) => term == self.term,
            _ => false,
        };
        if !expected {
            return;
        }
        self.votes.insert(from, granted);
        let yes = self.votes.values().filter(|v| **v).count();
        let no = self.votes.len() - yes;
        if yes >= self.quorum() {
            self.win_round(pre);
        } else if no >= self.quorum() {
            self.become_follower(self.term, None);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append(
        &mut self,
        from: NodeId,
        term: Term,
        prev_index: Index,
        prev_term: Term,
        entries: Vec<Entry>,
        leader_commit: Index,
        read_ctx: u64,
    ) {
        if term < self.term {
            let t = self.term;
            self.send(
                from,
                Msg::AppendResp {
                    term: t,
                    success: false,
                    match_index: 0,
                    rejected_prev: prev_index,
                    conflict_index: 0,
                    conflict_term: 0,
                    read_ctx,
                },
            );
            return;
        }
        if self.role == Role::Leader && term == self.term {
            self.violation(format!("two leaders in term {term}: self and {from}"));
            return;
        }
        if term > self.term || self.role != Role::Follower {
            self.become_follower(term, Some(from));
        } else {
            self.leader = Some(from);
            self.reset_election_timer();
        }

        let mut prev = prev_index;
        let mut entries = entries;
        let original_end = prev_index + entries.len() as u64;
        let mut trimmed = false;
        if prev < self.commit {
            // Everything at or below our commit point is already known to match.
            let skip = ((self.commit - prev) as usize).min(entries.len());
            entries.drain(..skip);
            prev = self.commit;
            trimmed = true;
        }

        if !trimmed && self.store.term_at(prev) != Some(prev_term) {
            let last = self.store.last_index();
            let (conflict_index, conflict_term) = if prev > last {
                (last + 1, 0)
            } else {
                if prev <= self.commit && prev > 0 {
                    self.violation(format!("leader {from} conflicts with committed entry {prev}"));
                    return;
                }
                let t = self.store.term_at(prev).unwrap_or(0);
                let floor = self.store.first_index().max(self.commit + 1);
                let mut i = prev;
                while i > floor && self.store.term_at(i - 1) == Some(t) {
                    i -= 1;
                }
                (i, t)
            };
            let t = self.term;
            self.send(
                from,
                Msg::AppendResp {
                    term: t,
                    success: false,
                    match_index: 0,
                    rejected_prev: prev_index,
                    conflict_index,
                    conflict_term,
                    read_ctx,
                },
            );
            return;
        }

        // Skip entries we already hold; truncate at the first real conflict.
        let mut start = 0;
        for e in &entries {
            match self.store.term_at(e.index) {
                Some(t) if t == e.term => start += 1,
                Some(_) if self.cfg.bugs.no_truncate => start += 1,
                Some(t) => {
                    if e.index <= self.commit {
                        let msg = format!("truncating committed entry {} (term {t} vs {})", e.index, e.term);
                        self.violation(msg);
                        return;
                    }
                    self.store.truncate_from(e.index);
                    self.stats.truncations += 1;
                    break;
                }
                None => break,
            }
        }
        if start < entries.len() {
            self.store.append(&entries[start..]);
        }

        let verified = original_end.max(prev);
        let new_commit = leader_commit.min(verified);
        if new_commit > self.commit {
            self.commit = new_commit;
        }
        let t = self.term;
        let match_index = verified.max(self.commit);
        self.send(
            from,
            Msg::AppendResp {
                term: t,
                success: true,
                match_index,
                rejected_prev: 0,
                conflict_index: 0,
                conflict_term: 0,
                read_ctx,
            },
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn on_append_resp(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: Index,
        rejected_prev: Index,
        conflict_index: Index,
        conflict_term: Term,
        read_ctx: u64,
    ) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        let last = self.store.last_index();
        let Some(pr) = self.progress.get_mut(&from) else { return };
        pr.active = true;
        pr.idle_ticks = 0;
        pr.ack_ctx = pr.ack_ctx.max(read_ctx);

        if success {
            if match_index > pr.matched {
                pr.matched = match_index;
                pr.stalled_ticks = 0;
            }
            pr.next = pr.next.max(pr.matched + 1);
            while pr.inflight.front().map(|i| *i <= pr.matched).unwrap_or(false) {
                pr.inflight.pop_front();
            }
            if matches!(pr.state, PeerState::Probe) {
                pr.state = PeerState::Replicate;
                pr.next = pr.matched + 1;
                pr.inflight.clear();
                pr.probe_sent = false;
            }
            self.advance_commit();
            self.pump(from);
            self.maybe_send_timeout_now(from);
        } else {
            let stale = match pr.state {
                PeerState::Snapshot(_) => true,
                PeerState::Probe => rejected_prev != pr.next - 1,
                PeerState::Replicate => rejected_prev <= pr.matched,
            };
            if !stale {
                let matched = pr.matched;
                let mut next = conflict_index.max(1);
                if conflict_term != 0 {
                    // Fast backup: jump past the follower's whole conflicting term.
                    let floor = self.store.first_index();
                    let mut i = rejected_prev.min(last);
                    while i >= floor && i > 0 {
                        match self.store.term_at(i) {
                            Some(t) if t > conflict_term => i -= 1,
                            Some(t) if t == conflict_term => {
                                next = i + 1;
                                break;
                            }
                            _ => break,
                        }
                    }
                }
                let next = next.max(matched + 1).min(last + 1);
                let pr = self.progress.get_mut(&from).unwrap();
                pr.next = next;
                pr.state = PeerState::Probe;
                pr.inflight.clear();
                pr.probe_sent = false;
                self.pump(from);
            }
        }
        self.confirm_reads();
    }

    fn on_snapshot(&mut self, from: NodeId, term: Term, meta: SnapshotMeta, offset: u64, data: Vec<u8>, done: bool) {
        if term < self.term {
            let t = self.term;
            self.send(from, Msg::SnapshotResp { term: t, last_index: meta.last_index, next_offset: 0, done: false });
            return;
        }
        if term > self.term || self.role != Role::Follower {
            self.become_follower(term, Some(from));
        } else {
            self.leader = Some(from);
            self.reset_election_timer();
        }
        let t = self.term;
        if offset == 0 {
            self.snap_in = Some(InSnapshot { from, meta, buf: Vec::new() });
        }
        let expected = match &self.snap_in {
            Some(s) if s.from == from && s.meta == meta => s.buf.len() as u64,
            _ => 0,
        };
        if offset != expected {
            self.send(
                from,
                Msg::SnapshotResp { term: t, last_index: meta.last_index, next_offset: expected, done: false },
            );
            return;
        }
        let incoming = self.snap_in.as_mut().unwrap();
        incoming.buf.extend_from_slice(&data);
        let next_offset = incoming.buf.len() as u64;
        if !done {
            self.send(from, Msg::SnapshotResp { term: t, last_index: meta.last_index, next_offset, done: false });
            return;
        }
        let incoming = self.snap_in.take().unwrap();
        if meta.last_index > self.commit {
            let data = Arc::new(incoming.buf);
            self.store.install_snapshot(meta, data.clone());
            self.commit = meta.last_index;
            self.restored = Some((meta, data));
            self.stats.snapshots_installed += 1;
        }
        self.send(from, Msg::SnapshotResp { term: t, last_index: meta.last_index, next_offset, done: true });
    }

    fn on_snapshot_resp(&mut self, from: NodeId, term: Term, last_index: Index, next_offset: u64, done: bool) {
        if term > self.term {
            self.become_follower(term, None);
            return;
        }
        if self.role != Role::Leader || term < self.term {
            return;
        }
        let Some(pr) = self.progress.get_mut(&from) else { return };
        pr.active = true;
        pr.idle_ticks = 0;
        let PeerState::Snapshot(s) = &mut pr.state else { return };
        if s.meta.last_index != last_index {
            return;
        }
        if done {
            pr.matched = pr.matched.max(last_index);
            pr.next = pr.matched + 1;
            pr.state = PeerState::Probe;
            pr.probe_sent = false;
            self.advance_commit();
            self.pump(from);
        } else if next_offset as usize != s.offset {
            // An ack that moves nothing is a duplicate; the retransmit timer
            // covers the case where a chunk really was lost.
            s.offset = (next_offset as usize).min(s.data.len());
            self.send_snapshot_chunk(from);
        }
    }
}
