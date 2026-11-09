//! Deterministic simulation: the real `Replica`, a hostile fake network, and a
//! fake disk that loses unsynced data. A seed reproduces a run exactly.
//!
//! Continuously checked, from Figure 3 of the Raft paper:
//! Election Safety, Log Matching, Leader Completeness, State Machine Safety.
//! At the end, the client history goes through the linearizability checker.

use crate::checker::*;
use crate::kv::*;
use crate::raft::{self, Envelope, Role};
use crate::replica::*;
use crate::storage::mem::MemStorage;
use crate::storage::Storage;
use crate::types::*;
use crate::util::Rng;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct SimOptions {
    pub seed: u64,
    pub nodes: u64,
    pub clients: u64,
    pub keys: u64,
    pub chaos_ticks: u64,
    pub snapshot_threshold: u64,
    pub snapshot_trailing: u64,
    pub max_sessions: usize,
    pub max_append_bytes: usize,
    pub bugs: Bugs,
}

impl SimOptions {
    /// Varies cluster size and pressure from the seed.
    pub fn from_seed(seed: u64, bugs: Bugs) -> Self {
        let mut r = Rng::new(seed ^ 0xABCD_EF01);
        SimOptions {
            seed,
            nodes: [3, 3, 5, 5, 7][r.below(5)],
            clients: r.range(2, 6),
            keys: r.range(1, 5),
            chaos_ticks: 5000,
            snapshot_threshold: [20, 60, 200, 100_000][r.below(4)],
            snapshot_trailing: [0, 5, 30][r.below(3)],
            max_sessions: [8, 1000][r.below(2)],
            max_append_bytes: [1, 512][r.below(2)],
            bugs,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SimReport {
    pub seed: u64,
    pub violations: Vec<String>,
    pub acked_writes: u64,
    pub indeterminate_writes: u64,
    pub ok_reads: u64,
    pub crashes: u64,
    pub torn_crashes: u64,
    pub partitions: u64,
    pub leaders_elected: u64,
    pub snapshots_installed: u64,
    pub truncations: u64,
    pub messages: u64,
    pub final_commit: u64,
}

#[derive(Clone, Debug)]
enum SimReq {
    Write(Command),
    Read(Vec<u8>),
}

enum Payload {
    Peer(Envelope),
    Request { node: NodeId, client: usize, op_id: u64, req: SimReq },
    Reply { client: usize, op_id: u64, reply: Reply },
}

struct Network {
    rng: Rng,
    now: u64,
    seq: u64,
    heap: BinaryHeap<Reverse<(u64, u64)>>,
    payloads: HashMap<u64, Payload>,
    blocked: HashMap<(NodeId, NodeId), bool>,
    drop_permille: u32,
    dup_permille: u32,
    max_delay: u64,
    sent: u64,
}

impl Network {
    fn is_blocked(&self, a: NodeId, b: NodeId) -> bool {
        self.blocked.get(&(a, b)).copied().unwrap_or(false)
    }

    fn enqueue(&mut self, delay: u64, p: Payload) {
        self.seq += 1;
        self.heap.push(Reverse((self.now + delay, self.seq)));
        self.payloads.insert(self.seq, p);
    }

    fn lossy_delay(&mut self) -> Option<u64> {
        if self.rng.chance(self.drop_permille) {
            return None;
        }
        Some(self.rng.range(1, self.max_delay))
    }

    fn send_peer(&mut self, env: Envelope) {
        self.sent += 1;
        if self.is_blocked(env.from, env.to) {
            return;
        }
        if self.rng.chance(self.dup_permille) {
            if let Some(d) = self.lossy_delay() {
                self.enqueue(d, Payload::Peer(env.clone()));
            }
        }
        if let Some(d) = self.lossy_delay() {
            self.enqueue(d, Payload::Peer(env));
        }
    }

    fn send_client(&mut self, p: Payload) {
        if let Some(d) = self.lossy_delay() {
            self.enqueue(d, p);
        }
    }

    fn pop_due(&mut self) -> Option<Payload> {
        match self.heap.peek() {
            Some(Reverse((at, seq))) if *at <= self.now => {
                let seq = *seq;
                self.heap.pop();
                self.payloads.remove(&seq)
            }
            _ => None,
        }
    }
}

struct Node {
    replica: Option<Replica<MemStorage>>,
    disk: Option<MemStorage>,
    restart_at: u64,
    max_term: Term,
    was_leader_in: Term,
}

#[derive(Clone)]
enum ClientOp {
    Register,
    Append { key: String, token: String, seq: u64 },
    Read { key: String },
}

struct Current {
    op: ClientOp,
    op_id: u64,
    invoke: u64,
    deadline: u64,
    next_send: u64,
    target: NodeId,
}

struct SimClient {
    session: Option<u64>,
    seq: u64,
    counter: u64,
    next_op_id: u64,
    current: Option<Current>,
    idle_until: u64,
}

struct Sim {
    o: SimOptions,
    rng: Rng,
    net: Network,
    nodes: Vec<Node>,
    clients: Vec<SimClient>,
    tokens: HashMap<u64, (usize, u64)>,
    next_token: u64,
    clock: u64,
    history: Vec<OpRecord>,
    report: SimReport,
    leaders: HashMap<Term, NodeId>,
    applied_log: Vec<Option<(Term, Arc<[u8]>)>>,
    stop_new_ops: bool,
    acks_after_heal: u64,
    healed: bool,
    drained_stats: (u64, u64),
}

const CLIENT_DEADLINE: u64 = 1200;

impl Sim {
    fn raft_config(&self, id: NodeId, restart_salt: u64) -> raft::Config {
        let voters: Vec<NodeId> = (1..=self.o.nodes).collect();
        let mut c = raft::Config::new(id, voters);
        c.seed = self.o.seed.wrapping_mul(31).wrapping_add(id * 1009 + restart_salt);
        c.snapshot_chunk = 64;
        // One entry per message opens the windows that Figure 8 depends on.
        c.max_append_bytes = self.o.max_append_bytes;
        c.max_inflight = 8;
        c.bugs = self.o.bugs;
        c
    }

    fn replica_config(&self) -> ReplicaConfig {
        ReplicaConfig {
            snapshot_threshold: self.o.snapshot_threshold,
            snapshot_trailing: self.o.snapshot_trailing,
            max_sessions: self.o.max_sessions,
            max_pending: 10_000,
        }
    }

    fn start_node(&mut self, i: usize, disk: MemStorage, salt: u64) {
        let id = (i + 1) as u64;
        let cfg = self.raft_config(id, salt);
        let mut r = Replica::new(cfg, self.replica_config(), disk).expect("restore");
        r.apply_trace = Some(Vec::new());
        self.nodes[i].replica = Some(r);
    }

    fn violation(&mut self, v: String) {
        if self.report.violations.len() < 64 {
            self.report.violations.push(v);
        }
    }

    fn tick_clock(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }

    fn crash(&mut self, i: usize, torn: bool) {
        let Some(replica) = self.nodes[i].replica.take() else { return };
        let stats = replica.raft.stats;
        self.drained_stats.0 += stats.snapshots_installed;
        self.drained_stats.1 += stats.truncations;
        self.report.leaders_elected += stats.became_leader;
        let mut disk = replica.raft.store;
        disk.crash(&mut self.rng);
        self.nodes[i].disk = Some(disk);
        self.nodes[i].restart_at = self.net.now + self.rng.range(10, 250);
        self.report.crashes += 1;
        if torn {
            self.report.torn_crashes += 1;
        }
    }

    fn apply_faults(&mut self) {
        let n = self.nodes.len();
        if self.rng.chance(4) {
            let i = self.rng.below(n);
            self.crash(i, false);
        }
        if self.rng.chance(2) {
            // Prefer killing the leader; that is where the bugs live.
            if let Some(i) =
                (0..n).find(|i| self.nodes[*i].replica.as_ref().map(|r| r.raft.is_leader()).unwrap_or(false))
            {
                self.crash(i, false);
            }
        }
        if self.rng.chance(4) {
            self.new_partition();
        }
        if self.rng.chance(6) {
            self.net.blocked.clear();
        }
        if self.rng.chance(3) {
            self.net.drop_permille = [0, 20, 100, 300][self.rng.below(4)];
            self.net.dup_permille = [0, 20, 100][self.rng.below(3)];
            self.net.max_delay = [3, 8, 25][self.rng.below(3)];
        }
    }

    fn new_partition(&mut self) {
        self.report.partitions += 1;
        self.net.blocked.clear();
        let n = self.nodes.len() as u64;
        let mut ids: Vec<NodeId> = (1..=n).collect();
        self.rng.shuffle(&mut ids);
        match self.rng.below(3) {
            0 => {
                // Clean split into two sides.
                let cut = self.rng.range(1, n - 1) as usize;
                for a in &ids[..cut] {
                    for b in &ids[cut..] {
                        self.net.blocked.insert((*a, *b), true);
                        self.net.blocked.insert((*b, *a), true);
                    }
                }
            }
            1 => {
                // One-way failure: the node can send but cannot hear.
                let a = ids[0];
                for b in &ids[1..] {
                    self.net.blocked.insert((*b, a), true);
                }
            }
            _ => {
                // Random individual links fail.
                for a in 1..=n {
                    for b in 1..=n {
                        if a != b && self.rng.chance(350) {
                            self.net.blocked.insert((a, b), true);
                        }
                    }
                }
            }
        }
    }

    fn deliver(&mut self, p: Payload) {
        match p {
            Payload::Peer(env) => {
                if self.net.is_blocked(env.from, env.to) {
                    return;
                }
                let i = (env.to - 1) as usize;
                if let Some(r) = self.nodes[i].replica.as_mut() {
                    r.step(env);
                }
            }
            Payload::Request { node, client, op_id, req } => {
                self.next_token += 1;
                let token = self.next_token;
                let i = (node - 1) as usize;
                if let Some(r) = self.nodes[i].replica.as_mut() {
                    self.tokens.insert(token, (client, op_id));
                    match req {
                        SimReq::Write(c) => r.submit(token, c),
                        SimReq::Read(k) => r.read(token, k),
                    }
                }
            }
            Payload::Reply { client, op_id, reply } => self.on_reply(client, op_id, reply),
        }
    }

    fn after_flush(&mut self, i: usize, out: Output) {
        for env in out.messages {
            self.net.send_peer(env);
        }
        for (token, reply) in out.replies {
            if let Some((client, op_id)) = self.tokens.remove(&token) {
                self.net.send_client(Payload::Reply { client, op_id, reply });
            }
        }
        let snap = {
            let r = self.nodes[i].replica.as_mut().unwrap();
            r.maybe_snapshot()
        };
        if let Some((meta, data)) = snap {
            let r = self.nodes[i].replica.as_mut().unwrap();
            r.snapshot_done(meta, Arc::new(data));
        }
        self.check_node(i);
    }

    fn check_node(&mut self, i: usize) {
        let id = (i + 1) as u64;
        let (role, term, safety, trace, leader_view) = {
            let r = self.nodes[i].replica.as_mut().unwrap();
            let trace = r.apply_trace.as_mut().map(std::mem::take).unwrap_or_default();
            (r.raft.role(), r.raft.term(), r.raft.safety_violation.clone(), trace, r.raft.store.last_index())
        };
        if let Some(v) = safety {
            self.violation(format!("SAFETY_ABORT {v}"));
        }
        if term < self.nodes[i].max_term {
            self.violation(format!("TERM_WENT_BACKWARDS node={id} {} -> {term}", self.nodes[i].max_term));
        }
        self.nodes[i].max_term = term;

        for (index, t, data) in trace {
            let slot = index as usize;
            if self.applied_log.len() <= slot {
                self.applied_log.resize(slot + 1, None);
            }
            match &self.applied_log[slot] {
                None => self.applied_log[slot] = Some((t, data)),
                Some((et, ed)) => {
                    if *et != t || **ed != *data {
                        self.violation(format!(
                            "STATE_MACHINE_SAFETY node={id} index={index}: term {t} but another node applied term {et}"
                        ));
                    }
                }
            }
        }

        if role == Role::Leader {
            match self.leaders.get(&term).copied() {
                Some(other) if other != id => {
                    self.violation(format!("ELECTION_SAFETY term={term}: leaders {other} and {id}"));
                }
                _ => {
                    self.leaders.insert(term, id);
                }
            }
            if self.nodes[i].was_leader_in != term {
                self.nodes[i].was_leader_in = term;
                self.check_leader_completeness(i, term, leader_view);
            }
        }
    }

    fn check_leader_completeness(&mut self, i: usize, term: Term, last: Index) {
        let id = (i + 1) as u64;
        let highest = self.applied_log.iter().rposition(|e| e.is_some()).unwrap_or(0) as Index;
        if highest > last {
            self.violation(format!(
                "LEADER_COMPLETENESS node={id} term={term}: log ends at {last}, but {highest} was committed"
            ));
            return;
        }
        let r = self.nodes[i].replica.as_ref().unwrap();
        let mut found = None;
        for idx in r.raft.store.first_index().max(1)..=highest {
            if let (Some((t, _)), Some(have)) = (&self.applied_log[idx as usize], r.raft.store.term_at(idx)) {
                if *t != have {
                    found = Some((idx, *t, have));
                    break;
                }
            }
        }
        if let Some((idx, t, have)) = found {
            self.violation(format!(
                "LEADER_COMPLETENESS node={id} term={term}: index {idx} committed in term {t}, leader has term {have}"
            ));
        }
    }

    fn check_log_matching(&mut self) {
        let mut v = Vec::new();
        for a in 0..self.nodes.len() {
            for b in (a + 1)..self.nodes.len() {
                let (Some(x), Some(y)) = (self.nodes[a].replica.as_ref(), self.nodes[b].replica.as_ref()) else {
                    continue;
                };
                let lo = x.raft.store.first_index().max(y.raft.store.first_index()).max(1);
                let hi = x.raft.store.last_index().min(y.raft.store.last_index());
                if lo > hi {
                    continue;
                }
                let ex = x.raft.store.entries(lo, hi, usize::MAX);
                let ey = y.raft.store.entries(lo, hi, usize::MAX);
                let mut must_match = false;
                for (p, q) in ex.iter().zip(ey.iter()).rev() {
                    if p.term == q.term {
                        must_match = true;
                        if p.data != q.data {
                            v.push(format!("LOG_MATCHING index={} same term, different data", p.index));
                            break;
                        }
                    } else if must_match {
                        v.push(format!("LOG_MATCHING index={} differs below a matching entry", p.index));
                        break;
                    }
                }
            }
        }
        for x in v {
            self.violation(x);
        }
    }

    // --------------------------------------------------------------- clients

    fn random_target(&mut self) -> NodeId {
        self.rng.range(1, self.o.nodes)
    }

    fn send_current(&mut self, c: usize) {
        let Some(cur) = self.clients[c].current.as_ref() else { return };
        let (op_id, target, op) = (cur.op_id, cur.target, cur.op.clone());
        let session = self.clients[c].session;
        let req = match op {
            ClientOp::Register => SimReq::Write(Command::Register),
            ClientOp::Append { key, token, seq } => {
                let session = session.unwrap_or(0);
                SimReq::Write(Command::Write {
                    session,
                    seq,
                    op: WriteOp::Append { key: key.into_bytes(), value: format!("{token},").into_bytes() },
                })
            }
            ClientOp::Read { key } => SimReq::Read(key.into_bytes()),
        };
        self.net.send_client(Payload::Request { node: target, client: c, op_id, req });
    }

    fn start_op(&mut self, c: usize, op: ClientOp) {
        let invoke = self.tick_clock();
        let target = self.random_target();
        let now = self.net.now;
        let wait = self.rng.range(30, 90);
        let cl = &mut self.clients[c];
        cl.next_op_id += 1;
        cl.current = Some(Current {
            op,
            op_id: cl.next_op_id,
            invoke,
            deadline: now + CLIENT_DEADLINE,
            next_send: now + wait,
            target,
        });
        self.send_current(c);
    }

    fn step_clients(&mut self) {
        let now = self.net.now;
        for c in 0..self.clients.len() {
            if let Some(cur) = self.clients[c].current.as_ref() {
                if now >= cur.deadline {
                    self.finish(c, Outcome::Indeterminate, None);
                } else if now >= cur.next_send {
                    let t = self.random_target();
                    let wait = self.rng.range(30, 90);
                    let cur = self.clients[c].current.as_mut().unwrap();
                    cur.target = t;
                    cur.next_send = now + wait;
                    self.send_current(c);
                }
                continue;
            }
            if self.stop_new_ops || now < self.clients[c].idle_until {
                continue;
            }
            if self.clients[c].session.is_none() {
                self.start_op(c, ClientOp::Register);
                continue;
            }
            let key = format!("k{}", self.rng.below(self.o.keys as usize));
            if self.rng.chance(700) {
                let cl = &mut self.clients[c];
                cl.seq += 1;
                cl.counter += 1;
                let token = format!("c{c}n{}", cl.counter);
                let seq = cl.seq;
                self.start_op(c, ClientOp::Append { key, token, seq });
            } else {
                self.start_op(c, ClientOp::Read { key });
            }
        }
    }

    fn finish(&mut self, c: usize, outcome: Outcome, value: Option<Option<Vec<u8>>>) {
        let Some(cur) = self.clients[c].current.take() else { return };
        let complete = self.tick_clock();
        self.clients[c].idle_until = self.net.now + self.rng.range(0, 10);
        match cur.op {
            ClientOp::Register => {}
            ClientOp::Append { key, token, .. } => {
                if outcome == Outcome::Ok && self.healed {
                    self.acks_after_heal += 1;
                }
                self.history.push(OpRecord {
                    client: c as u64,
                    key,
                    kind: OpKind::Append { token },
                    invoke: cur.invoke,
                    complete,
                    outcome,
                });
            }
            ClientOp::Read { key } => {
                if let Some(v) = value {
                    self.history.push(OpRecord {
                        client: c as u64,
                        key,
                        kind: OpKind::Read { observed: v.map(|b| parse_list(&b)).unwrap_or_default() },
                        invoke: cur.invoke,
                        complete,
                        outcome: Outcome::Ok,
                    });
                }
            }
        }
    }

    fn on_reply(&mut self, c: usize, op_id: u64, reply: Reply) {
        let now = self.net.now;
        let Some(cur) = self.clients[c].current.as_ref() else { return };
        if cur.op_id != op_id {
            return;
        }
        let op = cur.op.clone();
        match reply {
            Reply::NotLeader(hint) => {
                let delay = self.rng.range(2, 20);
                let t = match hint {
                    Some(h) => h,
                    None => self.random_target(),
                };
                let cur = self.clients[c].current.as_mut().unwrap();
                cur.target = t;
                cur.next_send = now + delay;
            }
            Reply::Retry | Reply::Busy => {
                let delay = self.rng.range(5, 30);
                self.clients[c].current.as_mut().unwrap().next_send = now + delay;
            }
            Reply::Value(v) => {
                if matches!(op, ClientOp::Read { .. }) {
                    self.finish(c, Outcome::Ok, Some(v));
                }
            }
            Reply::Applied(a) => match (op, a) {
                (ClientOp::Register, Applied::Registered(id)) => {
                    let cl = &mut self.clients[c];
                    cl.session = Some(id);
                    cl.seq = 0;
                    self.finish(c, Outcome::Ok, None);
                }
                (ClientOp::Append { .. }, Applied::Ok) => self.finish(c, Outcome::Ok, None),
                (ClientOp::Append { .. }, Applied::SessionExpired) => {
                    self.clients[c].session = None;
                    self.finish(c, Outcome::Indeterminate, None);
                }
                (ClientOp::Append { .. }, Applied::StaleSeq) => self.finish(c, Outcome::Indeterminate, None),
                _ => {}
            },
        }
    }

    // -------------------------------------------------------------- main loop

    fn step(&mut self, chaos: bool) {
        self.net.now += 1;
        // Alternate storms with calm stretches, so logs grow long enough to
        // need snapshots and elections can finish before the next fault.
        if chaos && (self.net.now / 400).is_multiple_of(2) {
            self.apply_faults();
        }
        let now = self.net.now;
        for i in 0..self.nodes.len() {
            if self.nodes[i].replica.is_none() && now >= self.nodes[i].restart_at {
                let disk = self.nodes[i].disk.take().unwrap();
                let salt = self.rng.next_u64();
                self.start_node(i, disk, salt);
            }
        }
        while let Some(p) = self.net.pop_due() {
            self.deliver(p);
        }
        for i in 0..self.nodes.len() {
            if self.nodes[i].replica.is_none() {
                continue;
            }
            self.nodes[i].replica.as_mut().unwrap().tick();
            // A crash here lands after the state changed but before fsync,
            // with nothing yet sent: the window a sloppy design gets wrong.
            if chaos && (self.net.now / 400).is_multiple_of(2) && self.rng.chance(2) {
                self.crash(i, true);
                continue;
            }
            let out = self.nodes[i].replica.as_mut().unwrap().flush();
            self.after_flush(i, out);
            if self.o.bugs.skip_fsync && self.rng.chance(5) {
                self.nodes[i].replica.as_mut().unwrap().raft.store.background_writeback();
            }
        }
        self.step_clients();
        if now.is_multiple_of(97) {
            self.check_log_matching();
        }
    }

    fn heal(&mut self) {
        self.net.blocked.clear();
        self.net.drop_permille = 0;
        self.net.dup_permille = 0;
        self.net.max_delay = 3;
        let now = self.net.now;
        for n in &mut self.nodes {
            if n.replica.is_none() {
                n.restart_at = now;
            }
        }
        self.healed = true;
    }

    fn run(mut self) -> SimReport {
        for i in 0..self.o.nodes as usize {
            let disk = MemStorage::new(self.o.bugs.skip_fsync);
            self.start_node(i, disk, 0);
        }
        for _ in 0..self.o.chaos_ticks {
            self.step(true);
            if self.halted() {
                return self.finish_early();
            }
        }
        self.heal();
        // Clients keep working long enough to prove the cluster recovered.
        for _ in 0..1000 {
            self.step(false);
            if self.halted() {
                return self.finish_early();
            }
        }
        self.stop_new_ops = true;
        for _ in 0..(CLIENT_DEADLINE + 400) {
            self.step(false);
            if self.halted() {
                return self.finish_early();
            }
            if self.clients.iter().all(|c| c.current.is_none()) {
                break;
            }
        }
        // Let anything still in flight land, then allow replication to finish.
        for _ in 0..600 {
            self.step(false);
        }
        self.check_log_matching();
        self.final_check();
        self.report.messages = self.net.sent;
        self.report
    }

    /// A violated invariant stops the run, as it would halt a real server.
    /// The message cap turns a protocol-level message storm into a failure
    /// instead of a run that never ends.
    fn halted(&mut self) -> bool {
        if self.net.sent > 1_000_000 {
            self.violation("LIVENESS message storm".into());
        }
        !self.report.violations.is_empty()
    }

    fn finish_early(mut self) -> SimReport {
        self.report.messages = self.net.sent;
        self.report
    }

    fn final_check(&mut self) {
        let leader = (0..self.nodes.len())
            .filter(|i| self.nodes[*i].replica.as_ref().map(|r| r.raft.is_leader()).unwrap_or(false))
            .max_by_key(|i| self.nodes[*i].replica.as_ref().unwrap().raft.term());
        let Some(l) = leader else {
            self.violation("LIVENESS no leader after healing".into());
            return;
        };
        if self.acks_after_heal == 0 {
            self.violation("LIVENESS no write was acknowledged after healing".into());
        }
        let (commit, digest) = {
            let r = self.nodes[l].replica.as_ref().unwrap();
            (r.raft.commit(), r.kv.digest())
        };
        self.report.final_commit = commit;
        let mut finals = HashMap::new();
        {
            let r = self.nodes[l].replica.as_ref().unwrap();
            for (k, v) in r.kv.iter() {
                finals.insert(String::from_utf8_lossy(k).to_string(), parse_list(v));
            }
        }
        let mut extra = Vec::new();
        for (i, n) in self.nodes.iter().enumerate() {
            let r = n.replica.as_ref().unwrap();
            if r.applied() != commit {
                extra.push(format!("LIVENESS node={} applied {} of {commit}", i + 1, r.applied()));
            } else if r.kv.digest() != digest {
                extra.push(format!("DIGEST_MISMATCH node={} differs from leader at index {commit}", i + 1));
            }
        }
        for v in extra {
            self.violation(v);
        }
        let check = check(&self.history, &finals);
        self.report.acked_writes = check.acked_writes;
        self.report.indeterminate_writes = check.indeterminate_writes;
        self.report.ok_reads = check.ok_reads;
        for v in check.violations {
            self.violation(v);
        }
        for n in &self.nodes {
            if let Some(r) = &n.replica {
                self.report.snapshots_installed += r.raft.stats.snapshots_installed;
                self.report.truncations += r.raft.stats.truncations;
                self.report.leaders_elected += r.raft.stats.became_leader;
            }
        }
        self.report.snapshots_installed += self.drained_stats.0;
        self.report.truncations += self.drained_stats.1;
    }
}

pub fn run(o: SimOptions) -> SimReport {
    let mut rng = Rng::new(o.seed);
    let net = Network {
        rng: Rng::new(rng.next_u64()),
        now: 0,
        seq: 0,
        heap: BinaryHeap::new(),
        payloads: HashMap::new(),
        blocked: HashMap::new(),
        drop_permille: 0,
        dup_permille: 0,
        max_delay: 3,
        sent: 0,
    };
    let nodes = (0..o.nodes)
        .map(|_| Node { replica: None, disk: None, restart_at: 0, max_term: 0, was_leader_in: 0 })
        .collect();
    let clients = (0..o.clients)
        .map(|_| SimClient { session: None, seq: 0, counter: 0, next_op_id: 0, current: None, idle_until: 0 })
        .collect();
    let report = SimReport { seed: o.seed, ..Default::default() };
    let sim = Sim {
        rng,
        net,
        nodes,
        clients,
        tokens: HashMap::new(),
        next_token: 0,
        clock: 0,
        history: Vec::new(),
        report,
        leaders: HashMap::new(),
        applied_log: Vec::new(),
        stop_new_ops: false,
        acks_after_heal: 0,
        healed: false,
        drained_stats: (0, 0),
        o,
    };
    sim.run()
}
