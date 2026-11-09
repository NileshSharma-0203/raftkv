//! The real-process chaos test: real servers, real TCP, real disks, SIGKILL.
//! Clients keep writing throughout; afterwards every acknowledged write must
//! be present exactly once and the whole history must be linearizable.

use crate::checker::*;
use crate::client::*;
use crate::harness::*;
use crate::types::*;
use crate::util::Rng;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ChaosOptions {
    pub nodes: u64,
    pub seconds: u64,
    pub clients: u64,
    pub seed: u64,
    pub dir: PathBuf,
    pub snapshot_threshold: u64,
    pub snapshot_trailing: u64,
    pub bug: Option<String>,
}

#[derive(Clone, Debug, Default)]
pub struct ChaosReport {
    pub seed: u64,
    pub nodes: u64,
    pub seconds: u64,
    pub violations: Vec<String>,
    pub check: CheckReport,
    pub faults: BTreeMap<String, u64>,
    pub leader_elections: u64,
    pub snapshots_taken: u64,
    pub snapshots_installed: u64,
    pub torn_tails_repaired: u64,
    pub process_starts: u64,
    pub final_commit: u64,
    pub client_retries: u64,
}

impl ChaosReport {
    pub fn passed(&self) -> bool {
        self.violations.is_empty()
    }

    pub fn to_json(&self) -> String {
        let faults = self.faults.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect::<Vec<_>>().join(",");
        let violations = self
            .violations
            .iter()
            .map(|v| format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "'")))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"seed\":{},\"nodes\":{},\"seconds\":{},\"passed\":{},\"acked_writes\":{},\"indeterminate_writes\":{},\"indeterminate_applied\":{},\"ok_reads\":{},\"keys\":{},\"faults\":{{{}}},\"leader_elections\":{},\"snapshots_taken\":{},\"snapshots_installed\":{},\"torn_tails_repaired\":{},\"process_starts\":{},\"final_commit\":{},\"client_retries\":{},\"violations\":[{}]}}",
            self.seed, self.nodes, self.seconds, self.passed(), self.check.acked_writes,
            self.check.indeterminate_writes, self.check.indeterminate_applied, self.check.ok_reads,
            self.check.keys, faults, self.leader_elections, self.snapshots_taken, self.snapshots_installed,
            self.torn_tails_repaired, self.process_starts, self.final_commit, self.client_retries, violations
        )
    }
}

/// Appends per key before moving on, which keeps values small.
const APPENDS_PER_KEY: u64 = 150;
const KEYS_PER_EPOCH: u64 = 8;

fn worker(
    id: u64,
    nodes: Vec<(NodeId, String)>,
    seed: u64,
    stop: Arc<AtomicBool>,
    total: Arc<AtomicU64>,
    t0: Instant,
) -> (Vec<OpRecord>, u64) {
    let opts = ClientOptions {
        op_deadline: Duration::from_secs(6),
        rpc_timeout: Duration::from_millis(800),
        ..ClientOptions::default()
    };
    let mut c = Client::new(nodes, opts);
    let mut rng = Rng::new(seed.wrapping_add(id * 7919));
    let mut history = Vec::new();
    let mut n = 0u64;
    let now = |t0: Instant| t0.elapsed().as_nanos() as u64;
    while !stop.load(Ordering::Relaxed) {
        let epoch = total.load(Ordering::Relaxed) / (APPENDS_PER_KEY * KEYS_PER_EPOCH);
        let key = format!("e{epoch}k{}", rng.below(KEYS_PER_EPOCH as usize));
        let invoke = now(t0);
        if rng.chance(700) {
            n += 1;
            let token = format!("c{id}n{n}");
            let value = format!("{token},");
            total.fetch_add(1, Ordering::Relaxed);
            let outcome = match c.append(key.as_bytes(), value.as_bytes()) {
                Ok(()) => Outcome::Ok,
                Err(_) => Outcome::Indeterminate,
            };
            history.push(OpRecord {
                client: id,
                key,
                kind: OpKind::Append { token },
                invoke,
                complete: now(t0),
                outcome,
            });
        } else if let Ok(v) = c.get(key.as_bytes()) {
            history.push(OpRecord {
                client: id,
                key,
                kind: OpKind::Read { observed: v.map(|b| parse_list(&b)).unwrap_or_default() },
                invoke,
                complete: now(t0),
                outcome: Outcome::Ok,
            });
        }
    }
    (history, c.retries)
}

fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}

fn random_split(n: u64, rng: &mut Rng) -> (Vec<NodeId>, Vec<NodeId>) {
    let mut ids: Vec<NodeId> = (1..=n).collect();
    rng.shuffle(&mut ids);
    let minority = rng.range(1, (n - 1) / 2) as usize;
    let right = ids.split_off(minority);
    (ids, right)
}

fn split(c: &Cluster, a: &[NodeId], b: &[NodeId], mode: u8) {
    for x in a {
        for y in b {
            c.set_link(*x, *y, mode);
            c.set_link(*y, *x, mode);
        }
    }
}

fn leader_id(c: &Cluster) -> Option<NodeId> {
    c.leader().map(|(i, _)| i as u64 + 1)
}

/// Apply one randomly chosen fault and return its name.
fn inject(c: &mut Cluster, n: u64, rng: &mut Rng) -> &'static str {
    let mode = if rng.chance(500) { RESET } else { BLACKHOLE };
    let all: Vec<NodeId> = (1..=n).collect();
    match rng.below(11) {
        0 => {
            if let Some(l) = leader_id(c) {
                c.kill((l - 1) as usize);
            }
            "kill_leader"
        }
        1 => {
            let i = rng.below(n as usize);
            c.kill(i);
            "kill_random"
        }
        2 => {
            let mut idx: Vec<usize> = (0..n as usize).collect();
            rng.shuffle(&mut idx);
            for i in idx.iter().take(((n - 1) / 2) as usize) {
                c.kill(*i);
            }
            "kill_minority"
        }
        3 => {
            for i in 0..n as usize {
                c.kill(i);
            }
            "kill_all"
        }
        4 => {
            let (a, b) = random_split(n, rng);
            split(c, &a, &b, mode);
            "partition_random"
        }
        5 => {
            if let Some(l) = leader_id(c) {
                let others: Vec<NodeId> = all.iter().copied().filter(|x| *x != l).collect();
                split(c, &[l], &others, mode);
            }
            "partition_isolate_leader"
        }
        6 => {
            // The leader can send but hears nothing back.
            if let Some(l) = leader_id(c) {
                for o in all.iter().filter(|x| **x != l) {
                    c.set_link(*o, l, mode);
                }
            }
            "partition_one_way_deaf_leader"
        }
        7 => {
            // Every node sees a majority, but no two agree on which one.
            if n >= 5 {
                for a in 0..n {
                    for d in [2u64, 3u64] {
                        let b = (a + d) % n;
                        c.set_link(a + 1, b + 1, mode);
                        c.set_link(b + 1, a + 1, mode);
                    }
                }
            } else {
                c.set_link(1, 2, mode);
                c.set_link(2, 1, mode);
            }
            "partition_bridge"
        }
        8 => {
            if let Some(l) = leader_id(c) {
                c.pause((l - 1) as usize);
            }
            "pause_leader"
        }
        9 => {
            c.delay_all(rng.range(20, 120));
            "slow_network"
        }
        _ => {
            // Partition and crash at once.
            let (a, b) = random_split(n, rng);
            split(c, &a, &b, mode);
            let i = (b[rng.below(b.len())] - 1) as usize;
            c.kill(i);
            "partition_plus_kill"
        }
    }
}

pub fn run(o: ChaosOptions) -> std::io::Result<ChaosReport> {
    let mut cluster_opts = ClusterOptions::new(o.nodes, o.dir.clone());
    cluster_opts.with_proxy = true;
    cluster_opts.snapshot_threshold = o.snapshot_threshold;
    cluster_opts.snapshot_trailing = o.snapshot_trailing;
    cluster_opts.segment_bytes = 256 * 1024;
    cluster_opts.bug = o.bug.clone();
    let mut c = Cluster::create(cluster_opts)?;
    let mut report = ChaosReport { seed: o.seed, nodes: o.nodes, seconds: o.seconds, ..Default::default() };
    let mut starts = o.nodes;

    if c.wait_for_leader(Duration::from_secs(15)).is_none() {
        report.violations.push("LIVENESS no initial leader".into());
        return Ok(report);
    }

    let stop = Arc::new(AtomicBool::new(false));
    let total = Arc::new(AtomicU64::new(0));
    let t0 = Instant::now();
    let nodes = c.client_nodes();
    let handles: Vec<_> = (0..o.clients)
        .map(|id| {
            let (nodes, stop, total) = (nodes.clone(), stop.clone(), total.clone());
            let seed = o.seed;
            thread::spawn(move || worker(id, nodes, seed, stop, total, t0))
        })
        .collect();

    let mut rng = Rng::new(o.seed);
    let end = Instant::now() + Duration::from_secs(o.seconds);
    let aborted: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    while Instant::now() < end {
        sleep_ms(rng.range(300, 1500));
        let down_before: BTreeSet<usize> = (0..c.len()).filter(|i| !c.is_up(*i)).collect();
        let fault = inject(&mut c, o.nodes, &mut rng);
        *report.faults.entry(fault.to_string()).or_default() += 1;
        // Some faults heal almost immediately, which catches recovery races.
        let hold = if rng.chance(250) { rng.range(30, 200) } else { rng.range(500, 3000) };
        sleep_ms(hold);
        for (i, st) in c.reap_exited() {
            if !st.contains("signal: 9") {
                aborted.lock().unwrap().push(format!("NODE_ABORTED node={} status={st}", i + 1));
            }
        }
        let down: usize = (0..c.len()).filter(|i| !c.is_up(*i) && !down_before.contains(i)).count();
        starts += down as u64;
        c.heal_all()?;
    }

    // Heal fully while clients still run, proving the cluster recovers.
    c.heal_all()?;
    let healed_total = total.load(Ordering::Relaxed);
    let mut progressed = false;
    let t = Instant::now();
    while t.elapsed() < Duration::from_secs(30) {
        if total.load(Ordering::Relaxed) >= healed_total + 20 {
            progressed = true;
            break;
        }
        sleep_ms(50);
    }
    if !progressed {
        report.violations.push("LIVENESS writes did not resume after healing".into());
    }

    stop.store(true, Ordering::Relaxed);
    let mut history = Vec::new();
    for h in handles {
        let (ops, retries) = h.join().expect("worker panicked");
        history.extend(ops);
        report.client_retries += retries;
    }

    for (i, st) in c.reap_exited() {
        aborted.lock().unwrap().push(format!("NODE_ABORTED node={} status={st}", i + 1));
    }
    report.violations.extend(aborted.lock().unwrap().iter().cloned());
    c.heal_all()?;

    match c.wait_converged(Duration::from_secs(45)) {
        Ok((commit, _)) => report.final_commit = commit,
        Err(e) => report.violations.push(format!("CONVERGENCE {e}")),
    }

    let keys: BTreeSet<String> = history.iter().map(|h| h.key.clone()).collect();
    let mut reader = Client::new(
        c.client_nodes(),
        ClientOptions { op_deadline: Duration::from_secs(20), ..ClientOptions::default() },
    );
    let mut finals = HashMap::new();
    let mut read_failed = false;
    for k in &keys {
        match reader.get(k.as_bytes()) {
            Ok(v) => {
                finals.insert(k.clone(), v.map(|b| parse_list(&b)).unwrap_or_default());
            }
            Err(e) => {
                report.violations.push(format!("FINAL_READ_FAILED key={k}: {e}"));
                read_failed = true;
                break;
            }
        }
    }
    if !read_failed {
        report.check = check(&history, &finals);
        report.violations.extend(report.check.violations.iter().cloned());
    }

    report.leader_elections = c.count_log("role=leader");
    report.snapshots_taken = c.count_log("event=snapshot_taken");
    report.snapshots_installed = c.count_log("event=snapshot_installed");
    report.torn_tails_repaired = c.count_log("event=wal_torn_tail");
    report.process_starts = starts.max(c.count_log("event=started"));
    Ok(report)
}
