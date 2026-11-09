//! Run many seeds through the deterministic simulator.
//!
//! raftkv-sim [--seeds N] [--start S] [--bug NAME] [--stop-on-failure]

use raftkv::sim::*;
use raftkv::types::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let value = |n: &str| args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned();
    let seeds: u64 = value("--seeds").map(|v| v.parse().unwrap()).unwrap_or(1000);
    let start: u64 = value("--start").map(|v| v.parse().unwrap()).unwrap_or(1);
    let stop_on_failure = args.iter().any(|a| a == "--stop-on-failure");
    let bugs = match value("--bug") {
        Some(b) => Bugs::single(&b).expect("unknown bug name"),
        None => Bugs::default(),
    };

    let t = Instant::now();
    let next = Arc::new(AtomicU64::new(start));
    let results = Arc::new(Mutex::new(Vec::<SimReport>::new()));
    let workers = thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
    let handles: Vec<_> = (0..workers)
        .map(|_| {
            let (next, results) = (next.clone(), results.clone());
            thread::spawn(move || loop {
                let seed = next.fetch_add(1, Ordering::Relaxed);
                if seed >= start + seeds {
                    return;
                }
                let r = run(SimOptions::from_seed(seed, bugs));
                let failed = !r.violations.is_empty();
                results.lock().unwrap().push(r);
                if failed && stop_on_failure {
                    next.store(start + seeds, Ordering::Relaxed);
                    return;
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let mut rs = results.lock().unwrap().clone();
    rs.sort_by_key(|r| r.seed);
    let failing: Vec<&SimReport> = rs.iter().filter(|r| !r.violations.is_empty()).collect();
    let sum = |f: fn(&SimReport) -> u64| rs.iter().map(f).sum::<u64>();
    let mut kinds: BTreeMap<String, u64> = BTreeMap::new();
    for r in &failing {
        for v in &r.violations {
            *kinds.entry(v.split(' ').next().unwrap_or("").to_string()).or_default() += 1;
        }
    }
    let kinds_json = kinds.iter().map(|(k, v)| format!("\"{k}\":{v}")).collect::<Vec<_>>().join(",");
    println!(
        "{{\"seeds\":{},\"failing_seeds\":{},\"first_failing_seed\":{},\"violations_by_kind\":{{{}}},\"acked_writes\":{},\"indeterminate_writes\":{},\"ok_reads\":{},\"crashes\":{},\"crashes_before_fsync\":{},\"partitions\":{},\"leaders_elected\":{},\"snapshots_installed\":{},\"log_truncations\":{},\"messages\":{},\"elapsed_seconds\":{:.1}}}",
        rs.len(), failing.len(), failing.first().map(|r| r.seed.to_string()).unwrap_or("null".into()), kinds_json,
        sum(|r| r.acked_writes), sum(|r| r.indeterminate_writes), sum(|r| r.ok_reads), sum(|r| r.crashes),
        sum(|r| r.torn_crashes), sum(|r| r.partitions), sum(|r| r.leaders_elected), sum(|r| r.snapshots_installed),
        sum(|r| r.truncations), sum(|r| r.messages), t.elapsed().as_secs_f64()
    );
    if let Some(r) = failing.first() {
        eprintln!("first failure, seed {}:", r.seed);
        for v in r.violations.iter().take(10) {
            eprintln!("  {v}");
        }
    }
    if !failing.is_empty() {
        std::process::exit(1);
    }
}
