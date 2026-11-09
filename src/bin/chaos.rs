//! Real-process chaos test.
//!
//! raftkv-chaos [--nodes 3] [--seconds 60] [--clients 8] [--seed 1] [--runs 1]
//!              [--dir /tmp/raftkv-chaos] [--snapshot-threshold 300] [--bug NAME]
//!
//! Prints one JSON line per run. Exits non-zero if any run found a violation.

use raftkv::chaos::*;
use std::path::PathBuf;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let value = |n: &str| args.iter().position(|a| a == n).and_then(|i| args.get(i + 1)).cloned();
    let num = |n: &str, d: u64| value(n).map(|v| v.parse::<u64>().expect("number")).unwrap_or(d);
    let runs = num("--runs", 1);
    let first_seed = num("--seed", 1);
    let dir = PathBuf::from(value("--dir").unwrap_or_else(|| "/tmp/raftkv-chaos".into()));
    let mut failed = false;
    for r in 0..runs {
        let o = ChaosOptions {
            nodes: num("--nodes", 3),
            seconds: num("--seconds", 60),
            clients: num("--clients", 8),
            seed: first_seed + r,
            dir: dir.join(format!("run{}", first_seed + r)),
            snapshot_threshold: num("--snapshot-threshold", 300),
            snapshot_trailing: num("--snapshot-trailing", 20),
            bug: value("--bug"),
        };
        let keep = o.dir.clone();
        let rep = run(o).expect("chaos run failed to start");
        println!("{}", rep.to_json());
        if rep.passed() {
            let _ = std::fs::remove_dir_all(keep);
        } else {
            failed = true;
            eprintln!("violations in run with seed {}; node logs kept in {}", rep.seed, keep.display());
        }
    }
    if failed {
        std::process::exit(1);
    }
}
