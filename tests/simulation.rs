//! Randomised simulation, plus proof that it catches real bugs.

use raftkv::sim::*;
use raftkv::types::*;

#[test]
fn correct_implementation_survives_chaos() {
    for seed in 1..=400 {
        let r = run(SimOptions::from_seed(seed, Bugs::default()));
        assert!(r.violations.is_empty(), "seed {seed}: {:?}", r.violations);
    }
}

#[test]
fn simulation_is_deterministic() {
    let summary = |r: SimReport| (r.acked_writes, r.crashes, r.partitions, r.messages, r.final_commit);
    let a = run(SimOptions::from_seed(1234, Bugs::default()));
    let b = run(SimOptions::from_seed(1234, Bugs::default()));
    assert_eq!(summary(a), summary(b));
}

/// Runs seeds until the bug shows up as one of the expected violations.
fn must_detect(bug: &str, expected: &[&str], seeds: u64) {
    let bugs = Bugs::single(bug).unwrap();
    for seed in 1..=seeds {
        let r = run(SimOptions::from_seed(seed, bugs));
        if r.violations.iter().any(|v| expected.iter().any(|e| v.starts_with(e))) {
            return;
        }
    }
    panic!("{bug} was not detected in {seeds} seeds");
}

#[test]
fn detects_missing_deduplication() {
    must_detect("no_dedup", &["DOUBLE_APPLY"], 100);
}

#[test]
fn detects_ack_before_commit() {
    must_detect("ack_before_commit", &["LOST_WRITE"], 100);
}

#[test]
fn detects_broken_election_restriction() {
    must_detect("vote_ignores_log", &["LEADER_COMPLETENESS", "STATE_MACHINE_SAFETY"], 100);
}

#[test]
fn detects_stale_reads() {
    must_detect("stale_read", &["REALTIME_VIOLATION"], 100);
}

#[test]
fn detects_forgotten_votes() {
    must_detect("forget_vote", &["ELECTION_SAFETY", "STATE_MACHINE_SAFETY", "LOG_MATCHING"], 1500);
}

#[test]
fn detects_missing_fsync() {
    must_detect("skip_fsync", &["LEADER_COMPLETENESS", "LOST_WRITE", "STATE_MACHINE_SAFETY"], 100);
}

#[test]
fn detects_missing_truncation() {
    must_detect("no_truncate", &["STATE_MACHINE_SAFETY", "LOG_MATCHING", "LEADER_COMPLETENESS"], 100);
}

#[test]
fn detects_snapshot_without_sessions() {
    must_detect("snapshot_drops_sessions", &["LOST_WRITE", "DIGEST_MISMATCH", "DOUBLE_APPLY", "DIVERGENT_READ"], 100);
}
