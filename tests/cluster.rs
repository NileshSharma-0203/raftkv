//! End-to-end tests against real server processes.

use raftkv::client::*;
use raftkv::harness::*;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn dir(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("raftkv-it-{}-{name}", std::process::id()))
}

fn start(nodes: u64, name: &str, tweak: impl FnOnce(&mut ClusterOptions)) -> Cluster {
    std::env::set_var("RAFTKV_SERVER_BIN", env!("CARGO_BIN_EXE_raftkv-server"));
    let mut o = ClusterOptions::new(nodes, dir(name));
    tweak(&mut o);
    let c = Cluster::create(o).unwrap();
    c.wait_for_leader(Duration::from_secs(20)).expect("leader");
    c
}

fn client(c: &Cluster) -> Client {
    Client::new(c.client_nodes(), ClientOptions { op_deadline: Duration::from_secs(20), ..Default::default() })
}

fn cleanup(mut c: Cluster) {
    c.shutdown();
    let _ = std::fs::remove_dir_all(c.dir());
}

#[test]
fn basic_operations() {
    let c = start(3, "basic", |_| {});
    let mut k = client(&c);
    k.put(b"a", b"1").unwrap();
    assert_eq!(k.get(b"a").unwrap(), Some(b"1".to_vec()));
    k.append(b"a", b"2").unwrap();
    assert_eq!(k.get(b"a").unwrap(), Some(b"12".to_vec()));
    assert!(k.cas(b"a", Some(b"12"), b"3").unwrap());
    assert!(!k.cas(b"a", Some(b"12"), b"4").unwrap());
    k.delete(b"a").unwrap();
    assert_eq!(k.get(b"a").unwrap(), None);
    cleanup(c);
}

#[test]
fn single_node_cluster_works_and_persists() {
    let mut c = start(1, "single", |_| {});
    client(&c).put(b"k", b"v").unwrap();
    c.kill(0);
    c.start(0).unwrap();
    c.wait_for_leader(Duration::from_secs(20)).unwrap();
    assert_eq!(client(&c).get(b"k").unwrap(), Some(b"v".to_vec()));
    cleanup(c);
}

#[test]
fn acknowledged_write_survives_leader_kill() {
    let mut c = start(3, "kill-leader", |_| {});
    let mut k = client(&c);
    for i in 0..50 {
        k.put(format!("k{i}").as_bytes(), b"v").unwrap();
    }
    let (l, _) = c.leader().unwrap();
    c.kill(l);
    for i in 0..50 {
        assert_eq!(k.get(format!("k{i}").as_bytes()).unwrap(), Some(b"v".to_vec()), "k{i}");
    }
    k.put(b"after", b"v").unwrap();
    cleanup(c);
}

#[test]
fn whole_cluster_restart_keeps_data() {
    let mut c = start(3, "restart-all", |_| {});
    let mut k = client(&c);
    for i in 0..30 {
        k.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes()).unwrap();
    }
    for i in 0..3 {
        c.kill(i);
    }
    for i in 0..3 {
        c.start(i).unwrap();
    }
    let mut k = client(&c);
    for i in 0..30 {
        assert_eq!(k.get(format!("k{i}").as_bytes()).unwrap(), Some(format!("v{i}").into_bytes()));
    }
    cleanup(c);
}

#[test]
fn minority_cannot_commit() {
    let mut c = start(3, "minority", |_| {});
    let (l, _) = c.leader().unwrap();
    let others: Vec<usize> = (0..3).filter(|i| *i != l).collect();
    c.kill(others[0]);
    c.kill(others[1]);
    let mut k =
        Client::new(c.client_nodes(), ClientOptions { op_deadline: Duration::from_secs(2), ..Default::default() });
    assert!(matches!(k.put(b"x", b"y"), Err(ClientError::Indeterminate(_) | ClientError::Failed(_))));
    cleanup(c);
}

#[test]
fn lagging_follower_catches_up_by_snapshot() {
    let mut c = start(3, "snapshot", |o| {
        o.snapshot_threshold = 100;
        o.snapshot_trailing = 5;
    });
    let (l, _) = c.leader().unwrap();
    let victim = (l + 1) % 3;
    c.kill(victim);
    let mut k = client(&c);
    for i in 0..400 {
        k.put(format!("k{}", i % 50).as_bytes(), format!("{i}").as_bytes()).unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    c.start(victim).unwrap();
    c.wait_converged(Duration::from_secs(30)).unwrap();
    assert!(c.count_log("event=snapshot_taken") > 0);
    assert!(c.count_log("event=snapshot_installed") > 0, "expected the follower to need a snapshot");
    cleanup(c);
}

#[test]
fn graceful_shutdown_hands_off_leadership_quickly() {
    let mut c = start(3, "graceful", |_| {});
    let mut k = client(&c);
    k.put(b"a", b"1").unwrap();
    let (l, term) = c.leader().unwrap();
    let t = Instant::now();
    assert!(c.stop_gracefully(l), "clean exit");
    let (_, new_term) = c.wait_for_leader(Duration::from_secs(10)).unwrap();
    assert!(new_term > term);
    assert!(t.elapsed() < Duration::from_secs(3), "took {:?}", t.elapsed());
    assert_eq!(k.get(b"a").unwrap(), Some(b"1".to_vec()));
    cleanup(c);
}

#[test]
fn oversized_values_are_rejected_without_side_effects() {
    let c = start(1, "oversize", |_| {});
    let mut k = client(&c);
    assert!(matches!(k.put(b"k", &vec![0u8; 2 * 1024 * 1024]), Err(ClientError::Failed(_))));
    assert_eq!(k.get(b"k").unwrap(), None);
    cleanup(c);
}
