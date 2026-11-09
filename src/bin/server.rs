//! A raftkv node.

use raftkv::server::{run, ServerConfig};
use raftkv::types::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

const USAGE: &str = "raftkv-server --id N --data-dir PATH --peer-listen HOST:PORT --client-listen HOST:PORT \\
    --peers ID=HOST:PORT,ID=HOST:PORT [options]

Options (defaults in brackets):
  --cluster-id N            refuse data or peers from another cluster [1]
  --metrics-listen HOST:PORT  serve /metrics, /healthz, /readyz [off]
  --tick-ms N               length of one logical tick [10]
  --election-min N          minimum election timeout, in ticks [30]
  --election-max N          maximum election timeout, in ticks [60]
  --heartbeat N             heartbeat interval, in ticks [5]
  --snapshot-threshold N    applied entries between snapshots [100000]
  --snapshot-trailing N     entries kept after a snapshot for slow followers [5000]
  --segment-bytes N         WAL segment size [16777216]
  --max-sessions N          client sessions retained [100000]
  --max-connections N       client connections accepted [4096]

A single-node cluster passes --peers \"\".";

static SHUTDOWN: AtomicBool = AtomicBool::new(false);

extern "C" {
    fn signal(sig: i32, handler: usize) -> usize;
}

extern "C" fn on_signal(_: i32) {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

fn fail(msg: &str) -> ! {
    eprintln!("error: {msg}\n\n{USAGE}");
    std::process::exit(2);
}

fn parse_peers(s: &str) -> Vec<(NodeId, String)> {
    s.split(',')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (id, addr) = p.split_once('=').unwrap_or_else(|| fail("--peers must be ID=HOST:PORT,..."));
            (id.parse().unwrap_or_else(|_| fail("peer id must be a number")), addr.to_string())
        })
        .collect()
}

fn bugs_from_env() -> Bugs {
    #[cfg(feature = "fault-injection")]
    if let Ok(name) = std::env::var("RAFTKV_BUG") {
        eprintln!("level=WARN event=fault_injection_enabled bug={name}");
        return Bugs::single(&name).unwrap_or_else(|| fail("unknown RAFTKV_BUG"));
    }
    Bugs::default()
}

fn main() {
    raftkv::logging::init_from_env();
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!("{USAGE}");
        return;
    }
    if !args.len().is_multiple_of(2) {
        fail("every flag needs a value");
    }
    let mut flags: HashMap<String, String> = HashMap::new();
    for pair in args.chunks(2) {
        let name = pair[0].strip_prefix("--").unwrap_or_else(|| fail("expected a --flag"));
        flags.insert(name.to_string(), pair[1].clone());
    }
    let required = |k: &str| flags.get(k).cloned().unwrap_or_else(|| fail(&format!("missing --{k}")));
    let number = |k: &str, d: u64| -> u64 {
        flags.get(k).map(|v| v.parse().unwrap_or_else(|_| fail(&format!("--{k} must be a number")))).unwrap_or(d)
    };
    let known = [
        "id",
        "cluster-id",
        "data-dir",
        "peer-listen",
        "client-listen",
        "metrics-listen",
        "peers",
        "tick-ms",
        "election-min",
        "election-max",
        "heartbeat",
        "snapshot-threshold",
        "snapshot-trailing",
        "segment-bytes",
        "max-sessions",
        "max-connections",
    ];
    for k in flags.keys() {
        if !known.contains(&k.as_str()) {
            fail(&format!("unknown flag --{k}"));
        }
    }

    let cfg = ServerConfig {
        id: required("id").parse().unwrap_or_else(|_| fail("--id must be a number")),
        cluster_id: number("cluster-id", 1),
        data_dir: PathBuf::from(required("data-dir")),
        peer_listen: required("peer-listen"),
        client_listen: required("client-listen"),
        metrics_listen: flags.get("metrics-listen").cloned(),
        peers: parse_peers(&required("peers")),
        tick: Duration::from_millis(number("tick-ms", 10)),
        election_min: number("election-min", 30) as u32,
        election_max: number("election-max", 60) as u32,
        heartbeat: number("heartbeat", 5) as u32,
        snapshot_threshold: number("snapshot-threshold", 100_000),
        snapshot_trailing: number("snapshot-trailing", 5_000),
        segment_bytes: number("segment-bytes", 16 * 1024 * 1024),
        max_sessions: number("max-sessions", 100_000) as usize,
        max_connections: number("max-connections", 4096),
        bugs: bugs_from_env(),
    };
    if cfg.election_min <= cfg.heartbeat * 2 || cfg.election_max < cfg.election_min {
        fail("need election-max >= election-min > 2 * heartbeat");
    }
    if cfg.peers.iter().any(|(id, _)| *id == cfg.id) {
        fail("--peers must not include this node");
    }

    unsafe {
        signal(15, on_signal as *const () as usize);
        signal(2, on_signal as *const () as usize);
        // A vanished client must not kill the node.
        signal(13, 1);
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let flag = shutdown.clone();
    std::thread::spawn(move || loop {
        if SHUTDOWN.load(Ordering::Relaxed) {
            flag.store(true, Ordering::Relaxed);
            return;
        }
        std::thread::sleep(Duration::from_millis(20));
    });

    if let Err(e) = run(cfg, shutdown) {
        eprintln!("level=ERROR event=startup_failed err={e}");
        std::process::exit(1);
    }
}
