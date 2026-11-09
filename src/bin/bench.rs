//! Benchmarks on a real cluster.
//!
//! raftkv-bench load --nodes 3 --clients 32 --seconds 20 --value-size 128 [--rate OPS]
//! raftkv-bench recovery --nodes 3 --trials 30 --clients 8 --rate 400
//! raftkv-bench fsync
//!
//! Without --rate the load is closed-loop (each client waits for its reply).
//! With --rate it is open-loop and latency is measured from each operation's
//! scheduled start, so queueing during a stall is not hidden.

use raftkv::client::*;
use raftkv::harness::*;
use raftkv::util::*;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

struct Args(Vec<String>);

impl Args {
    fn get(&self, n: &str) -> Option<String> {
        self.0.iter().position(|a| a == n).and_then(|i| self.0.get(i + 1)).cloned()
    }
    fn num(&self, n: &str, d: u64) -> u64 {
        self.get(n).map(|v| v.parse().expect("number")).unwrap_or(d)
    }
}

fn opts() -> ClientOptions {
    ClientOptions {
        op_deadline: Duration::from_secs(10),
        rpc_timeout: Duration::from_millis(1000),
        ..Default::default()
    }
}

fn cluster(a: &Args, name: &str) -> Cluster {
    let dir = PathBuf::from(a.get("--dir").unwrap_or_else(|| "/tmp/raftkv-bench".into())).join(name);
    let mut o = ClusterOptions::new(a.num("--nodes", 3), dir);
    o.election_min = a.num("--election-min", 30) as u32;
    o.election_max = a.num("--election-max", 60) as u32;
    o.heartbeat = a.num("--heartbeat", 5) as u32;
    o.snapshot_threshold = a.num("--snapshot-threshold", 1_000_000);
    let c = Cluster::create(o).expect("start cluster");
    c.wait_for_leader(Duration::from_secs(15)).expect("no leader");
    c
}

fn load(a: &Args) {
    let c = cluster(a, "load");
    let clients = a.num("--clients", 32);
    let seconds = a.num("--seconds", 20);
    let warmup = a.num("--warmup", 3);
    let value = vec![b'x'; a.num("--value-size", 128) as usize];
    let rate = a.num("--rate", 0);
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let measure_from = Duration::from_secs(warmup);
    let measure_to = Duration::from_secs(warmup + seconds);
    let errors = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..clients)
        .map(|id| {
            let (nodes, stop, value, errors) = (c.client_nodes(), stop.clone(), value.clone(), errors.clone());
            thread::spawn(move || {
                let mut cl = Client::new(nodes, opts());
                let mut lat: Vec<u64> = Vec::with_capacity(1 << 18);
                let interval =
                    if rate > 0 { Some(Duration::from_secs_f64(clients as f64 / rate as f64)) } else { None };
                let mut scheduled = Instant::now() + Duration::from_micros(id * 997);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    let started = match interval {
                        Some(iv) => {
                            let now = Instant::now();
                            if scheduled > now {
                                thread::sleep(scheduled - now);
                            }
                            let s = scheduled;
                            scheduled += iv;
                            s
                        }
                        None => Instant::now(),
                    };
                    n += 1;
                    let key = format!("key-{id}-{}", n % 1000);
                    let ok = cl.put(key.as_bytes(), &value).is_ok();
                    let elapsed = t0.elapsed();
                    if elapsed >= measure_from && elapsed < measure_to {
                        if ok {
                            lat.push(started.elapsed().as_micros() as u64);
                        } else {
                            errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                lat
            })
        })
        .collect();
    thread::sleep(measure_to + Duration::from_millis(100));
    stop.store(true, Ordering::Relaxed);
    let mut all: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    let elections = c.count_log("role=leader");
    let mean = all.iter().sum::<u64>() as f64 / all.len().max(1) as f64;
    println!(
        "{{\"nodes\":{},\"clients\":{},\"mode\":\"{}\",\"target_rate\":{},\"value_size\":{},\"seconds\":{},\"ops\":{},\"ops_per_sec\":{:.0},\"mean_us\":{:.0},\"p50_us\":{},\"p90_us\":{},\"p99_us\":{},\"p999_us\":{},\"max_us\":{},\"errors\":{},\"leader_elections\":{}}}",
        a.num("--nodes", 3), clients, if rate > 0 { "open" } else { "closed" }, rate, value.len(), seconds, all.len(),
        all.len() as f64 / seconds as f64, mean, percentile(&all, 0.50), percentile(&all, 0.90), percentile(&all, 0.99),
        percentile(&all, 0.999), all.last().copied().unwrap_or(0), errors.load(Ordering::Relaxed), elections
    );
}

fn recovery(a: &Args) {
    let mut c = cluster(a, "recovery");
    let trials = a.num("--trials", 30);
    let clients = a.num("--clients", 8);
    let rate = a.num("--rate", 400);
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    // Monotonic nanoseconds at which the most recent ack landed, and the first
    // ack observed since the current kill.
    let armed_at = Arc::new(AtomicU64::new(u64::MAX));
    let first_ack = Arc::new(AtomicU64::new(0));
    let handles: Vec<_> = (0..clients)
        .map(|id| {
            let (nodes, stop, armed_at, first_ack) =
                (c.client_nodes(), stop.clone(), armed_at.clone(), first_ack.clone());
            thread::spawn(move || {
                let mut o = opts();
                o.rpc_timeout = Duration::from_millis(500);
                o.backoff_max = Duration::from_millis(50);
                let mut cl = Client::new(nodes, o);
                let iv = Duration::from_secs_f64(clients as f64 / rate as f64);
                let mut n = 0u64;
                while !stop.load(Ordering::Relaxed) {
                    n += 1;
                    let invoked = t0.elapsed().as_nanos() as u64;
                    let ok = cl.put(format!("r-{id}-{}", n % 100).as_bytes(), b"v").is_ok();
                    // Count only writes that began after the old leader was dead.
                    if ok && invoked >= armed_at.load(Ordering::SeqCst) {
                        let now = t0.elapsed().as_nanos() as u64;
                        let _ = first_ack.compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
                    }
                    thread::sleep(iv);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_secs(2));
    let mut election_ms = Vec::new();
    let mut write_ms = Vec::new();
    let mut failed = 0;
    for trial in 0..trials {
        let Some((leader, old_term)) = c.wait_for_leader(Duration::from_secs(10)) else {
            failed += 1;
            continue;
        };
        first_ack.store(0, Ordering::SeqCst);
        let killed_at = Instant::now();
        c.kill(leader);
        let killed_ns = t0.elapsed().as_nanos() as u64;
        armed_at.store(killed_ns, Ordering::SeqCst);

        let mut elected = None;
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            if let Some((_, term)) = c.leader() {
                if term > old_term {
                    elected = Some(killed_at.elapsed());
                    break;
                }
            }
            thread::sleep(Duration::from_millis(1));
        }
        let mut acked = None;
        while Instant::now() < deadline {
            let v = first_ack.load(Ordering::SeqCst);
            if v != 0 {
                acked = Some(Duration::from_nanos(v.saturating_sub(killed_ns)));
                break;
            }
            thread::sleep(Duration::from_millis(1));
        }
        match (elected, acked) {
            (Some(e), Some(w)) => {
                election_ms.push(e.as_micros() as u64);
                write_ms.push(w.as_micros() as u64);
            }
            _ => {
                failed += 1;
                eprintln!("trial {trial}: no recovery within 10s");
            }
        }
        armed_at.store(u64::MAX, Ordering::SeqCst);
        c.start(leader).expect("restart");
        thread::sleep(Duration::from_millis(1500));
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    election_ms.sort_unstable();
    write_ms.sort_unstable();
    let row = |v: &[u64]| {
        format!(
            "{{\"min_ms\":{:.1},\"p50_ms\":{:.1},\"p90_ms\":{:.1},\"p99_ms\":{:.1},\"max_ms\":{:.1},\"mean_ms\":{:.1}}}",
            v.first().copied().unwrap_or(0) as f64 / 1000.0,
            percentile(v, 0.5) as f64 / 1000.0,
            percentile(v, 0.9) as f64 / 1000.0,
            percentile(v, 0.99) as f64 / 1000.0,
            v.last().copied().unwrap_or(0) as f64 / 1000.0,
            v.iter().sum::<u64>() as f64 / v.len().max(1) as f64 / 1000.0
        )
    };
    println!(
        "{{\"nodes\":{},\"trials\":{},\"completed\":{},\"failed\":{},\"election_min_ticks\":{},\"election_max_ticks\":{},\"new_leader\":{},\"first_write_acked\":{}}}",
        a.num("--nodes", 3), trials, election_ms.len(), failed, a.num("--election-min", 30), a.num("--election-max", 60),
        row(&election_ms), row(&write_ms)
    );
}

fn fsync(a: &Args) {
    let dir = PathBuf::from(a.get("--dir").unwrap_or_else(|| "/tmp/raftkv-bench".into()));
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join("fsync-probe");
    let mut f = std::fs::File::create(&p).unwrap();
    let mut v = Vec::new();
    for _ in 0..a.num("--count", 2000) {
        f.write_all(&[7u8; 256]).unwrap();
        let t = Instant::now();
        f.sync_data().unwrap();
        v.push(t.elapsed().as_micros() as u64);
    }
    let _ = std::fs::remove_file(&p);
    v.sort_unstable();
    println!(
        "{{\"fsyncs\":{},\"mean_us\":{:.0},\"p50_us\":{},\"p99_us\":{},\"max_us\":{}}}",
        v.len(),
        v.iter().sum::<u64>() as f64 / v.len() as f64,
        percentile(&v, 0.5),
        percentile(&v, 0.99),
        v.last().unwrap()
    );
}

fn main() {
    let args = Args(std::env::args().skip(1).collect());
    match args.0.first().map(|s| s.as_str()) {
        Some("load") => load(&args),
        Some("recovery") => recovery(&args),
        Some("fsync") => fsync(&args),
        _ => {
            eprintln!("usage: raftkv-bench load|recovery|fsync [options]");
            std::process::exit(2);
        }
    }
}
