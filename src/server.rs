//! A real raftkv node: one single-threaded consensus loop, surrounded by I/O
//! threads that talk to it through a channel.

use crate::kv::*;
use crate::protocol::*;
use crate::raft::{self, Envelope, Role};
use crate::replica::*;
use crate::storage::disk::*;
use crate::storage::Storage;
use crate::transport::*;
use crate::types::*;
use crate::{error_log, info, warn_log};
use std::collections::HashMap;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{channel, sync_channel, RecvTimeoutError, Sender, SyncSender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub id: NodeId,
    pub cluster_id: u64,
    pub data_dir: PathBuf,
    pub peer_listen: String,
    pub client_listen: String,
    pub metrics_listen: Option<String>,
    /// Where to dial each other node.
    pub peers: Vec<(NodeId, String)>,
    pub tick: Duration,
    pub election_min: u32,
    pub election_max: u32,
    pub heartbeat: u32,
    pub snapshot_threshold: u64,
    pub snapshot_trailing: u64,
    pub segment_bytes: u64,
    pub max_sessions: usize,
    pub max_connections: u64,
    pub bugs: Bugs,
}

enum Event {
    Peer(Envelope),
    Client { req: Request, reply: Sender<Response> },
    SnapshotWritten { meta: SnapshotMeta, data: Arc<Vec<u8>> },
}

const EVENT_QUEUE: usize = 65536;
const MAX_EVENTS_PER_PASS: usize = 2048;
const CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(300);

#[derive(Default)]
pub struct Metrics {
    role: AtomicU64,
    term: AtomicU64,
    commit: AtomicU64,
    applied: AtomicU64,
    last_index: AtomicU64,
    snapshot_index: AtomicU64,
    has_leader: AtomicU64,
    writes: AtomicU64,
    reads: AtomicU64,
    elections: AtomicU64,
    became_leader: AtomicU64,
    snapshots_sent: AtomicU64,
    snapshots_installed: AtomicU64,
    fsyncs: AtomicU64,
    fsync_micros: AtomicU64,
    wal_bytes: AtomicU64,
    connections: AtomicU64,
}

impl Metrics {
    fn render(&self) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let rows: [(&str, &str, u64, &str); 16] = [
            ("raftkv_role", "gauge", g(&self.role), "0 follower, 1 pre-candidate, 2 candidate, 3 leader"),
            ("raftkv_term", "gauge", g(&self.term), "Current Raft term"),
            ("raftkv_commit_index", "gauge", g(&self.commit), "Highest committed index"),
            ("raftkv_applied_index", "gauge", g(&self.applied), "Highest applied index"),
            ("raftkv_last_log_index", "gauge", g(&self.last_index), "Last index in the log"),
            ("raftkv_snapshot_index", "gauge", g(&self.snapshot_index), "Index of the latest snapshot"),
            ("raftkv_has_leader", "gauge", g(&self.has_leader), "1 if this node knows a leader"),
            ("raftkv_client_writes_total", "counter", g(&self.writes), "Write requests received"),
            ("raftkv_client_reads_total", "counter", g(&self.reads), "Read requests received"),
            ("raftkv_elections_started_total", "counter", g(&self.elections), "Elections started"),
            ("raftkv_became_leader_total", "counter", g(&self.became_leader), "Times this node won"),
            ("raftkv_snapshots_sent_total", "counter", g(&self.snapshots_sent), "Snapshots sent to followers"),
            (
                "raftkv_snapshots_installed_total",
                "counter",
                g(&self.snapshots_installed),
                "Snapshots installed from leader",
            ),
            ("raftkv_wal_fsync_total", "counter", g(&self.fsyncs), "WAL fsync calls"),
            ("raftkv_wal_fsync_micros_total", "counter", g(&self.fsync_micros), "Time spent in WAL fsync"),
            ("raftkv_wal_bytes_total", "counter", g(&self.wal_bytes), "Bytes written to the WAL"),
        ];
        let mut out = String::with_capacity(2048);
        for (name, kind, value, help) in rows {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n{name} {value}\n"));
        }
        out
    }
}

fn metrics_thread(listener: TcpListener, m: Arc<Metrics>) {
    for conn in listener.incoming() {
        let Ok(mut s) = conn else { continue };
        let _ = s.set_read_timeout(Some(Duration::from_secs(2)));
        let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
        let mut buf = [0u8; 1024];
        let n = s.read(&mut buf).unwrap_or(0);
        let head = String::from_utf8_lossy(&buf[..n]);
        let path = head.split_whitespace().nth(1).unwrap_or("");
        let (status, body) = match path {
            "/metrics" => ("200 OK", m.render()),
            "/healthz" => ("200 OK", "ok\n".to_string()),
            "/readyz" => {
                if m.has_leader.load(Ordering::Relaxed) == 1 {
                    ("200 OK", "ready\n".to_string())
                } else {
                    ("503 Service Unavailable", "no leader\n".to_string())
                }
            }
            _ => ("404 Not Found", "not found\n".to_string()),
        };
        let _ = write!(
            s,
            "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
    }
}

fn validate(req: &Request) -> Option<String> {
    let too_big = |n: usize, max: usize| n > max;
    match req {
        Request::Write { op, .. } => {
            if too_big(op.key().len(), MAX_KEY) || op.key().is_empty() {
                Some(format!("key must be 1..={MAX_KEY} bytes"))
            } else if too_big(op.value_len(), MAX_VALUE) {
                Some(format!("value must be at most {MAX_VALUE} bytes"))
            } else {
                None
            }
        }
        Request::Read { key } if too_big(key.len(), MAX_KEY) => Some(format!("key must be at most {MAX_KEY} bytes")),
        _ => None,
    }
}

fn client_conn(stream: TcpStream, tx: SyncSender<Event>) -> io::Result<()> {
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(CLIENT_IDLE_TIMEOUT))?;
    let mut r = BufReader::with_capacity(1 << 16, stream.try_clone()?);
    let mut w = BufWriter::with_capacity(1 << 16, stream);
    loop {
        let frame = read_frame(&mut r, MAX_CLIENT_FRAME)?;
        let (id, req) = decode_request(&frame).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let resp = if let Some(msg) = validate(&req) {
            Response::Invalid(msg)
        } else {
            let (reply_tx, reply_rx) = channel();
            if tx.send(Event::Client { req, reply: reply_tx }).is_err() {
                return Ok(());
            }
            match reply_rx.recv() {
                Ok(r) => r,
                Err(_) => return Ok(()),
            }
        };
        write_frame(&mut w, &encode_response(id, &resp))?;
        w.flush()?;
    }
}

fn start_client_listener(listener: TcpListener, tx: SyncSender<Event>, m: Arc<Metrics>, max: u64) {
    thread::Builder::new()
        .name("client-accept".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let Ok(stream) = conn else { continue };
                if m.connections.load(Ordering::Relaxed) >= max {
                    warn_log!("event=connection_limit max={max}");
                    continue;
                }
                m.connections.fetch_add(1, Ordering::Relaxed);
                let tx = tx.clone();
                let m = m.clone();
                let spawned = thread::Builder::new().name("client".into()).stack_size(256 * 1024).spawn({
                    let m = m.clone();
                    move || {
                        let _ = client_conn(stream, tx);
                        m.connections.fetch_sub(1, Ordering::Relaxed);
                    }
                });
                if spawned.is_err() {
                    m.connections.fetch_sub(1, Ordering::Relaxed);
                }
            }
        })
        .expect("spawn client accept");
}

/// Run a node until `shutdown` becomes true.
pub fn run(cfg: ServerConfig, shutdown: Arc<AtomicBool>) -> io::Result<()> {
    let store = DiskStorage::open(
        &cfg.data_dir,
        DiskOptions {
            node_id: cfg.id,
            cluster_id: cfg.cluster_id,
            segment_bytes: cfg.segment_bytes,
            skip_fsync: cfg.bugs.skip_fsync,
        },
    )?;
    let hs = store.hard_state();
    info!(
        "event=recovered node={} term={} voted_for={:?} snapshot_index={} first_index={} last_index={}",
        cfg.id,
        hs.term,
        hs.voted_for,
        store.snapshot_meta().last_index,
        store.first_index(),
        store.last_index()
    );

    let mut voters: Vec<NodeId> = cfg.peers.iter().map(|p| p.0).collect();
    voters.push(cfg.id);
    voters.sort_unstable();

    let mut raft_cfg = raft::Config::new(cfg.id, voters.clone());
    raft_cfg.election_min = cfg.election_min;
    raft_cfg.election_max = cfg.election_max;
    raft_cfg.heartbeat = cfg.heartbeat;
    raft_cfg.seed = crate::util::entropy_seed();
    raft_cfg.bugs = cfg.bugs;
    let replica_cfg = ReplicaConfig {
        snapshot_threshold: cfg.snapshot_threshold,
        snapshot_trailing: cfg.snapshot_trailing,
        max_sessions: cfg.max_sessions,
        ..ReplicaConfig::default()
    };
    let mut replica =
        Replica::new(raft_cfg, replica_cfg, store).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let (tx, rx) = sync_channel::<Event>(EVENT_QUEUE);
    let metrics = Arc::new(Metrics::default());

    // Bind everything only after recovery succeeded.
    let peer_listener = TcpListener::bind(&cfg.peer_listen)?;
    let client_listener = TcpListener::bind(&cfg.client_listen)?;
    if let Some(addr) = &cfg.metrics_listen {
        let l = TcpListener::bind(addr)?;
        let m = metrics.clone();
        thread::Builder::new().name("metrics".into()).spawn(move || metrics_thread(l, m))?;
    }

    let peers = PeerSender::start(cfg.id, cfg.cluster_id, &cfg.peers);
    let peer_tx = tx.clone();
    start_listener(peer_listener, cfg.id, cfg.cluster_id, voters, move |env| peer_tx.send(Event::Peer(env)).is_ok());
    start_client_listener(client_listener, tx.clone(), metrics.clone(), cfg.max_connections);
    info!("event=started node={} peer_addr={} client_addr={}", cfg.id, cfg.peer_listen, cfg.client_listen);

    let mut waiting: HashMap<u64, Sender<Response>> = HashMap::new();
    let mut next_token = 1u64;
    let mut last_tick = Instant::now();
    let mut seen_role = Role::Follower;
    let mut seen_term = replica.raft.term();
    let mut seen_snap_installed = 0u64;
    let mut draining_since: Option<Instant> = None;

    loop {
        let wait = cfg.tick.saturating_sub(last_tick.elapsed());
        let first = match rx.recv_timeout(wait) {
            Ok(e) => Some(e),
            Err(RecvTimeoutError::Timeout) => None,
            Err(RecvTimeoutError::Disconnected) => break,
        };
        let mut next_event = first;
        let mut handled = 0;
        while let Some(ev) = next_event.take() {
            match ev {
                Event::Peer(env) => replica.step(env),
                Event::SnapshotWritten { meta, data } => {
                    info!("event=snapshot_taken node={} index={} bytes={}", cfg.id, meta.last_index, data.len());
                    replica.snapshot_done(meta, data);
                }
                Event::Client { req, reply } => match req {
                    Request::Status { digest } => {
                        let s = StatusInfo {
                            id: cfg.id,
                            role: replica.raft.role().as_u8(),
                            term: replica.raft.term(),
                            leader: replica.raft.leader(),
                            commit: replica.raft.commit(),
                            applied: replica.applied(),
                            first_index: replica.raft.store.first_index(),
                            last_index: replica.raft.store.last_index(),
                            snapshot_index: replica.raft.store.snapshot_meta().last_index,
                            keys: replica.kv.len() as u64,
                            sessions: replica.kv.session_count() as u64,
                            digest: digest.then(|| replica.kv.digest()),
                        };
                        let _ = reply.send(Response::Status(s));
                    }
                    Request::Read { key } => {
                        metrics.reads.fetch_add(1, Ordering::Relaxed);
                        let token = next_token;
                        next_token += 1;
                        waiting.insert(token, reply);
                        replica.read(token, key);
                    }
                    Request::Register => {
                        let token = next_token;
                        next_token += 1;
                        waiting.insert(token, reply);
                        replica.submit(token, Command::Register);
                    }
                    Request::Write { session, seq, op } => {
                        metrics.writes.fetch_add(1, Ordering::Relaxed);
                        let token = next_token;
                        next_token += 1;
                        waiting.insert(token, reply);
                        replica.submit(token, Command::Write { session, seq, op });
                    }
                },
            }
            handled += 1;
            if handled < MAX_EVENTS_PER_PASS {
                next_event = rx.try_recv().ok();
            }
        }

        // Messages are handled before ticks: after a stall, a waiting heartbeat
        // should reset the election timer rather than lose the race to it.
        let mut ticks = 0;
        while last_tick.elapsed() >= cfg.tick && ticks < 5 {
            replica.tick();
            last_tick += cfg.tick;
            ticks += 1;
        }
        if last_tick.elapsed() >= cfg.tick {
            last_tick = Instant::now();
        }

        let out = replica.flush();
        if let Some(v) = &replica.raft.safety_violation {
            error_log!("event=safety_violation detail={v:?}");
            std::process::abort();
        }
        for env in out.messages {
            peers.send(env);
        }
        for (token, reply) in out.replies {
            if let Some(tx) = waiting.remove(&token) {
                let _ = tx.send(Response::Reply(reply));
            }
        }

        if replica.raft.role() != seen_role || replica.raft.term() != seen_term {
            seen_role = replica.raft.role();
            seen_term = replica.raft.term();
            info!(
                "event=role_change node={} role={} term={} leader={:?}",
                cfg.id,
                Role::name(seen_role.as_u8()),
                seen_term,
                replica.raft.leader()
            );
        }
        if replica.raft.stats.snapshots_installed != seen_snap_installed {
            seen_snap_installed = replica.raft.stats.snapshots_installed;
            info!("event=snapshot_installed node={} index={}", cfg.id, replica.applied());
        }

        if let Some((meta, data)) = replica.maybe_snapshot() {
            let data = Arc::new(data);
            let dir = cfg.data_dir.clone();
            let tx = tx.clone();
            thread::Builder::new().name("snapshot".into()).spawn(move || {
                if let Err(e) = write_snapshot_file(&dir, meta, &data) {
                    error_log!("event=fatal_io ctx=\"snapshot write\" err={e}");
                    std::process::abort();
                }
                let _ = tx.send(Event::SnapshotWritten { meta, data });
            })?;
        }

        let r = &replica.raft;
        metrics.role.store(r.role().as_u8() as u64, Ordering::Relaxed);
        metrics.term.store(r.term(), Ordering::Relaxed);
        metrics.commit.store(r.commit(), Ordering::Relaxed);
        metrics.applied.store(replica.applied(), Ordering::Relaxed);
        metrics.last_index.store(r.store.last_index(), Ordering::Relaxed);
        metrics.snapshot_index.store(r.store.snapshot_meta().last_index, Ordering::Relaxed);
        metrics.has_leader.store(r.leader().is_some() as u64, Ordering::Relaxed);
        metrics.elections.store(r.stats.elections_started, Ordering::Relaxed);
        metrics.became_leader.store(r.stats.became_leader, Ordering::Relaxed);
        metrics.snapshots_sent.store(r.stats.snapshots_sent, Ordering::Relaxed);
        metrics.snapshots_installed.store(r.stats.snapshots_installed, Ordering::Relaxed);
        metrics.fsyncs.store(r.store.stats.fsyncs, Ordering::Relaxed);
        metrics.fsync_micros.store(r.store.stats.fsync_micros, Ordering::Relaxed);
        metrics.wal_bytes.store(r.store.stats.bytes_written, Ordering::Relaxed);

        if shutdown.load(Ordering::Relaxed) {
            match draining_since {
                None => {
                    info!("event=shutdown_requested node={} leader={}", cfg.id, replica.raft.is_leader());
                    draining_since = Some(Instant::now());
                    if !replica.raft.transfer_leadership() {
                        break;
                    }
                }
                Some(t) => {
                    if !replica.raft.is_leader() || t.elapsed() > Duration::from_secs(2) {
                        break;
                    }
                }
            }
        }
    }
    // Let the final replies and the TimeoutNow leave the process.
    thread::sleep(Duration::from_millis(50));
    info!("event=stopped node={}", cfg.id);
    Ok(())
}
