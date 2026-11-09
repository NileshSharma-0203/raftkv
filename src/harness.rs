//! Runs a real cluster of `raftkv-server` processes on this machine, with an
//! optional fault-injecting TCP proxy on every directed link between nodes.

use crate::client;
use crate::protocol::StatusInfo;
use crate::types::*;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

extern "C" {
    fn kill(pid: i32, sig: i32) -> i32;
}
// These two numbers differ between Linux and macOS.
const SIGSTOP: i32 = if cfg!(target_os = "macos") { 17 } else { 19 };
const SIGCONT: i32 = if cfg!(target_os = "macos") { 19 } else { 18 };
const SIGTERM: i32 = 15;

pub const PASS: u8 = 0;
/// Connections are cut and refused.
pub const RESET: u8 = 1;
/// Bytes vanish while the connection appears healthy. The nastier failure.
pub const BLACKHOLE: u8 = 2;

pub struct Link {
    pub mode: AtomicU8,
    pub delay_ms: AtomicU64,
    target: String,
}

/// Pick a port for a node to listen on, below the kernel's ephemeral range.
///
/// Asking the OS for port 0 and then closing the socket only borrows a number
/// from the ephemeral range. The OS is free to hand it out again, for example
/// to a proxy listener or an outgoing connection, before the node binds it.
/// The node then fails with "address already in use".
fn free_port() -> u16 {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    const LOW: u64 = 15_000;
    const SPAN: u64 = 15_000;
    let base = std::process::id() as u64 * 37;
    loop {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let port = (LOW + (base + n) % SPAN) as u16;
        if TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
}

fn forward(mut inbound: TcpStream, link: Arc<Link>, stop: Arc<AtomicBool>) {
    let _ = inbound.set_read_timeout(Some(Duration::from_millis(10)));
    let _ = inbound.set_nodelay(true);
    let mut out: Option<TcpStream> = None;
    let mut dirty = false;
    let mut buf = vec![0u8; 1 << 16];
    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let mode = link.mode.load(Ordering::Relaxed);
        if mode == RESET {
            return;
        }
        if mode == PASS && dirty {
            // Bytes were lost mid-stream, so this connection cannot be resumed.
            return;
        }
        if mode == PASS && out.is_none() {
            match TcpStream::connect_timeout(&link.target.parse().unwrap(), Duration::from_millis(200)) {
                Ok(s) => {
                    let _ = s.set_nodelay(true);
                    let _ = s.set_write_timeout(Some(Duration::from_secs(2)));
                    out = Some(s);
                }
                Err(_) => return,
            }
        }
        match inbound.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                if link.mode.load(Ordering::Relaxed) != PASS {
                    dirty = true;
                    continue;
                }
                let d = link.delay_ms.load(Ordering::Relaxed);
                if d > 0 {
                    thread::sleep(Duration::from_millis(d));
                }
                // The link can heal between the checks above and this read, so
                // there may be no outbound connection yet. Those bytes are lost.
                let Some(target) = out.as_mut() else {
                    dirty = true;
                    continue;
                };
                if target.write_all(&buf[..n]).is_err() {
                    return;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::WouldBlock || e.kind() == io::ErrorKind::TimedOut => {}
            Err(_) => return,
        }
    }
}

fn start_link(target: String, stop: Arc<AtomicBool>) -> (String, Arc<Link>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let link = Arc::new(Link { mode: AtomicU8::new(PASS), delay_ms: AtomicU64::new(0), target });
    let l = link.clone();
    thread::spawn(move || {
        for c in listener.incoming() {
            if stop.load(Ordering::Relaxed) {
                return;
            }
            let Ok(s) = c else { continue };
            let (l, st) = (l.clone(), stop.clone());
            thread::spawn(move || forward(s, l, st));
        }
    });
    (addr, link)
}

#[derive(Clone, Debug)]
pub struct ClusterOptions {
    pub nodes: u64,
    pub dir: PathBuf,
    pub tick_ms: u64,
    pub election_min: u32,
    pub election_max: u32,
    pub heartbeat: u32,
    pub snapshot_threshold: u64,
    pub snapshot_trailing: u64,
    pub segment_bytes: u64,
    pub with_proxy: bool,
    pub bug: Option<String>,
}

impl ClusterOptions {
    pub fn new(nodes: u64, dir: PathBuf) -> Self {
        ClusterOptions {
            nodes,
            dir,
            tick_ms: 10,
            election_min: 30,
            election_max: 60,
            heartbeat: 5,
            snapshot_threshold: 100_000,
            snapshot_trailing: 5_000,
            segment_bytes: 16 * 1024 * 1024,
            with_proxy: false,
            bug: None,
        }
    }
}

struct Proc {
    id: NodeId,
    peer_port: u16,
    client_addr: String,
    dial: Vec<(NodeId, String)>,
    child: Option<Child>,
    paused: bool,
}

pub struct Cluster {
    o: ClusterOptions,
    procs: Vec<Proc>,
    pub links: HashMap<(NodeId, NodeId), Arc<Link>>,
    stop: Arc<AtomicBool>,
}

fn server_binary() -> PathBuf {
    if let Ok(p) = std::env::var("RAFTKV_SERVER_BIN") {
        return PathBuf::from(p);
    }
    let me = std::env::current_exe().expect("current_exe");
    let dir = me.parent().unwrap();
    let beside = dir.join("raftkv-server");
    if beside.exists() {
        beside
    } else {
        // Integration tests live one level down, in target/<profile>/deps.
        dir.parent().unwrap().join("raftkv-server")
    }
}

impl Cluster {
    pub fn create(o: ClusterOptions) -> io::Result<Self> {
        let _ = fs::remove_dir_all(&o.dir);
        fs::create_dir_all(&o.dir)?;
        let stop = Arc::new(AtomicBool::new(false));
        let ids: Vec<NodeId> = (1..=o.nodes).collect();
        let ports: HashMap<NodeId, u16> = ids.iter().map(|i| (*i, free_port())).collect();
        let mut links = HashMap::new();
        let mut procs = Vec::new();
        for &id in &ids {
            let mut dial = Vec::new();
            for &other in &ids {
                if other == id {
                    continue;
                }
                let direct = format!("127.0.0.1:{}", ports[&other]);
                if o.with_proxy {
                    let (addr, link) = start_link(direct, stop.clone());
                    links.insert((id, other), link);
                    dial.push((other, addr));
                } else {
                    dial.push((other, direct));
                }
            }
            procs.push(Proc {
                id,
                peer_port: ports[&id],
                client_addr: format!("127.0.0.1:{}", free_port()),
                dial,
                child: None,
                paused: false,
            });
        }
        let mut c = Cluster { o, procs, links, stop };
        for i in 0..c.procs.len() {
            c.start(i)?;
        }
        Ok(c)
    }

    pub fn len(&self) -> usize {
        self.procs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.procs.is_empty()
    }

    pub fn node_dir(&self, i: usize) -> PathBuf {
        self.o.dir.join(format!("node{}", self.procs[i].id))
    }

    pub fn log_path(&self, i: usize) -> PathBuf {
        self.o.dir.join(format!("node{}.log", self.procs[i].id))
    }

    pub fn client_nodes(&self) -> Vec<(NodeId, String)> {
        self.procs.iter().map(|p| (p.id, p.client_addr.clone())).collect()
    }

    pub fn is_up(&self, i: usize) -> bool {
        self.procs[i].child.is_some()
    }

    pub fn is_paused(&self, i: usize) -> bool {
        self.procs[i].paused
    }

    pub fn start(&mut self, i: usize) -> io::Result<()> {
        if self.procs[i].child.is_some() {
            return Ok(());
        }
        let dir = self.node_dir(i);
        let log = OpenOptions::new().create(true).append(true).open(self.log_path(i))?;
        let o = &self.o;
        let p = &self.procs[i];
        let peers = p.dial.iter().map(|(id, a)| format!("{id}={a}")).collect::<Vec<_>>().join(",");
        let mut cmd = Command::new(server_binary());
        cmd.arg("--id")
            .arg(p.id.to_string())
            .arg("--data-dir")
            .arg(&dir)
            .arg("--peer-listen")
            .arg(format!("127.0.0.1:{}", p.peer_port))
            .arg("--client-listen")
            .arg(&p.client_addr)
            .arg("--peers")
            .arg(peers)
            .arg("--tick-ms")
            .arg(o.tick_ms.to_string())
            .arg("--election-min")
            .arg(o.election_min.to_string())
            .arg("--election-max")
            .arg(o.election_max.to_string())
            .arg("--heartbeat")
            .arg(o.heartbeat.to_string())
            .arg("--snapshot-threshold")
            .arg(o.snapshot_threshold.to_string())
            .arg("--snapshot-trailing")
            .arg(o.snapshot_trailing.to_string())
            .arg("--segment-bytes")
            .arg(o.segment_bytes.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log));
        if let Some(b) = &o.bug {
            cmd.env("RAFTKV_BUG", b);
        }
        let child = cmd.spawn()?;
        let p = &mut self.procs[i];
        p.child = Some(child);
        p.paused = false;
        Ok(())
    }

    /// SIGKILL: no flush, no goodbye.
    pub fn kill(&mut self, i: usize) {
        if let Some(mut c) = self.procs[i].child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
        self.procs[i].paused = false;
    }

    /// SIGTERM, then wait for a clean exit.
    pub fn stop_gracefully(&mut self, i: usize) -> bool {
        let Some(mut c) = self.procs[i].child.take() else { return false };
        unsafe { kill(c.id() as i32, SIGTERM) };
        let t = Instant::now();
        while t.elapsed() < Duration::from_secs(10) {
            if let Ok(Some(st)) = c.try_wait() {
                return st.success();
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = c.kill();
        let _ = c.wait();
        false
    }

    /// SIGSTOP: models a long GC pause or a hung VM.
    pub fn pause(&mut self, i: usize) {
        if let Some(c) = &self.procs[i].child {
            unsafe { kill(c.id() as i32, SIGSTOP) };
            self.procs[i].paused = true;
        }
    }

    pub fn resume(&mut self, i: usize) {
        if let Some(c) = &self.procs[i].child {
            unsafe { kill(c.id() as i32, SIGCONT) };
        }
        self.procs[i].paused = false;
    }

    /// Reap nodes that exited on their own, e.g. a detected safety violation.
    pub fn reap_exited(&mut self) -> Vec<(usize, String)> {
        let mut out = Vec::new();
        for (i, p) in self.procs.iter_mut().enumerate() {
            if let Some(c) = &mut p.child {
                if let Ok(Some(st)) = c.try_wait() {
                    out.push((i, st.to_string()));
                    p.child = None;
                    p.paused = false;
                }
            }
        }
        out
    }

    pub fn status(&self, i: usize, digest: bool) -> Option<StatusInfo> {
        if !self.is_up(i) || self.is_paused(i) {
            return None;
        }
        client::status(&self.procs[i].client_addr, digest, Duration::from_millis(300)).ok()
    }

    /// The node reporting leadership in the highest term, if any.
    pub fn leader(&self) -> Option<(usize, Term)> {
        let mut best = None;
        for i in 0..self.len() {
            if let Some(s) = self.status(i, false) {
                if s.role == 3 && best.map(|(_, t)| s.term > t).unwrap_or(true) {
                    best = Some((i, s.term));
                }
            }
        }
        best
    }

    pub fn wait_for_leader(&self, timeout: Duration) -> Option<(usize, Term)> {
        let t = Instant::now();
        while t.elapsed() < timeout {
            if let Some(l) = self.leader() {
                return Some(l);
            }
            thread::sleep(Duration::from_millis(5));
        }
        None
    }

    pub fn heal_links(&self) {
        for l in self.links.values() {
            l.mode.store(PASS, Ordering::Relaxed);
            l.delay_ms.store(0, Ordering::Relaxed);
        }
    }

    pub fn set_link(&self, from: NodeId, to: NodeId, mode: u8) {
        if let Some(l) = self.links.get(&(from, to)) {
            l.mode.store(mode, Ordering::Relaxed);
        }
    }

    pub fn delay_all(&self, ms: u64) {
        for l in self.links.values() {
            l.delay_ms.store(ms, Ordering::Relaxed);
        }
    }

    /// Return everything to health: links open, nodes running.
    pub fn heal_all(&mut self) -> io::Result<()> {
        self.heal_links();
        for i in 0..self.len() {
            if self.is_paused(i) {
                self.resume(i);
            }
            self.start(i)?;
        }
        Ok(())
    }

    /// Wait until every node has applied the same index with the same state.
    pub fn wait_converged(&self, timeout: Duration) -> Result<(Index, u64), String> {
        let t = Instant::now();
        let mut last = String::new();
        while t.elapsed() < timeout {
            let all: Vec<Option<StatusInfo>> = (0..self.len()).map(|i| self.status(i, false)).collect();
            if all.iter().all(|s| s.is_some()) {
                let all: Vec<StatusInfo> = all.into_iter().flatten().collect();
                let c = all[0].commit;
                if c > 0 && all.iter().all(|s| s.applied == c && s.commit == c && s.last_index == c) {
                    let digests: Vec<Option<u64>> =
                        (0..self.len()).map(|i| self.status(i, true).and_then(|s| s.digest)).collect();
                    let again: Vec<Option<StatusInfo>> = (0..self.len()).map(|i| self.status(i, false)).collect();
                    let stable = again.iter().all(|s| s.as_ref().map(|s| s.applied == c).unwrap_or(false));
                    if stable && digests.iter().all(|d| d.is_some()) {
                        let first = digests[0].unwrap();
                        if digests.iter().all(|d| *d == Some(first)) {
                            return Ok((c, first));
                        }
                        return Err(format!("DIGEST_MISMATCH at index {c}: {digests:?}"));
                    }
                }
                last = format!(
                    "{:?}",
                    all.iter().map(|s| (s.id, s.role, s.term, s.commit, s.applied, s.last_index)).collect::<Vec<_>>()
                );
            } else {
                last = "some nodes are unreachable".into();
            }
            thread::sleep(Duration::from_millis(50));
        }
        Err(format!("did not converge: {last}"))
    }

    /// Count lines containing `needle` across all node logs.
    pub fn count_log(&self, needle: &str) -> u64 {
        (0..self.len())
            .map(|i| {
                let mut s = String::new();
                if File::open(self.log_path(i)).and_then(|mut f| f.read_to_string(&mut s)).is_ok() {
                    s.lines().filter(|l| l.contains(needle)).count() as u64
                } else {
                    0
                }
            })
            .sum()
    }

    pub fn dir(&self) -> &Path {
        &self.o.dir
    }

    pub fn shutdown(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for i in 0..self.len() {
            if self.is_paused(i) {
                self.resume(i);
            }
            self.kill(i);
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}
