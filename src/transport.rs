//! Peer-to-peer TCP. Each direction of each pair gets its own connection.
//!
//! Sending never blocks the consensus loop: if a peer's queue is full the
//! message is dropped. Raft already handles lost messages by retrying.

use crate::protocol::*;
use crate::raft::Envelope;
use crate::types::*;
use crate::warn_log;
use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::time::Duration;

const QUEUE: usize = 4096;
const WRITE_BATCH: usize = 256;

pub struct PeerSender {
    queues: HashMap<NodeId, SyncSender<Envelope>>,
}

impl PeerSender {
    pub fn start(self_id: NodeId, cluster_id: u64, peers: &[(NodeId, String)]) -> Self {
        let mut queues = HashMap::new();
        for (id, addr) in peers {
            let (tx, rx) = sync_channel(QUEUE);
            queues.insert(*id, tx);
            let addr = addr.clone();
            let id = *id;
            thread::Builder::new()
                .name(format!("peer-{id}"))
                .spawn(move || peer_loop(self_id, cluster_id, addr, rx))
                .expect("spawn peer thread");
        }
        PeerSender { queues }
    }

    pub fn send(&self, env: Envelope) {
        if let Some(q) = self.queues.get(&env.to) {
            let _ = q.try_send(env);
        }
    }
}

fn connect(addr: &str) -> Option<TcpStream> {
    let target = addr.to_socket_addrs().ok()?.next()?;
    let s = TcpStream::connect_timeout(&target, Duration::from_millis(300)).ok()?;
    s.set_nodelay(true).ok()?;
    s.set_write_timeout(Some(Duration::from_secs(1))).ok()?;
    Some(s)
}

fn peer_loop(self_id: NodeId, cluster_id: u64, addr: String, rx: Receiver<Envelope>) {
    loop {
        let Some(stream) = connect(&addr) else {
            // Anything queued is stale by the time we reconnect.
            while rx.try_recv().is_ok() {}
            thread::sleep(Duration::from_millis(30));
            continue;
        };
        let mut w = BufWriter::with_capacity(1 << 18, stream);
        let mut hello = crate::codec::Writer::new();
        hello.u64(PEER_MAGIC);
        hello.u64(cluster_id);
        hello.u64(self_id);
        if write_frame(&mut w, &hello.buf).and_then(|_| w.flush()).is_err() {
            continue;
        }
        'conn: loop {
            let first = match rx.recv_timeout(Duration::from_secs(5)) {
                Ok(e) => e,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => return,
            };
            if write_frame(&mut w, &encode_envelope(&first)).is_err() {
                break 'conn;
            }
            for _ in 0..WRITE_BATCH {
                match rx.try_recv() {
                    Ok(e) => {
                        if write_frame(&mut w, &encode_envelope(&e)).is_err() {
                            break 'conn;
                        }
                    }
                    Err(_) => break,
                }
            }
            if w.flush().is_err() {
                break 'conn;
            }
        }
    }
}

/// Accept inbound peer connections and pass decoded messages to `deliver`.
pub fn start_listener(
    listener: TcpListener,
    self_id: NodeId,
    cluster_id: u64,
    voters: Vec<NodeId>,
    deliver: impl Fn(Envelope) -> bool + Send + Sync + Clone + 'static,
) {
    thread::Builder::new()
        .name("peer-accept".into())
        .spawn(move || {
            for conn in listener.incoming() {
                let Ok(stream) = conn else { continue };
                let deliver = deliver.clone();
                let voters = voters.clone();
                let _ = thread::Builder::new().name("peer-recv".into()).spawn(move || {
                    let _ = stream.set_nodelay(true);
                    // A silent peer gets dropped; it reconnects if it is alive.
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
                    let mut r = BufReader::with_capacity(1 << 18, stream);
                    let Ok(hello) = read_frame(&mut r, 64) else { return };
                    let mut h = crate::codec::Reader::new(&hello);
                    let (Ok(magic), Ok(cluster), Ok(from)) = (h.u64(), h.u64(), h.u64()) else { return };
                    if magic != PEER_MAGIC || cluster != cluster_id || !voters.contains(&from) {
                        warn_log!("event=peer_rejected from={from} cluster={cluster}");
                        return;
                    }
                    while let Ok(frame) = read_frame(&mut r, MAX_PEER_FRAME) {
                        match decode_envelope(&frame) {
                            Ok(env) if env.from == from && env.to == self_id => {
                                if !deliver(env) {
                                    return;
                                }
                            }
                            _ => {
                                warn_log!("event=peer_bad_message from={from}");
                                return;
                            }
                        }
                    }
                });
            }
        })
        .expect("spawn accept thread");
}
