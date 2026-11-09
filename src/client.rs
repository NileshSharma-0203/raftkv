//! The client library. It retries for as long as its deadline allows, and a
//! retry can never apply a write twice.
//!
//! How: a write keeps the same (session, seq) across every attempt, on every
//! node, so the state machine runs it once. If the deadline passes, the result
//! is `Indeterminate`, because the write may have committed even though no
//! reply arrived. Reporting failure there would be a lie.

use crate::kv::*;
use crate::protocol::*;
use crate::replica::Reply;
use crate::types::*;
use crate::util::{entropy_seed, Rng};
use std::fmt;
use std::io::{self, BufReader, BufWriter, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientError {
    /// The write may or may not have been applied.
    Indeterminate(String),
    /// The operation definitely had no effect (for example a read timed out,
    /// or the request was rejected as invalid).
    Failed(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Indeterminate(m) => write!(f, "outcome unknown: {m}"),
            ClientError::Failed(m) => write!(f, "failed: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Clone, Debug)]
pub struct ClientOptions {
    pub op_deadline: Duration,
    pub rpc_timeout: Duration,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            op_deadline: Duration::from_secs(10),
            rpc_timeout: Duration::from_secs(1),
            backoff_min: Duration::from_millis(5),
            backoff_max: Duration::from_millis(200),
        }
    }
}

struct Conn {
    id: NodeId,
    r: BufReader<TcpStream>,
    w: BufWriter<TcpStream>,
}

pub struct Client {
    nodes: Vec<(NodeId, String)>,
    opts: ClientOptions,
    conn: Option<Conn>,
    target: usize,
    session: Option<u64>,
    seq: u64,
    next_rpc: u64,
    rng: Rng,
    /// Attempts beyond the first, for observability.
    pub retries: u64,
}

enum Attempt {
    Done(Response),
    Redirect(Option<NodeId>),
    Backoff,
}

impl Client {
    pub fn new(nodes: Vec<(NodeId, String)>, opts: ClientOptions) -> Self {
        let mut rng = Rng::new(entropy_seed());
        let target = rng.below(nodes.len());
        Client { nodes, opts, conn: None, target, session: None, seq: 0, next_rpc: 1, rng, retries: 0 }
    }

    pub fn session(&self) -> Option<u64> {
        self.session
    }

    fn connect(&mut self) -> io::Result<()> {
        if self.conn.as_ref().map(|c| c.id == self.nodes[self.target].0).unwrap_or(false) {
            return Ok(());
        }
        self.conn = None;
        let (id, addr) = self.nodes[self.target].clone();
        let sa = addr.to_socket_addrs()?.next().ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no address"))?;
        let s = TcpStream::connect_timeout(&sa, self.opts.rpc_timeout.min(Duration::from_millis(500)))?;
        s.set_nodelay(true)?;
        s.set_read_timeout(Some(self.opts.rpc_timeout))?;
        s.set_write_timeout(Some(self.opts.rpc_timeout))?;
        self.conn = Some(Conn { id, r: BufReader::new(s.try_clone()?), w: BufWriter::new(s) });
        Ok(())
    }

    fn rpc(&mut self, req: &Request) -> io::Result<Response> {
        self.connect()?;
        let id = self.next_rpc;
        self.next_rpc += 1;
        let c = self.conn.as_mut().unwrap();
        write_frame(&mut c.w, &encode_request(id, req))?;
        c.w.flush()?;
        // After a timeout the caller drops this connection, so a late reply
        // can never be mistaken for the answer to a later request.
        let frame = read_frame(&mut c.r, MAX_CLIENT_FRAME)?;
        let (rid, resp) = decode_response(&frame).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if rid != id {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "mismatched response id"));
        }
        Ok(resp)
    }

    fn attempt(&mut self, req: &Request) -> Attempt {
        match self.rpc(req) {
            Ok(Response::Reply(Reply::NotLeader(h))) => Attempt::Redirect(h),
            Ok(Response::Reply(Reply::Retry)) | Ok(Response::Reply(Reply::Busy)) => Attempt::Backoff,
            Ok(r) => Attempt::Done(r),
            Err(_) => {
                self.conn = None;
                Attempt::Redirect(None)
            }
        }
    }

    fn sleep_backoff(&mut self, n: u32) {
        let min = self.opts.backoff_min.as_micros() as u64;
        let max = self.opts.backoff_max.as_micros() as u64;
        let cap = min.saturating_mul(1u64 << n.min(16)).min(max);
        thread::sleep(Duration::from_micros(self.rng.range(min.min(cap), cap)));
    }

    /// Send `req` to whichever node will accept it, until `deadline`.
    fn call(&mut self, req: &Request, deadline: Instant) -> Option<Response> {
        let mut n = 0u32;
        loop {
            match self.attempt(req) {
                Attempt::Done(r) => return Some(r),
                Attempt::Redirect(Some(h)) => {
                    match self.nodes.iter().position(|(id, _)| *id == h) {
                        Some(i) if i != self.target => {
                            // Nodes can briefly point at each other mid-election.
                            if n >= 3 {
                                self.sleep_backoff(n);
                            }
                            self.target = i;
                        }
                        // The node names itself or a peer we cannot map.
                        _ => {
                            self.sleep_backoff(n);
                            self.target = self.rng.below(self.nodes.len());
                        }
                    }
                }
                Attempt::Redirect(None) => {
                    self.sleep_backoff(n);
                    self.target = self.rng.below(self.nodes.len());
                }
                Attempt::Backoff => self.sleep_backoff(n),
            }
            n += 1;
            self.retries += 1;
            if Instant::now() >= deadline {
                return None;
            }
        }
    }

    fn ensure_session(&mut self, deadline: Instant) -> Result<u64, ClientError> {
        if let Some(s) = self.session {
            return Ok(s);
        }
        // Retrying Register is harmless: an extra session is simply never used.
        match self.call(&Request::Register, deadline) {
            Some(Response::Reply(Reply::Applied(Applied::Registered(id)))) => {
                self.session = Some(id);
                self.seq = 0;
                Ok(id)
            }
            Some(other) => Err(ClientError::Failed(format!("unexpected register reply: {other:?}"))),
            None => Err(ClientError::Failed("could not register a session before the deadline".into())),
        }
    }

    /// Apply a write. A retry never applies twice.
    pub fn write(&mut self, op: WriteOp) -> Result<Applied, ClientError> {
        if op.key().is_empty() || op.key().len() > MAX_KEY || op.value_len() > MAX_VALUE {
            return Err(ClientError::Failed(format!(
                "key must be 1..={MAX_KEY} bytes and value at most {MAX_VALUE} bytes"
            )));
        }
        let deadline = Instant::now() + self.opts.op_deadline;
        let session = self.ensure_session(deadline)?;
        self.seq += 1;
        let req = Request::Write { session, seq: self.seq, op };
        match self.call(&req, deadline) {
            Some(Response::Reply(Reply::Applied(Applied::SessionExpired))) => {
                self.session = None;
                Err(ClientError::Indeterminate("session expired while retrying".into()))
            }
            Some(Response::Reply(Reply::Applied(Applied::StaleSeq))) => {
                Err(ClientError::Indeterminate("sequence number is stale".into()))
            }
            Some(Response::Reply(Reply::Applied(Applied::ValueTooLarge))) => {
                Err(ClientError::Failed("value would exceed the maximum stored size".into()))
            }
            Some(Response::Reply(Reply::Applied(a))) => Ok(a),
            Some(Response::Invalid(m)) => Err(ClientError::Failed(m)),
            Some(other) => Err(ClientError::Indeterminate(format!("unexpected reply: {other:?}"))),
            None => Err(ClientError::Indeterminate("deadline exceeded".into())),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.write(WriteOp::Put { key: key.to_vec(), value: value.to_vec() }).map(|_| ())
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) -> Result<(), ClientError> {
        self.write(WriteOp::Append { key: key.to_vec(), value: value.to_vec() }).map(|_| ())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), ClientError> {
        self.write(WriteOp::Delete { key: key.to_vec() }).map(|_| ())
    }

    /// Compare-and-swap. Returns whether the swap happened.
    pub fn cas(&mut self, key: &[u8], expected: Option<&[u8]>, value: &[u8]) -> Result<bool, ClientError> {
        let op = WriteOp::Cas { key: key.to_vec(), expected: expected.map(|e| e.to_vec()), value: value.to_vec() };
        match self.write(op)? {
            Applied::Cas { swapped, .. } => Ok(swapped),
            other => Err(ClientError::Indeterminate(format!("unexpected reply: {other:?}"))),
        }
    }

    /// Linearizable read.
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ClientError> {
        let deadline = Instant::now() + self.opts.op_deadline;
        match self.call(&Request::Read { key: key.to_vec() }, deadline) {
            Some(Response::Reply(Reply::Value(v))) => Ok(v),
            Some(other) => Err(ClientError::Failed(format!("unexpected reply: {other:?}"))),
            None => Err(ClientError::Failed("deadline exceeded".into())),
        }
    }
}

/// Ask one specific node for its status. Used by tooling.
pub fn status(addr: &str, digest: bool, timeout: Duration) -> io::Result<StatusInfo> {
    let sa = addr.to_socket_addrs()?.next().ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no address"))?;
    let s = TcpStream::connect_timeout(&sa, timeout)?;
    s.set_nodelay(true)?;
    s.set_read_timeout(Some(timeout))?;
    s.set_write_timeout(Some(timeout))?;
    let mut w = BufWriter::new(s.try_clone()?);
    let mut r = BufReader::new(s);
    write_frame(&mut w, &encode_request(1, &Request::Status { digest }))?;
    w.flush()?;
    let frame = read_frame(&mut r, MAX_CLIENT_FRAME)?;
    match decode_response(&frame).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))? {
        (_, Response::Status(info)) => Ok(info),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected status reply")),
    }
}
