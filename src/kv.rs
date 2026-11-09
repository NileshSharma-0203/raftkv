//! The replicated state machine: a key-value map plus client sessions.
//!
//! Sessions give exactly-once writes (dissertation §6.3). A client gets a unique
//! session id from the log and numbers its writes. A retried command can land in
//! the log more than once, but `apply` sees the repeated sequence number and
//! returns the saved answer instead of running it again. Every replica reaches
//! the same decision because the decision is made while applying the log.

use crate::codec::*;
use crate::types::*;
use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Append { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Cas { key: Vec<u8>, expected: Option<Vec<u8>>, value: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Command {
    Register,
    Write { session: u64, seq: u64, op: WriteOp },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Applied {
    Ok,
    Cas {
        swapped: bool,
        current: Option<Vec<u8>>,
    },
    Registered(u64),
    SessionExpired,
    StaleSeq,
    /// The append would exceed `MAX_STORED_VALUE`. Nothing changed.
    ValueTooLarge,
}

/// Largest value an append can build up. It must fit in one client reply frame.
pub const MAX_STORED_VALUE: usize = 2 * 1024 * 1024;

impl WriteOp {
    pub(crate) fn encode(&self, w: &mut Writer) {
        match self {
            WriteOp::Put { key, value } => {
                w.u8(1);
                w.bytes(key);
                w.bytes(value);
            }
            WriteOp::Append { key, value } => {
                w.u8(2);
                w.bytes(key);
                w.bytes(value);
            }
            WriteOp::Delete { key } => {
                w.u8(3);
                w.bytes(key);
            }
            WriteOp::Cas { key, expected, value } => {
                w.u8(4);
                w.bytes(key);
                w.opt_bytes(expected.as_deref());
                w.bytes(value);
            }
        }
    }

    pub(crate) fn decode(r: &mut Reader) -> DResult<WriteOp> {
        Ok(match r.u8()? {
            1 => WriteOp::Put { key: r.vec()?, value: r.vec()? },
            2 => WriteOp::Append { key: r.vec()?, value: r.vec()? },
            3 => WriteOp::Delete { key: r.vec()? },
            4 => WriteOp::Cas { key: r.vec()?, expected: r.opt_vec()?, value: r.vec()? },
            _ => return Err(DecodeError("unknown write op")),
        })
    }

    pub fn key(&self) -> &[u8] {
        match self {
            WriteOp::Put { key, .. }
            | WriteOp::Append { key, .. }
            | WriteOp::Delete { key }
            | WriteOp::Cas { key, .. } => key,
        }
    }

    pub fn value_len(&self) -> usize {
        match self {
            WriteOp::Put { value, .. } | WriteOp::Append { value, .. } | WriteOp::Cas { value, .. } => value.len(),
            WriteOp::Delete { .. } => 0,
        }
    }
}

impl Command {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(64);
        match self {
            Command::Register => w.u8(1),
            Command::Write { session, seq, op } => {
                w.u8(2);
                w.u64(*session);
                w.u64(*seq);
                op.encode(&mut w);
            }
        }
        w.buf
    }

    pub fn decode(b: &[u8]) -> DResult<Command> {
        let mut r = Reader::new(b);
        let c = match r.u8()? {
            1 => Command::Register,
            2 => Command::Write { session: r.u64()?, seq: r.u64()?, op: WriteOp::decode(&mut r)? },
            _ => return Err(DecodeError("unknown command")),
        };
        r.finish()?;
        Ok(c)
    }
}

impl Applied {
    pub(crate) fn encode(&self, w: &mut Writer) {
        match self {
            Applied::Ok => w.u8(1),
            Applied::Cas { swapped, current } => {
                w.u8(2);
                w.bool(*swapped);
                w.opt_bytes(current.as_deref());
            }
            Applied::Registered(id) => {
                w.u8(3);
                w.u64(*id);
            }
            Applied::SessionExpired => w.u8(4),
            Applied::StaleSeq => w.u8(5),
            Applied::ValueTooLarge => w.u8(6),
        }
    }

    pub(crate) fn decode(r: &mut Reader) -> DResult<Applied> {
        Ok(match r.u8()? {
            1 => Applied::Ok,
            2 => Applied::Cas { swapped: r.bool()?, current: r.opt_vec()? },
            3 => Applied::Registered(r.u64()?),
            4 => Applied::SessionExpired,
            5 => Applied::StaleSeq,
            6 => Applied::ValueTooLarge,
            _ => return Err(DecodeError("unknown response")),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Session {
    last_seq: u64,
    last_response: Applied,
    last_used: Index,
}

#[derive(Clone)]
pub struct KvState {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
    sessions: HashMap<u64, Session>,
    /// (last_used, session), for deterministic least-recently-used eviction.
    lru: BTreeSet<(Index, u64)>,
    max_sessions: usize,
    no_dedup: bool,
    snapshot_drops_sessions: bool,
}

impl KvState {
    pub fn new(max_sessions: usize, bugs: Bugs) -> Self {
        KvState {
            map: BTreeMap::new(),
            sessions: HashMap::new(),
            lru: BTreeSet::new(),
            max_sessions,
            no_dedup: bugs.no_dedup,
            snapshot_drops_sessions: bugs.snapshot_drops_sessions,
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.map.get(key).cloned()
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn session_count(&self) -> usize {
        self.sessions.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Vec<u8>)> {
        self.map.iter()
    }

    fn touch(&mut self, session: u64, index: Index) {
        if let Some(s) = self.sessions.get_mut(&session) {
            self.lru.remove(&(s.last_used, session));
            s.last_used = index;
            self.lru.insert((index, session));
        }
    }

    fn execute(&mut self, op: WriteOp) -> Applied {
        match op {
            WriteOp::Put { key, value } => {
                self.map.insert(key, value);
                Applied::Ok
            }
            WriteOp::Append { key, value } => {
                let current = self.map.get(&key).map(|v| v.len()).unwrap_or(0);
                if current + value.len() > MAX_STORED_VALUE {
                    return Applied::ValueTooLarge;
                }
                self.map.entry(key).or_default().extend_from_slice(&value);
                Applied::Ok
            }
            WriteOp::Delete { key } => {
                self.map.remove(&key);
                Applied::Ok
            }
            WriteOp::Cas { key, expected, value } => {
                let current = self.map.get(&key).cloned();
                if current == expected {
                    self.map.insert(key, value);
                    Applied::Cas { swapped: true, current: None }
                } else {
                    Applied::Cas { swapped: false, current }
                }
            }
        }
    }

    /// Apply one committed command. Must be deterministic.
    pub fn apply(&mut self, index: Index, cmd: Command) -> Applied {
        match cmd {
            Command::Register => {
                // The log index is unique across the cluster, so it is the id.
                self.sessions.insert(index, Session { last_seq: 0, last_response: Applied::Ok, last_used: index });
                self.lru.insert((index, index));
                while self.sessions.len() > self.max_sessions {
                    let (_, victim) = self.lru.pop_first().expect("lru tracks every session");
                    self.sessions.remove(&victim);
                }
                Applied::Registered(index)
            }
            Command::Write { session, seq, op } => {
                if self.no_dedup {
                    return self.execute(op);
                }
                let (last_seq, cached) = match self.sessions.get(&session) {
                    None => return Applied::SessionExpired,
                    Some(s) => (s.last_seq, s.last_response.clone()),
                };
                if seq < last_seq {
                    return Applied::StaleSeq;
                }
                self.touch(session, index);
                if seq == last_seq {
                    return cached;
                }
                let response = self.execute(op);
                let s = self.sessions.get_mut(&session).unwrap();
                s.last_seq = seq;
                s.last_response = response.clone();
                response
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::with_capacity(1024);
        w.u64(self.map.len() as u64);
        for (k, v) in &self.map {
            w.bytes(k);
            w.bytes(v);
        }
        if self.snapshot_drops_sessions {
            w.u64(0);
            return w.buf;
        }
        let mut ids: Vec<&u64> = self.sessions.keys().collect();
        ids.sort_unstable();
        w.u64(ids.len() as u64);
        for id in ids {
            let s = &self.sessions[id];
            w.u64(*id);
            w.u64(s.last_seq);
            w.u64(s.last_used);
            s.last_response.encode(&mut w);
        }
        w.buf
    }

    pub fn restore(&mut self, b: &[u8]) -> DResult<()> {
        let mut r = Reader::new(b);
        let mut map = BTreeMap::new();
        for _ in 0..r.u64()? {
            let k = r.vec()?;
            let v = r.vec()?;
            map.insert(k, v);
        }
        let mut sessions = HashMap::new();
        let mut lru = BTreeSet::new();
        for _ in 0..r.u64()? {
            let id = r.u64()?;
            let last_seq = r.u64()?;
            let last_used = r.u64()?;
            let last_response = Applied::decode(&mut r)?;
            sessions.insert(id, Session { last_seq, last_response, last_used });
            lru.insert((last_used, id));
        }
        r.finish()?;
        self.map = map;
        self.sessions = sessions;
        self.lru = lru;
        Ok(())
    }

    /// Hash of the complete state, used to prove replicas converged.
    pub fn digest(&self) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for b in self.encode() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        h
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn append(session: u64, seq: u64, v: &str) -> Command {
        Command::Write { session, seq, op: WriteOp::Append { key: b"k".to_vec(), value: v.as_bytes().to_vec() } }
    }

    fn kv() -> KvState {
        KvState::new(100, Bugs::default())
    }

    #[test]
    fn retried_write_applies_once() {
        let mut s = kv();
        let id = match s.apply(1, Command::Register) {
            Applied::Registered(id) => id,
            r => panic!("{r:?}"),
        };
        assert_eq!(s.apply(2, append(id, 1, "a")), Applied::Ok);
        assert_eq!(s.apply(3, append(id, 1, "a")), Applied::Ok);
        assert_eq!(s.get(b"k").unwrap(), b"a");
    }

    #[test]
    fn delayed_older_write_is_rejected() {
        let mut s = kv();
        s.apply(1, Command::Register);
        s.apply(2, append(1, 2, "b"));
        assert_eq!(s.apply(3, append(1, 1, "a")), Applied::StaleSeq);
        assert_eq!(s.get(b"k").unwrap(), b"b");
    }

    #[test]
    fn unknown_session_never_applies() {
        let mut s = kv();
        assert_eq!(s.apply(1, append(42, 1, "a")), Applied::SessionExpired);
        assert_eq!(s.get(b"k"), None);
    }

    #[test]
    fn cas_retry_returns_the_original_answer() {
        let mut s = kv();
        s.apply(1, Command::Register);
        let cas = Command::Write {
            session: 1,
            seq: 1,
            op: WriteOp::Cas { key: b"k".to_vec(), expected: None, value: b"v".to_vec() },
        };
        let first = s.apply(2, cas.clone());
        assert_eq!(first, Applied::Cas { swapped: true, current: None });
        assert_eq!(s.apply(3, cas), first);
    }

    #[test]
    fn eviction_is_least_recently_used() {
        let mut s = KvState::new(2, Bugs::default());
        s.apply(1, Command::Register);
        s.apply(2, Command::Register);
        s.apply(3, append(1, 1, "a"));
        s.apply(4, Command::Register);
        assert_eq!(s.session_count(), 2);
        assert_eq!(s.apply(5, append(2, 1, "x")), Applied::SessionExpired);
        assert_eq!(s.apply(6, append(1, 2, "b")), Applied::Ok);
    }

    #[test]
    fn snapshot_preserves_dedup_table() {
        let mut s = kv();
        s.apply(1, Command::Register);
        s.apply(2, append(1, 1, "a"));
        let mut t = kv();
        t.restore(&s.encode()).unwrap();
        assert_eq!(s.digest(), t.digest());
        t.apply(3, append(1, 1, "a"));
        assert_eq!(t.get(b"k").unwrap(), b"a");
    }

    #[test]
    fn append_cannot_grow_past_the_limit() {
        let mut s = kv();
        s.apply(1, Command::Register);
        let big = vec![b'x'; MAX_STORED_VALUE];
        let op = |seq, value: &[u8]| Command::Write {
            session: 1,
            seq,
            op: WriteOp::Append { key: b"k".to_vec(), value: value.to_vec() },
        };
        assert_eq!(s.apply(2, op(1, &big)), Applied::Ok);
        assert_eq!(s.apply(3, op(2, b"y")), Applied::ValueTooLarge);
        assert_eq!(s.get(b"k").unwrap().len(), MAX_STORED_VALUE);
    }

    #[test]
    fn command_codec_roundtrip() {
        let c = Command::Write {
            session: 9,
            seq: 3,
            op: WriteOp::Cas { key: b"a".to_vec(), expected: Some(b"b".to_vec()), value: b"c".to_vec() },
        };
        assert_eq!(Command::decode(&c.encode()).unwrap(), c);
    }
}
