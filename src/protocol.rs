//! Wire formats. Every frame is `[len u32][crc32 u32][payload]`.

use crate::codec::*;
use crate::kv::*;
use crate::raft::*;
use crate::replica::Reply;
use crate::types::*;
use std::io::{self, Read, Write};

pub const MAX_PEER_FRAME: usize = 64 * 1024 * 1024;
pub const MAX_CLIENT_FRAME: usize = 4 * 1024 * 1024;
pub const MAX_KEY: usize = 64 * 1024;
pub const MAX_VALUE: usize = 1024 * 1024;
pub const PEER_MAGIC: u64 = 0x5241_4654_4b56_3031;

pub fn write_frame(w: &mut impl Write, payload: &[u8]) -> io::Result<()> {
    w.write_all(&(payload.len() as u32).to_le_bytes())?;
    w.write_all(&crc32(payload).to_le_bytes())?;
    w.write_all(payload)
}

pub fn read_frame(r: &mut impl Read, max: usize) -> io::Result<Vec<u8>> {
    let mut h = [0u8; 8];
    r.read_exact(&mut h)?;
    let len = u32::from_le_bytes(h[..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(h[4..].try_into().unwrap());
    if len > max {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    if crc32(&buf) != crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum mismatch"));
    }
    Ok(buf)
}

pub fn encode_envelope(env: &Envelope) -> Vec<u8> {
    let mut w = Writer::with_capacity(128);
    w.u64(env.from);
    w.u64(env.to);
    match &env.msg {
        Msg::Vote { pre, force, term, last_index, last_term } => {
            w.u8(1);
            w.bool(*pre);
            w.bool(*force);
            w.u64(*term);
            w.u64(*last_index);
            w.u64(*last_term);
        }
        Msg::VoteResp { pre, term, granted } => {
            w.u8(2);
            w.bool(*pre);
            w.u64(*term);
            w.bool(*granted);
        }
        Msg::Append { term, prev_index, prev_term, entries, commit, read_ctx } => {
            w.u8(3);
            w.u64(*term);
            w.u64(*prev_index);
            w.u64(*prev_term);
            w.u64(*commit);
            w.u64(*read_ctx);
            w.u32(entries.len() as u32);
            for e in entries {
                w.u64(e.term);
                w.u64(e.index);
                w.bytes(&e.data);
            }
        }
        Msg::AppendResp { term, success, match_index, rejected_prev, conflict_index, conflict_term, read_ctx } => {
            w.u8(4);
            w.u64(*term);
            w.bool(*success);
            w.u64(*match_index);
            w.u64(*rejected_prev);
            w.u64(*conflict_index);
            w.u64(*conflict_term);
            w.u64(*read_ctx);
        }
        Msg::Snapshot { term, meta, offset, data, done } => {
            w.u8(5);
            w.u64(*term);
            w.u64(meta.last_index);
            w.u64(meta.last_term);
            w.u64(*offset);
            w.bytes(data);
            w.bool(*done);
        }
        Msg::SnapshotResp { term, last_index, next_offset, done } => {
            w.u8(6);
            w.u64(*term);
            w.u64(*last_index);
            w.u64(*next_offset);
            w.bool(*done);
        }
        Msg::TimeoutNow { term } => {
            w.u8(7);
            w.u64(*term);
        }
    }
    w.buf
}

pub fn decode_envelope(b: &[u8]) -> DResult<Envelope> {
    let mut r = Reader::new(b);
    let from = r.u64()?;
    let to = r.u64()?;
    let msg = match r.u8()? {
        1 => Msg::Vote { pre: r.bool()?, force: r.bool()?, term: r.u64()?, last_index: r.u64()?, last_term: r.u64()? },
        2 => Msg::VoteResp { pre: r.bool()?, term: r.u64()?, granted: r.bool()? },
        3 => {
            let term = r.u64()?;
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let commit = r.u64()?;
            let read_ctx = r.u64()?;
            let n = r.u32()? as usize;
            let mut entries = Vec::with_capacity(n.min(1 << 16));
            for _ in 0..n {
                let term = r.u64()?;
                let index = r.u64()?;
                entries.push(Entry { term, index, data: r.bytes()?.into() });
            }
            Msg::Append { term, prev_index, prev_term, entries, commit, read_ctx }
        }
        4 => Msg::AppendResp {
            term: r.u64()?,
            success: r.bool()?,
            match_index: r.u64()?,
            rejected_prev: r.u64()?,
            conflict_index: r.u64()?,
            conflict_term: r.u64()?,
            read_ctx: r.u64()?,
        },
        5 => Msg::Snapshot {
            term: r.u64()?,
            meta: SnapshotMeta { last_index: r.u64()?, last_term: r.u64()? },
            offset: r.u64()?,
            data: r.vec()?,
            done: r.bool()?,
        },
        6 => Msg::SnapshotResp { term: r.u64()?, last_index: r.u64()?, next_offset: r.u64()?, done: r.bool()? },
        7 => Msg::TimeoutNow { term: r.u64()? },
        _ => return Err(DecodeError("unknown message")),
    };
    r.finish()?;
    Ok(Envelope { from, to, msg })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Register,
    Write { session: u64, seq: u64, op: WriteOp },
    Read { key: Vec<u8> },
    Status { digest: bool },
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatusInfo {
    pub id: NodeId,
    pub role: u8,
    pub term: Term,
    pub leader: Option<NodeId>,
    pub commit: Index,
    pub applied: Index,
    pub first_index: Index,
    pub last_index: Index,
    pub snapshot_index: Index,
    pub keys: u64,
    pub sessions: u64,
    pub digest: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Reply(Reply),
    Invalid(String),
    Status(StatusInfo),
}

pub fn encode_request(id: u64, req: &Request) -> Vec<u8> {
    let mut w = Writer::with_capacity(64);
    w.u64(id);
    match req {
        Request::Register => w.u8(1),
        Request::Write { session, seq, op } => {
            w.u8(2);
            w.u64(*session);
            w.u64(*seq);
            op.encode(&mut w);
        }
        Request::Read { key } => {
            w.u8(3);
            w.bytes(key);
        }
        Request::Status { digest } => {
            w.u8(4);
            w.bool(*digest);
        }
    }
    w.buf
}

pub fn decode_request(b: &[u8]) -> DResult<(u64, Request)> {
    let mut r = Reader::new(b);
    let id = r.u64()?;
    let req = match r.u8()? {
        1 => Request::Register,
        2 => Request::Write { session: r.u64()?, seq: r.u64()?, op: WriteOp::decode(&mut r)? },
        3 => Request::Read { key: r.vec()? },
        4 => Request::Status { digest: r.bool()? },
        _ => return Err(DecodeError("unknown request")),
    };
    r.finish()?;
    Ok((id, req))
}

pub fn encode_response(id: u64, resp: &Response) -> Vec<u8> {
    let mut w = Writer::with_capacity(64);
    w.u64(id);
    match resp {
        Response::Reply(Reply::Applied(a)) => {
            w.u8(1);
            a.encode(&mut w);
        }
        Response::Reply(Reply::Value(v)) => {
            w.u8(2);
            w.opt_bytes(v.as_deref());
        }
        Response::Reply(Reply::NotLeader(h)) => {
            w.u8(3);
            w.opt_u64(*h);
        }
        Response::Reply(Reply::Retry) => w.u8(4),
        Response::Reply(Reply::Busy) => w.u8(5),
        Response::Invalid(m) => {
            w.u8(6);
            w.str(m);
        }
        Response::Status(s) => {
            w.u8(7);
            w.u64(s.id);
            w.u8(s.role);
            w.u64(s.term);
            w.opt_u64(s.leader);
            w.u64(s.commit);
            w.u64(s.applied);
            w.u64(s.first_index);
            w.u64(s.last_index);
            w.u64(s.snapshot_index);
            w.u64(s.keys);
            w.u64(s.sessions);
            w.opt_u64(s.digest);
        }
    }
    w.buf
}

pub fn decode_response(b: &[u8]) -> DResult<(u64, Response)> {
    let mut r = Reader::new(b);
    let id = r.u64()?;
    let resp = match r.u8()? {
        1 => Response::Reply(Reply::Applied(Applied::decode(&mut r)?)),
        2 => Response::Reply(Reply::Value(r.opt_vec()?)),
        3 => Response::Reply(Reply::NotLeader(r.opt_u64()?)),
        4 => Response::Reply(Reply::Retry),
        5 => Response::Reply(Reply::Busy),
        6 => Response::Invalid(r.string()?),
        7 => Response::Status(StatusInfo {
            id: r.u64()?,
            role: r.u8()?,
            term: r.u64()?,
            leader: r.opt_u64()?,
            commit: r.u64()?,
            applied: r.u64()?,
            first_index: r.u64()?,
            last_index: r.u64()?,
            snapshot_index: r.u64()?,
            keys: r.u64()?,
            sessions: r.u64()?,
            digest: r.opt_u64()?,
        }),
        _ => return Err(DecodeError("unknown response")),
    };
    r.finish()?;
    Ok((id, resp))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_roundtrip() {
        let env = Envelope {
            from: 1,
            to: 2,
            msg: Msg::Append {
                term: 3,
                prev_index: 4,
                prev_term: 2,
                entries: vec![Entry::new(3, 5, b"hello".to_vec())],
                commit: 4,
                read_ctx: 9,
            },
        };
        assert_eq!(decode_envelope(&encode_envelope(&env)).unwrap(), env);
    }

    #[test]
    fn frame_detects_corruption() {
        let mut b = Vec::new();
        write_frame(&mut b, b"payload").unwrap();
        let last = b.len() - 1;
        b[last] ^= 1;
        assert!(read_frame(&mut &b[..], 100).is_err());
    }

    #[test]
    fn frame_rejects_oversize() {
        let mut b = Vec::new();
        write_frame(&mut b, &[0u8; 200]).unwrap();
        assert!(read_frame(&mut &b[..], 100).is_err());
    }

    #[test]
    fn decoding_garbage_never_panics() {
        let mut rng = crate::util::Rng::new(7);
        for _ in 0..20_000 {
            let n = rng.below(80);
            let b: Vec<u8> = (0..n).map(|_| rng.next_u64() as u8).collect();
            let _ = decode_envelope(&b);
            let _ = decode_request(&b);
            let _ = decode_response(&b);
        }
    }
}
