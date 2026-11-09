//! raftkv: a Raft-replicated key-value store.
//!
//! Layout, bottom to top:
//! - `codec`: binary encoding and CRC32
//! - `storage`: the `Storage` trait, the on-disk WAL, and a fault-injecting
//!   in-memory disk for the simulator
//! - `raft`: the consensus algorithm, with no I/O of its own
//! - `kv`: the replicated state machine, including client sessions
//! - `replica`: glues `raft` and `kv` together; shared by server and simulator
//! - `protocol`, `transport`, `server`, `client`: the real networked system
//! - `checker`, `sim`, `harness`, `chaos`: the verification tooling

pub mod chaos;
pub mod checker;
pub mod client;
pub mod codec;
pub mod harness;
pub mod kv;
pub mod logging;
pub mod protocol;
pub mod raft;
pub mod replica;
pub mod server;
pub mod sim;
pub mod storage;
pub mod transport;
pub mod types;
pub mod util;
