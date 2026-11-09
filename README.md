# raftkv

A multi-node key-value database in Rust that stays correct when nodes crash or the
network splits. It implements Raft from scratch, using only the standard library.

- Leader election with PreVote and CheckQuorum, log replication, chunked snapshots
- Crash-safe segmented write-ahead log with checksums and torn-write recovery
- A client that retries safely: writes apply exactly once through client sessions
- Linearizable reads with ReadIndex
- Two layers of failure testing: a deterministic simulator and a real-process chaos harness

## Results

Measured on one 2-vCPU cloud VM, with all nodes and clients sharing it over loopback.
Full tables and caveats are in [docs/RESULTS.md](docs/RESULTS.md).

| Measure | Result |
|---|---|
| Peak throughput, 3 nodes | 36,293 writes/sec (p99 23.4 ms, 512 clients) |
| Peak throughput, 5 nodes | 16,684 writes/sec (p99 6.6 ms, 64 clients; highest load tested) |
| p99 write latency at a fixed 10,000 writes/sec, 3 nodes | 5.2 ms |
| Leader failure to first acknowledged write, 3 nodes | 532 ms median, 887 ms worst of 40 |
| Same, with 150–300 ms election timeouts | 287 ms median, 437 ms worst of 40 |
| Chaos runs on real processes | 24 of 24 passed, 630 faults, 3.78M acknowledged writes, 0 lost, 0 applied twice |
| Simulated failure runs | 100,000 seeds, 2.87M crashes, 0 violations |

Nine classic Raft bugs were injected on purpose to confirm the tests can fail. The
results, including what each layer missed, are in the same report.

## Quick start

Requires Rust 1.89 or newer. There are no other dependencies.

```bash
cargo build --release
cargo test --release
./target/release/raftkv-sim --seeds 20000
./target/release/raftkv-chaos --nodes 3 --seconds 60
```

To run a cluster by hand, see Step 11 of the [build guide](docs/BUILD_GUIDE.md).

## How it fits together

```text
client ── retries with (session, seq) ──> leader
                                            │
  event loop: handle batch → fsync WAL → send to peers → apply committed → reply
                                            │
                     followers: fsync, then acknowledge
```

| Path | Contents |
|---|---|
| `src/raft.rs` | Consensus algorithm, with no I/O |
| `src/storage/` | Storage trait, on-disk WAL, fault-injecting simulated disk |
| `src/kv.rs` | State machine and exactly-once client sessions |
| `src/replica.rs` | Persist-before-send ordering, shared by server and simulator |
| `src/server.rs`, `transport.rs`, `client.rs` | The networked system |
| `src/checker.rs` | Linearizability and lost-write checker |
| `src/sim.rs` | Deterministic simulator |
| `src/chaos.rs`, `harness.rs` | Real-process chaos harness |
| `deploy/` | Dockerfile, Compose, systemd, Prometheus alerts |

To build on another storage engine such as NucleusDB, implement the `Storage` trait
in `src/storage/mod.rs`.

## Documentation

- [Step-by-step build guide](docs/BUILD_GUIDE.md)
- [Results and caveats](docs/RESULTS.md)
- [Operations runbook and known limits](docs/OPERATIONS.md)

## Status

This is well tested for correctness, but it has not run in production. It has no TLS or
authentication and no online membership changes, so keep it on a private network. The
Docker, Compose and GitHub Actions files have not been executed yet.
