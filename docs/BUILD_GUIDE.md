# Building raftkv from scratch, step by step

This guide walks through the build in the order that keeps every step testable.
Each step says what to build, why it matters, how to check it, and what an
interviewer is likely to ask.

Read the Raft paper first, and keep Figure 2 open the whole time:
<https://raft.github.io/raft.pdf>. Most Raft bugs are a line of Figure 2 that was
skipped or "simplified".

**How to use this guide.** The finished code is in this repository. To learn the
most, start an empty crate, do each step yourself, and look at the matching file
only when stuck. Commit after each step, so your Git history shows the progression.

## The build order at a glance

| Step | Build | File | Proof it works |
|---|---|---|---|
| 1 | Binary codec and checksums | `src/codec.rs` | Known CRC vector, garbage input never panics |
| 2 | The storage contract | `src/storage/mod.rs` | Compiles; everything else depends on it |
| 3 | Write-ahead log and recovery | `src/storage/disk.rs` | Torn write at every byte |
| 4 | Leader election | `src/raft.rs` | One vote per term, stale logs refused |
| 5 | Log replication and commit | `src/raft.rs` | Figure 8 directed test |
| 6 | Snapshots | `src/raft.rs`, `disk.rs` | Lagging follower catches up |
| 7 | Exactly-once state machine | `src/kv.rs` | A retried write applies once |
| 8 | Replica: order of operations | `src/replica.rs` | Reviewed against the durability contract |
| 9 | History checker | `src/checker.rs` | Each violation type has a failing example |
| 10 | Deterministic simulator | `src/sim.rs` | Tens of thousands of seeds; injected bugs caught |
| 11 | Server, transport, client | `server.rs`, `transport.rs`, `client.rs` | Real processes pass end-to-end tests |
| 12 | Chaos harness | `src/chaos.rs`, `src/harness.rs` | No acknowledged write lost |
| 13 | Measurement | `src/bin/bench.rs` | Throughput, p99, recovery time |
| 14 | Production packaging | `deploy/`, `.github/` | Runbook, metrics, CI |

## Step 0: Set up

```bash
rustup toolchain install stable   # needs Rust 1.89 or newer
cargo new raftkv --lib && cd raftkv
git init
```

Two decisions shape everything after this.

**No dependencies.** This project uses only the Rust standard library. Every line
of the system is then yours to explain. In a real product you would likely use
`tokio` and `serde`; the trade-off is worth mentioning in an interview.

**The consensus code does no I/O.** `Raft` never touches a socket, a clock or a
thread. It takes inputs (`tick`, `step`, `propose`) and queues outputs. That one
choice is what makes Step 10 possible. If Raft owns its own sockets and timers,
you can never replay a failure.

## Step 1: Codec and checksums

Build a `Writer` and `Reader` for integers and length-prefixed bytes, and a CRC32.

The rule: **every read is bounds-checked and returns a `Result`.** Disks and
networks hand you bad bytes, and a corrupt length prefix must not crash the node
or allocate 4 GB.

```bash
cargo test --lib codec
```

The CRC of `123456789` must be `0xCBF43926`. The garbage-input test in
`src/protocol.rs` runs 20,000 random buffers through every decoder.

## Step 2: Write down the durability contract

Before writing the log, define what "durable" means (`src/storage/mod.rs`):

- `save_hard_state` and `install_snapshot` are durable when they return.
- `append` and `truncate_from` may be buffered. They become durable only when
  `sync` returns.
- The driver always calls `sync` before anything leaves the process.

The third rule is what makes group commit safe. Many writes can share one fsync,
and no client or peer can learn about a write before that fsync completes.

**This trait is also where NucleusDB plugs in.** Implement `Storage` over your
NucleusDB storage layer and the rest of the system works unchanged. The test suite
in `tests/storage_recovery.rs` shows what the implementation must survive.

> **I/O errors are fatal on purpose.** After a failed `fsync`, Linux may mark the
> dirty pages clean, so a retry can report success while the data is gone. The
> only safe response is to stop the process and recover from the log.

## Step 3: The write-ahead log

Record format: `[payload length][CRC32][term][index][data]`, in numbered segment
files.

Recovery reads every segment and has to tell two situations apart:

| What recovery sees | What it means | What to do |
|---|---|---|
| Bad record at the very end of the last segment | The process died mid-write. That write was never fsynced or acknowledged | Cut it off and continue |
| Bad record with valid records after it | Bit rot in data that may have been acknowledged | **Refuse to start** |
| Bad record in an earlier segment | Same | **Refuse to start** |
| Missing segment, or the `meta` file is gone | Lost history or lost vote | **Refuse to start** |

Refusing to start feels harsh, but quietly dropping an acknowledged entry can break
the guarantee Raft depends on: that any majority contains every committed entry.

Details that are easy to miss:

- **fsync the directory** after creating, renaming or deleting a file. Otherwise
  the file's data can be safe while its name is lost.
- **Replace the `meta` file atomically:** write `meta.tmp`, fsync it, rename it,
  fsync the directory.
- **Delete in the right order.** Compaction removes the oldest segment first, and
  a full reset removes the newest first. A crash part-way through then leaves a
  contiguous log, never one with a hole.

```bash
cargo test --release --test storage_recovery
```

The most important test is `torn_write_at_every_byte_recovers_a_clean_prefix`. It
cuts a real log at every possible byte, recovers, and then proves the log still
accepts writes. `every_single_bit_flip_is_detected` flips each byte in turn and
requires that recovery never returns wrong data.

**Interview question:** *Why isn't a CRC on each record enough?* A CRC tells you a
record is bad. It can't tell you whether that record was ever acknowledged, which
is why position in the file matters.

## Step 4: Leader election

Build Figure 2's RequestVote rules in `src/raft.rs`. In order of how often people
get them wrong:

1. **Persist the term and vote before replying.** Otherwise a node can crash,
   restart, and vote twice in one term.
2. **The election restriction.** Only vote for a candidate whose log is at least as
   up to date: compare last term first, then last index.
3. **Randomised timeouts** (300 to 600 ms here). They keep nodes from splitting the
   vote forever.
4. Any message with a higher term makes the receiver a follower.

Two extras are worth adding:

- **PreVote.** A node asks "would you vote for me?" before raising its term, so a
  node returning from a partition doesn't force a pointless election.
- **CheckQuorum.** A leader that can't reach a majority steps down, so clients stop
  waiting on it.

```bash
cargo test --release --test raft_protocol
```

## Step 5: Log replication and the commit rule

Implement AppendEntries. The follower side is mostly careful bookkeeping:

- Reject if the entry at `prev_index` doesn't have `prev_term`, and include a hint
  so the leader can skip back a whole term at a time.
- Truncate **only at a real conflict**. A delayed old message must never remove
  newer entries.
- Advance commit to `min(leader_commit, last index this message verified)`. Using
  the follower's own last index is a classic bug, because entries past the verified
  point may still be wrong.

The leader tracks each follower in one of three states: `Probe` (position unknown,
one message at a time), `Replicate` (pipelined), or `Snapshot`.

**The subtlest rule in Raft (§5.4.2):** a leader may only commit an entry from its
*own* term by counting replicas. An older entry can sit on a majority and still be
overwritten; that is Figure 8 of the paper. Each new leader therefore appends a
no-op entry, so everything before it can commit.

`figure8_old_term_entry_needs_a_current_term_entry` tests this directly, and
`figure8_mutation_is_detected` proves the test would notice the mistake. Random
testing did not hit this bug in the simulator, so a directed test is the right tool.

**A bug this project actually had.** The simulator found a follower stuck one entry
behind forever. A lost AppendEntries was only resent once the follower "went
quiet", but heartbeat replies kept arriving, so it never looked quiet. The fix
tracks replication progress separately from liveness. The regression test is
`lost_append_is_retransmitted_despite_heartbeat_replies`. Tell this story in
interviews: it shows you found something by testing rather than by reading.

## Step 6: Snapshots

Without compaction the log grows forever. Three parts:

1. **Taking a snapshot.** Serialise the state machine on the main loop (fast), then
   write the file on a background thread (slow), so heartbeats keep flowing.
2. **Compacting.** Delete whole segments the snapshot covers, but keep a trailing
   window of entries so slightly slow followers don't need a full snapshot.
3. **InstallSnapshot.** Send chunks to a follower that needs entries the leader has
   already discarded. The receiver follows Figure 13: keep the log tail if it holds
   the snapshot's last entry with the same term, otherwise discard the whole log.

**The snapshot must include the client session table.** Leave it out and retried
writes apply twice after a restart. The `snapshot_drops_sessions` mutation proves
the tests catch exactly this.

Crash case to handle: the snapshot file reached disk but the process died before
the old log was reset. Recovery sees a log that doesn't line up with the snapshot,
and the snapshot wins.

## Step 7: Exactly-once writes

Raft alone gives you *at-least-once*. Here's why:

1. The client sends a write to the leader.
2. The leader commits it, then crashes before replying.
3. The client retries on the new leader.
4. The write is now in the log twice.

The fix is in `src/kv.rs` (dissertation §6.3):

- A client registers a **session**. The session id is the log index of the
  registration, so it's unique across the cluster.
- Each write carries `(session, seq)`. **A retry reuses the same pair.**
- When applying, a repeated `seq` returns the saved reply instead of running again.

Duplicates can still appear in the log. They just have no effect, and every replica
reaches the same decision because it's made while applying the log.

Session tables can't grow forever, so old sessions are evicted. Eviction has to be
deterministic (least recently used by log index), and a write on an evicted session
returns `SessionExpired` rather than risk applying twice.

**The honest client outcome.** If the deadline passes with no reply, the write may
or may not have happened. `Client::write` returns `Indeterminate`, not "failed". The
chaos harness treats these separately, and so should real applications.

## Step 8: The order of operations

`Replica::flush` in `src/replica.rs` is short and is the most important function in
the project:

```text
1. Handle a batch of events
2. Persist everything (sync)
3. Release messages to peers
4. Apply committed entries
5. Reply to clients
```

Move step 3 before step 2, and a follower can acknowledge an entry it then loses in
a crash. This is the point of the durability contract from Step 2.

Linearizable reads use **ReadIndex**: record the commit index, confirm with a
majority that this node is still leader, wait until that index is applied, then
serve the read. Without the confirmation, a partitioned old leader returns stale
data. The `stale_read` mutation shows the checker catches that.

## Step 9: The history checker

A test harness can only find what its checker can see, so build this before the
chaos test. The workload appends **unique tokens** to keys:

```text
key "k0" ends up holding:  c1n1,c2n1,c1n2,c3n1,
```

The final list reveals the exact order the cluster applied the writes. That turns
linearizability checking, which is NP-hard in general, into a single linear pass:

| Violation | Meaning |
|---|---|
| `LOST_WRITE` | An acknowledged token is missing |
| `DOUBLE_APPLY` | A token appears twice |
| `PHANTOM_WRITE` | A token nobody issued appears |
| `DIVERGENT_READ` | A read isn't a prefix of the final list |
| `REALTIME_VIOLATION` | The order contradicts wall-clock order |

Each violation type has a unit test built from a deliberately broken history. This
is the same list-append idea that Jepsen's Elle checker uses.

## Step 10: The deterministic simulator

`src/sim.rs` runs the real `Replica` code against a fake network and a fake disk,
all from one seed:

- **Network:** delay, reorder, drop, duplicate, one-way and partial partitions.
- **Disk:** a crash keeps a random prefix of unsynced writes.
- **Crash timing:** including after state changes but before the fsync.

It checks the Raft safety properties continuously: Election Safety, Log Matching,
Leader Completeness and State Machine Safety. At the end, it runs the client history
through the checker from Step 9.

```bash
cargo run --release --bin raftkv-sim -- --seeds 20000
```

A failure prints its seed, and rerunning that seed reproduces it exactly.

**Why this is needed even with a chaos test:** `kill -9` does not lose data the
kernel has already accepted, because the page cache survives process death. So a
SIGKILL test *cannot* tell whether you call fsync. Only a simulated disk can, and
the `skip_fsync` mutation shows the difference (see `RESULTS.md`).

**Test your tests.** The `Bugs` struct reintroduces nine classic mistakes, and
`tests/simulation.rs` asserts they get caught. A test suite that has never failed
has proven nothing.

**A determinism bug worth knowing about.** Rust's `HashMap` has a random iteration
order per process. Iterating one while handling a leadership change made seeds
replay differently from run to run. Replacing it with `BTreeMap` fixed it, and the
`simulation_is_deterministic` test now guards against it.

## Step 11: Server, transport and client

Now wrap the tested core in real I/O (`src/server.rs`):

```text
TCP peer threads ──┐
TCP client threads ─┼─> channel ─> single consensus loop ─> WAL fsync ─> send
snapshot thread ───┘
```

One thread owns all consensus state, so there are no locks to get wrong, and each
pass through the loop forms a natural group commit.

Details that matter:

- Handle incoming messages **before** timer ticks. After a stall, a waiting
  heartbeat should reset the election timer instead of losing the race to it.
- Peer sends never block the consensus loop. If a queue is full, the message is
  dropped, and Raft retries.
- Every frame carries a CRC, and peers exchange a cluster id at connection time.
- SIGTERM hands leadership to the most caught-up follower before exiting.

Try it by hand:

```bash
cargo build --release
./scripts/cluster.sh start
./scripts/cluster.sh put hello world
./scripts/cluster.sh get hello
./scripts/cluster.sh status
curl -s 127.0.0.1:7201/metrics | grep raftkv_term
```

Stop it with `./scripts/cluster.sh stop`. Then run the end-to-end tests:

```bash
cargo test --release --test cluster
```

## Step 12: The chaos harness

`raftkv-chaos` starts real server processes and runs a fault-injecting TCP proxy on
every directed link between nodes, while clients keep writing.

| Fault | How |
|---|---|
| Kill leader, random node, minority, or all nodes | SIGKILL |
| Long pause (GC or hung VM) | SIGSTOP, later SIGCONT |
| Clean or random partition | Proxy cuts the links |
| Silent packet loss | Proxy discards bytes while connections look healthy |
| One-way failure | Leader can send but hears nothing back |
| Bridge partition | Each node sees a majority, but no two agree on which |
| Slow network | Proxy adds latency |
| Partition plus crash | Both at once |

Clients connect straight to the nodes, so they can reach a cut-off old leader. That
is exactly the case where stale reads and false acknowledgements show up.

At the end, the harness heals everything, requires writes to resume, requires every
replica to reach an identical state hash, and runs the checker.

```bash
cargo build --release
./target/release/raftkv-chaos --nodes 5 --seconds 60 --runs 5 --seed 7
```

**A harness bug this caught.** One run reported a node that could never start:
`Address already in use`, on all 30 attempts. Consensus was fine, and the other two
nodes kept serving. The harness had picked listen ports by asking the OS for a free
ephemeral port and then closing the socket, so the OS could hand the same number to
something else before the node bound it. The fix is to allocate node ports below the
ephemeral range. Real deployments should follow the same rule.

The JSON output includes coverage counts (faults, elections, snapshots installed).
Check them: a pass means little if no snapshots were installed.

## Step 13: Measure

```bash
./target/release/raftkv-bench fsync
./target/release/raftkv-bench load --nodes 3 --clients 64 --seconds 20
./target/release/raftkv-bench load --nodes 3 --clients 32 --seconds 20 --rate 5000
./target/release/raftkv-bench recovery --nodes 3 --trials 40
```

- **Throughput:** closed-loop, ramping up clients until it stops rising.
- **p99 latency:** report it next to the load that produced it. A p99 at peak
  throughput mostly measures queueing. Closed-loop numbers also hide stalls,
  because a stalled client stops sending; `--rate` measures from each operation's
  scheduled start to avoid that.
- **Recovery:** the time from SIGKILL to the first acknowledged write that *began*
  after the old leader was dead. Stating it that way stops replies already in flight
  from flattering the number.

Always report the hardware. See `RESULTS.md` for the numbers from this repository
and their caveats.

## Step 14: Production packaging

| Need | Where |
|---|---|
| Container image | `deploy/Dockerfile` |
| Local 3-node cluster | `deploy/docker-compose.yml` |
| Service on Linux hosts | `deploy/raftkv.service` |
| Metrics and health | `/metrics`, `/healthz`, `/readyz` on `--metrics-listen` |
| Alerts | `deploy/prometheus-alerts.yml` |
| Continuous testing | `.github/workflows/ci.yml` |
| Day-to-day operations | `docs/OPERATIONS.md` |

The `fault-injection` Cargo feature keeps the deliberate bugs out of real builds:
without it, the server ignores `RAFTKV_BUG` entirely.

**What this project does not do yet** is in `docs/OPERATIONS.md`. Knowing and
stating those limits is part of the signal. The big ones are no encryption or
authentication between nodes, and no online membership changes.

## Interview preparation

Be ready to answer these from your own code:

1. Walk through a write from client to acknowledgement. Where exactly is the fsync?
2. Why can't a leader commit an old-term entry by counting replicas?
3. A client times out on a write. Did it happen? What should the client do?
4. How do linearizable reads work without writing to the log?
5. Why does `kill -9` testing not prove that fsync works?
6. What happens to a leader on the minority side of a partition?
7. What happens if a node restarts with an empty disk but the same id? (It can vote
   twice and break safety. That is why replacing a node needs care.)
8. How do you know your tests can detect a bug? (Mutation testing.)
9. What is the biggest thing you would change? (Membership changes, then TLS.)
