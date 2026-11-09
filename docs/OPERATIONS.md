# Operating raftkv

## Before you run it in production

raftkv has been tested hard for correctness under crashes and partitions. It has
**not** been run in production. These gaps are real:

| Gap | Risk | What to do |
|---|---|---|
| No TLS or authentication | Anyone who can reach a port can read and write data, or pose as a peer | Keep all ports on a private network or a WireGuard/mTLS mesh. Never expose them to the internet |
| No online membership change | The node set is fixed at startup | Replacing a node is an offline procedure (below). Adding nodes requires a new cluster |
| Data is held in memory | The dataset and log tail must fit in RAM | Size hosts for dataset size, plus a second copy during snapshots |
| No per-client rate limits | One client can saturate the cluster | Put limits in the application or a proxy |
| Real power loss not tested | Lost-fsync behaviour is covered by simulation and file-level tests, not real power cuts | Validate on your actual disks before trusting them |
| Tested on one host | The chaos test ran on loopback | Repeat it across real machines |
| Packaging not executed | The Docker, Compose and CI files were written but not run; the build environment had no Docker daemon or CI runner | Run them once and fix anything that surfaces |

## Sizing and placement

- Use **3 nodes** to survive 1 failure, **5 nodes** to survive 2.
- Put each node on its own machine, and ideally its own failure domain.
- Use an SSD with honest fsync. Slow fsync raises latency directly and can cause
  leader churn.
- Disk: allow at least 2× the dataset size, plus `segment-bytes` × a few segments.
- Choose listen ports **outside the kernel's ephemeral range** (`/proc/sys/net/ipv4/ip_local_port_range`,
  usually 32768–60999). Otherwise another process or an outgoing connection can take the
  port first, and the node then can't start.

## Configuration

| Flag | Default | Notes |
|---|---|---|
| `--id` | required | Unique and permanent for this data directory |
| `--cluster-id` | 1 | Use a different value per cluster |
| `--data-dir` | required | Must be on durable storage |
| `--peer-listen` | required | Node-to-node traffic |
| `--client-listen` | required | Client traffic |
| `--metrics-listen` | off | `/metrics`, `/healthz`, `/readyz` |
| `--peers` | required | `ID=HOST:PORT,...` for every *other* node |
| `--tick-ms` | 10 | Length of one tick |
| `--election-min` / `--election-max` | 30 / 60 | In ticks (300–600 ms) |
| `--heartbeat` | 5 | In ticks. Keep well under `election-min` |
| `--snapshot-threshold` | 100000 | Applied entries between snapshots |
| `--snapshot-trailing` | 5000 | Entries kept after a snapshot for slow followers |
| `--segment-bytes` | 16 MiB | WAL segment size |
| `--max-sessions` | 100000 | Oldest client sessions are evicted beyond this |
| `--max-connections` | 4096 | Concurrent client connections |

Limits: keys up to 64 KiB, values up to 1 MiB, and a value built by appends up to
2 MiB.

Across a real network, keep `election-min` at 10× your round-trip time or more.
Raising the election timeout reduces false elections but lengthens recovery.

## Health and monitoring

```bash
raftkv-cli --nodes 1=host1:7100,2=host2:7100,3=host3:7100 status
curl host1:9100/readyz    # 200 once this node knows a leader
curl host1:9100/metrics
```

Alert rules are in `deploy/prometheus-alerts.yml`. The most useful signals:

| Metric | Watch for |
|---|---|
| `raftkv_has_leader` | 0 anywhere for more than a few seconds |
| `raftkv_became_leader_total` | Repeated increases mean instability |
| `raftkv_commit_index` minus `raftkv_applied_index` | A growing gap means a replica is behind |
| `raftkv_wal_fsync_micros_total` / `raftkv_wal_fsync_total` | Mean fsync time; over about 20 ms is trouble |

Logs are `key=value` lines on stderr. Set verbosity with `RAFTKV_LOG=error|warn|info|debug`.

## Routine procedures

### Rolling restart or upgrade

Restart one node at a time. SIGTERM makes a leader hand off first.

1. Stop the node: `systemctl stop raftkv`.
2. Install the new binary and start it.
3. Wait until `status` shows its `applied` value has caught up with the others.
4. Move to the next node.

### Backup

Copy the newest `snap-*.snap` file and every `wal-*.seg` and `meta` file from one
node's data directory while that node is stopped. Test your restore process.

## When things go wrong

### A node refuses to start

Exit code 1 with `event=startup_failed` means recovery found damage that a crash
can't explain. **Do not delete files to make it start.** Follow "Replace a failed
node" while the rest of the cluster serves traffic.

### A node aborted

`event=fatal_io` means a disk write or fsync failed. Check the disk, because
restarting on a bad disk is unsafe.

`event=safety_violation` should never happen. Keep the data directory and logs and
treat it as a critical bug.

### Replace a failed node

Starting a new, empty node under an old id is **unsafe**, because it has forgotten
how it voted and which entries it acknowledged. Make sure the old node can never
return, and only do this while the remaining nodes hold a majority.

The current release has no online procedure for this. The safest options are:

1. Restore the node from a recent copy of its **own** data directory, if one exists.
2. Otherwise, stop writes, take a backup, and build a new cluster with a new
   `--cluster-id`.

### Lost a majority of nodes

The cluster stops accepting writes, by design. Bring the failed nodes back with
their data intact. If they can't return, the data on the survivors may be missing
acknowledged writes. Treat this as a disaster recovery event and restore from backup.
