#!/usr/bin/env bash
# Proves the checks can fail: rebuild with deliberate bugs and see what is caught.
# Requires: cargo build --release --features fault-injection --target-dir target-fi
set -u
SIM=./target-fi/release/raftkv-sim
C=./target-fi/release/raftkv-chaos
S=results/mutations_sim.jsonl
R=results/mutations_chaos.jsonl
D=${CHAOS_DIR:-/tmp/raftkv-mut}
: > "$S"; : > "$R"
BUGS="no_dedup ack_before_commit vote_ignores_log commit_old_term stale_read forget_vote skip_fsync no_truncate snapshot_drops_sessions"
for b in $BUGS; do
  echo "{\"bug\":\"$b\",\"result\":$($SIM --seeds 5000 --bug $b 2>/dev/null)}" >> "$S"
done
for b in $BUGS; do
  for seed in 1 2 3; do
    line=$($C --nodes 3 --seconds 30 --seed $((seed*100)) --bug $b --dir "$D/$b" 2>>results/mutations.stderr)
    echo "{\"bug\":\"$b\",\"result\":$line}" >> "$R"
    rm -rf "$D/$b"
    echo "$line" | grep -q '"passed":false' && break
  done
done
