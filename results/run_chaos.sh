#!/usr/bin/env bash
# Reproduces results/chaos.jsonl: real processes, SIGKILL, partitions, pauses.
set -u
C=./target/release/raftkv-chaos
O=results/chaos.jsonl
D=${CHAOS_DIR:-/tmp/raftkv-chaos}
: > "$O"
$C --nodes 3 --seconds 60 --runs 12 --seed 1001 --dir "$D" >> "$O" 2>> results/chaos.stderr
$C --nodes 5 --seconds 60 --runs 12 --seed 2001 --dir "$D" >> "$O" 2>> results/chaos.stderr
