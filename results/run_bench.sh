#!/usr/bin/env bash
# Reproduces results/bench.jsonl. Run on an otherwise idle machine.
set -u
B=./target/release/raftkv-bench
O=results/bench.jsonl
: > "$O"
echo "{\"probe\":\"fsync\",\"result\":$($B fsync --count 3000)}" >> "$O"
for c in 1 4 16 32 64 128; do
  echo "{\"probe\":\"load\",\"result\":$($B load --nodes 3 --clients $c --seconds 12)}" >> "$O"
done
for c in 32 64; do
  echo "{\"probe\":\"load\",\"result\":$($B load --nodes 5 --clients $c --seconds 12)}" >> "$O"
done
echo "{\"probe\":\"recovery\",\"result\":$($B recovery --nodes 3 --trials 40 --clients 8 --rate 400)}" >> "$O"
echo "{\"probe\":\"recovery\",\"result\":$($B recovery --nodes 5 --trials 40 --clients 8 --rate 400)}" >> "$O"
echo "{\"probe\":\"recovery\",\"result\":$($B recovery --nodes 3 --trials 40 --clients 8 --rate 400 --election-min 15 --election-max 30 --heartbeat 3)}" >> "$O"
