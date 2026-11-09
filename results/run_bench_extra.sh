#!/usr/bin/env bash
# Appends the saturation and open-loop runs to results/bench.jsonl.
set -u
B=./target/release/raftkv-bench
O=results/bench.jsonl
for c in 256 512; do
  echo "{\"probe\":\"load\",\"result\":$($B load --nodes 3 --clients $c --seconds 12)}" >> "$O"
done
for r in 5000 10000 20000; do
  echo "{\"probe\":\"load\",\"result\":$($B load --nodes 3 --clients 128 --seconds 12 --rate $r)}" >> "$O"
done
