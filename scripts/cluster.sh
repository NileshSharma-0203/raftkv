#!/usr/bin/env bash
# Run a local 3-node cluster. Works on macOS and Linux.
#
#   ./scripts/cluster.sh start          start all three nodes (or restart stopped ones)
#   ./scripts/cluster.sh status         show each node's role and progress
#   ./scripts/cluster.sh kill N         SIGKILL node N (1, 2 or 3)
#   ./scripts/cluster.sh stop           shut down cleanly, keeping data
#   ./scripts/cluster.sh wipe           stop and delete all data
#   ./scripts/cluster.sh put K V        also: get K, del K, append K V, cas K OLD NEW
set -u
cd "$(dirname "$0")/.."
BIN=./target/release
DATA=${RAFTKV_DATA:-/tmp/raftkv-local}
NODES="1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103"

ensure_built() {
  if [ ! -x "$BIN/raftkv-server" ]; then
    echo "Not built yet. Run: cargo build --release" >&2
    exit 1
  fi
}

running() {
  local f="$DATA/n$1.pid"
  [ -f "$f" ] && kill -0 "$(cat "$f")" 2>/dev/null
}

start_node() {
  local i=$1 peers="" j
  for j in 1 2 3; do
    if [ "$j" != "$i" ]; then
      peers="$peers${peers:+,}$j=127.0.0.1:700$j"
    fi
  done
  nohup "$BIN/raftkv-server" --id "$i" --data-dir "$DATA/n$i" \
    --peer-listen "127.0.0.1:700$i" --client-listen "127.0.0.1:710$i" \
    --metrics-listen "127.0.0.1:720$i" --peers "$peers" \
    >>"$DATA/n$i.log" 2>&1 &
  echo $! >"$DATA/n$i.pid"
}

stop_all() {
  local i
  for i in 1 2 3; do
    if running "$i"; then
      kill -TERM "$(cat "$DATA/n$i.pid")"
    fi
  done
  for i in 1 2 3; do
    while running "$i"; do sleep 0.1; done
    rm -f "$DATA/n$i.pid"
  done
}

case "${1:-}" in
  start)
    ensure_built
    mkdir -p "$DATA"
    for i in 1 2 3; do
      if running "$i"; then
        echo "node $i already running"
      else
        start_node "$i"
        echo "node $i started"
      fi
    done
    sleep 2
    "$BIN/raftkv-cli" --nodes "$NODES" status
    ;;
  kill)
    if [ -z "${2:-}" ] || ! running "$2"; then
      echo "node ${2:-?} is not running" >&2
      exit 1
    fi
    kill -9 "$(cat "$DATA/n$2.pid")"
    rm -f "$DATA/n$2.pid"
    echo "node $2 killed"
    ;;
  stop)
    stop_all
    echo "stopped; data kept in $DATA"
    ;;
  wipe)
    stop_all
    rm -rf "$DATA"
    echo "stopped and deleted $DATA"
    ;;
  "")
    sed -n '2,10p' "$0"
    ;;
  *)
    ensure_built
    exec "$BIN/raftkv-cli" --nodes "$NODES" "$@"
    ;;
esac
