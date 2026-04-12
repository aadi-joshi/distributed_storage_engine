#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/build/dse-server"
SMOKE="$ROOT/build/cluster-smoke"
DATA="$ROOT/data/cluster-test"

cleanup() {
    pkill -f "dse-server.*cluster-test" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$DATA"/{node1,node2,node3}

PEERS12="node-2:127.0.0.1:6380,node-3:127.0.0.1:6381"
PEERS23="node-1:127.0.0.1:6379,node-3:127.0.0.1:6381"
PEERS31="node-1:127.0.0.1:6379,node-2:127.0.0.1:6380"

"$BIN" --port 6379 --node node-1 --data "$DATA/node1" --peers "$PEERS12" &
"$BIN" --port 6380 --node node-2 --data "$DATA/node2" --peers "$PEERS23" &
"$BIN" --port 6381 --node node-3 --data "$DATA/node3" --peers "$PEERS31" &

sleep 2
"$SMOKE"
