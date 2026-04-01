#!/usr/bin/env bash
set -euo pipefail

# start 3-node cluster for local testing
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$ROOT/build/dse-server"
DATA="$ROOT/data"

mkdir -p "$DATA"/{node1,node2,node3}

"$BIN" --port 6379 --node node-1 --data "$DATA/node1" &
"$BIN" --port 6380 --node node-2 --data "$DATA/node2" &
"$BIN" --port 6381 --node node-3 --data "$DATA/node3" &

echo "cluster started on 6379 6380 6381"
wait
