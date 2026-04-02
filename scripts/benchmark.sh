#!/usr/bin/env bash
set -eu

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUILD="$ROOT/build"

mkdir -p "$BUILD"
cd "$BUILD"
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j"$(nproc)"

echo "--- throughput ---"
./bench-throughput

echo "--- read compare ---"
./bench-read-compare

echo "--- concurrent clients ---"
./bench-clients 10000
