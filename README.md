# Distributed Storage Engine

A Redis-style in-memory key-value store I wrote in C++17 to learn how real databases handle concurrency, durability, and distribution. It runs on Linux with an epoll-driven network layer, a sharded execution engine, WAL-based persistence, and a small cluster layer with consistent hashing plus leader-follower replication.

I built this over two weeks in March 2026. The goal was not to replace Redis, but to understand the pieces: how you keep 10k TCP clients alive without one thread per connection, how you get read throughput without a global lock, and what happens when a node stops responding.

## Results

Measured on 8 CPU cores inside Docker (`gcc 13`, `-O3 -march=native`, Release build):

| Test | Result |
|------|--------|
| Mixed GET/SET throughput | ~5M ops/sec |
| Sharded reads vs single mutex map | 4.6x to 7x faster |
| Concurrent TCP clients connected | 10,000 |

```bash
./build/bench-throughput
./build/bench-read-compare
./build/bench-clients 10000
```

The throughput bench runs 8 worker threads for 3 seconds with a mostly-read workload. The read comparison preloads 100k keys and hammers GET on both a mutex-backed map and the sharded store. The client bench opens 10k connections to a live server and sends PING on each.

## Architecture

```
Clients (TCP/RESP)
        |
   epoll event loop  ---- accept / read / write (edge-triggered)
        |
   command queue
        |
   worker thread pool (N threads)
        |
   sharded hash map (512 shards, shared_mutex per shard)
        |
   WAL append ---- replication ---- snapshot (every 60s)
        |
   consistent hash ring ---- recovery monitor
```

**Networking.** One thread runs `epoll_wait` and handles all socket I/O. Connections are non-blocking. Reads feed a RESP parser; complete commands go to the executor queue. Responses are buffered and flushed on `EPOLLOUT`.

**Storage.** Keys are hashed into 512 shards. Reads take a shared lock on one shard. Writes take an exclusive lock. This is why read throughput scales better than a single `std::mutex` around one `unordered_map`.

**Durability.** Every SET/DEL appends a binary record to the WAL. On startup the server replays the WAL into memory. A background thread writes length-prefixed snapshot files every 60 seconds.

**Cluster.** Each node sits on a consistent hash ring with 128 virtual nodes. The leader replicates WAL entries to followers after local append. A recovery thread tracks heartbeats; if a peer is silent for 3 seconds it is marked down and its keyspace moves to the next ring successor.

## Supported Commands

```
PING
GET key
SET key value [ttl_ms]
DEL key
EXISTS key
INCR key
DECR key
INFO
```

Protocol is a small subset of Redis RESP (inline and bulk commands).

## Build

Linux only. epoll is required.

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

Docker (what I used for benchmarks):

```bash
docker build -t dse .
docker run --rm --cpus=8 dse bash scripts/benchmark.sh
```

## Run

Single node:

```bash
./build/dse-server --port 6379 --threads 8 --data ./data
```

Flags: `--bind`, `--port`, `--threads`, `--data`, `--node`.

Local 3-node cluster:

```bash
bash scripts/run_cluster.sh
```

Connect with `nc` or `redis-cli` (partial command support):

```bash
echo "SET foo bar" | nc localhost 6379
echo "GET foo" | nc localhost 6379
```

## Project Layout

```
include/dse/
  types.hpp store.hpp wal.hpp snapshot.hpp resp.hpp
  net/          epoll loop, tcp server
  engine/       sharded map, thread pool executor
  cluster/      hash ring, replication, recovery
src/            implementations + main.cpp
bench/          throughput, read comparison, client load
scripts/        benchmark.sh, run_cluster.sh
```

## What I Would Do Next

- Add RDB-style incremental snapshots instead of full dumps each time
- Proper TCP replication stream instead of the current stub sender
- Raft or at least quorum commit before acknowledging writes
- Memory limits and eviction policy (LRU)
- TLS and AUTH

## Requirements

- Linux with epoll
- g++ 11+ or clang 14+
- cmake 3.16+
- pthreads
