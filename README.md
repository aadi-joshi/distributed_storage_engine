# Distributed Storage Engine

A Redis-inspired in-memory key-value store I built in C++17. It uses an epoll event loop for TCP I/O, a sharded in-memory store with per-shard reader-writer locks, WAL plus snapshot durability, and a small cluster layer with consistent-hash partitioning, TCP replication, and automatic failover.

I spent about two weeks on this in March/April 2026. The main thing I wanted to learn was how to keep thousands of TCP connections efficient on one thread while still getting good read throughput across CPU cores.

## Results

8 cores, Release build, `-O3 -march=native`, Docker (`gcc 13`):

| Test | Result |
|------|--------|
| Mixed GET/SET throughput | ~5M ops/sec |
| Sharded reads vs mutex map | 4x to 7x faster |
| Concurrent TCP clients | 10,000 |

```bash
./build/bench-throughput
./build/bench-read-compare
./build/bench-clients 10000
```

## Architecture

```
Clients (TCP/RESP)
        |
   epoll event loop
        |
   partition router ---- forward to key owner if remote
        |
   worker thread pool
        |
   sharded map (512 shards, shared_mutex)
        |
   WAL ---- TCP REPL to followers
        |
   consistent hash ring ---- heartbeat / failover
```

**Networking.** Single-threaded epoll loop, edge-triggered, non-blocking sockets. Supports 12k connections.

**Partitioning.** Keys are mapped with a consistent hash ring (128 vnodes per node). Any node can accept a client connection; if the key belongs elsewhere the request is forwarded over TCP to the owner.

**Replication.** The key owner appends to its WAL then sends a `REPL` command to the next nodes on the ring. Followers apply the WAL record locally.

**Failover.** Nodes exchange `HEARTBEAT` messages. If a peer is silent for 3 seconds, recovery marks it down on the ring and remaps its vnodes to the next alive successor.

## Commands

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

Internal cluster commands: `REPL`, `HEARTBEAT` (not for regular clients).

## Build

Linux only.

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

Docker:

```bash
docker build -t dse .
docker run --rm --cpus=8 dse bash scripts/benchmark.sh
```

## Run

Single node:

```bash
./build/dse-server --port 6379 --threads 8 --data ./data
```

3-node cluster:

```bash
bash scripts/run_cluster.sh
```

With explicit peers:

```bash
./build/dse-server --port 6379 --node node-1 --data ./data/node1 \
  --peers node-2:127.0.0.1:6380,node-3:127.0.0.1:6381
```

Cluster smoke test:

```bash
bash scripts/test_cluster.sh
```

## Layout

```
include/dse/
  net/       epoll, tcp server
  engine/    sharded map, executor
  cluster/   hash ring, router, replication, recovery, peer client
src/
bench/
scripts/
```

## What I Would Do Next

- Quorum ack before returning on writes
- Streaming snapshot transfer to new replicas
- LRU eviction under memory pressure

## Requirements

- Linux + epoll
- g++ 11+ or clang 14+
- cmake 3.16+
