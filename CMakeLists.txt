cmake_minimum_required(VERSION 3.16)
project(distributed_storage_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(DSE_BUILD_BENCH "build benchmarks" ON)

add_library(dse
    src/store.cpp
    src/wal.cpp
    src/snapshot.cpp
    src/resp.cpp
    src/net/epoll_loop.cpp
    src/net/tcp_server.cpp
    src/engine/sharded_map.cpp
    src/engine/executor.cpp
    src/cluster/hash_ring.cpp
    src/cluster/replication.cpp
    src/cluster/recovery.cpp
    src/cluster/peer_client.cpp
    src/cluster/partition_router.cpp
)

target_include_directories(dse PUBLIC include)
target_compile_options(dse PRIVATE -Wall -Wextra -O3 -march=native)

if(UNIX AND NOT APPLE)
    target_compile_definitions(dse PRIVATE DSE_USE_EPOLL=1)
    target_link_libraries(dse PUBLIC pthread)
endif()

add_executable(dse-server src/main.cpp)
target_link_libraries(dse-server PRIVATE dse)

if(DSE_BUILD_BENCH)
    add_executable(bench-throughput bench/bench_throughput.cpp)
    target_link_libraries(bench-throughput PRIVATE dse pthread)

    add_executable(bench-read-compare bench/bench_read_compare.cpp)
    target_link_libraries(bench-read-compare PRIVATE dse pthread)

    add_executable(bench-clients bench/bench_concurrent_clients.cpp)
    target_link_libraries(bench-clients PRIVATE dse pthread)

    add_executable(cluster-smoke bench/cluster_smoke.cpp)
    target_link_libraries(cluster-smoke PRIVATE dse pthread)

    add_executable(tcp-ping bench/tcp_ping.cpp)
    target_link_libraries(tcp-ping PRIVATE dse pthread)
endif()
