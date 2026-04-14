#pragma once

#include "dse/types.hpp"
#include "dse/cluster/partition_router.hpp"
#include "dse/cluster/replication.hpp"
#include "dse/cluster/recovery.hpp"
#include "dse/engine/sharded_map.hpp"
#include "dse/wal.hpp"
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace dse::engine {

struct cluster_hooks {
    cluster::partition_router* router = nullptr;
    cluster::replication* repl = nullptr;
    cluster::recovery* recover = nullptr;
};

struct exec_request {
    command cmd;
    std::function<void(bytes)> on_done;
};

class executor {
public:
    struct stats {
        std::atomic<uint64_t> ops{0};
        std::atomic<uint64_t> reads{0};
        std::atomic<uint64_t> writes{0};
    };

    executor(size_t threads, sharded_map* store, wal* log = nullptr,
             cluster_hooks hooks = {});
    ~executor();

    void start();
    void stop();

    void submit(exec_request req);
    bytes execute_sync(const command& cmd);

    stats& get_stats() { return stats_; }
    sharded_map& store() { return *store_; }

    void set_hooks(cluster_hooks hooks) { hooks_ = hooks; }

private:
    size_t thread_count_;
    sharded_map* store_;
    wal* wal_;
    cluster_hooks hooks_;
    stats stats_;

    std::vector<std::thread> workers_;
    std::deque<exec_request> queue_;
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::atomic<bool> running_{false};

    bytes dispatch(const command& cmd);
    bytes apply_repl(const command& cmd);
    void worker_loop();
};

}  // namespace dse::engine
