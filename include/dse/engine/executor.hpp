#pragma once

#include "dse/types.hpp"
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

    executor(size_t threads, sharded_map* store, wal* log = nullptr);
    ~executor();

    void start();
    void stop();

    void submit(exec_request req);
    bytes execute_sync(const command& cmd);

    stats& get_stats() { return stats_; }
    sharded_map& store() { return *store_; }

private:
    size_t thread_count_;
    sharded_map* store_;
    wal* wal_;
    stats stats_;

    std::vector<std::thread> workers_;
    std::deque<exec_request> queue_;
    std::mutex q_mu_;
    std::condition_variable q_cv_;
    std::atomic<bool> running_{false};

    bytes dispatch(const command& cmd);
    void worker_loop();
};

}  // namespace dse::engine
