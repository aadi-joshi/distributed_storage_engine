#pragma once

#include "dse/cluster/hash_ring.hpp"
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace dse::cluster {

struct peer_state {
    std::string id;
    bool alive = true;
    std::chrono::steady_clock::time_point last_seen;
    uint64_t missed_beats = 0;
};

class recovery {
public:
    using failover_cb = std::function<void(const std::string& failed, const std::string& successor)>;

    recovery(hash_ring* ring, failover_cb on_failover);

    void heartbeat(const std::string& node_id);
    void start(int check_interval_ms = 1000, int timeout_ms = 3000, int grace_ms = 15000);
    void stop();

    size_t live_nodes() const;

private:
    hash_ring* ring_;
    failover_cb on_failover_;
    std::unordered_map<std::string, peer_state> peers_;
    mutable std::mutex mu_;
    std::thread monitor_;
    std::atomic<bool> running_{false};
    int check_ms_ = 1000;
    int timeout_ms_ = 3000;
    int grace_ms_ = 15000;
    std::chrono::steady_clock::time_point started_at_{};

    void monitor_loop();
};

}  // namespace dse::cluster
