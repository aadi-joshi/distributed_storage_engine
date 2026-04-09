#include "dse/cluster/recovery.hpp"

namespace dse::cluster {

recovery::recovery(hash_ring* ring, failover_cb on_failover)
    : ring_(ring), on_failover_(std::move(on_failover)) {}

void recovery::heartbeat(const std::string& node_id) {
    std::lock_guard lock(mu_);
    auto& p = peers_[node_id];
    p.id = node_id;
    p.alive = true;
    p.last_seen = std::chrono::steady_clock::now();
    p.missed_beats = 0;
    if (ring_) ring_->mark_up(node_id);
}

void recovery::start(int check_interval_ms, int timeout_ms, int grace_ms) {
    check_ms_ = check_interval_ms;
    timeout_ms_ = timeout_ms;
    grace_ms_ = grace_ms;
    started_at_ = std::chrono::steady_clock::now();
    if (running_.exchange(true)) return;
    monitor_ = std::thread(&recovery::monitor_loop, this);
}

void recovery::stop() {
    if (!running_.exchange(false)) return;
    if (monitor_.joinable()) monitor_.join();
}

size_t recovery::live_nodes() const {
    std::lock_guard lock(mu_);
    size_t n = 0;
    for (const auto& [_, p] : peers_)
        if (p.alive) n++;
    return n;
}

void recovery::monitor_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(check_ms_));
        auto now = std::chrono::steady_clock::now();
        auto since_start = std::chrono::duration_cast<std::chrono::milliseconds>(
            now - started_at_).count();
        if (since_start < grace_ms_) continue;

        std::vector<std::string> failed;

        {
            std::lock_guard lock(mu_);
            for (auto& [id, p] : peers_) {
                auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                    now - p.last_seen).count();
                if (elapsed > timeout_ms_) {
                    if (p.alive) {
                        p.alive = false;
                        failed.push_back(id);
                    }
                }
            }
        }

        for (const auto& id : failed) {
            ring_->mark_down(id);
            auto successor = ring_->locate(id + ":failover");
            if (!successor.empty() && on_failover_)
                on_failover_(id, successor);
        }
    }
}

}  // namespace dse::cluster
