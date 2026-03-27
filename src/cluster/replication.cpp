#include "dse/cluster/replication.hpp"
#include <chrono>
#include <thread>

namespace dse::cluster {

replication::replication(config cfg,
                         std::function<bool(const std::string&, const repl_entry&)> send_fn)
    : cfg_(std::move(cfg)), send_(std::move(send_fn)) {}

replication::~replication() {
    stop();
}

void replication::on_local_write(const wal_record& rec) {
    repl_entry e{++seq_, rec};
    if (cfg_.node_role == role::leader)
        replicate_to_followers(e);
    committed_.store(e.seq);
}

void replication::on_remote_append(const repl_entry& e) {
    if (e.seq > committed_.load())
        committed_.store(e.seq);
}

void replication::replicate_to_followers(const repl_entry& e) {
    if (!cfg_.ring) return;
    auto targets = cfg_.ring->replicas(cfg_.node_id, cfg_.repl_factor + 1);
    for (const auto& t : targets) {
        if (t == cfg_.node_id) continue;
        send_(t, e);
    }
}

void replication::start_heartbeat(int interval_ms) {
    if (running_.exchange(true)) return;
    hb_thread_ = std::thread(&replication::heartbeat_loop, this, interval_ms);
}

void replication::stop() {
    if (!running_.exchange(false)) return;
    if (hb_thread_.joinable()) hb_thread_.join();
}

void replication::heartbeat_loop(int interval_ms) {
    while (running_) {
        // ping followers - stub for now
        // send_(follower, heartbeat_pkt);
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

}  // namespace dse::cluster
