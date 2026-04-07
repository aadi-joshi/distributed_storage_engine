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

bool replication::send_to_peer(const std::string& node_id, const repl_entry& e) {
    if (send_) return send_(node_id, e);
    if (!cfg_.client || !cfg_.ring) return false;
    const node* n = cfg_.ring->get_node(node_id);
    if (!n || !n->alive) return false;
    return cfg_.client->send_repl(*n, e);
}

void replication::on_local_write(const wal_record& rec) {
    repl_entry e{++seq_, rec};
    replicate_to_followers(e, rec.key);
    committed_.store(e.seq);
}

void replication::on_remote_append(const repl_entry& e) {
    if (e.seq > committed_.load())
        committed_.store(e.seq);
}

void replication::replicate_to_followers(const repl_entry& e, const bytes& key) {
    if (!cfg_.ring) return;
    auto targets = cfg_.ring->replicas(key, cfg_.repl_factor + 1);
    for (const auto& t : targets) {
        if (t == cfg_.node_id) continue;
        send_to_peer(t, e);
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
        if (cfg_.ring && cfg_.client) {
            for (const auto& n : cfg_.ring->all_nodes()) {
                if (n.id == cfg_.node_id || !n.alive) continue;
                cfg_.client->send_heartbeat(n, cfg_.node_id);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(interval_ms));
    }
}

}  // namespace dse::cluster
