#pragma once

#include "dse/cluster/hash_ring.hpp"
#include "dse/wal.hpp"
#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dse::cluster {

enum class role { leader, follower };

struct repl_entry {
    uint64_t seq;
    wal_record record;
};

class replication {
public:
    struct config {
        std::string node_id;
        role node_role = role::leader;
        hash_ring* ring = nullptr;
        int repl_factor = 2;
    };

    replication(config cfg, std::function<bool(const std::string&, const repl_entry&)> send_fn);
    ~replication();

    void on_local_write(const wal_record& rec);
    void on_remote_append(const repl_entry& e);
    uint64_t committed_seq() const { return committed_.load(); }

    void start_heartbeat(int interval_ms = 500);
    void stop();

private:
    config cfg_;
    std::function<bool(const std::string&, const repl_entry&)> send_;
    std::atomic<uint64_t> seq_{0};
    std::atomic<uint64_t> committed_{0};
    std::mutex followers_mu_;
    std::vector<std::string> followers_;
    std::thread hb_thread_;
    std::atomic<bool> running_{false};

    void heartbeat_loop(int interval_ms);
    void replicate_to_followers(const repl_entry& e);
};

}  // namespace dse::cluster
