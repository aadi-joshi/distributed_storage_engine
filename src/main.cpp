#include "dse/engine/executor.hpp"
#include "dse/engine/sharded_map.hpp"
#include "dse/net/tcp_server.hpp"
#include "dse/wal.hpp"
#include "dse/snapshot.hpp"
#include "dse/cluster/hash_ring.hpp"
#include "dse/cluster/replication.hpp"
#include "dse/cluster/recovery.hpp"
#include "dse/cluster/peer_client.hpp"
#include "dse/cluster/partition_router.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <filesystem>
#include <sstream>
#include <thread>
#include <unordered_map>

static std::atomic<bool> g_stop{false};

static void on_signal(int) {
    g_stop = true;
}

static std::vector<dse::cluster::node> parse_peers(const std::string& spec) {
    std::vector<dse::cluster::node> out;
    std::stringstream ss(spec);
    std::string item;
    while (std::getline(ss, item, ',')) {
        if (item.empty()) continue;
        auto c1 = item.find(':');
        auto c2 = item.rfind(':');
        if (c1 == std::string::npos || c2 == c1) continue;
        dse::cluster::node n;
        n.id = item.substr(0, c1);
        n.host = item.substr(c1 + 1, c2 - c1 - 1);
        n.port = static_cast<uint16_t>(std::stoi(item.substr(c2 + 1)));
        n.alive = true;
        out.push_back(std::move(n));
    }
    return out;
}

int main(int argc, char** argv) {
    std::string bind = "0.0.0.0";
    uint16_t port = 6379;
    int threads = 8;
    std::string data_dir = "./data";
    std::string node_id = "node-1";
    std::string peers_spec;
    std::string role_str = "leader";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (a == "--bind" && i + 1 < argc) bind = argv[++i];
        else if (a == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
        else if (a == "--data" && i + 1 < argc) data_dir = argv[++i];
        else if (a == "--node" && i + 1 < argc) node_id = argv[++i];
        else if (a == "--peers" && i + 1 < argc) peers_spec = argv[++i];
        else if (a == "--role" && i + 1 < argc) role_str = argv[++i];
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    std::filesystem::create_directories(data_dir);

    dse::engine::sharded_map kv;
    dse::wal log(data_dir + "/store.wal");
    dse::snapshot snaps(data_dir + "/snapshots");

    log.replay([&](const dse::wal_record& r) {
        if (r.op == dse::wal_op::set)
            kv.set(r.key, r.value, r.expires_at);
        else if (r.op == dse::wal_op::del)
            kv.del(r.key);
        else if (r.op == dse::wal_op::incr)
            kv.incr(r.key, 0);
    });

    dse::cluster::hash_ring ring;
    ring.add_node({node_id, bind == "0.0.0.0" ? "127.0.0.1" : bind, port, true});

    for (const auto& p : parse_peers(peers_spec))
        ring.add_node(p);

    dse::cluster::peer_client peers;
    dse::cluster::partition_router router(node_id, &ring, &peers);

    dse::cluster::replication::config repl_cfg;
    repl_cfg.node_id = node_id;
    repl_cfg.node_role = (role_str == "follower") ? dse::cluster::role::follower
                                                  : dse::cluster::role::leader;
    repl_cfg.ring = &ring;
    repl_cfg.client = &peers;
    repl_cfg.repl_factor = 2;

    dse::cluster::replication repl(repl_cfg);

    dse::cluster::recovery recover(&ring, [&](const std::string& failed, const std::string& succ) {
        std::cerr << "failover " << failed << " -> " << succ << "\n";
    });

    for (const auto& n : ring.all_nodes()) {
        if (n.id != node_id)
            recover.heartbeat(n.id);
    }

    recover.start();
    repl.start_heartbeat();

    dse::engine::cluster_hooks hooks;
    hooks.router = &router;
    hooks.repl = &repl;
    hooks.recover = &recover;

    dse::engine::executor exec(static_cast<size_t>(threads), &kv, &log, hooks);
    exec.start();

    dse::net::tcp_server::config srv_cfg;
    srv_cfg.bind_addr = bind;
    srv_cfg.port = port;
    srv_cfg.worker_threads = threads;
    srv_cfg.max_connections = 12000;

    dse::net::tcp_server server(srv_cfg, &exec);
    if (!server.start()) {
        std::cerr << "server start failed\n";
        return 1;
    }

    std::cout << "dse listening on " << bind << ":" << port
              << " node=" << node_id
              << " peers=" << ring.node_count() - 1
              << " threads=" << threads << "\n";

    std::thread snap_thread([&]() {
        while (!g_stop) {
            std::this_thread::sleep_for(std::chrono::seconds(60));
            if (g_stop) break;
            dse::store tmp;
            std::unordered_map<dse::bytes, dse::entry> dump;
            kv.snapshot_copy(dump);
            for (const auto& [k, e] : dump)
                tmp.apply(e);
            snaps.save(tmp);
            log.sync();
        }
    });

    std::thread net_thread([&]() { server.run(); });

    while (!g_stop)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

    server.stop();
    g_stop = true;
    if (net_thread.joinable()) net_thread.join();
    if (snap_thread.joinable()) snap_thread.join();

    repl.stop();
    recover.stop();
    exec.stop();

    std::cout << "shutdown ok ops=" << exec.get_stats().ops.load() << "\n";
    return 0;
}
