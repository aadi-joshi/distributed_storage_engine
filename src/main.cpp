#include "dse/engine/executor.hpp"
#include "dse/engine/sharded_map.hpp"
#include "dse/net/tcp_server.hpp"
#include "dse/wal.hpp"
#include "dse/snapshot.hpp"
#include "dse/cluster/hash_ring.hpp"
#include "dse/cluster/replication.hpp"
#include "dse/cluster/recovery.hpp"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>
#include <unordered_map>

static std::atomic<bool> g_stop{false};

static void on_signal(int) {
    g_stop = true;
}

int main(int argc, char** argv) {
    std::string bind = "0.0.0.0";
    uint16_t port = 6379;
    int threads = 8;
    std::string data_dir = "./data";
    std::string node_id = "node-1";

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) port = static_cast<uint16_t>(std::stoi(argv[++i]));
        else if (a == "--bind" && i + 1 < argc) bind = argv[++i];
        else if (a == "--threads" && i + 1 < argc) threads = std::stoi(argv[++i]);
        else if (a == "--data" && i + 1 < argc) data_dir = argv[++i];
        else if (a == "--node" && i + 1 < argc) node_id = argv[++i];
    }

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    dse::engine::sharded_map kv;
    dse::wal log(data_dir + "/store.wal");
    dse::snapshot snaps(data_dir + "/snapshots");

    // replay wal on boot
    log.replay([&](const dse::wal_record& r) {
        if (r.op == dse::wal_op::set)
            kv.set(r.key, r.value, r.expires_at);
        else if (r.op == dse::wal_op::del)
            kv.del(r.key);
    });

    dse::cluster::hash_ring ring;
    ring.add_node({node_id, bind, port, true});

    dse::cluster::replication::config repl_cfg;
    repl_cfg.node_id = node_id;
    repl_cfg.node_role = dse::cluster::role::leader;
    repl_cfg.ring = &ring;

    auto send_stub = [](const std::string&, const dse::cluster::repl_entry&) { return true; };
    dse::cluster::replication repl(repl_cfg, send_stub);

    dse::cluster::recovery recover(&ring, [&](const std::string& failed, const std::string& succ) {
        std::cerr << "failover " << failed << " -> " << succ << "\n";
    });
    recover.start();
    repl.start_heartbeat();

    dse::engine::executor exec(static_cast<size_t>(threads), &kv, &log);
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
              << " threads=" << threads << "\n";

    // snapshot every 60s
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
