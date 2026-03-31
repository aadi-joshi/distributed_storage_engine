#include "dse/engine/executor.hpp"
#include "dse/engine/sharded_map.hpp"
#include "dse/net/tcp_server.hpp"

#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

static bool send_ping(int fd) {
    const char* msg = "PING\r\n";
    return write(fd, msg, 6) == 6;
}

static int make_conn(const char* host, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int main(int argc, char** argv) {
    int target_clients = 10000;
    uint16_t port = 16379;
    if (argc > 1) target_clients = std::stoi(argv[1]);

    dse::engine::sharded_map store;
    dse::engine::executor exec(4, &store, nullptr);
    exec.start();

    dse::net::tcp_server::config cfg;
    cfg.port = port;
    cfg.max_connections = 15000;
    cfg.worker_threads = 4;

    dse::net::tcp_server srv(cfg, &exec);
    if (!srv.start()) {
        std::cerr << "server failed\n";
        return 1;
    }

    std::thread srv_th([&]() { srv.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    std::vector<int> fds;
    fds.reserve(target_clients);

    int connected = 0;
    for (int i = 0; i < target_clients; ++i) {
        int fd = make_conn("127.0.0.1", port);
        if (fd >= 0) {
            fds.push_back(fd);
            connected++;
        }
        if (i % 1000 == 0 && i > 0)
            std::cout << "connected " << connected << "\n";
    }

    std::atomic<int> pings{0};
    std::vector<std::thread> workers;
    int chunk = std::max(1, connected / 8);
    for (int w = 0; w < 8; ++w) {
        int start = w * chunk;
        int end = (w == 7) ? connected : (w + 1) * chunk;
        workers.emplace_back([&, start, end]() {
            for (int i = start; i < end; ++i) {
                if (send_ping(fds[i])) pings++;
            }
        });
    }
    for (auto& t : workers) t.join();

    std::cout << "target_clients: " << target_clients << "\n";
    std::cout << "connected: " << connected << "\n";
    std::cout << "pings_ok: " << pings.load() << "\n";
    std::cout << (connected >= 10000 ? "PASS" : "FAIL") << "\n";

    for (int fd : fds) close(fd);
    srv.stop();
    if (srv_th.joinable()) srv_th.join();
    exec.stop();

    return connected >= 10000 ? 0 : 1;
}
