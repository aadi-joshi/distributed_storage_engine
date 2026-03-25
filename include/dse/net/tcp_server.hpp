#pragma once

#include "dse/net/epoll_loop.hpp"
#include "dse/resp.hpp"
#include "dse/engine/executor.hpp"
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dse::net {

struct conn {
    int fd = -1;
    resp_parser parser;
    std::string read_buf;
    std::string write_buf;
    bool closing = false;
};

class tcp_server {
public:
    struct config {
        std::string bind_addr = "0.0.0.0";
        uint16_t port = 6379;
        int backlog = 4096;
        int max_connections = 12000;
        int worker_threads = 8;
    };

    explicit tcp_server(config cfg, engine::executor* exec);
    ~tcp_server();

    bool start();
    void run();
    void stop();

    uint64_t total_connections() const { return total_conns_; }
    uint64_t active_connections() const { return active_conns_.load(); }

private:
    config cfg_;
    engine::executor* exec_;
    int listen_fd_ = -1;
    epoll_loop loop_;
    std::mutex conns_mu_;
    std::unordered_map<int, std::unique_ptr<conn>> conns_;
    std::atomic<uint64_t> active_conns_{0};
    uint64_t total_conns_ = 0;
    std::atomic<bool> running_{false};

    bool set_nonblock(int fd);
    conn* find_conn(int fd);
    void accept_new();
    void on_readable(conn* c);
    void on_writable(conn* c);
    void close_conn(int fd);
    void queue_response(int fd, bytes resp);
};

}  // namespace dse::net
