#include "dse/net/tcp_server.hpp"

#if defined(DSE_USE_EPOLL)

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>

namespace dse::net {

tcp_server::tcp_server(config cfg, engine::executor* exec)
    : cfg_(std::move(cfg)), exec_(exec) {}

tcp_server::~tcp_server() {
    stop();
}

bool tcp_server::set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool tcp_server::start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;

    int yes = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    setsockopt(listen_fd_, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(cfg_.port);
    inet_pton(AF_INET, cfg_.bind_addr.c_str(), &addr.sin_addr);

    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0)
        return false;
    if (listen(listen_fd_, cfg_.backlog) < 0)
        return false;
    if (!set_nonblock(listen_fd_))
        return false;

    loop_.add(listen_fd_, EPOLLIN, reinterpret_cast<void*>(static_cast<intptr_t>(-1)));
    running_ = true;
    return true;
}

conn* tcp_server::find_conn(int fd) {
    std::lock_guard lock(conns_mu_);
    auto it = conns_.find(fd);
    return it != conns_.end() ? it->second.get() : nullptr;
}

void tcp_server::accept_new() {
    while (active_conns_ < static_cast<uint64_t>(cfg_.max_connections)) {
        sockaddr_in peer{};
        socklen_t plen = sizeof(peer);
        int fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &plen);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            break;
        }

        set_nonblock(fd);
        auto c = std::make_unique<conn>();
        c->fd = fd;
        {
            std::lock_guard lock(conns_mu_);
            loop_.add(fd, EPOLLIN | EPOLLOUT | EPOLLRDHUP, reinterpret_cast<void*>(static_cast<intptr_t>(fd)));
            conns_[fd] = std::move(c);
        }
        active_conns_++;
        total_conns_++;
    }
}

void tcp_server::queue_response(int fd, bytes resp) {
    std::lock_guard lock(conns_mu_);
    auto it = conns_.find(fd);
    if (it == conns_.end()) return;
    it->second->write_buf.append(resp);
}

void tcp_server::on_readable(conn* c) {
    char buf[8192];
    while (true) {
        ssize_t n = read(c->fd, buf, sizeof(buf));
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            c->closing = true;
            return;
        }
        if (n == 0) {
            c->closing = true;
            return;
        }
        auto cmds = c->parser.feed(buf, static_cast<size_t>(n));
        int fd = c->fd;
        for (auto& cmd : cmds) {
            exec_->submit({std::move(cmd), [this, fd](bytes resp) {
                queue_response(fd, std::move(resp));
            }});
        }
    }
}

void tcp_server::on_writable(conn* c) {
    while (!c->write_buf.empty()) {
        ssize_t n = write(c->fd, c->write_buf.data(), c->write_buf.size());
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) break;
            c->closing = true;
            return;
        }
        c->write_buf.erase(0, static_cast<size_t>(n));
    }
}

void tcp_server::close_conn(int fd) {
    loop_.del(fd);
    close(fd);
    {
        std::lock_guard lock(conns_mu_);
        conns_.erase(fd);
    }
    active_conns_--;
}

void tcp_server::run() {
    while (running_) {
        loop_.poll([this](const io_event& ev) {
            intptr_t tag = reinterpret_cast<intptr_t>(ev.userdata);
            if (tag == -1) {
                accept_new();
                return;
            }
            int fd = static_cast<int>(tag);
            auto* c = find_conn(fd);
            if (!c) return;

            if (ev.events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR))
                c->closing = true;
            if (!c->closing && (ev.events & EPOLLIN))
                on_readable(c);
            if (!c->closing && (ev.events & EPOLLOUT))
                on_writable(c);
            if (c->closing && c->write_buf.empty())
                close_conn(fd);
        }, 100);
    }
}

void tcp_server::stop() {
    running_ = false;
    loop_.stop();
    std::vector<int> fds;
    {
        std::lock_guard lock(conns_mu_);
        for (auto& [fd, c] : conns_)
            fds.push_back(fd);
        conns_.clear();
    }
    for (int fd : fds)
        close(fd);
    if (listen_fd_ >= 0) {
        close(listen_fd_);
        listen_fd_ = -1;
    }
}

}  // namespace dse::net

#else

namespace dse::net {

tcp_server::tcp_server(config cfg, engine::executor* exec)
    : cfg_(std::move(cfg)), exec_(exec) {}

tcp_server::~tcp_server() { stop(); }
bool tcp_server::start() { return false; }
void tcp_server::run() {}
void tcp_server::stop() { running_ = false; }
conn* tcp_server::find_conn(int) { return nullptr; }

}  // namespace dse::net

#endif
