#include "dse/cluster/peer_client.hpp"
#include "dse/cluster/replication.hpp"
#include "dse/resp.hpp"

#if defined(DSE_USE_EPOLL)

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <algorithm>
#include <stdexcept>

namespace dse::cluster {

int peer_client::connect_to(const node& peer, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(peer.port);
    inet_pton(AF_INET, peer.host.c_str(), &addr.sin_addr);

    int rc = connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (rc < 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }

    pollfd pfd{fd, POLLOUT, 0};
    if (poll(&pfd, 1, timeout_ms) <= 0) {
        close(fd);
        return -1;
    }

    int err = 0;
    socklen_t len = sizeof(err);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (err != 0) {
        close(fd);
        return -1;
    }

    fcntl(fd, F_SETFL, flags);
    int yes = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes));
    return fd;
}

bool peer_client::write_all(int fd, const char* data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, data + off, len - off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        off += static_cast<size_t>(n);
    }
    return true;
}

bytes peer_client::read_response(int fd) {
    std::string buf;
    char chunk[4096];
    using clock = std::chrono::steady_clock;
    auto deadline = clock::now() + std::chrono::seconds(3);

    auto complete = [&]() -> bool {
        if (buf.size() < 3) return false;
        char t = buf[0];
        if (t == '+' || t == '-' || t == ':') {
            return buf.find("\r\n") != std::string::npos;
        }
        if (t == '$') {
            auto eol = buf.find("\r\n");
            if (eol == std::string::npos) return false;
            int blen = std::stoi(buf.substr(1, eol - 1));
            if (blen < 0) return true;
            size_t need = eol + 2 + static_cast<size_t>(blen) + 2;
            return buf.size() >= need;
        }
        return false;
    };

    while (clock::now() < deadline) {
        if (complete()) break;

        auto remain = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - clock::now()).count();
        if (remain <= 0) break;

        pollfd pfd{fd, POLLIN, 0};
        if (poll(&pfd, 1, static_cast<int>(std::min(remain, 200L))) <= 0)
            continue;

        ssize_t n = read(fd, chunk, sizeof(chunk));
        if (n <= 0) break;
        buf.append(chunk, static_cast<size_t>(n));
    }
    return buf;
}

bytes peer_client::request(const node& peer, const bytes& payload, int timeout_ms) {
    int fd = connect_to(peer, timeout_ms);
    if (fd < 0) return resp_encoder::err("ERR peer connect failed");

    bool ok = write_all(fd, payload.data(), payload.size());
    if (!ok) {
        close(fd);
        return resp_encoder::err("ERR peer write failed");
    }

    auto resp = read_response(fd);
    close(fd);
    return resp.empty() ? resp_encoder::err("ERR peer read failed") : resp;
}

static bytes encode_bulk_cmd(const std::vector<bytes>& parts) {
    bytes out = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto& p : parts)
        out += "$" + std::to_string(p.size()) + "\r\n" + p + "\r\n";
    return out;
}

bool peer_client::send_repl(const node& peer, const repl_entry& entry) {
    std::vector<bytes> parts = {
        "REPL",
        std::to_string(entry.seq),
        std::to_string(static_cast<int>(entry.record.op)),
        entry.record.key,
        entry.record.value,
        std::to_string(entry.record.expires_at)
    };
    auto resp = request(peer, encode_bulk_cmd(parts), 3000);
    return !resp.empty() && resp[0] == '+';
}

bool peer_client::send_heartbeat(const node& peer, const std::string& from_id) {
    std::vector<bytes> parts = {"HEARTBEAT", from_id};
    auto resp = request(peer, encode_bulk_cmd(parts), 1000);
    return !resp.empty() && resp[0] == '+';
}

}  // namespace dse::cluster

#else

namespace dse::cluster {

bytes peer_client::request(const node&, const bytes&, int) {
    return resp_encoder::err("ERR no tcp on this platform");
}
bool peer_client::send_repl(const node&, const repl_entry&) { return false; }
bool peer_client::send_heartbeat(const node&, const std::string&) { return false; }

}  // namespace dse::cluster

#endif
