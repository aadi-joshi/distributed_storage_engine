#pragma once

#include "dse/cluster/hash_ring.hpp"
#include "dse/types.hpp"
#include <string>

namespace dse::cluster {

struct repl_entry;

class peer_client {
public:
    bytes request(const node& peer, const bytes& payload, int timeout_ms = 2000);
    bool send_repl(const node& peer, const repl_entry& entry);
    bool send_heartbeat(const node& peer, const std::string& from_id);

private:
    int connect_to(const node& peer, int timeout_ms);
    bool write_all(int fd, const char* data, size_t len);
    bytes read_response(int fd);
};

}  // namespace dse::cluster
