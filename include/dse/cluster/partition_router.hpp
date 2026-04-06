#pragma once

#include "dse/cluster/hash_ring.hpp"
#include "dse/cluster/peer_client.hpp"
#include "dse/types.hpp"
#include <optional>
#include <string>

namespace dse::cluster {

class partition_router {
public:
    partition_router(std::string local_id, const hash_ring* ring, peer_client* client);

    bool owns_key(const bytes& key) const;
    std::optional<bytes> forward(const command& cmd) const;
    bytes encode_command(const command& cmd) const;

private:
    std::string local_id_;
    const hash_ring* ring_;
    peer_client* client_;

    const node* owner_node(const bytes& key) const;
};

}  // namespace dse::cluster
