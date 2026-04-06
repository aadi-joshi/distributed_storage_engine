#include "dse/cluster/partition_router.hpp"
#include "dse/resp.hpp"

namespace dse::cluster {

partition_router::partition_router(std::string local_id, const hash_ring* ring, peer_client* client)
    : local_id_(std::move(local_id)), ring_(ring), client_(client) {}

const node* partition_router::owner_node(const bytes& key) const {
    if (!ring_ || ring_->node_count() <= 1) return nullptr;
    auto owner = ring_->locate(key);
    if (owner.empty()) return nullptr;
    return ring_->get_node(owner);
}

bool partition_router::owns_key(const bytes& key) const {
    if (!ring_ || ring_->node_count() <= 1) return true;
    return ring_->locate(key) == local_id_;
}

bytes partition_router::encode_command(const command& cmd) const {
    std::vector<bytes> parts;
    switch (cmd.type) {
    case cmd_type::get: parts = {"GET", cmd.args[0]}; break;
    case cmd_type::set:
        parts = {"SET", cmd.args[0], cmd.args[1]};
        if (cmd.args.size() >= 3) parts.push_back(cmd.args[2]);
        break;
    case cmd_type::del: parts = {"DEL", cmd.args[0]}; break;
    case cmd_type::exists: parts = {"EXISTS", cmd.args[0]}; break;
    case cmd_type::incr: parts = {"INCR", cmd.args[0]}; break;
    case cmd_type::decr: parts = {"DECR", cmd.args[0]}; break;
    case cmd_type::ping: parts = {"PING"}; break;
    default: return {};
    }

    bytes out = "*" + std::to_string(parts.size()) + "\r\n";
    for (const auto& p : parts)
        out += "$" + std::to_string(p.size()) + "\r\n" + p + "\r\n";
    return out;
}

std::optional<bytes> partition_router::forward(const command& cmd) const {
    if (!ring_ || ring_->node_count() <= 1 || !client_) return std::nullopt;

    bytes key;
    switch (cmd.type) {
    case cmd_type::get:
    case cmd_type::del:
    case cmd_type::exists:
    case cmd_type::incr:
    case cmd_type::decr:
        if (cmd.args.empty()) return std::nullopt;
        key = cmd.args[0];
        break;
    case cmd_type::set:
        if (cmd.args.empty()) return std::nullopt;
        key = cmd.args[0];
        break;
    default:
        return std::nullopt;
    }

    if (owns_key(key)) return std::nullopt;

    const node* owner = owner_node(key);
    if (!owner)
        return resp_encoder::err("ERR no owner for key");

    auto payload = encode_command(cmd);
    if (payload.empty()) return std::nullopt;
    return client_->request(*owner, payload);
}

}  // namespace dse::cluster
