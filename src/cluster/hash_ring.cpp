#include "dse/cluster/hash_ring.hpp"
#include <functional>

namespace dse::cluster {

uint32_t hash_ring::hash(const std::string& s) {
    // murmur-ish 32bit
    uint32_t h = 0x811c9dc5;
    for (unsigned char c : s) {
        h ^= c;
        h *= 0x01000193;
    }
    return h;
}

void hash_ring::rebuild() {
    ring_.clear();
    for (const auto& [id, n] : nodes_) {
        if (!n.alive) continue;
        for (int v = 0; v < k_vnodes; ++v) {
            std::string tag = id + "#" + std::to_string(v);
            ring_[hash(tag)] = id;
        }
    }
}

void hash_ring::add_node(const node& n) {
    nodes_[n.id] = n;
    rebuild();
}

void hash_ring::remove_node(const std::string& id) {
    nodes_.erase(id);
    rebuild();
}

void hash_ring::mark_down(const std::string& id) {
    auto it = nodes_.find(id);
    if (it != nodes_.end()) {
        it->second.alive = false;
        rebuild();
    }
}

void hash_ring::mark_up(const std::string& id) {
    auto it = nodes_.find(id);
    if (it != nodes_.end()) {
        it->second.alive = true;
        rebuild();
    }
}

std::string hash_ring::locate(const std::string& key) const {
    if (ring_.empty()) return {};
    uint32_t h = hash(key);
    auto it = ring_.lower_bound(h);
    if (it == ring_.end()) it = ring_.begin();
    return it->second;
}

std::vector<std::string> hash_ring::replicas(const std::string& key, int count) const {
    std::vector<std::string> out;
    if (ring_.empty() || count <= 0) return out;

    uint32_t h = hash(key);
    auto it = ring_.lower_bound(h);
    std::map<std::string, bool> seen;

    while (static_cast<int>(out.size()) < count) {
        if (it == ring_.end()) it = ring_.begin();
        if (!seen[it->second]) {
            seen[it->second] = true;
            out.push_back(it->second);
        }
        ++it;
        if (seen.size() >= nodes_.size()) break;
    }
    return out;
}

}  // namespace dse::cluster
