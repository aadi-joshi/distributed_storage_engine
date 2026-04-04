#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace dse::cluster {

struct node {
    std::string id;
    std::string host;
    uint16_t port = 0;
    bool alive = true;
};

class hash_ring {
public:
    static constexpr int k_vnodes = 128;

    void add_node(const node& n);
    void remove_node(const std::string& id);
    void mark_down(const std::string& id);
    void mark_up(const std::string& id);

    std::string locate(const std::string& key) const;
    std::vector<std::string> replicas(const std::string& key, int count) const;

    const node* get_node(const std::string& id) const;
    std::vector<node> all_nodes() const;
    size_t node_count() const { return nodes_.size(); }

    const std::map<uint32_t, std::string>& ring() const { return ring_; }

private:
    std::map<std::string, node> nodes_;
    std::map<uint32_t, std::string> ring_;

    static uint32_t hash(const std::string& s);
    void rebuild();
};

}  // namespace dse::cluster
