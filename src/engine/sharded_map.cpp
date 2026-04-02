#include "dse/engine/sharded_map.hpp"
#include <chrono>
#include <mutex>
#include <shared_mutex>

namespace dse::engine {

int64_t sharded_map::now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool sharded_map::expired(const entry& e) {
    return e.expires_at > 0 && now_ms() >= e.expires_at;
}

sharded_map::sharded_map() = default;

size_t sharded_map::shard_idx(const bytes& key) const {
    // maybe switch to xxhash later
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : key) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return h % k_shards;
}

bool sharded_map::set(const bytes& key, const bytes& value, int64_t ttl_ms) {
    auto& s = shards_[shard_idx(key)];
    std::unique_lock lock(s.mu);
    entry e{key, value, ttl_ms > 0 ? now_ms() + ttl_ms : 0};
    s.data[key] = std::move(e);
    return true;
}

std::optional<bytes> sharded_map::get(const bytes& key) const {
    auto& s = shards_[shard_idx(key)];
    std::shared_lock lock(s.mu);
    auto it = s.data.find(key);
    if (it == s.data.end() || expired(it->second))
        return std::nullopt;
    return it->second.value;
}

bool sharded_map::del(const bytes& key) {
    auto& s = shards_[shard_idx(key)];
    std::unique_lock lock(s.mu);
    return s.data.erase(key) > 0;
}

int64_t sharded_map::incr(const bytes& key, int64_t delta) {
    auto& s = shards_[shard_idx(key)];
    std::unique_lock lock(s.mu);
    auto& e = s.data[key];
    int64_t cur = 0;
    if (!e.value.empty()) {
        try { cur = std::stoll(e.value); } catch (...) {}
    }
    cur += delta;
    e.value = std::to_string(cur);
    e.key = key;
    return cur;
}

size_t sharded_map::size() const {
    size_t n = 0;
    for (auto& s : shards_) {
        std::shared_lock lock(s.mu);
        n += s.data.size();
    }
    return n;
}

void sharded_map::snapshot_copy(std::unordered_map<bytes, entry>& out) const {
    for (auto& s : shards_) {
        std::shared_lock lock(s.mu);
        for (const auto& [k, e] : s.data)
            out[k] = e;
    }
}

}  // namespace dse::engine
