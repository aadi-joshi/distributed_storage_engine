#pragma once

#include "dse/types.hpp"
#include <array>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace dse::engine {

// sharded concurrent map - read path avoids global lock
class sharded_map {
public:
    static constexpr size_t k_shards = 512;

    sharded_map();

    bool set(const bytes& key, const bytes& value, int64_t ttl_ms = 0);
    std::optional<bytes> get(const bytes& key) const;
    bool del(const bytes& key);
    int64_t incr(const bytes& key, int64_t delta);

    size_t size() const;
    void snapshot_copy(std::unordered_map<bytes, entry>& out) const;

private:
    struct shard {
        mutable std::shared_mutex mu;
        std::unordered_map<bytes, entry> data;
    };

    std::array<shard, k_shards> shards_;

    size_t shard_idx(const bytes& key) const;
    static int64_t now_ms();
    static bool expired(const entry& e);
};

}  // namespace dse::engine
