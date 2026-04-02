#pragma once

#include "dse/types.hpp"
#include <mutex>
#include <unordered_map>
#include <functional>

namespace dse {

class store {
public:
    using snapshot_cb = std::function<void(const std::unordered_map<bytes, entry>&)>;

    store() = default;

    bool set(const bytes& key, const bytes& value, int64_t ttl_ms = 0);
    std::optional<bytes> get(const bytes& key);
    bool del(const bytes& key);
    bool exists(const bytes& key);
    int64_t incr(const bytes& key, int64_t delta = 1);

    size_t size() const;
    void foreach_entry(snapshot_cb cb) const;

    // for replication apply
    void apply(const entry& e);
    void remove(const bytes& key);

private:
    mutable std::mutex mu_;
    std::unordered_map<bytes, entry> data_;

    bool expired(const entry& e) const;
    void purge_expired();
};

}  // namespace dse
