#include "dse/store.hpp"
#include <chrono>

namespace dse {

static int64_t now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

bool store::expired(const entry& e) const {
    return e.expires_at > 0 && now_ms() >= e.expires_at;
}

void store::purge_expired() {
    for (auto it = data_.begin(); it != data_.end();) {
        if (expired(it->second))
            it = data_.erase(it);
        else
            ++it;
    }
}

bool store::set(const bytes& key, const bytes& value, int64_t ttl_ms) {
    std::lock_guard lock(mu_);
    entry e{key, value, ttl_ms > 0 ? now_ms() + ttl_ms : 0};
    data_[key] = std::move(e);
    return true;
}

std::optional<bytes> store::get(const bytes& key) {
    std::lock_guard lock(mu_);
    auto it = data_.find(key);
    if (it == data_.end() || expired(it->second))
        return std::nullopt;
    return it->second.value;
}

bool store::del(const bytes& key) {
    std::lock_guard lock(mu_);
    return data_.erase(key) > 0;
}

bool store::exists(const bytes& key) {
    std::lock_guard lock(mu_);
    auto it = data_.find(key);
    return it != data_.end() && !expired(it->second);
}

int64_t store::incr(const bytes& key, int64_t delta) {
    std::lock_guard lock(mu_);
    auto& e = data_[key];
    int64_t cur = 0;
    if (!e.value.empty()) {
        try { cur = std::stoll(e.value); } catch (...) {}
    }
    cur += delta;
    e.value = std::to_string(cur);
    e.key = key;
    return cur;
}

size_t store::size() const {
    std::lock_guard lock(mu_);
    return data_.size();
}

void store::foreach_entry(snapshot_cb cb) const {
    std::lock_guard lock(mu_);
    cb(data_);
}

void store::apply(const entry& e) {
    std::lock_guard lock(mu_);
    if (e.expires_at > 0 && expired(e))
        data_.erase(e.key);
    else
        data_[e.key] = e;
}

void store::remove(const bytes& key) {
    std::lock_guard lock(mu_);
    data_.erase(key);
}

}  // namespace dse
