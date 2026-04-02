#include "dse/engine/sharded_map.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

using steady_clk = std::chrono::steady_clock;

static std::vector<std::string> preload_keys(int n) {
    std::vector<std::string> keys;
    keys.reserve(n);
    for (int i = 0; i < n; ++i)
        keys.push_back("k" + std::to_string(i));
    return keys;
}

class mutex_store {
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::string> data_;
    const std::vector<std::string>& keys_;

public:
    explicit mutex_store(const std::vector<std::string>& keys) : keys_(keys) {
        for (const auto& k : keys_)
            data_[k] = "v";
    }

    void read_loop(std::atomic<bool>& run, std::atomic<uint64_t>& cnt, uint64_t seed) {
        uint64_t i = seed;
        while (run) {
            const auto& k = keys_[i % keys_.size()];
            std::optional<std::string> v;
            {
                std::lock_guard lock(mu_);
                auto it = data_.find(k);
                if (it != data_.end()) v = it->second;
            }
            (void)v;
            cnt++;
            i++;
        }
    }
};

int main() {
    const int threads = 8;
    const int key_count = 100000;
    const int bench_sec = 5;

    auto keys = preload_keys(key_count);

    mutex_store ms(keys);
    dse::engine::sharded_map sm;
    for (const auto& k : keys)
        sm.set(k, "v");

    std::atomic<bool> running{true};

    // mutex baseline
    std::atomic<uint64_t> mutex_ops{0};
    std::vector<std::thread> mthreads;
    for (int t = 0; t < threads; ++t)
        mthreads.emplace_back([&]() { ms.read_loop(running, mutex_ops, t); });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    mutex_ops = 0;
    auto m0 = steady_clk::now();
    std::this_thread::sleep_for(std::chrono::seconds(bench_sec));
    auto m1 = steady_clk::now();
    uint64_t mutex_count = mutex_ops.load();
    double mutex_rps = mutex_count / std::chrono::duration<double>(m1 - m0).count();

    running = false;
    for (auto& th : mthreads) th.join();

    // sharded
    running = true;
    std::atomic<uint64_t> shard_ops{0};
    std::vector<std::thread> sthreads;
    for (int t = 0; t < threads; ++t) {
        sthreads.emplace_back([&, t]() {
            uint64_t i = t;
            while (running) {
                sm.get(keys[i % keys.size()]);
                shard_ops++;
                i++;
            }
        });
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    shard_ops = 0;
    auto s0 = steady_clk::now();
    std::this_thread::sleep_for(std::chrono::seconds(bench_sec));
    auto s1 = steady_clk::now();
    uint64_t shard_count = shard_ops.load();
    double shard_rps = shard_count / std::chrono::duration<double>(s1 - s0).count();

    running = false;
    for (auto& th : sthreads) th.join();

    double ratio = shard_rps / mutex_rps;

    std::cout << "mutex reads/sec: " << static_cast<uint64_t>(mutex_rps) << "\n";
    std::cout << "sharded reads/sec: " << static_cast<uint64_t>(shard_rps) << "\n";
    std::cout << "speedup: " << ratio << "x\n";
    std::cout << (ratio >= 4.0 ? "PASS" : "FAIL") << "\n";

    return ratio >= 4.0 ? 0 : 1;
}
