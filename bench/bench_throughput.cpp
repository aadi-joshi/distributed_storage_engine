#include "dse/engine/executor.hpp"
#include "dse/engine/sharded_map.hpp"
#include "dse/types.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

using namespace dse;
using steady_clk = std::chrono::steady_clock;

static uint64_t run_bench(engine::executor& exec, size_t thread_count, int duration_sec) {
    std::atomic<uint64_t> count{0};
    auto deadline = steady_clk::now() + std::chrono::seconds(duration_sec);

    // prebuild keys to avoid alloc in hot loop
    std::vector<std::string> keys;
    keys.reserve(10000);
    for (int i = 0; i < 10000; ++i)
        keys.push_back("key" + std::to_string(i));

    auto worker = [&]() {
        uint64_t local = 0;
        uint64_t i = 0;
        while (steady_clk::now() < deadline) {
            command c;
            if ((i & 7) < 6) {
                c.type = cmd_type::get;
                c.args = {keys[i % 10000]};
            } else {
                c.type = cmd_type::set;
                c.args = {keys[i % 10000], "v"};
            }
            exec.execute_sync(c);
            local++;
            i++;
        }
        count += local;
    };

    std::vector<std::thread> threads;
    for (size_t t = 0; t < thread_count; ++t)
        threads.emplace_back(worker);
    for (auto& th : threads) th.join();
    return count.load();
}

// baseline: single mutex map
class mutex_map {
    mutable std::mutex mu_;
    std::unordered_map<std::string, std::string> data_;
public:
    void set(const std::string& k, const std::string& v) {
        std::lock_guard lock(mu_);
        data_[k] = v;
    }
    std::optional<std::string> get(const std::string& k) const {
        std::lock_guard lock(mu_);
        auto it = data_.find(k);
        if (it == data_.end()) return std::nullopt;
        return it->second;
    }
};

static uint64_t bench_mutex_reads(int threads, int duration_sec) {
    mutex_map m;
    std::vector<std::string> keys;
    keys.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
        keys.push_back("key" + std::to_string(i));
        m.set(keys.back(), "val");
    }

    std::atomic<uint64_t> count{0};
    auto deadline = steady_clk::now() + std::chrono::seconds(duration_sec);

    auto worker = [&]() {
        uint64_t local = 0;
        uint64_t i = 0;
        while (steady_clk::now() < deadline) {
            m.get(keys[i % 10000]);
            local++;
            i++;
        }
        count += local;
    };

    std::vector<std::thread> ths;
    for (int t = 0; t < threads; ++t)
        ths.emplace_back(worker);
    for (auto& th : ths) th.join();
    return count.load();
}

static uint64_t bench_sharded_reads(engine::sharded_map& m, int threads, int duration_sec) {
    std::vector<std::string> keys;
    keys.reserve(10000);
    for (int i = 0; i < 10000; ++i) {
        keys.push_back("key" + std::to_string(i));
        m.set(keys.back(), "val");
    }

    std::atomic<uint64_t> count{0};
    auto deadline = steady_clk::now() + std::chrono::seconds(duration_sec);

    auto worker = [&]() {
        uint64_t local = 0;
        uint64_t i = 0;
        while (steady_clk::now() < deadline) {
            m.get(keys[i % 10000]);
            local++;
            i++;
        }
        count += local;
    };

    std::vector<std::thread> ths;
    for (int t = 0; t < threads; ++t)
        ths.emplace_back(worker);
    for (auto& th : ths) th.join();
    return count.load();
}

int main() {
    int threads = 8;
    int duration = 3;

    engine::sharded_map store;
    engine::executor exec(static_cast<size_t>(threads), &store, nullptr);
    exec.start();

    auto t0 = steady_clk::now();
    uint64_t ops = run_bench(exec, static_cast<size_t>(threads), duration);
    auto elapsed = std::chrono::duration<double>(steady_clk::now() - t0).count();
    double ops_sec = ops / elapsed;

    engine::sharded_map read_store;
    uint64_t mutex_reads = bench_mutex_reads(threads, duration);
    uint64_t sharded_reads = bench_sharded_reads(read_store, threads, duration);
    double mutex_rps = mutex_reads / elapsed;
    double sharded_rps = sharded_reads / elapsed;
    double read_ratio = sharded_rps / mutex_rps;

    exec.stop();

    std::cout << "=== dse throughput bench ===\n";
    std::cout << "threads: " << threads << "\n";
    std::cout << "duration: " << duration << "s\n";
    std::cout << "total_ops: " << ops << "\n";
    std::cout << "ops/sec: " << static_cast<uint64_t>(ops_sec) << "\n";
    std::cout << "mutex_read_ops/sec: " << static_cast<uint64_t>(mutex_rps) << "\n";
    std::cout << "sharded_read_ops/sec: " << static_cast<uint64_t>(sharded_rps) << "\n";
    std::cout << "read_speedup: " << read_ratio << "x\n";

    bool pass_ops = ops_sec >= 450000;
    bool pass_read = read_ratio >= 4.0;
    std::cout << "target_450k: " << (pass_ops ? "PASS" : "FAIL") << "\n";
    std::cout << "target_4x_read: " << (pass_read ? "PASS" : "FAIL") << "\n";

    return (pass_ops && pass_read) ? 0 : 1;
}
