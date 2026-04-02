#include "dse/engine/executor.hpp"
#include "dse/resp.hpp"

namespace dse::engine {

executor::executor(size_t threads, sharded_map* store, wal* log)
    : thread_count_(threads), store_(store), wal_(log) {}

executor::~executor() {
    stop();
}

void executor::start() {
    if (running_.exchange(true)) return;
    for (size_t i = 0; i < thread_count_; ++i)
        workers_.emplace_back(&executor::worker_loop, this);
}

void executor::stop() {
    if (!running_.exchange(false)) return;
    q_cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
    workers_.clear();
}

void executor::submit(exec_request req) {
    {
        std::lock_guard lock(q_mu_);
        queue_.push_back(std::move(req));
    }
    q_cv_.notify_one();
}

bytes executor::execute_sync(const command& cmd) {
    return dispatch(cmd);
}

bytes executor::dispatch(const command& cmd) {
    stats_.ops++;

    switch (cmd.type) {
    case cmd_type::ping:
        return resp_encoder::pong();
    case cmd_type::get: {
        stats_.reads++;
        if (cmd.args.empty()) return resp_encoder::err("ERR missing key");
        auto v = store_->get(cmd.args[0]);
        return v ? resp_encoder::bulk(*v) : resp_encoder::nil();
    }
    case cmd_type::set: {
        stats_.writes++;
        if (cmd.args.size() < 2) return resp_encoder::err("ERR missing args");
        int64_t ttl = 0;
        if (cmd.args.size() >= 3) {
            try { ttl = std::stoll(cmd.args[2]); } catch (...) {}
        }
        store_->set(cmd.args[0], cmd.args[1], ttl);
        if (wal_) {
            wal_record r{wal_op::set, cmd.args[0], cmd.args[1], ttl > 0 ? ttl : 0};
            wal_->append(r);
        }
        return resp_encoder::ok();
    }
    case cmd_type::del: {
        stats_.writes++;
        if (cmd.args.empty()) return resp_encoder::err("ERR missing key");
        bool n = store_->del(cmd.args[0]);
        if (wal_ && n) {
            wal_record r{wal_op::del, cmd.args[0], "", 0};
            wal_->append(r);
        }
        return resp_encoder::integer(n ? 1 : 0);
    }
    case cmd_type::exists: {
        stats_.reads++;
        if (cmd.args.empty()) return resp_encoder::err("ERR missing key");
        auto v = store_->get(cmd.args[0]);
        return resp_encoder::integer(v ? 1 : 0);
    }
    case cmd_type::incr:
    case cmd_type::decr: {
        stats_.writes++;
        if (cmd.args.empty()) return resp_encoder::err("ERR missing key");
        int64_t d = (cmd.type == cmd_type::decr) ? -1 : 1;
        int64_t n = store_->incr(cmd.args[0], d);
        return resp_encoder::integer(n);
    }
    case cmd_type::info: {
        std::string info = "role:master\n";
        info += "keys:" + std::to_string(store_->size()) + "\n";
        info += "ops:" + std::to_string(stats_.ops.load()) + "\n";
        return resp_encoder::bulk(info);
    }
    default:
        return resp_encoder::err("ERR unknown command");
    }
}

void executor::worker_loop() {
    while (running_) {
        exec_request req;
        {
            std::unique_lock lock(q_mu_);
            q_cv_.wait(lock, [&] { return !queue_.empty() || !running_; });
            if (!running_ && queue_.empty()) break;
            req = std::move(queue_.front());
            queue_.pop_front();
        }
        auto resp = dispatch(req.cmd);
        if (req.on_done) req.on_done(std::move(resp));
    }
}

}  // namespace dse::engine
