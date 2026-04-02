#include "dse/resp.hpp"
#include <algorithm>
#include <cctype>

namespace dse {

cmd_type resp_parser::infer_type(const bytes& cmd) const {
    bytes lower = cmd;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower == "get") return cmd_type::get;
    if (lower == "set") return cmd_type::set;
    if (lower == "del") return cmd_type::del;
    if (lower == "exists") return cmd_type::exists;
    if (lower == "incr") return cmd_type::incr;
    if (lower == "decr") return cmd_type::decr;
    if (lower == "ping") return cmd_type::ping;
    if (lower == "info") return cmd_type::info;
    if (lower == "repl") return cmd_type::repl;
    return cmd_type::unknown;
}

void resp_parser::reset() {
    st_ = state::idle;
    buf_.clear();
    cur_args_.clear();
    bulk_len_ = 0;
}

std::vector<command> resp_parser::feed(const char* data, size_t len) {
    std::vector<command> out;
    buf_.append(data, len);

    while (!buf_.empty()) {
        if (st_ == state::idle) {
            if (buf_[0] == '*') {
                st_ = state::bulk_len;
                cur_args_.clear();
                buf_.erase(0, 1);
                continue;
            }
            // inline: PING\r\n or GET key\r\n
            auto eol = buf_.find("\r\n");
            if (eol == std::string::npos) break;
            std::string line = buf_.substr(0, eol);
            buf_.erase(0, eol + 2);

            command cmd;
            size_t sp = line.find(' ');
            bytes op = (sp == std::string::npos) ? line : line.substr(0, sp);
            cmd.type = infer_type(op);
            if (sp != std::string::npos)
                cmd.args.push_back(line.substr(sp + 1));
            out.push_back(std::move(cmd));
            continue;
        }

        if (st_ == state::bulk_len) {
            auto eol = buf_.find("\r\n");
            if (eol == std::string::npos) break;
            bulk_len_ = std::stoi(buf_.substr(0, eol));
            buf_.erase(0, eol + 2);
            st_ = state::bulk_data;
            continue;
        }

        if (st_ == state::bulk_data) {
            if (buf_.size() < 1 || buf_[0] != '$') break;
            buf_.erase(0, 1);
            auto eol = buf_.find("\r\n");
            if (eol == std::string::npos) break;
            int arg_len = std::stoi(buf_.substr(0, eol));
            buf_.erase(0, eol + 2);
            if (buf_.size() < static_cast<size_t>(arg_len + 2)) break;
            cur_args_.emplace_back(buf_.substr(0, arg_len));
            buf_.erase(0, arg_len + 2);
            bulk_len_--;
            if (bulk_len_ == 0) {
                command cmd;
                if (!cur_args_.empty()) {
                    cmd.type = infer_type(cur_args_[0]);
                    cmd.args.assign(cur_args_.begin() + 1, cur_args_.end());
                }
                out.push_back(std::move(cmd));
                st_ = state::idle;
            }
        }
    }
    return out;
}

bytes resp_encoder::ok(const bytes& payload) {
    return "+" + payload + "\r\n";
}

bytes resp_encoder::err(const bytes& msg) {
    return "-" + msg + "\r\n";
}

bytes resp_encoder::bulk(const bytes& data) {
    return "$" + std::to_string(data.size()) + "\r\n" + data + "\r\n";
}

bytes resp_encoder::nil() {
    return "$-1\r\n";
}

bytes resp_encoder::integer(int64_t n) {
    return ":" + std::to_string(n) + "\r\n";
}

bytes resp_encoder::pong() {
    return "+PONG\r\n";
}

}  // namespace dse
