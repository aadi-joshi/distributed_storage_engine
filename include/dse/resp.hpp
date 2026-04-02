#pragma once

#include "dse/types.hpp"
#include <string>
#include <vector>

namespace dse {

class resp_parser {
public:
    // feed bytes, returns complete commands when ready
    std::vector<command> feed(const char* data, size_t len);

    void reset();

private:
    enum class state { idle, bulk_len, bulk_data, inline_cmd };
    state st_ = state::idle;
    std::string buf_;
    int bulk_len_ = 0;
    std::vector<bytes> cur_args_;
    cmd_type infer_type(const bytes& cmd) const;
};

class resp_encoder {
public:
    static bytes ok(const bytes& payload = "OK");
    static bytes err(const bytes& msg);
    static bytes bulk(const bytes& data);
    static bytes nil();
    static bytes integer(int64_t n);
    static bytes pong();
};

}  // namespace dse
