#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace dse {

using bytes = std::string;

struct entry {
    bytes key;
    bytes value;
    int64_t expires_at = 0;  // 0 = no expiry
};

enum class cmd_type : uint8_t {
    get, set, del, exists, incr, decr,
    ping, info, repl, heartbeat, unknown
};

struct command {
    cmd_type type = cmd_type::unknown;
    std::vector<bytes> args;
};

}  // namespace dse
