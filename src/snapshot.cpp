#include "dse/snapshot.hpp"
#include <chrono>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <filesystem>

namespace dse {

snapshot::snapshot(const std::string& dir) : dir_(dir) {
    std::filesystem::create_directories(dir_);
}

bool snapshot::save(const store& s) {
    auto now = std::chrono::system_clock::now().time_since_epoch().count();
    std::ostringstream oss;
    oss << dir_ << "/snap-" << now << ".bin";
    last_path_ = oss.str();

    std::ofstream out(last_path_, std::ios::binary | std::ios::trunc);
    if (!out.is_open()) return false;

    bool ok = true;
    s.foreach_entry([&](const auto& data) {
        for (const auto& [k, e] : data) {
            uint32_t klen = static_cast<uint32_t>(k.size());
            uint32_t vlen = static_cast<uint32_t>(e.value.size());
            out.write(reinterpret_cast<const char*>(&klen), 4);
            out.write(k.data(), klen);
            out.write(reinterpret_cast<const char*>(&vlen), 4);
            out.write(e.value.data(), vlen);
            out.write(reinterpret_cast<const char*>(&e.expires_at), 8);
            if (!out) ok = false;
        }
    });

    // terminator
    uint32_t zero = 0;
    out.write(reinterpret_cast<const char*>(&zero), 4);
    return ok;
}

bool snapshot::load(store& s) {
    // find latest snap in dir - simplified: use last_path if set
    if (last_path_.empty()) return false;

    std::ifstream in(last_path_, std::ios::binary);
    if (!in.is_open()) return false;

    while (in.peek() != EOF) {
        uint32_t klen = 0;
        if (!in.read(reinterpret_cast<char*>(&klen), 4)) break;
        if (klen == 0) break;

        bytes key(klen, '\0');
        if (!in.read(key.data(), klen)) break;

        uint32_t vlen = 0;
        if (!in.read(reinterpret_cast<char*>(&vlen), 4)) break;

        bytes val(vlen, '\0');
        if (!in.read(val.data(), vlen)) break;

        int64_t exp = 0;
        if (!in.read(reinterpret_cast<char*>(&exp), 8)) break;

        entry e{key, val, exp};
        s.apply(e);
    }
    return true;
}

}  // namespace dse
