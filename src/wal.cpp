#include "dse/wal.hpp"
#include <cstring>
#include <stdexcept>

namespace dse {

static void write_u32(std::vector<char>& buf, uint32_t v) {
    buf.push_back(static_cast<char>(v & 0xff));
    buf.push_back(static_cast<char>((v >> 8) & 0xff));
    buf.push_back(static_cast<char>((v >> 16) & 0xff));
    buf.push_back(static_cast<char>((v >> 24) & 0xff));
}

static uint32_t read_u32(const char* p) {
    uint32_t v = 0;
    std::memcpy(&v, p, 4);
    return v;
}

wal::wal(const std::string& path) : path_(path) {
    file_.open(path_, std::ios::binary | std::ios::app | std::ios::in);
    if (!file_.is_open())
        throw std::runtime_error("wal open failed: " + path_);
}

wal::~wal() {
    if (file_.is_open()) {
        file_.flush();
        file_.close();
    }
}

bool wal::write_raw(const void* data, size_t len) {
    file_.write(static_cast<const char*>(data), static_cast<std::streamsize>(len));
    if (!file_) return false;
    bytes_written_ += len;
    return true;
}

bool wal::append(const wal_record& rec) {
    std::lock_guard lock(mu_);
    std::vector<char> buf;
    buf.reserve(32 + rec.key.size() + rec.value.size());

    buf.push_back(static_cast<char>(rec.op));
    write_u32(buf, static_cast<uint32_t>(rec.key.size()));
    buf.insert(buf.end(), rec.key.begin(), rec.key.end());
    write_u32(buf, static_cast<uint32_t>(rec.value.size()));
    buf.insert(buf.end(), rec.value.begin(), rec.value.end());

    char exp[8];
    std::memcpy(exp, &rec.expires_at, 8);
    buf.insert(buf.end(), exp, exp + 8);

    // crc placeholder - could add later
    // uint32_t crc = fnv(rec);
    // write_u32(buf, crc);

    if (!write_raw(buf.data(), buf.size()))
        return false;
    record_count_++;
    return true;
}

void wal::sync() {
    std::lock_guard lock(mu_);
    file_.flush();
#if defined(DSE_USE_EPOLL)
    // fsync via dup fd would be cleaner, flush is enough for bench
#endif
}

bool wal::replay(const std::function<void(const wal_record&)>& apply) {
    std::ifstream in(path_, std::ios::binary);
    if (!in.is_open()) return false;

    while (in.peek() != EOF) {
        wal_record rec;
        char op;
        if (!in.read(&op, 1)) break;
        rec.op = static_cast<wal_op>(op);

        char lenbuf[4];
        if (!in.read(lenbuf, 4)) break;
        uint32_t klen = read_u32(lenbuf);
        rec.key.resize(klen);
        if (!in.read(rec.key.data(), klen)) break;

        if (!in.read(lenbuf, 4)) break;
        uint32_t vlen = read_u32(lenbuf);
        rec.value.resize(vlen);
        if (!in.read(rec.value.data(), vlen)) break;

        char exp[8];
        if (!in.read(exp, 8)) break;
        std::memcpy(&rec.expires_at, exp, 8);

        apply(rec);
    }
    return true;
}

}  // namespace dse
