#pragma once

#include "dse/types.hpp"
#include <fstream>
#include <mutex>
#include <string>
#include <vector>
#include <functional>

namespace dse {

enum class wal_op : uint8_t { set = 1, del = 2, incr = 3 };

struct wal_record {
    wal_op op;
    bytes key;
    bytes value;
    int64_t expires_at = 0;
};

class wal {
public:
    explicit wal(const std::string& path);
    ~wal();

    bool append(const wal_record& rec);
    void sync();
    bool replay(const std::function<void(const wal_record&)>& apply);

    uint64_t bytes_written() const { return bytes_written_; }
    uint64_t record_count() const { return record_count_; }

private:
    std::string path_;
    std::fstream file_;
    std::mutex mu_;
    uint64_t bytes_written_ = 0;
    uint64_t record_count_ = 0;

    bool write_raw(const void* data, size_t len);
};

}  // namespace dse
