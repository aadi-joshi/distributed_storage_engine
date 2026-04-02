#pragma once

#include "dse/store.hpp"
#include <string>

namespace dse {

class snapshot {
public:
    explicit snapshot(const std::string& dir);

    bool save(const store& s);
    bool load(store& s);

    std::string last_path() const { return last_path_; }

private:
    std::string dir_;
    std::string last_path_;
};

}  // namespace dse
