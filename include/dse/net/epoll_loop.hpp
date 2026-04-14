#pragma once

#include <cstdint>
#include <functional>
#include <vector>

namespace dse::net {

struct io_event {
    int fd;
    uint32_t events;
    void* userdata;
};

class epoll_loop {
public:
    using handler = std::function<void(const io_event&)>;

    epoll_loop();
    ~epoll_loop();

    epoll_loop(const epoll_loop&) = delete;
    epoll_loop& operator=(const epoll_loop&) = delete;

    bool add(int fd, uint32_t events, void* userdata);
    bool mod(int fd, uint32_t events, void* userdata);
    bool del(int fd);

    // timeout_ms: -1 block forever
    int poll(handler cb, int timeout_ms = -1);
    void stop();

    int native_fd() const { return epfd_; }

private:
    int epfd_ = -1;
    bool running_ = true;
};

}  // namespace dse::net
