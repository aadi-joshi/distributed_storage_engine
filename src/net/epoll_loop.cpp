#include "dse/net/epoll_loop.hpp"

#if defined(DSE_USE_EPOLL)

#include <sys/epoll.h>
#include <unistd.h>
#include <cerrno>
#include <vector>

namespace dse::net {

epoll_loop::epoll_loop() {
    epfd_ = epoll_create1(EPOLL_CLOEXEC);
}

epoll_loop::~epoll_loop() {
    if (epfd_ >= 0)
        close(epfd_);
}

bool epoll_loop::add(int fd, uint32_t events, void* userdata) {
    epoll_event ev{};
    ev.events = events | EPOLLET;
    ev.data.ptr = userdata;
    return epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) == 0;
}

bool epoll_loop::mod(int fd, uint32_t events, void* userdata) {
    epoll_event ev{};
    ev.events = events | EPOLLET;
    ev.data.ptr = userdata;
    return epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &ev) == 0;
}

bool epoll_loop::del(int fd) {
    return epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, nullptr) == 0;
}

int epoll_loop::poll(handler cb, int timeout_ms) {
    if (!running_) return 0;
    static thread_local std::vector<epoll_event> events(4096);
    int n = epoll_wait(epfd_, events.data(), static_cast<int>(events.size()), timeout_ms);
    if (n < 0) {
        if (errno == EINTR) return 0;
        return -1;
    }
    for (int i = 0; i < n; ++i) {
        io_event e;
        e.fd = -1;
        e.events = events[i].events;
        e.userdata = events[i].data.ptr;
        cb(e);
    }
    return n;
}

void epoll_loop::stop() {
    running_ = false;
}

}  // namespace dse::net

#else

namespace dse::net {

epoll_loop::epoll_loop() {}
epoll_loop::~epoll_loop() {}
bool epoll_loop::add(int, uint32_t, void*) { return false; }
bool epoll_loop::mod(int, uint32_t, void*) { return false; }
bool epoll_loop::del(int) { return false; }
int epoll_loop::poll(handler, int) { return -1; }
void epoll_loop::stop() {}

}  // namespace dse::net

#endif
