#include "dse/cluster/peer_client.hpp"
#include <iostream>

int main() {
    dse::cluster::node n{"node-1", "127.0.0.1", 6379};
    dse::cluster::peer_client c;
    auto r = c.request(n, "PING\r\n");
    std::cout << "resp=[" << r << "]\n";
    return r.find("PONG") != std::string::npos ? 0 : 1;
}
