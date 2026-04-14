#include "dse/cluster/peer_client.hpp"

#include <iostream>
#include <string>

int main() {
    dse::cluster::node n1{"node-1", "127.0.0.1", 6379};
    dse::cluster::node n2{"node-2", "127.0.0.1", 6380};

    dse::cluster::peer_client client;

    auto set = std::string("*3\r\n$3\r\nSET\r\n$10\r\nclusterkey\r\n$5\r\nhello\r\n");
    auto get = std::string("*2\r\n$3\r\nGET\r\n$10\r\nclusterkey\r\n");

    auto set_resp = client.request(n1, set);
    if (set_resp.empty() || set_resp[0] != '+') {
        std::cerr << "set failed: " << set_resp << "\n";
        return 1;
    }

    auto get_resp = client.request(n2, get);
    std::cout << "get: " << get_resp << "\n";

    if (get_resp.find("hello") == std::string::npos) {
        std::cerr << "FAIL routing\n";
        return 1;
    }

    std::cout << "PASS cluster routing\n";
    return 0;
}
