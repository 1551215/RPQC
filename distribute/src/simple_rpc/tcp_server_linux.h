#pragma once
#include "simple_message.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <functional>
#include <thread>
#include <atomic>
#include <iostream>
#include <cstdlib>
#include <chrono>

typedef int SOCKET;
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close

namespace simple_rpc {

/**
 * 简单的 TCP 服务端（Linux 版本）
 */
class TcpServer {
private:
    SOCKET listen_socket_ = INVALID_SOCKET;
    std::atomic<bool> running_{false};
    int port_ = 0;
    std::function<Message(const Message&)> handler_;

    static uint32_t MaxMsgBytes() {
        const char* v = std::getenv("SIMPLE_RPC_MAX_MSG_MB");
        if (!v || !*v) return 2048U * 1024U * 1024U;
        long long mb = std::atoll(v);
        if (mb <= 0) return 2048U * 1024U * 1024U;
        long long bytes = mb * 1024LL * 1024LL;
        if (bytes > 0xFFFFFFFFLL) bytes = 0xFFFFFFFFLL;
        return static_cast<uint32_t>(bytes);
    }

    static int RpcSocketBufferBytes() {
        const char* v = std::getenv("SIMPLE_RPC_SOCKET_BUF_MB");
        if (!v || !*v) return 4 * 1024 * 1024;
        int mb = std::atoi(v);
        if (mb <= 0) return 4 * 1024 * 1024;
        return mb * 1024 * 1024;
    }

    static void ConfigureSocketOptions(SOCKET s) {
        const int one = 1;
        const int buf = RpcSocketBufferBytes();
        (void)setsockopt(s, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        (void)setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
        (void)setsockopt(s, SOL_SOCKET, SO_SNDBUF, &buf, sizeof(buf));
        (void)setsockopt(s, SOL_SOCKET, SO_RCVBUF, &buf, sizeof(buf));
    }

    static int RecvAll(SOCKET s, void* buf, int len) {
        char* p = static_cast<char*>(buf);
        int got = 0;
        while (got < len) {
            int r = recv(s, p + got, len - got, 0);
            if (r < 0 && errno == EINTR) continue;
            if (r <= 0) return r;
            got += r;
        }
        return got;
    }

    static bool SendAll(SOCKET s, const char* buf, int len) {
        int sent_total = 0;
        while (sent_total < len) {
#ifdef MSG_NOSIGNAL
            int sent = send(s, buf + sent_total, len - sent_total, MSG_NOSIGNAL);
#else
            int sent = send(s, buf + sent_total, len - sent_total, 0);
#endif
            if (sent < 0 && errno == EINTR) continue;
            if (sent <= 0) return false;
            sent_total += sent;
        }
        return true;
    }

public:
    TcpServer(int port) : port_(port) {}

    ~TcpServer() {
        Stop();
    }

    void SetHandler(std::function<Message(const Message&)> handler) {
        handler_ = handler;
    }

    bool Start() {
        listen_socket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listen_socket_ == INVALID_SOCKET) {
            std::cerr << "socket failed: " << strerror(errno) << std::endl;
            return false;
        }

        int opt = 1;
        if (setsockopt(listen_socket_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            std::cerr << "setsockopt failed: " << strerror(errno) << std::endl;
            closesocket(listen_socket_);
            listen_socket_ = INVALID_SOCKET;
            return false;
        }
        ConfigureSocketOptions(listen_socket_);

        sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = inet_addr("0.0.0.0");
        addr.sin_port = htons(port_);

        if (bind(listen_socket_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
            std::cerr << "bind failed: " << strerror(errno) << std::endl;
            closesocket(listen_socket_);
            listen_socket_ = INVALID_SOCKET;
            return false;
        }

        if (listen(listen_socket_, SOMAXCONN) == SOCKET_ERROR) {
            std::cerr << "listen failed: " << strerror(errno) << std::endl;
            closesocket(listen_socket_);
            listen_socket_ = INVALID_SOCKET;
            return false;
        }

        running_ = true;
        std::cout << "[Server] Listening on port " << port_ << std::endl;

        std::thread([this]() {
            while (running_) {
                SOCKET client_socket = accept(listen_socket_, nullptr, nullptr);
                if (client_socket == INVALID_SOCKET) {
                    if (!running_) break;
                    if (errno == EINTR) continue;
                    std::cerr << "[Server] accept failed: " << strerror(errno) << std::endl;
                    continue;
                }

                ConfigureSocketOptions(client_socket);

                std::thread([this, client_socket]() {
                    HandleClient(client_socket);
                }).detach();
            }
        }).detach();

        return true;
    }

    void Stop() {
        running_ = false;
        if (listen_socket_ != INVALID_SOCKET) {
            closesocket(listen_socket_);
            listen_socket_ = INVALID_SOCKET;
        }
    }

private:
    void HandleClient(SOCKET client_socket) {
        try {
            while (running_) {
                char header_buf[MessageHeader::kWireBytes];
                int hr = RecvAll(client_socket, header_buf, static_cast<int>(MessageHeader::kWireBytes));
                if (hr != static_cast<int>(MessageHeader::kWireBytes)) break;

                MessageHeader header;
                header.Deserialize(header_buf);
                if (header.length > MaxMsgBytes()) {
                    std::cerr << "[Server] Message too large: " << header.length
                              << " bytes (limit " << MaxMsgBytes() << " bytes)" << std::endl;
                    break;
                }

                const uint64_t receive_start_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count());
                std::string data;
                if (header.length > 0) {
                    data.resize(header.length);
                    int br = RecvAll(client_socket, &data[0], (int)header.length);
                    if (br != (int)header.length) break;
                }
                const uint64_t receive_end_ns = static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch()).count());

                Message request = Message::FromOwnedPayload(header, std::move(data));
                request.transport_receive_start_ns = receive_start_ns;
                request.transport_receive_end_ns = receive_end_ns;

                Message response;
                if (handler_) {
                    response = handler_(request);
                } else {
                    response = Message(MessageType::RESPONSE_ERROR);
                    response.data = "No handler set";
                }

                auto response_data = response.Serialize();
                if (!SendAll(client_socket, response_data.data(), (int)response_data.size())) {
                    break;
                }
            }
        } catch (const std::exception& e) {
            std::cerr << "[Server] Error handling client: " << e.what() << std::endl;
        }

        closesocket(client_socket);
    }
};

} // namespace simple_rpc

