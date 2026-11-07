#pragma once
#include "simple_message.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <sys/time.h>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>
#include <cstdlib>
#include <array>
#include <chrono>
#include <limits>
#include <poll.h>

typedef int SOCKET;
#define INVALID_SOCKET -1
#define SOCKET_ERROR -1
#define closesocket close

namespace simple_rpc {

/**
 * 简单的 TCP 客户端（Linux 版本）
 */
class TcpClient {
private:
    SOCKET socket_ = INVALID_SOCKET;
    std::string host_;
    int port_ = 0;
    bool connected_ = false;
    bool response_pending_ = false;
    std::array<char, MessageHeader::kWireBytes> pending_header_{};
    size_t pending_header_bytes_ = 0, pending_body_bytes_ = 0;
    MessageHeader pending_response_header_;
    std::string pending_body_;
    std::chrono::steady_clock::time_point response_progress_at_{};

    bool ReadAvailable(char* destination, size_t size, size_t& received) {
        while (received < size) {
            const ssize_t count = recv(socket_, destination + received,
                                       size - received, MSG_DONTWAIT);
            if (count < 0 && errno == EINTR) continue;
            if (count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return false;
            if (count <= 0) throw std::runtime_error("RPC response connection closed");
            received += static_cast<size_t>(count);
            response_progress_at_ = std::chrono::steady_clock::now();
        }
        return true;
    }

    bool TryReceiveResponse(Message& response) {
        try {
            if (!ReadAvailable(pending_header_.data(), pending_header_.size(),
                               pending_header_bytes_)) return false;
            if (pending_body_.empty() && pending_body_bytes_ == 0) {
                pending_response_header_.Deserialize(pending_header_.data());
                if (pending_response_header_.length > MAX_MSG)
                    throw std::runtime_error("RPC response exceeds wire length limit");
                pending_body_.resize(pending_response_header_.length);
            }
            if (!ReadAvailable(pending_body_.data(), pending_body_.size(),
                               pending_body_bytes_)) return false;
            response = Message::FromOwnedPayload(pending_response_header_,
                                                std::move(pending_body_));
            response_pending_ = false;
            pending_header_bytes_ = pending_body_bytes_ = 0;
            return true;
        } catch (const std::exception& error) {
            Disconnect();
            throw std::runtime_error(std::string(error.what()) + " host=" + host_ +
                                     " port=" + std::to_string(port_));
        }
    }

    // Leave room for the wire header within signed-int send/receive lengths.
    static constexpr uint32_t MAX_MSG = 2047U * 1024U * 1024U;

    static int RpcTimeoutSec() {
        const char* v = std::getenv("SIMPLE_RPC_TIMEOUT_SEC");
        if (!v || !*v) return 300;
        int sec = std::atoi(v);
        if (sec <= 0) return 300;
        return sec;
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

    bool ConnectImpl(std::string* err) {
        if (connected_) return true;

        socket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_ == INVALID_SOCKET) {
            if (err) *err = "socket failed: " + std::string(strerror(errno));
            return false;
        }

        ConfigureSocketOptions(socket_);

        // Best-effort timeouts to avoid hanging forever on dead peers.
        struct timeval tv;
        tv.tv_sec = RpcTimeoutSec();
        tv.tv_usec = 0;
        (void)setsockopt(socket_, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        (void)setsockopt(socket_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port_);

        if (inet_pton(AF_INET, host_.c_str(), &server_addr.sin_addr) <= 0) {
            if (err) *err = "Invalid address: " + host_;
            closesocket(socket_);
            socket_ = INVALID_SOCKET;
            return false;
        }

        if (connect(socket_, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
            if (err) *err = "connect failed: " + std::string(strerror(errno));
            closesocket(socket_);
            socket_ = INVALID_SOCKET;
            return false;
        }

        connected_ = true;
        return true;
    }

public:
    TcpClient(const std::string& host, int port) : host_(host), port_(port) {}

    ~TcpClient() {
        Disconnect();
    }

    bool Connect() {
        std::string err;
        if (!ConnectImpl(&err)) {
            throw std::runtime_error(err.empty() ? "connect failed" : err);
        }
        return true;
    }

    void Disconnect() {
        if (socket_ != INVALID_SOCKET) {
            closesocket(socket_);
            socket_ = INVALID_SOCKET;
        }
        connected_ = false;
        response_pending_ = false;
        pending_header_bytes_ = pending_body_bytes_ = 0;
        pending_body_.clear();
    }

    // One outstanding request per connection. Different connections can make
    // progress while the calling thread receives another worker's response.
    void BeginRequest(const Message& request) {
        if (response_pending_) throw std::runtime_error("RPC response still pending");
        const auto wire = request.Serialize();
        for (int attempt = 0; attempt < 2; ++attempt) {
            Connect();
            if (SendAll(socket_, wire.data(), static_cast<int>(wire.size()))) {
                response_pending_ = true;
                pending_header_bytes_ = pending_body_bytes_ = 0;
                pending_body_.clear();
                response_progress_at_ = std::chrono::steady_clock::now();
                return;
            }
            Disconnect();
        }
        throw std::runtime_error("RPC request send failed host=" + host_ +
                                 " port=" + std::to_string(port_));
    }

    static bool TryReceiveAnyResponse(const std::vector<TcpClient*>& clients,
                                      Message& response, size_t& index) {
        try {
            for (size_t i = 0; i < clients.size(); ++i) {
                auto* client = clients[i];
                if (client == nullptr) continue;
                if (!client->response_pending_)
                    throw std::runtime_error("No pending RPC response");
                if (client->TryReceiveResponse(response)) {
                    index = i;
                    return true;
                }
            }
            return false;
        } catch (...) {
            for (auto* client : clients) if (client != nullptr) client->Disconnect();
            throw;
        }
    }

    // Drain partial frames on one calling thread. A slow response must not
    // prevent decoding a complete response already available on another socket.
    // The caller removes each returned client before calling again.
    static size_t ReceiveAnyResponse(const std::vector<TcpClient*>& clients,
                                    Message& response) {
        try {
            for (;;) {
                std::vector<pollfd> pending;
                int timeout_ms = std::numeric_limits<int>::max();
                for (size_t index = 0; index < clients.size(); ++index) {
                    auto* client = clients[index];
                    if (client == nullptr) continue;
                    if (!client->response_pending_)
                        throw std::runtime_error("No pending RPC response");
                    if (client->TryReceiveResponse(response)) return index;
                    const auto idle_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - client->response_progress_at_).count();
                    const int64_t remaining = static_cast<int64_t>(RpcTimeoutSec()) * 1000 - idle_ms;
                    if (remaining <= 0)
                        throw std::runtime_error("RPC response timeout host=" + client->host_);
                    timeout_ms = std::min<int64_t>(timeout_ms, remaining);
                    pending.push_back(pollfd{client->socket_, POLLIN, 0});
                }
                if (pending.empty()) throw std::runtime_error("No pending RPC clients");
                const int ready = poll(pending.data(), pending.size(), timeout_ms);
                if (ready < 0 && errno != EINTR)
                    throw std::runtime_error("RPC response poll failed");
                for (const auto& socket : pending)
                    if (socket.revents & POLLNVAL)
                        throw std::runtime_error("Invalid pending RPC socket");
            }
        } catch (...) {
            for (auto* client : clients) if (client != nullptr) client->Disconnect();
            throw;
        }
    }

    Message ReceiveResponse() {
        if (!response_pending_) throw std::runtime_error("No pending RPC response");
        try {
            char wire[MessageHeader::kWireBytes];
            if (RecvAll(socket_, wire, sizeof(wire)) != sizeof(wire))
                throw std::runtime_error("RPC response header unavailable");
            MessageHeader header;
            header.Deserialize(wire);
            if (header.length > MAX_MSG)
                throw std::runtime_error("Response too large: bytes=" +
                    std::to_string(header.length) + " limit=" + std::to_string(MAX_MSG));
            std::string data(header.length, '\0');
            if (header.length && RecvAll(socket_, &data[0], header.length) !=
                                     static_cast<int>(header.length))
                throw std::runtime_error("RPC response body unavailable");
            response_pending_ = false;
            return Message::FromOwnedPayload(header, std::move(data));
        } catch (const std::exception& error) {
            Disconnect();
            throw std::runtime_error(std::string(error.what()) + " host=" + host_ +
                                     " port=" + std::to_string(port_));
        }
    }

    Message SendRequest(const Message& request) {
        if (response_pending_) throw std::runtime_error("RPC response still pending");
        // Keep-alive: reuse a single TCP connection across requests.
        // Retry once on broken connection.
        std::string last_error = "unknown";
        for (int attempt = 0; attempt < 2; attempt++) {
            if (!connected_) {
                std::string err;
                if (!ConnectImpl(&err)) {
                    last_error = err.empty() ? "connect failed" : err;
                    throw std::runtime_error(last_error);
                }
            }

            auto request_data = request.Serialize();
            if (!SendAll(socket_, request_data.data(), (int)request_data.size())) {
                last_error = "send failed: " + std::string(strerror(errno));
                Disconnect();
                continue;
            }

            char header_buf[MessageHeader::kWireBytes];
            int hr = RecvAll(socket_, header_buf, static_cast<int>(MessageHeader::kWireBytes));
            if (hr != static_cast<int>(MessageHeader::kWireBytes)) {
                if (hr == 0) {
                    last_error = "recv header failed: peer closed connection";
                } else {
                    last_error = "recv header failed: " + std::string(strerror(errno));
                }
                Disconnect();
                continue;
            }

            MessageHeader header;
            header.Deserialize(header_buf);
            if (header.length > MAX_MSG) {
                Disconnect();
                throw std::runtime_error(
                    "Response too large: bytes=" + std::to_string(header.length) +
                    " limit=" + std::to_string(MAX_MSG) +
                    " host=" + host_ + " port=" + std::to_string(port_));
            }

            std::string data;
            if (header.length > 0) {
                data.resize(header.length);
                int br = RecvAll(socket_, &data[0], (int)header.length);
                if (br != (int)header.length) {
                    if (br == 0) {
                        last_error = "recv body failed: peer closed connection";
                    } else {
                        last_error = "recv body failed: " + std::string(strerror(errno));
                    }
                    Disconnect();
                    continue;
                }
            }

            return Message::FromOwnedPayload(header, std::move(data));
        }

        throw std::runtime_error(
            "RPC failed after reconnect attempt"
            " host=" + host_ +
            " port=" + std::to_string(port_) +
            " timeout_sec=" + std::to_string(RpcTimeoutSec()) +
            " last_error=" + last_error);
    }

    bool IsConnected() const {
        return connected_;
    }
};

} // namespace simple_rpc

