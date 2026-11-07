#pragma once
#ifdef _WIN32
#include "simple_message.h"
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <stdexcept>

#pragma comment(lib, "ws2_32.lib")

namespace simple_rpc {

/**
 * 简单的 TCP 客户端
 */
class TcpClient {
private:
    SOCKET socket_;
    std::string host_;
    int port_;
    bool connected_;
    
public:
    TcpClient(const std::string& host, int port) 
        : socket_(INVALID_SOCKET), host_(host), port_(port), connected_(false) {
        // 初始化 Winsock
        WSADATA wsaData;
        int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (result != 0) {
            throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
        }
    }
    
    ~TcpClient() {
        Disconnect();
        WSACleanup();
    }
    
    // 连接到服务器
    bool Connect() {
        if (connected_) return true;
        
        // 创建 socket
        socket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (socket_ == INVALID_SOCKET) {
            std::cerr << "[Client] socket failed: " << WSAGetLastError() << std::endl;
            return false;
        }
        
        // 连接到服务器
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port_);
        inet_pton(AF_INET, host_.c_str(), &addr.sin_addr);
        
        if (connect(socket_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
            std::cerr << "[Client] connect failed: " << WSAGetLastError() << std::endl;
            closesocket(socket_);
            socket_ = INVALID_SOCKET;
            return false;
        }
        
        connected_ = true;
        return true;
    }
    
    // 断开连接
    void Disconnect() {
        if (socket_ != INVALID_SOCKET) {
            closesocket(socket_);
            socket_ = INVALID_SOCKET;
        }
        connected_ = false;
    }
    
    static int RecvAll(SOCKET s, char* buf, int len) {
        int got = 0;
        while (got < len) {
            int r = recv(s, buf + got, len - got, 0);
            if (r <= 0) return r;
            got += r;
        }
        return got;
    }

    static bool SendAll(SOCKET s, const char* buf, int len) {
        int sent_total = 0;
        while (sent_total < len) {
            int sent = send(s, buf + sent_total, len - sent_total, 0);
            if (sent <= 0) return false;
            sent_total += sent;
        }
        return true;
    }

    // 发送消息并接收响应
    Message SendRequest(const Message& request) {
        // Keep-alive: reuse a single TCP connection across requests.
        // Retry once on broken connection.
        for (int attempt = 0; attempt < 2; attempt++) {
            if (!connected_) {
                if (!Connect()) {
                    throw std::runtime_error("Failed to connect to server");
                }
            }

            // 发送请求
            auto request_data = request.Serialize();
            if (!SendAll(socket_, request_data.data(), (int)request_data.size())) {
                Disconnect();
                continue;
            }

            // 接收响应头（必须循环接收）
            char header_buf[8];
            int hr = RecvAll(socket_, header_buf, 8);
            if (hr != 8) {
                Disconnect();
                continue;
            }

            MessageHeader header;
            header.Deserialize(header_buf);

            // 简单防护：限制单条消息最大长度（避免异常 length 导致内存问题）
            const uint32_t MAX_MSG = 512U * 1024U * 1024U; // 512MB
            if (header.length > MAX_MSG) {
                Disconnect();
                throw std::runtime_error("Response too large");
            }

            // 接收响应体
            std::string data;
            if (header.length > 0) {
                data.resize(header.length);
                int br = RecvAll(socket_, data.data(), (int)header.length);
                if (br != (int)header.length) {
                    Disconnect();
                    continue;
                }
            }

            Message resp(static_cast<MessageType>(header.type));
            resp.data = std::move(data);
            return resp;
        }

        throw std::runtime_error("RPC failed after reconnect attempt");
    }
    
    bool IsConnected() const { return connected_; }
};

} // namespace simple_rpc
#else
#include "tcp_client_linux.h"
#endif
