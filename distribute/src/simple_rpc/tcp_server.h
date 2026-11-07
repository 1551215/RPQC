#pragma once
#ifdef _WIN32
#include "simple_message.h"
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iostream>
#include <functional>
#include <thread>
#include <atomic>

#pragma comment(lib, "ws2_32.lib")

namespace simple_rpc {

/**
 * 简单的 TCP 服务器
 */
class TcpServer {
private:
    SOCKET listen_socket_;
    std::atomic<bool> running_;
    int port_;
    std::function<Message(const Message&)> handler_;
    
public:
    TcpServer(int port) : listen_socket_(INVALID_SOCKET), running_(false), port_(port) {
        // 初始化 Winsock
        WSADATA wsaData;
        int result = WSAStartup(MAKEWORD(2, 2), &wsaData);
        if (result != 0) {
            throw std::runtime_error("WSAStartup failed: " + std::to_string(result));
        }
    }
    
    ~TcpServer() {
        Stop();
        WSACleanup();
    }
    
    // 设置消息处理函数
    void SetHandler(std::function<Message(const Message&)> handler) {
        handler_ = handler;
    }
    
    // 启动服务器
    bool Start() {
        // 创建监听 socket
        listen_socket_ = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (listen_socket_ == INVALID_SOCKET) {
            std::cerr << "socket failed: " << WSAGetLastError() << std::endl;
            return false;
        }
        
        // 绑定端口
        sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);
        
        if (bind(listen_socket_, (sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
            std::cerr << "bind failed: " << WSAGetLastError() << std::endl;
            closesocket(listen_socket_);
            return false;
        }
        
        // 开始监听
        if (listen(listen_socket_, SOMAXCONN) == SOCKET_ERROR) {
            std::cerr << "listen failed: " << WSAGetLastError() << std::endl;
            closesocket(listen_socket_);
            return false;
        }
        
        running_ = true;
        std::cout << "[Server] Listening on port " << port_ << std::endl;
        
        // 接受连接（在单独的线程中）
        std::thread accept_thread([this]() {
            while (running_) {
                SOCKET client_socket = accept(listen_socket_, nullptr, nullptr);
                if (client_socket == INVALID_SOCKET) {
                    if (running_) {
                        std::cerr << "[Server] accept failed: " << WSAGetLastError() << std::endl;
                    }
                    continue;
                }
                
                // 处理客户端（在新线程中）
                std::thread([this, client_socket]() {
                    HandleClient(client_socket);
                }).detach();
            }
        });
        accept_thread.detach();
        
        return true;
    }
    
    // 停止服务器
    void Stop() {
        running_ = false;
        if (listen_socket_ != INVALID_SOCKET) {
            closesocket(listen_socket_);
            listen_socket_ = INVALID_SOCKET;
        }
    }
    
private:
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

    void HandleClient(SOCKET client_socket) {
        try {
            // Keep-alive: handle multiple requests on one TCP connection until client closes.
            while (true) {
                // 读取消息头（必须循环接收）
                char header_buf[8];
                int hr = RecvAll(client_socket, header_buf, 8);
                if (hr != 8) {
                    break; // connection closed
                }

                MessageHeader header;
                header.Deserialize(header_buf);

                const uint32_t MAX_MSG = 512U * 1024U * 1024U; // 512MB
                if (header.length > MAX_MSG) {
                    std::cerr << "[Server] Message too large" << std::endl;
                    break;
                }

                std::string data;
                if (header.length > 0) {
                    data.resize(header.length);
                    int br = RecvAll(client_socket, data.data(), (int)header.length);
                    if (br != (int)header.length) {
                        break;
                    }
                }

                Message request(static_cast<MessageType>(header.type));
                request.data = std::move(data);

                Message response;
                if (handler_) {
                    response = handler_(request);
                } else {
                    response = Message(MessageType::RESPONSE_ERROR);
                    response.data = "No handler set";
                }

                auto response_data = response.Serialize();
                if (!SendAll(client_socket, response_data.data(), (int)response_data.size())) {
                    std::cerr << "[Server] Failed to send response" << std::endl;
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
#else
#include "tcp_server_linux.h"
#endif
