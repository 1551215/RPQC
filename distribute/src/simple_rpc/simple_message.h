#pragma once
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <cstdlib>
#include <chrono>
#include <zlib.h>

namespace simple_rpc {

/**
 * �򵥵���Ϣ��ʽ
 *
 * ��ʽ��[��Ϣ����:4�ֽ�][payload���ȣ�4�ֽ�][flags:4�ֽ�][raw_len:4�ֽ�][payload:N�ֽ�]
 */

enum class MessageType : uint32_t {
    // Preserve existing numeric IDs; retired prototype IDs are not reused.
    STEP_BASIC = 26,
    EXECUTE_OPEN_SCC = 94,

    // Demand-driven incremental exact-count evaluator. Requests are ACKed
    // immediately; completed relation rows are delivered through callbacks.
    INCREMENTAL_INIT_QUERY = 110,
    INCREMENTAL_START_ROOT = 111,
    INCREMENTAL_REQUEST_ROW = 112,
    INCREMENTAL_ROW_READY = 113,
    INCREMENTAL_BASE_EXPAND = 114,
    INCREMENTAL_BASE_EXPAND_RESULT = 115,
    INCREMENTAL_QUERY_STATUS = 116,
    INCREMENTAL_FETCH_RESULTS = 117,
    INCREMENTAL_ROW_DELTA = 118,
    INCREMENTAL_ROW_COMPLETE = 119,
    INCREMENTAL_ROW_DELTA_BATCH = 120,
    INCREMENTAL_REQUEST_ROW_BATCH = 121,
    INCREMENTAL_ROW_COMPLETE_BATCH = 122,
    INCREMENTAL_ROW_STREAM_BATCH = 123,
    // Frontier-level demand batches.  Unlike REQUEST_ROW_BATCH these messages
    // carry one active-source set and return relation-row deltas as a unit.
    INCREMENTAL_DEMAND_BATCH_REQUEST = 124,
    INCREMENTAL_DEMAND_BATCH_STREAM = 125,
    // Matrix-sharded join data plane for the demand-driven evaluator.
    // The destination worker owns C[i,l] and returns only its new C delta.
    INCREMENTAL_MATRIX_JOIN_UPDATE = 126,
    INCREMENTAL_MATRIX_JOIN_RESULT = 127,
    // Continue a macro expression from a destination-resident matrix block.
    // Kept separate from MATRIX_JOIN_UPDATE because this is control traffic:
    // it contains handles only, never endpoint relation rows.
    INCREMENTAL_MATRIX_RESIDENT_CONTINUE = 128,
    INCREMENTAL_BASE_EXPAND_BATCH = 129,
    INCREMENTAL_BASE_EXPAND_RESULT_BATCH = 130,
    INCREMENTAL_BALANCE_CONTROL = 131,

    // ��Ӧ
    RESPONSE_OK = 100,
    RESPONSE_ERROR = 101,
    RESPONSE_DATA = 102,

    // ����
    HEALTH_CHECK = 200,
    SHUTDOWN = 201
};

/**
 * ��Ϣͷ
 */
struct MessageHeader {
    uint32_t type;      // MessageType
    uint32_t length;    // stored payload length
    uint32_t flags;     // compression flags
    uint32_t raw_length;  // original payload length

    static constexpr uint32_t kFlagCompressedZlib = 1U;
    static constexpr size_t kWireBytes = 16U;

    void Serialize(char* buffer) const {
        memcpy(buffer, &type, 4);
        memcpy(buffer + 4, &length, 4);
        memcpy(buffer + 8, &flags, 4);
        memcpy(buffer + 12, &raw_length, 4);
    }

    void Deserialize(const char* buffer) {
        memcpy(&type, buffer, 4);
        memcpy(&length, buffer + 4, 4);
        memcpy(&flags, buffer + 8, 4);
        memcpy(&raw_length, buffer + 12, 4);
    }
};

/**
 * �򵥵���Ϣ��
 */
class Message {
public:
    MessageType type;
    std::string data;
    // Local-only transport metadata; these fields are intentionally excluded
    // from Serialize() and therefore do not change the wire protocol.
    // Peer timing uses this timestamp to include domain payload construction
    // and serialization before the message enters the sender queue.
    std::chrono::steady_clock::time_point transport_build_started_at =
        std::chrono::steady_clock::now();
    uint64_t transport_receive_start_ns = 0;
    uint64_t transport_receive_end_ns = 0;

private:
    static std::string DecompressZlib(const char* input, uint32_t stored_size, uint32_t raw_size) {
        if (raw_size == 0) return {};
        std::string out;
        out.resize(raw_size);
        uLongf actual = static_cast<uLongf>(raw_size);
        int rc = uncompress(
            reinterpret_cast<Bytef*>(&out[0]),
            &actual,
            reinterpret_cast<const Bytef*>(input),
            static_cast<uLong>(stored_size));
        if (rc != Z_OK || actual != raw_size) {
            throw std::runtime_error("zlib uncompress failed: " + std::to_string(rc));
        }
        return out;
    }

public:
    Message() : type(MessageType::RESPONSE_OK) {}
    Message(MessageType t) : type(t) {}
    Message(MessageType t, const std::string& d) : type(t), data(d) {}

    static bool OwnedBuffersEnabled() {
        return true;
    }

    static Message FromOwnedPayload(const MessageHeader& header, std::string payload) {
        if (payload.size() != header.length)
            throw std::runtime_error("Truncated message payload");
        Message message(static_cast<MessageType>(header.type));
        if (!payload.empty() && (header.flags & MessageHeader::kFlagCompressedZlib) != 0)
            message.data = DecompressZlib(payload.data(), header.length, header.raw_length);
        else message.data = std::move(payload);
        return message;
    }

    // ���л�Ϊ�ֽ���
    std::vector<char> Serialize() const {
        MessageHeader header;
        header.type = static_cast<uint32_t>(type);
        header.length = static_cast<uint32_t>(data.size());
        header.flags = 0;
        header.raw_length = static_cast<uint32_t>(data.size());
        std::vector<char> buffer(MessageHeader::kWireBytes + data.size());
        header.Serialize(buffer.data());
        if (!data.empty())
            memcpy(buffer.data() + MessageHeader::kWireBytes, data.data(), data.size());
        return buffer;
    }

    static Message Deserialize(const char* buffer, size_t size) {
        if (size < MessageHeader::kWireBytes) {
            throw std::runtime_error("Invalid message size");
        }

        MessageHeader header;
        header.Deserialize(buffer);
        if (size < MessageHeader::kWireBytes + header.length) {
            throw std::runtime_error("Truncated message");
        }

        Message msg(static_cast<MessageType>(header.type));
        if (header.length > 0) {
            const char* payload = buffer + MessageHeader::kWireBytes;
            if ((header.flags & MessageHeader::kFlagCompressedZlib) != 0) {
                msg.data = DecompressZlib(payload, header.length, header.raw_length);
            } else {
                msg.data.assign(payload, header.length);
            }
        }
        return msg;
    }

    // ��������������ֶ�
    void AddField(const std::string& key, const std::string& value) {
        data += key + ":" + value + "\n";
    }

    void AddField(const std::string& key, uint64_t value) {
        data += key + ":" + std::to_string(value) + "\n";
    }

    void AddField(const std::string& key, int value) {
        data += key + ":" + std::to_string(value) + "\n";
    }

    // ������������ȡ�ֶ�
    std::string GetField(const std::string& key) const {
        std::istringstream iss(data);
        std::string line;
        while (std::getline(iss, line)) {
            size_t pos = line.find(':');
            if (pos != std::string::npos) {
                std::string k = line.substr(0, pos);
                if (k == key) {
                    return line.substr(pos + 1);
                }
            }
        }
        return "";
    }

    int GetFieldInt(const std::string& key) const {
        std::string value = GetField(key);
        return value.empty() ? 0 : std::stoi(value);
    }

    uint64_t GetFieldUInt64(const std::string& key) const {
        std::string value = GetField(key);
        return value.empty() ? 0 : std::stoull(value);
    }
};

} // namespace simple_rpc
