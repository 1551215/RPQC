#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <zlib.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#include "final_config.h"
#include "vendor/roaring/roaring.hh"
#include "FixedBitmapUnion.h"
#include "SharedBitmap64.h"
#include "partition_router.h"
#include "graph.h"
#include "src/partition/graph_partition.h"
#include "src/simple_rpc/tcp_client.h"
#include "src/simple_rpc/tcp_server.h"

using namespace simple_rpc;
using namespace rpq;
using namespace std;

static int64_t IncrementalTraceWallUs() {
    return chrono::duration_cast<chrono::microseconds>(
        chrono::system_clock::now().time_since_epoch()).count();
}

static bool IncrementalScalingTraceEnabled() {
    static const bool enabled = [] {
        const char* value = getenv("INCREMENTAL_TRACE_SCALING");
        return value != nullptr && string(value) == "1";
    }();
    return enabled;
}

#include "../../modules/query_regex.inc"

static bool VerboseEnabled() {
    const char* v = std::getenv("DO_BYMYSELF_VERBOSE");
    if (!v || !*v) return false;
    std::string s(v);
    return s != "0" && s != "false" && s != "FALSE";
}

static string NowTimeString() {
    using namespace std::chrono;
    auto now = system_clock::now();
    time_t tt = system_clock::to_time_t(now);
    tm tm_buf{};
#ifdef _WIN32
    localtime_s(&tm_buf, &tt);
#else
    localtime_r(&tt, &tm_buf);
#endif
    char buf[32];
    strftime(buf, sizeof(buf), "%F %T", &tm_buf);
    return string(buf);
}

static void LogVerbose(const string& who, const string& msg) {
    if (!VerboseEnabled()) return;
    cerr << "[" << NowTimeString() << "][" << who << "] " << msg << endl;
}

static void SortUnique(vector<uint64_t>& values) {
    sort(values.begin(), values.end());
    values.erase(unique(values.begin(), values.end()), values.end());
}

static string EncodeMidMaskMap(const unordered_map<uint64_t, uint32_t>& map) {
    vector<pair<uint64_t, uint32_t>> items(map.begin(), map.end());
    sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    ostringstream oss;
    bool first = true;
    for (const auto& [mid, mask] : items) {
        if (!first) oss << ";";
        first = false;
        oss << mid << "," << mask;
    }
    return oss.str();
}

static unordered_map<uint64_t, uint32_t> DecodeMidMaskMap(const string& text) {
    unordered_map<uint64_t, uint32_t> out;
    stringstream ss(text);
    string token;
    while (getline(ss, token, ';')) {
        if (token.empty()) continue;
        stringstream ts(token);
        string a, b;
        if (!getline(ts, a, ',')) continue;
        if (!getline(ts, b, ',')) continue;
        out[stoull(a)] |= static_cast<uint32_t>(stoul(b));
    }
    return out;
}

static string EncodeU32Set(const unordered_set<uint32_t>& values) {
    vector<uint32_t> ordered(values.begin(), values.end());
    sort(ordered.begin(), ordered.end());
    ostringstream oss;
    bool first = true;
    for (uint32_t v : ordered) {
        if (!first) oss << ",";
        first = false;
        oss << v;
    }
    return oss.str();
}

static unordered_set<uint32_t> DecodeU32Set(const string& text) {
    unordered_set<uint32_t> out;
    stringstream ss(text);
    string token;
    while (getline(ss, token, ',')) {
        if (token.empty()) continue;
        out.insert(static_cast<uint32_t>(stoul(token)));
    }
    return out;
}

static uint64_t PopcountU32(uint32_t v) {
    return static_cast<uint64_t>(__builtin_popcount(v));
}

static constexpr uint32_t kCompressedPayloadMagic = 0x504d4f43;  // COMP
static constexpr uint32_t kCompressedPayloadVersion = 1;

static void AppendRawU32(string& out, uint32_t v) {
    out.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static void AppendRawU64(string& out, uint64_t v) {
    out.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static bool ReadRawU32(const string& in, size_t& off, uint32_t& v) {
    if (off + sizeof(uint32_t) > in.size()) return false;
    memcpy(&v, in.data() + off, sizeof(v));
    off += sizeof(v);
    return true;
}

static bool ReadRawU64(const string& in, size_t& off, uint64_t& v) {
    if (off + sizeof(uint64_t) > in.size()) return false;
    memcpy(&v, in.data() + off, sizeof(v));
    off += sizeof(v);
    return true;
}

static string WrapCompressedPayload(const string& raw) {
    string compressed;
    uLongf bound = compressBound(static_cast<uLong>(raw.size()));
    compressed.resize(static_cast<size_t>(bound));
    uLongf actual = bound;
    int zrc = compress2(
        reinterpret_cast<Bytef*>(compressed.data()),
        &actual,
        reinterpret_cast<const Bytef*>(raw.data()),
        static_cast<uLong>(raw.size()),
        Z_BEST_SPEED);
    bool use_compressed = (zrc == Z_OK && actual < raw.size());
    if (use_compressed) compressed.resize(static_cast<size_t>(actual));
    else compressed.clear();

    string out;
    AppendRawU32(out, kCompressedPayloadMagic);
    AppendRawU32(out, kCompressedPayloadVersion);
    AppendRawU32(out, use_compressed ? 1u : 0u);
    AppendRawU64(out, static_cast<uint64_t>(raw.size()));
    out.append(use_compressed ? compressed : raw);
    return out;
}

static bool UnwrapCompressedPayload(const string& payload, string& raw, string& err) {
    raw.clear();
    if (payload.empty()) return true;
    size_t off = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t flag = 0;
    uint64_t raw_size = 0;
    if (!ReadRawU32(payload, off, magic) || !ReadRawU32(payload, off, version) ||
        !ReadRawU32(payload, off, flag) || !ReadRawU64(payload, off, raw_size)) {
        err = "Bad compressed payload header";
        return false;
    }
    if (magic != kCompressedPayloadMagic || version != kCompressedPayloadVersion) {
        err = "Unexpected compressed payload format";
        return false;
    }
    if (flag == 0) {
        raw.assign(payload.data() + off, payload.size() - off);
        return true;
    }
    raw.resize(static_cast<size_t>(raw_size));
    uLongf dest_len = static_cast<uLongf>(raw.size());
    int zrc = uncompress(
        reinterpret_cast<Bytef*>(raw.data()),
        &dest_len,
        reinterpret_cast<const Bytef*>(payload.data() + off),
        static_cast<uLong>(payload.size() - off));
    if (zrc != Z_OK) {
        err = "Compressed payload inflate failed";
        raw.clear();
        return false;
    }
    raw.resize(static_cast<size_t>(dest_len));
    return true;
}

static string HexEncode(const string& raw) {
    static const char* kHex = "0123456789abcdef";
    string out;
    out.resize(raw.size() * 2);
    for (size_t i = 0; i < raw.size(); ++i) {
        unsigned char c = static_cast<unsigned char>(raw[i]);
        out[i * 2] = kHex[c >> 4];
        out[i * 2 + 1] = kHex[c & 0x0f];
    }
    return out;
}

static bool HexDecode(const string& text, string& raw, string& err) {
    raw.clear();
    if (text.empty()) return true;
    if ((text.size() & 1u) != 0) {
        err = "Bad hex payload length";
        return false;
    }
    auto hex_value = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return 10 + (c - 'a');
        if (c >= 'A' && c <= 'F') return 10 + (c - 'A');
        return -1;
    };
    raw.resize(text.size() / 2);
    for (size_t i = 0; i < raw.size(); ++i) {
        int hi = hex_value(text[i * 2]);
        int lo = hex_value(text[i * 2 + 1]);
        if (hi < 0 || lo < 0) {
            err = "Bad hex payload digit";
            raw.clear();
            return false;
        }
        raw[i] = static_cast<char>((hi << 4) | lo);
    }
    return true;
}

static constexpr uint32_t kMidMaskPayloadMagic = 0x4b53414d;  // MASK
static constexpr uint32_t kMidMaskPayloadVersion = 1;

static string EncodeMidMaskMapBinary(const unordered_map<uint64_t, uint32_t>& map) {
    string raw;
    AppendRawU32(raw, kMidMaskPayloadMagic);
    AppendRawU32(raw, kMidMaskPayloadVersion);
    AppendRawU32(raw, static_cast<uint32_t>(map.size()));
    vector<pair<uint64_t, uint32_t>> items(map.begin(), map.end());
    sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [mid, mask] : items) {
        AppendRawU64(raw, mid);
        AppendRawU32(raw, mask);
    }
    return HexEncode(WrapCompressedPayload(raw));
}

static bool DecodeMidMaskMapBinary(const string& payload, unordered_map<uint64_t, uint32_t>& out, string& err) {
    out.clear();
    if (payload.empty()) return true;
    string binary_payload;
    if (!HexDecode(payload, binary_payload, err)) return false;
    string raw;
    if (!UnwrapCompressedPayload(binary_payload, raw, err)) return false;
    size_t off = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t count = 0;
    if (!ReadRawU32(raw, off, magic) || !ReadRawU32(raw, off, version) || !ReadRawU32(raw, off, count)) {
        err = "Bad mid-mask payload header";
        return false;
    }
    if (magic != kMidMaskPayloadMagic || version != kMidMaskPayloadVersion) {
        err = "Unexpected mid-mask payload format";
        return false;
    }
    for (uint32_t i = 0; i < count; ++i) {
        uint64_t mid = 0;
        uint32_t mask = 0;
        if (!ReadRawU64(raw, off, mid) || !ReadRawU32(raw, off, mask)) {
            err = "Bad mid-mask payload entry";
            out.clear();
            return false;
        }
        out[mid] |= mask;
    }
    return true;
}

static constexpr uint32_t kRsfmProbePayloadMagic = 0x42505352;  // RSPB
static constexpr uint32_t kRsfmProbePayloadVersion = 1;

static string SerializeRsfmProbePayload(
    bool active,
    const unordered_map<uint64_t, uint32_t>& mid_masks)
{
    string raw;
    AppendRawU32(raw, kRsfmProbePayloadMagic);
    AppendRawU32(raw, kRsfmProbePayloadVersion);
    AppendRawU32(raw, active ? 1u : 0u);
    AppendRawU32(raw, static_cast<uint32_t>(mid_masks.size()));
    vector<pair<uint64_t, uint32_t>> items(mid_masks.begin(), mid_masks.end());
    sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [mid, mask] : items) {
        AppendRawU64(raw, mid);
        AppendRawU32(raw, mask);
    }
    return WrapCompressedPayload(raw);
}

static bool ParseRsfmProbePayload(
    const string& payload,
    bool& active,
    unordered_map<uint64_t, uint32_t>& mid_masks,
    string& err)
{
    mid_masks.clear();
    string raw;
    if (!UnwrapCompressedPayload(payload, raw, err)) return false;
    size_t off = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t active_u = 0;
    uint32_t mid_count = 0;
    if (!ReadRawU32(raw, off, magic) || !ReadRawU32(raw, off, version) || !ReadRawU32(raw, off, active_u)) {
        err = "Bad rsfm probe payload header";
        return false;
    }
    if (magic != kRsfmProbePayloadMagic || version != kRsfmProbePayloadVersion) {
        err = "Unexpected rsfm probe payload format";
        return false;
    }
    active = (active_u != 0);
    if (!ReadRawU32(raw, off, mid_count)) {
        err = "Bad rsfm probe mid count";
        return false;
    }
    for (uint32_t i = 0; i < mid_count; ++i) {
        uint64_t mid = 0;
        uint32_t mask = 0;
        if (!ReadRawU64(raw, off, mid) || !ReadRawU32(raw, off, mask)) {
            err = "Bad rsfm probe mid entry";
            return false;
        }
        mid_masks[mid] |= mask;
    }
    return true;
}

static constexpr uint32_t kRsfmLeftDispatchPayloadMagic = 0x44505352;  // RSPD
static constexpr uint32_t kRsfmLeftDispatchPayloadVersion = 4;

enum class RsfmWindowMode : uint32_t {
    PairLimit = 0,
    FixedCount = 1,
};

static string SerializeRsfmLeftDispatchPayload(
    int query_id,
    int left_len,
    int right_len,
    int output_len,
    int mid,
    int output_shards,
    int thread_count,
    uint64_t blpc_block_size,
    uint32_t window_mode,
    uint64_t window_pair_limit,
    uint32_t fixed_window_count,
    const unordered_map<uint64_t, uint32_t>& rsfm_signature_by_middle)
{
    string raw;
    AppendRawU32(raw, kRsfmLeftDispatchPayloadMagic);
    AppendRawU32(raw, kRsfmLeftDispatchPayloadVersion);
    AppendRawU32(raw, static_cast<uint32_t>(query_id));
    AppendRawU32(raw, static_cast<uint32_t>(left_len));
    AppendRawU32(raw, static_cast<uint32_t>(right_len));
    AppendRawU32(raw, static_cast<uint32_t>(output_len));
    AppendRawU32(raw, static_cast<uint32_t>(mid));
    AppendRawU32(raw, static_cast<uint32_t>(output_shards));
    AppendRawU32(raw, static_cast<uint32_t>(thread_count));
    AppendRawU64(raw, blpc_block_size);
    AppendRawU32(raw, window_mode);
    AppendRawU64(raw, window_pair_limit);
    AppendRawU32(raw, fixed_window_count);
    AppendRawU32(raw, static_cast<uint32_t>(rsfm_signature_by_middle.size()));
    vector<pair<uint64_t, uint32_t>> items(rsfm_signature_by_middle.begin(), rsfm_signature_by_middle.end());
    sort(items.begin(), items.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
    for (const auto& [mid_vertex, mask] : items) {
        AppendRawU64(raw, mid_vertex);
        AppendRawU32(raw, mask);
    }
    return WrapCompressedPayload(raw);
}

static bool ParseRsfmLeftDispatchPayload(
    const string& payload,
    int& query_id,
    int& left_len,
    int& right_len,
    int& output_len,
    int& mid,
    int& output_shards,
    int& thread_count,
    uint64_t& blpc_block_size,
    uint32_t& window_mode,
    uint64_t& window_pair_limit,
    uint32_t& fixed_window_count,
    unordered_map<uint64_t, uint32_t>& rsfm_signature_by_middle,
    string& err)
{
    rsfm_signature_by_middle.clear();
    string raw;
    if (!UnwrapCompressedPayload(payload, raw, err)) return false;
    size_t off = 0;
    uint32_t magic = 0;
    uint32_t version = 0;
    uint32_t qid = 0;
    uint32_t ll = 0;
    uint32_t rl = 0;
    uint32_t out_len = 0;
    uint32_t mid_u = 0;
    uint32_t out_shards = 0;
    uint32_t threads = 0;
    uint32_t mode = 0;
    uint64_t block_size = 0;
    uint64_t limit = 0;
    uint32_t count = 0;
    if (!ReadRawU32(raw, off, magic) || !ReadRawU32(raw, off, version) ||
        !ReadRawU32(raw, off, qid) || !ReadRawU32(raw, off, ll) ||
        !ReadRawU32(raw, off, rl) || !ReadRawU32(raw, off, out_len) ||
        !ReadRawU32(raw, off, mid_u) || !ReadRawU32(raw, off, out_shards) ||
        !ReadRawU32(raw, off, threads) || !ReadRawU64(raw, off, block_size) ||
        !ReadRawU32(raw, off, mode) || !ReadRawU64(raw, off, limit) ||
        !ReadRawU32(raw, off, count)) {
        err = "Bad rsfm left dispatch payload header";
        return false;
    }
    if (magic != kRsfmLeftDispatchPayloadMagic || version != kRsfmLeftDispatchPayloadVersion) {
        err = "Unexpected rsfm left dispatch payload format";
        return false;
    }
    query_id = static_cast<int>(qid);
    left_len = static_cast<int>(ll);
    right_len = static_cast<int>(rl);
    output_len = static_cast<int>(out_len);
    mid = static_cast<int>(mid_u);
    output_shards = static_cast<int>(out_shards);
    thread_count = static_cast<int>(threads);
    blpc_block_size = block_size;
    window_mode = mode;
    window_pair_limit = limit;
    fixed_window_count = count;
    uint32_t mid_count = 0;
    if (!ReadRawU32(raw, off, mid_count)) {
        err = "Bad rsfm left dispatch mid count";
        return false;
    }
    for (uint32_t i = 0; i < mid_count; ++i) {
        uint64_t mid_vertex = 0;
        uint32_t mask = 0;
        if (!ReadRawU64(raw, off, mid_vertex) || !ReadRawU32(raw, off, mask)) {
            err = "Bad rsfm left dispatch mid entry";
            return false;
        }
        rsfm_signature_by_middle[mid_vertex] |= mask;
    }
    return true;
}

struct RelationDstSet {
    SharedBitmap64 bitmap;

    void push_back(uint64_t value) { bitmap.add(value); }
    void reserve(size_t) {}
    bool empty() const { return bitmap.isEmpty(); }
    size_t size() const { return static_cast<size_t>(bitmap.cardinality()); }
    uint64_t cardinality() const { return bitmap.cardinality(); }
    auto begin() const { return bitmap.begin(); }
    auto end() const { return bitmap.end(); }
    RelationDstSet& operator|=(const RelationDstSet& other) {
        bitmap |= other.bitmap;
        return *this;
    }
    template <typename DummyIt, typename It>
    void insert(DummyIt, It first, It last) {
        for (; first != last; ++first) {
            bitmap.add(*first);
        }
    }
};

using RelationRows = unordered_map<uint64_t, RelationDstSet>;

static uint64_t BlpcBlockSize() {
    return 1024;
}

static size_t RelationDstSetBytes(const RelationDstSet& values) {
    return values.bitmap.getSizeInBytes(true);
}

struct BlpcLocalPattern {
    uint64_t block_id = 0;
    RelationDstSet local_mids;
};

struct BlpcPatternEntry {
    uint64_t block_id = 0;
    string pattern_key;
    RelationDstSet local_mids;
    size_t freq = 0;
    size_t sum_rhs_bytes = 0;
    bool cached = false;
    RelationDstSet cached_result;
};

static vector<BlpcLocalPattern> SplitRelationRowIntoBlpcPatterns(const RelationDstSet& mids) {
    vector<BlpcLocalPattern> out;
    const uint64_t block_size = BlpcBlockSize();
    uint64_t current_block = numeric_limits<uint64_t>::max();
    for (uint64_t mid : mids) {
        uint64_t block_id = mid / block_size;
        if (out.empty() || block_id != current_block) {
            out.push_back(BlpcLocalPattern{block_id, {}});
            current_block = block_id;
        }
        out.back().local_mids.push_back(mid - block_id * block_size);
    }
    return out;
}

static string BuildBlpcPatternKey(uint64_t block_id, const RelationDstSet& local_mids) {
    string key;
    AppendRawU64(key, block_id);
    size_t bitmap_bytes = local_mids.bitmap.getSizeInBytes(true);
    AppendRawU32(key, static_cast<uint32_t>(bitmap_bytes));
    size_t begin = key.size();
    key.resize(begin + bitmap_bytes);
    local_mids.bitmap.write(key.data() + begin, true);
    return key;
}

static RelationDstSet ComposeBlpcPatternDirect(uint64_t block_id, const RelationDstSet& local_mids, const RelationRows& right, size_t* rhs_bytes = nullptr) {
    RelationDstSet out;
    const uint64_t block_base = block_id * BlpcBlockSize();
    for (uint64_t local_mid : local_mids) {
        uint64_t mid = block_base + local_mid;
        auto it = right.find(mid);
        if (it == right.end()) continue;
        if (rhs_bytes) *rhs_bytes += RelationDstSetBytes(it->second);
        out |= it->second;
    }
    return out;
}

static RelationRows ComposeRelationRowsNaive(const RelationRows& left, const RelationRows& right) {
    RelationRows out;
    for (const auto& [src, mids] : left) {
        auto& dst_bucket = out[src];
        for (uint64_t mid : mids) {
            auto it = right.find(mid);
            if (it == right.end()) continue;
            dst_bucket |= it->second;
        }
        if (dst_bucket.empty()) out.erase(src);
    }
    return out;
}

static RelationRows ComposeRelationRowsBlpc(const RelationRows& left, const RelationRows& right) {
    vector<BlpcPatternEntry> patterns;
    unordered_map<string, size_t> pattern_index;
    patterns.reserve(left.size() * 2 + 1);

    for (const auto& [_, mids] : left) {
        auto local_patterns = SplitRelationRowIntoBlpcPatterns(mids);
        for (auto& part : local_patterns) {
            string key = BuildBlpcPatternKey(part.block_id, part.local_mids);
            auto it = pattern_index.find(key);
            if (it == pattern_index.end()) {
                BlpcPatternEntry entry;
                entry.block_id = part.block_id;
                entry.pattern_key = key;
                entry.local_mids = std::move(part.local_mids);
                entry.freq = 1;
                entry.sum_rhs_bytes = 0;
                ComposeBlpcPatternDirect(entry.block_id, entry.local_mids, right, &entry.sum_rhs_bytes);
                pattern_index.emplace(entry.pattern_key, patterns.size());
                patterns.push_back(std::move(entry));
            } else {
                patterns[it->second].freq++;
            }
        }
    }

    for (auto& entry : patterns) {
        if (entry.freq < 2) continue;
        entry.cached_result = ComposeBlpcPatternDirect(entry.block_id, entry.local_mids, right);
        size_t cached_bytes = RelationDstSetBytes(entry.cached_result);
        if (entry.sum_rhs_bytes > cached_bytes) {
            entry.cached = true;
        } else {
            entry.cached_result = RelationDstSet{};
        }
    }

    RelationRows out;
    for (const auto& [src, mids] : left) {
        auto& dst_bucket = out[src];
        auto local_patterns = SplitRelationRowIntoBlpcPatterns(mids);
        for (auto& part : local_patterns) {
            string key = BuildBlpcPatternKey(part.block_id, part.local_mids);
            auto it = pattern_index.find(key);
            if (it != pattern_index.end() && patterns[it->second].cached) {
                dst_bucket |= patterns[it->second].cached_result;
            } else {
                dst_bucket |= ComposeBlpcPatternDirect(part.block_id, part.local_mids, right);
            }
        }
        if (dst_bucket.empty()) out.erase(src);
    }
    return out;
}

static int PartitionOfVertex(const PartRouter* router, uint64_t v, int total_parts) {
    if (router) return router->PartitionOf(v, total_parts);
    return GraphPartition::GetPartitionID(v, total_parts);
}

struct WorkerInfo {
    string host;
    int port = 0;
    unique_ptr<TcpClient> client;
    unique_ptr<mutex> mu;

    WorkerInfo() = default;
    WorkerInfo(string h, int p)
        : host(std::move(h)), port(p), client(make_unique<TcpClient>(host, port)), mu(make_unique<mutex>()) {}
};

struct PeerInfo {
    string host;
    int port = 0;
};

static vector<WorkerInfo> ParseWorkers(const string& workers_str) {
    vector<WorkerInfo> workers;
    stringstream ss(workers_str);
    string token;
    while (getline(ss, token, ',')) {
        if (token.empty()) continue;
        size_t colon = token.find(':');
        if (colon == string::npos) throw runtime_error("Bad worker address: " + token);
        workers.emplace_back(token.substr(0, colon), stoi(token.substr(colon + 1)));
    }
    return workers;
}

static vector<PeerInfo> ParsePeers(const string& workers_str) {
    vector<PeerInfo> peers;
    stringstream ss(workers_str);
    string token;
    while (getline(ss, token, ',')) {
        if (token.empty()) continue;
        size_t colon = token.find(':');
        if (colon == string::npos) throw runtime_error("Bad peer address: " + token);
        peers.push_back(PeerInfo{token.substr(0, colon), stoi(token.substr(colon + 1))});
    }
    return peers;
}

static bool HealthCheckWorkers(vector<WorkerInfo>& workers) {
    bool ok = true;
    vector<future<bool>> futures;
    futures.reserve(workers.size());
    for (size_t i = 0; i < workers.size(); ++i) {
        futures.push_back(async(launch::deferred, [&workers, i]() {
            Message req(MessageType::HEALTH_CHECK);
            lock_guard<mutex> lk(*workers[i].mu);
            Message resp = workers[i].client->SendRequest(req);
            return resp.type == MessageType::RESPONSE_OK;
        }));
    }
    for (auto& f : futures) ok = ok && f.get();
    return ok;
}

struct QuerySpec {
    string body;
    int min_rep = 0;
    int max_rep = 0;
    RegexQueryPattern pattern;
};

static bool ParseQuerySpec(const string& expr, QuerySpec& spec, string& err) {
    if (!ParseRegexQueryPattern(expr, true, true, -1, spec.pattern, err)) return false;
    spec.body = spec.pattern.normalized_body;
    spec.min_rep = spec.pattern.min_rep;
    spec.max_rep = spec.pattern.max_rep;
    return true;
}

struct CompositeQuerySpec {
    enum class Kind {
        SIMPLE,
        SEQUENCE,
        CHOICE,
    };

    Kind kind = Kind::SIMPLE;
    string normalized_expr;
    string lhs_expr;
    string rhs_expr;
    QuerySpec simple;
    QuerySpec lhs;
    QuerySpec rhs;
};

static string StripQuerySpaces(const string& expr) {
    string out;
    out.reserve(expr.size());
    for (char c : expr) {
        if (!isspace(static_cast<unsigned char>(c))) out.push_back(c);
    }
    return out;
}

static bool SplitTopLevelBinaryQuery(
    const string& expr,
    char& op,
    string& lhs,
    string& rhs,
    string& err)
{
    int paren_depth = 0;
    int brace_depth = 0;
    size_t split_pos = string::npos;
    char split_op = '\0';
    for (size_t i = 0; i < expr.size(); ++i) {
        char c = expr[i];
        if (c == '(') {
            ++paren_depth;
            continue;
        }
        if (c == ')') {
            --paren_depth;
            if (paren_depth < 0) {
                err = "Unbalanced ')'";
                return false;
            }
            continue;
        }
        if (c == '{') {
            ++brace_depth;
            continue;
        }
        if (c == '}') {
            --brace_depth;
            if (brace_depth < 0) {
                err = "Unbalanced '}'";
                return false;
            }
            continue;
        }
        if (paren_depth == 0 && brace_depth == 0 && (c == '/' || c == '|')) {
            if (split_pos != string::npos) {
                err = "Only one top-level binary operator is supported";
                return false;
            }
            split_pos = i;
            split_op = c;
        }
    }
    if (paren_depth != 0 || brace_depth != 0) {
        err = "Unbalanced query expression";
        return false;
    }
    if (split_pos == string::npos) return false;
    lhs = expr.substr(0, split_pos);
    rhs = expr.substr(split_pos + 1);
    if (lhs.empty() || rhs.empty()) {
        err = "Top-level binary operator is missing an operand";
        return false;
    }
    op = split_op;
    return true;
}

static bool ParseCompositeQuerySpec(const string& expr, CompositeQuerySpec& spec, string& err) {
    spec = CompositeQuerySpec{};
    spec.normalized_expr = StripQuerySpaces(expr);
    char op = '\0';
    string lhs;
    string rhs;
    string split_err;
    bool has_binary = SplitTopLevelBinaryQuery(spec.normalized_expr, op, lhs, rhs, split_err);
    if (has_binary) {
        spec.kind = (op == '/') ? CompositeQuerySpec::Kind::SEQUENCE : CompositeQuerySpec::Kind::CHOICE;
        spec.lhs_expr = lhs;
        spec.rhs_expr = rhs;
        if (!ParseQuerySpec(lhs, spec.lhs, err)) {
            err = "Bad left counted subexpression: " + err;
            return false;
        }
        if (!ParseQuerySpec(rhs, spec.rhs, err)) {
            err = "Bad right counted subexpression: " + err;
            return false;
        }
        return true;
    }
    if (!split_err.empty()) {
        err = split_err;
        return false;
    }
    if (!ParseQuerySpec(spec.normalized_expr, spec.simple, err)) return false;
    spec.kind = CompositeQuerySpec::Kind::SIMPLE;
    return true;
}

using GlobalAdj = unordered_map<uint64_t, unordered_map<string, vector<uint64_t>>>;

static GlobalAdj LoadGlobalAdjacencyFromPartitionRoot(const string& partition_root) {
    namespace fs = std::filesystem;
    GlobalAdj adj;
    vector<fs::path> part_dirs;
    for (const auto& entry : fs::directory_iterator(partition_root)) {
        if (!entry.is_directory()) continue;
        string name = entry.path().filename().string();
        if (name.rfind("partition_", 0) == 0) part_dirs.push_back(entry.path());
    }
    sort(part_dirs.begin(), part_dirs.end());
    for (const auto& dir : part_dirs) {
        ifstream in((dir / "edges.txt").string());
        string line;
        while (getline(in, line)) {
            if (line.empty()) continue;
            stringstream ss(line);
            uint64_t src = 0;
            uint64_t dst = 0;
            string label;
            if (!(ss >> src >> label >> dst)) continue;
            adj[src][label].push_back(dst);
        }
    }
    for (auto& [_, by_label] : adj) {
        for (auto& [_, dsts] : by_label) SortUnique(dsts);
    }
    return adj;
}

struct RelationBlockCoord {
    int row = 0;
    int col = 0;

    bool operator==(const RelationBlockCoord& other) const {
        return row == other.row && col == other.col;
    }
};

struct RelationBlockCoordHash {
    size_t operator()(const RelationBlockCoord& key) const {
        return (static_cast<size_t>(key.row) << 32) ^ static_cast<size_t>(key.col);
    }
};

using RelationMatrix = unordered_map<RelationBlockCoord, RelationRows, RelationBlockCoordHash>;

static int BlockOwner(int row, int col, int workers) {
    (void)row;
    (void)workers;
    return col;
}

static uint64_t CountRelationPairs(const RelationRows& rows) {
    uint64_t total = 0;
    for (const auto& [_, dsts] : rows) total += dsts.size();
    return total;
}

static uint64_t CountRelationPairs(const RelationMatrix& matrix) {
    uint64_t total = 0;
    for (const auto& [_, rows] : matrix) total += CountRelationPairs(rows);
    return total;
}


static void MergeRelationRows(RelationRows& dst, const RelationRows& src) {
    for (const auto& [src_v, src_dsts] : src) {
        auto& bucket = dst[src_v];
        bucket |= src_dsts;
    }
}

static RelationRows ComposeRelationRows(const RelationRows& left, const RelationRows& right) {
    return ComposeRelationRowsBlpc(left, right);
}

static string EncodeIntList(const vector<int>& values) {
    ostringstream oss;
    for (size_t i = 0; i < values.size(); ++i) {
        if (i) oss << ",";
        oss << values[i];
    }
    return oss.str();
}

static vector<int> DecodeIntList(const string& text) {
    vector<int> out;
    if (text.empty()) return out;
    stringstream ss(text);
    string token;
    while (getline(ss, token, ',')) {
        if (token.empty()) continue;
        out.push_back(stoi(token));
    }
    return out;
}

static string EncodeSliceTargets(const vector<vector<int>>& targets) {
    ostringstream oss;
    for (size_t pid = 0; pid < targets.size(); ++pid) {
        if (pid) oss << ";";
        oss << pid << "=" << EncodeIntList(targets[pid]);
    }
    return oss.str();
}

static vector<vector<int>> DecodeSliceTargets(const string& text, int total_parts) {
    vector<vector<int>> out(static_cast<size_t>(total_parts));
    if (text.empty()) return out;
    stringstream ss(text);
    string token;
    while (getline(ss, token, ';')) {
        if (token.empty()) continue;
        size_t eq = token.find('=');
        if (eq == string::npos) continue;
        int pid = stoi(token.substr(0, eq));
        if (pid < 0 || pid >= total_parts) continue;
        out[static_cast<size_t>(pid)] = DecodeIntList(token.substr(eq + 1));
    }
    return out;
}

static uint64_t StableHash64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

static int OutputShardOfSrc(uint64_t src, int shard_count) {
    if (shard_count <= 1) return 0;
    return static_cast<int>(StableHash64(src) % static_cast<uint64_t>(shard_count));
}

// The synchronous matrix variants are intentionally self-contained.  These
// helpers used to live in a deleted experimental directory, but they are part
// of the relation/GCNP ABI used by the current worker runtime.
static string WrapStrongCompressedPayload(const string& raw) {
    string compressed;
    uLongf bound = compressBound(static_cast<uLong>(raw.size()));
    compressed.resize(static_cast<size_t>(bound));
    uLongf actual = bound;
    const int zrc = compress2(
        reinterpret_cast<Bytef*>(compressed.data()), &actual,
        reinterpret_cast<const Bytef*>(raw.data()),
        static_cast<uLong>(raw.size()), Z_BEST_COMPRESSION);
    const bool use_compressed = zrc == Z_OK && actual < raw.size();
    if (use_compressed) compressed.resize(static_cast<size_t>(actual));
    else compressed.clear();

    string out;
    AppendRawU32(out, kCompressedPayloadMagic);
    AppendRawU32(out, kCompressedPayloadVersion);
    AppendRawU32(out, use_compressed ? 1u : 0u);
    AppendRawU64(out, static_cast<uint64_t>(raw.size()));
    out.append(use_compressed ? compressed : raw);
    return out;
}

struct GcnpCoverageSignature {
    static constexpr size_t kWords = 4;
    array<uint64_t, kWords> words{};
};

static uint64_t GcnpSignatureHash(uint64_t value, uint64_t salt) {
    value ^= salt + 0x9e3779b97f4a7c15ULL + (value << 6) + (value >> 2);
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return value ^ (value >> 31);
}

static void GcnpAddCoverageValue(GcnpCoverageSignature& signature, uint64_t value) {
    constexpr uint64_t kSalts[] = {
        0x243f6a8885a308d3ULL, 0x13198a2e03707344ULL,
        0xa4093822299f31d0ULL, 0x082efa98ec4e6c89ULL};
    constexpr uint64_t kBits = GcnpCoverageSignature::kWords * 64ULL;
    for (uint64_t salt : kSalts) {
        const uint64_t bit = GcnpSignatureHash(value, salt) % kBits;
        signature.words[static_cast<size_t>(bit / 64)] |= 1ULL << (bit % 64);
    }
}

static void GcnpAddCoverageSignature(
    GcnpCoverageSignature& signature, const RelationDstSet& values) {
    for (uint64_t value : values) GcnpAddCoverageValue(signature, value);
}

static GcnpCoverageSignature GcnpMakeCoverageSignature(
    const RelationDstSet& values) {
    GcnpCoverageSignature signature;
    GcnpAddCoverageSignature(signature, values);
    return signature;
}

static bool GcnpSignatureMayContain(
    const GcnpCoverageSignature& container,
    const GcnpCoverageSignature& candidate) {
    for (size_t index = 0; index < GcnpCoverageSignature::kWords; ++index) {
        if ((candidate.words[index] & ~container.words[index]) != 0) return false;
    }
    return true;
}

static bool GcnpCoverageSampleMayContain(
    const GcnpCoverageSignature& container, const RelationDstSet& candidate) {
    // Sampling is only a rejection filter.  A positive answer is always
    // followed by an exact Roaring subset test at the caller.
    size_t sampled = 0;
    const size_t stride = max<size_t>(1, candidate.size() / 8);
    size_t position = 0;
    for (uint64_t value : candidate) {
        if (position++ % stride != 0) continue;
        GcnpCoverageSignature one;
        GcnpAddCoverageValue(one, value);
        if (!GcnpSignatureMayContain(container, one)) return false;
        if (++sampled == 8) break;
    }
    return true;
}

static bool GcnpCoverageDirectSampleMayContain(
    const RelationDstSet& container, const RelationDstSet& candidate) {
    const uint64_t cardinality = candidate.cardinality();
    if (cardinality == 0) return true;
    constexpr uint64_t kSamples = 8;
    const uint64_t sample_count = min<uint64_t>(kSamples, cardinality);
    for (uint64_t sample = 0; sample < sample_count; ++sample) {
        const uint64_t rank = sample_count == 1
            ? 0
            : ((cardinality - 1) * sample) / (sample_count - 1);
        uint64_t value = 0;
        if (!candidate.bitmap.select(rank, &value) ||
            !container.bitmap.contains(value)) {
            return false;
        }
    }
    return true;
}

struct GcnpCoverageFilterStats {
    uint64_t candidate_mids = 0;
    uint64_t signature_rejections = 0;
    uint64_t subset_tests = 0;
    uint64_t skipped_mids = 0;
    uint64_t skipped_rhs_pairs = 0;
};

struct GcnpCoverageFilterAtomicStats {
    atomic<uint64_t> candidate_mids{0};
    atomic<uint64_t> signature_rejections{0};
    atomic<uint64_t> subset_tests{0};
    atomic<uint64_t> skipped_mids{0};
    atomic<uint64_t> skipped_rhs_pairs{0};
};

static GcnpCoverageFilterAtomicStats& GcnpCoverageFilterCounters() {
    static GcnpCoverageFilterAtomicStats counters;
    return counters;
}

static void ResetGcnpCoverageFilterStats() {
    auto& counters = GcnpCoverageFilterCounters();
    counters.candidate_mids.store(0, memory_order_relaxed);
    counters.signature_rejections.store(0, memory_order_relaxed);
    counters.subset_tests.store(0, memory_order_relaxed);
    counters.skipped_mids.store(0, memory_order_relaxed);
    counters.skipped_rhs_pairs.store(0, memory_order_relaxed);
}

static GcnpCoverageFilterStats ReadGcnpCoverageFilterStats() {
    const auto& counters = GcnpCoverageFilterCounters();
    return GcnpCoverageFilterStats{
        counters.candidate_mids.load(memory_order_relaxed),
        counters.signature_rejections.load(memory_order_relaxed),
        counters.subset_tests.load(memory_order_relaxed),
        counters.skipped_mids.load(memory_order_relaxed),
        counters.skipped_rhs_pairs.load(memory_order_relaxed)};
}

class GcnpCoverageFilterPool {
public:
    template <typename Function>
    void ParallelFor(size_t count, Function&& function,
                     bool force_parallel = false,
                     size_t thread_limit = rpq_final::kCoverageThreads) {
        // Keep the existing call sites, but never spawn coverage helpers or
        // choose an execution path from a row-count threshold.
        (void)force_parallel;
        (void)thread_limit;
        for (size_t index = 0; index < count; ++index) function(index);
    }

private:
    static size_t ThreadCount() {
        return rpq_final::kCoverageThreads;
    }
};

static GcnpCoverageFilterPool& GetGcnpCoverageFilterPool() {
    static GcnpCoverageFilterPool pool;
    return pool;
}

struct GcnpGroup {
    roaring::Roaring sources;
    RelationDstSet common_mids;
};

// Same ascending postings and borrowed source-row views as the accepted
// single-machine planner; 64-bit keys retain distributed vertex semantics.
class GcnpColumnIndex {
public:
    struct Column {
        roaring::Roaring sources;
        vector<uint32_t> source_ordinals;
        bool present = false;
    };
    Column& insert(uint64_t mid) {
        auto& page = pages_[mid >> page_bits];
        if (!page) page = make_unique<Column[]>(page_size);
        auto& column = page[mid & page_mask];
        column.present = true;
        return column;
    }
    Column* find(uint64_t mid) const {
        auto page = pages_.find(mid >> page_bits);
        if (page == pages_.end()) return nullptr;
        auto* column = &page->second[mid & page_mask];
        return column->present ? column : nullptr;
    }
    Column& at(uint64_t mid) const {
        auto* column = find(mid);
        if (!column) throw out_of_range("Missing GCNP column");
        return *column;
    }
    void finalize() {
        for (auto& entry : pages_) {
            for (size_t i = 0; i < page_size; ++i) {
                auto& column = entry.second[i];
                if (!column.present) continue;
                column.sources = roaring::Roaring(column.source_ordinals.size(),
                                                  column.source_ordinals.data());
                vector<uint32_t>().swap(column.source_ordinals);
            }
        }
    }
    template <typename Function>
    void forEach(Function&& function) const {
        for (const auto& entry : pages_) {
            for (size_t i = 0; i < page_size; ++i) {
                const auto& column = entry.second[i];
                if (column.present) function((entry.first << page_bits) | i, column.sources);
            }
        }
    }
    void release() { pages_.clear(); }
private:
    static constexpr size_t page_bits = 8;
    static constexpr size_t page_size = size_t{1} << page_bits;
    static constexpr size_t page_mask = page_size - 1;
    unordered_map<uint64_t, unique_ptr<Column[]>> pages_;
};

static void SortGcnpSourceRows(vector<pair<uint64_t, const RelationDstSet*>>& sources) {
    constexpr size_t digit_bits = 16;
    constexpr size_t digit_count = size_t{1} << digit_bits;
    vector<pair<uint64_t, const RelationDstSet*>> scratch(sources.size());
    for (size_t shift = 0; shift < numeric_limits<uint64_t>::digits; shift += digit_bits) {
        array<size_t, digit_count> positions{};
        for (const auto& source : sources) ++positions[(source.first >> shift) & (digit_count - 1)];
        size_t offset = 0;
        for (auto& position : positions) {
            const size_t count = position;
            position = offset;
            offset += count;
        }
        for (const auto& source : sources)
            scratch[positions[(source.first >> shift) & (digit_count - 1)]++] = source;
        sources.swap(scratch);
    }
}

struct GcnpFactorizedSharedBlock {
    RelationDstSet sources;
    RelationDstSet destinations;
};

struct GcnpFactorizedResult {
    vector<GcnpFactorizedSharedBlock> shared_blocks;
    RelationRows residual_rows;
};

struct GcnpExecutionStats {
    uint64_t prepare_us = 0;
    uint64_t group_extract_us = 0;
    uint64_t pattern_cache_us = 0;
    uint64_t union_us = 0;
    uint64_t shared_target_us = 0;
    uint64_t source_materialize_us = 0;
    uint64_t groups = 0;
    uint64_t reusable_groups = 0;
    uint64_t grouped_source_uses = 0;
    uint64_t shared_source_uses = 0;
    uint64_t external_memo_hits = 0;
    uint64_t external_memo_stores = 0;
    uint64_t cancelled = 0;
    uint64_t shared_rhs_rows = 0;
    uint64_t residual_rhs_rows = 0;
    uint64_t shared_copy_ns = 0;
    uint64_t residual_lookup_ns = 0;
    uint64_t residual_union_ns = 0;
    uint64_t shared_attach_ns = 0;
    uint64_t output_store_ns = 0;
    uint64_t usable_source_rows = 0;
    uint64_t usable_input_mids = 0;
};

struct GcnpPatternTargetMemo {
    mutex mu;
    unordered_map<string, RelationDstSet> targets_by_pattern;
};

static string GcnpPatternKey(const RelationDstSet& pattern) {
    const size_t bytes = pattern.bitmap.getSizeInBytes(true);
    string key(bytes, '\0');
    pattern.bitmap.write(key.data(), true);
    return key;
}

struct GcnpBitmapUnionScratch {
    FixedBitmapUnionScratch containers;
    vector<const roaring::Roaring64Map*> native_inputs;
    unordered_map<const roaring::Roaring64Map*, uint64_t> input_epochs;
    uint64_t input_epoch = 0;
    roaring::Roaring64Map::ShardUnionScratch shards;
    uint64_t nary_calls = 0;
    uint64_t nary_input_rows = 0;
    uint64_t nary_unique_inputs = 0;
    bool profile = false;
    array<uint64_t, 4> arity_calls{};
    array<uint64_t, 4> arity_ns{};
};

static RelationDstSet GcnpUnionBitmapInputs(
    const vector<const SharedBitmap64*>& inputs, GcnpBitmapUnionScratch& scratch) {
    const auto begin = scratch.profile ? chrono::steady_clock::now()
                                       : chrono::steady_clock::time_point{};
    struct Profile {
        GcnpBitmapUnionScratch& scratch;
        size_t arity;
        chrono::steady_clock::time_point begin;
        ~Profile() {
            if (!scratch.profile) return;
            ++scratch.arity_calls[arity];
            scratch.arity_ns[arity] += chrono::duration_cast<chrono::nanoseconds>(
                chrono::steady_clock::now() - begin).count();
        }
    } profile{scratch, min<size_t>(inputs.size(), 3), begin};
    RelationDstSet output;
    if (inputs.empty()) return output;
    if (inputs.size() == 1) {
        output.bitmap = *inputs.front();
    } else if (inputs.size() == 2) {
        const bool first_is_smaller = inputs.front()->cardinality() <= inputs.back()->cardinality();
        const auto* smaller = first_is_smaller ? inputs.front() : inputs.back();
        const auto* larger = first_is_smaller ? inputs.back() : inputs.front();
        if (smaller->isSubset(*larger)) output.bitmap = *larger;
        else output.bitmap = *inputs.front() | *inputs.back();
    } else {
        ++scratch.nary_calls;
        scratch.nary_input_rows += inputs.size();
        if (++scratch.input_epoch == 0) {
            scratch.input_epochs.clear();
            scratch.input_epoch = 1;
        }
        scratch.native_inputs.clear();
        scratch.native_inputs.reserve(inputs.size());
        // A shared immutable row is one physical operand even when several
        // RHS keys refer to it. The original RHS-row work counters stay intact.
        // Epochs retain allocation capacity, never a computed union result.
        for (const auto* input : inputs) {
            const auto* native = &input->view();
            auto& epoch = scratch.input_epochs[native];
            if (epoch == scratch.input_epoch) continue;
            epoch = scratch.input_epoch;
            scratch.native_inputs.push_back(native);
        }
        scratch.nary_unique_inputs += scratch.native_inputs.size();
        if (scratch.native_inputs.size() == 1) {
            output.bitmap = *inputs.front();
            scratch.native_inputs.clear();
            return output;
        }
        output.bitmap = roaring::Roaring64Map::unionByShard(
            scratch.native_inputs.size(), scratch.native_inputs.data(),
            [&](const vector<const roaring::Roaring*>& rows) {
                return scratch.containers.merge(rows);
            }, scratch.shards);
        scratch.native_inputs.clear();
    }
    return output;
}

// Borrow immutable rows, as in the single-machine executor. A cursor hashes
// once per occupied 256-key page while walking ascending mids; keys stay 64-bit.
class GcnpReadOnlyRightRows {
    static constexpr unsigned page_bits = 8;
    static constexpr size_t page_size = 1ULL << page_bits;
    using Page = array<const RelationDstSet*, page_size>;
    unordered_map<uint64_t, unique_ptr<Page>> pages_;
    uint64_t input_rows_ = 0;
    uint64_t distinct_values_ = 0;
    uint64_t equal_value_aliases_ = 0;

public:
    struct Cursor {
        uint64_t page_key = 0;
        const Page* page = nullptr;
        bool initialized = false;
    };

    explicit GcnpReadOnlyRightRows(const RelationRows& rows) {
        unordered_map<const void*, const RelationDstSet*> known_storage;
        unordered_map<size_t, vector<const RelationDstSet*>> values_by_hash;
        known_storage.reserve(rows.size());
        values_by_hash.reserve(rows.size());
        string encoded;
        for (const auto& [key, row] : rows) {
            if (row.empty()) continue;
            ++input_rows_;
            const RelationDstSet* canonical = nullptr;
            auto known = known_storage.find(row.bitmap.identity());
            if (known != known_storage.end()) {
                canonical = known->second;
            } else {
                // Intern immutable values, not source keys or GCNP patterns.
                // Hash collisions always require complete bitmap equality.
                encoded.resize(row.bitmap.getSizeInBytes(true));
                row.bitmap.write(encoded.data(), true);
                auto& candidates = values_by_hash[hash<string>{}(encoded)];
                for (const auto* candidate : candidates) {
                    if (row.bitmap == candidate->bitmap) {
                        canonical = candidate;
                        ++equal_value_aliases_;
                        break;
                    }
                }
                if (canonical == nullptr) {
                    canonical = &row;
                    candidates.push_back(canonical);
                    ++distinct_values_;
                }
                known_storage.emplace(row.bitmap.identity(), canonical);
            }
            auto& page = pages_[key >> page_bits];
            if (!page) page = make_unique<Page>();
            (*page)[key & (page_size - 1)] = canonical;
        }
    }
    uint64_t input_rows() const { return input_rows_; }
    uint64_t distinct_values() const { return distinct_values_; }
    uint64_t equal_value_aliases() const { return equal_value_aliases_; }

    const RelationDstSet* find(uint64_t key, Cursor& cursor) const {
        const uint64_t page_key = key >> page_bits;
        if (!cursor.initialized || cursor.page_key != page_key) {
            const auto it = pages_.find(page_key);
            cursor.page_key = page_key;
            cursor.page = it == pages_.end() ? nullptr : it->second.get();
            cursor.initialized = true;
        }
        return cursor.page ? (*cursor.page)[key & (page_size - 1)] : nullptr;
    }
};

static RelationDstSet GcnpBuildPatternTargets(
    const RelationDstSet& pattern, const GcnpReadOnlyRightRows& right,
    GcnpBitmapUnionScratch& scratch,
    uint64_t* merged_rows = nullptr) {
    vector<const SharedBitmap64*> inputs;
    inputs.reserve(pattern.size());
    GcnpReadOnlyRightRows::Cursor cursor;
    for (uint64_t mid : pattern) {
        const auto* row = right.find(mid, cursor);
        if (row) inputs.push_back(&row->bitmap);
    }
    if (merged_rows != nullptr) *merged_rows += inputs.size();
    return GcnpUnionBitmapInputs(inputs, scratch);
}

static roaring::Roaring GcnpSeedCorePrefixGroup(
    const RelationDstSet& seed_pattern, const GcnpColumnIndex& columns,
    const roaring::Roaring& unassigned, uint32_t seed,
    size_t& guaranteed_common_count) {
    vector<pair<uint64_t, uint64_t>> ordered_mids;
    ordered_mids.reserve(seed_pattern.size());
    for (uint64_t mid : seed_pattern) {
        ordered_mids.emplace_back(columns.at(mid).sources.and_cardinality(unassigned), mid);
    }
    sort(ordered_mids.begin(), ordered_mids.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.first != rhs.first ? lhs.first > rhs.first : lhs.second < rhs.second;
    });

    roaring::Roaring prefix_sources;
    roaring::Roaring best_sources;
    uint64_t best_score = 0;
    uint64_t best_group_size = 0;
    uint64_t prefix_size = 0;
    for (const auto& [_, mid] : ordered_mids) {
        if (prefix_size == 0) prefix_sources = columns.at(mid).sources & unassigned;
        else prefix_sources &= columns.at(mid).sources;
        if (prefix_sources.isEmpty()) break;
        ++prefix_size;
        const uint64_t group_size = prefix_sources.cardinality();
        const uint64_t score = group_size > 1 ? (group_size - 1) * prefix_size : 0;
        if (best_sources.isEmpty() || score > best_score ||
            (score == best_score && group_size > best_group_size)) {
            best_sources = prefix_sources;
            best_score = score;
            best_group_size = group_size;
            guaranteed_common_count = prefix_size;
        }
    }
    if (best_sources.isEmpty()) best_sources.add(seed);
    return best_sources;
}

static RelationRows ComposeRelationRowsGcnp(
    const RelationRows& left, const RelationRows& right,
    const void* = nullptr, const void* = nullptr,
    GcnpExecutionStats* stats = nullptr,
    GcnpPatternTargetMemo* external_memo = nullptr,
    const atomic<bool>* cancel = nullptr, bool memo_only = false,
    GcnpFactorizedResult* factorized = nullptr,
    bool factorized_only = false) {
    GcnpExecutionStats local_stats;
    GcnpExecutionStats& execution = stats != nullptr ? *stats : local_stats;
    const char* profile_env = getenv("GCNP_PROFILE_MATERIALIZE");
    const bool profile = profile_env != nullptr && string(profile_env) == "1";
    if (left.empty() || right.empty()) return RelationRows{};

    const auto prepare_begin = chrono::steady_clock::now();
    vector<pair<uint64_t, const RelationDstSet*>> sources;
    sources.reserve(left.size());
    for (const auto& entry : left) sources.emplace_back(entry.first, &entry.second);
    SortGcnpSourceRows(sources);
    if (sources.size() > numeric_limits<uint32_t>::max())
        throw length_error("GCNP source ordinals exceed 32 bits");
    vector<uint64_t> usable_cardinalities(sources.size());
    RelationDstSet right_keys;
    GcnpColumnIndex columns;
    for (const auto& entry : right) {
        if (entry.second.empty()) continue;
        right_keys.push_back(entry.first);
        columns.insert(entry.first);
    }
    size_t usable_source_count = 0;
    for (uint32_t source_index = 0; source_index < sources.size(); ++source_index) {
        uint64_t usable_count = 0;
        for (uint64_t mid : *sources[source_index].second) {
            auto* column = columns.find(mid);
            if (!column) continue;
            ++usable_count;
            column->source_ordinals.push_back(static_cast<uint32_t>(usable_source_count));
        }
        if (usable_count == 0) continue;
        // Compact borrowed views, not bitmap rows. Preserve the same ordinals
        // and output-map capacity as an explicitly filtered LHS would have.
        sources[usable_source_count] = sources[source_index];
        usable_cardinalities[usable_source_count] = usable_count;
        ++usable_source_count;
        execution.usable_input_mids += usable_count;
    }
    sources.resize(usable_source_count);
    usable_cardinalities.resize(usable_source_count);
    execution.usable_source_rows += usable_source_count;
    columns.finalize();
    execution.prepare_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - prepare_begin).count());

    const auto group_begin = chrono::steady_clock::now();
    roaring::Roaring unassigned;
    vector<pair<size_t, uint32_t>> seeds;
    seeds.reserve(sources.size());
    roaring::api::roaring_bulk_context_t unassigned_bulk{};
    for (uint32_t source_index = 0; source_index < sources.size(); ++source_index) {
        if (usable_cardinalities[source_index] == 0) continue;
        roaring::api::roaring_bitmap_add_bulk(&unassigned.roaring, &unassigned_bulk, source_index);
        seeds.emplace_back(usable_cardinalities[source_index], source_index);
    }
    priority_queue<pair<size_t, uint32_t>> heap(less<pair<size_t, uint32_t>>{}, std::move(seeds));
    vector<GcnpGroup> groups;
    groups.reserve(sources.size());
    while (!unassigned.isEmpty()) {
        while (!heap.empty() && !unassigned.contains(heap.top().second)) heap.pop();
        if (heap.empty()) break;
        const uint32_t seed = heap.top().second;
        heap.pop();
        RelationDstSet seed_pattern;
        seed_pattern.bitmap = sources[seed].second->bitmap & right_keys.bitmap;
        size_t guaranteed_common_count = 0;
        roaring::Roaring group_sources = GcnpSeedCorePrefixGroup(
            seed_pattern, columns, unassigned, seed, guaranteed_common_count);
        RelationDstSet common_mids;
        bool initialized = false;
        for (uint32_t source_index : group_sources) {
            if (!initialized) {
                common_mids.bitmap = sources[source_index].second->bitmap & right_keys.bitmap;
                initialized = true;
            } else {
                common_mids.bitmap &= sources[source_index].second->bitmap;
            }
            if (common_mids.cardinality() == guaranteed_common_count) break;
        }
        groups.push_back(GcnpGroup{std::move(group_sources), std::move(common_mids)});
        unassigned -= groups.back().sources;
    }
    execution.group_extract_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - group_begin).count());
    execution.groups += groups.size();
    if (!factorized_only) columns.release();
    decltype(heap){}.swap(heap);
    right_keys = RelationDstSet{};
    unassigned = roaring::Roaring{};

    const auto lookup_begin = chrono::steady_clock::now();
    GcnpReadOnlyRightRows right_lookup(right);
    const auto lookup_us = chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - lookup_begin).count();
    execution.prepare_us += static_cast<uint64_t>(lookup_us);
    cerr << "[gcnp-rhs-storage] rows=" << right_lookup.input_rows()
         << " distinct_values=" << right_lookup.distinct_values()
         << " equal_value_aliases=" << right_lookup.equal_value_aliases()
         << " view_build_us=" << lookup_us
         << " equality=complete_bitmap scope=compose threads=1" << '\n';
    RelationRows output;
    auto union_scratch = make_unique<GcnpBitmapUnionScratch>();
    union_scratch->profile = profile;
    union_scratch->input_epochs.reserve(right.size() + 1);
    if (!memo_only && !factorized_only) output.reserve(sources.size() * 2 + 1);
    for (const auto& group : groups) {
        if (cancel != nullptr && cancel->load(memory_order_relaxed)) {
            ++execution.cancelled;
            break;
        }
        const uint64_t source_uses = group.sources.cardinality();
        execution.grouped_source_uses += source_uses;
        const bool reusable = source_uses > 1 && !group.common_mids.empty();
        if (reusable) {
            ++execution.reusable_groups;
            execution.shared_source_uses += source_uses;
        }

        RelationDstSet shared_targets;
        if (reusable) {
            bool memo_hit = false;
            string key;
            const auto cache_begin = chrono::steady_clock::now();
            if (external_memo != nullptr) {
                key = GcnpPatternKey(group.common_mids);
                lock_guard<mutex> lock(external_memo->mu);
                auto cached = external_memo->targets_by_pattern.find(key);
                if (cached != external_memo->targets_by_pattern.end()) {
                    shared_targets = cached->second;
                    memo_hit = true;
                    ++execution.external_memo_hits;
                }
            }
            execution.pattern_cache_us += static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - cache_begin).count());
            if (!memo_hit) {
                const auto union_begin = chrono::steady_clock::now();
                shared_targets = GcnpBuildPatternTargets(
                    group.common_mids, right_lookup, *union_scratch, &execution.shared_rhs_rows);
                const uint64_t elapsed = static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - union_begin).count());
                execution.union_us += elapsed;
                execution.shared_target_us += elapsed;
                if (external_memo != nullptr) {
                    lock_guard<mutex> lock(external_memo->mu);
                    auto [_, inserted] = external_memo->targets_by_pattern.emplace(
                        std::move(key), shared_targets);
                    if (inserted) ++execution.external_memo_stores;
                    else ++execution.external_memo_hits;
                }
            }
        }
        if (memo_only) continue;

        if (factorized != nullptr && reusable && !shared_targets.empty()) {
            GcnpFactorizedSharedBlock block;
            block.destinations = shared_targets;
            for (uint32_t source_index : group.sources) {
                block.sources.push_back(sources[source_index].first);
            }
            factorized->shared_blocks.push_back(std::move(block));
        }

        if (factorized_only) {
            if (reusable) {
                for (uint64_t mid : group.common_mids) {
                    auto column = columns.find(mid);
                    if (column != nullptr) {
                        column->sources -= group.sources;
                    }
                }
            }
            continue;
        }

        const auto materialize_begin = chrono::steady_clock::now();
        const uint64_t common_count = reusable ? group.common_mids.cardinality() : 0;
        vector<uint64_t> common_values;
        common_values.reserve(common_count);
        if (reusable) {
            for (uint64_t mid : group.common_mids) common_values.push_back(mid);
        }
        vector<const SharedBitmap64*> residuals;
        vector<const SharedBitmap64*> output_inputs;
        for (uint32_t source_index : group.sources) {
            auto tick = profile ? chrono::steady_clock::now()
                                : chrono::steady_clock::time_point{};
            const auto account = [&](uint64_t& counter) {
                if (!profile) return;
                const auto now = chrono::steady_clock::now();
                counter += chrono::duration_cast<chrono::nanoseconds>(now - tick).count();
                tick = now;
            };
            RelationDstSet row;
            account(execution.shared_copy_ns);
            residuals.clear();
            const bool fully_shared = reusable && usable_cardinalities[source_index] == common_count;
            if (!fully_shared) {
                size_t common_index = 0;
                GcnpReadOnlyRightRows::Cursor cursor;
                for (uint64_t mid : *sources[source_index].second) {
                    while (common_index < common_count && common_values[common_index] < mid)
                        ++common_index;
                    if (common_index < common_count && common_values[common_index] == mid) continue;
                    const auto* rhs = right_lookup.find(mid, cursor);
                    if (rhs) residuals.push_back(&rhs->bitmap);
                }
            }
            execution.residual_rhs_rows += residuals.size();
            account(execution.residual_lookup_ns);
            RelationDstSet residual;
            if (fully_shared) {
                row = shared_targets;
            } else if (factorized != nullptr) {
                // The wire representation needs the residual separately.
                residual = GcnpUnionBitmapInputs(residuals, *union_scratch);
                output_inputs.clear();
                if (reusable && !shared_targets.empty()) output_inputs.push_back(&shared_targets.bitmap);
                if (!residual.empty()) output_inputs.push_back(&residual.bitmap);
                row = GcnpUnionBitmapInputs(output_inputs, *union_scratch);
            } else {
                output_inputs.clear();
                if (reusable && !shared_targets.empty()) output_inputs.push_back(&shared_targets.bitmap);
                output_inputs.insert(output_inputs.end(), residuals.begin(), residuals.end());
                row = GcnpUnionBitmapInputs(output_inputs, *union_scratch);
            }
            account(execution.residual_union_ns);
            account(execution.shared_attach_ns);
            if (factorized != nullptr && !residual.empty()) {
                factorized->residual_rows.emplace(
                    sources[source_index].first, std::move(residual));
            }
            if (!row.empty()) output.emplace(sources[source_index].first, std::move(row));
            account(execution.output_store_ns);
        }
        execution.source_materialize_us += static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - materialize_begin).count());
    }
    if (factorized_only) {
        struct ResidualColumn {
            RelationDstSet sources;
            const RelationDstSet* destinations = nullptr;
        };
        vector<pair<uint64_t, const roaring::Roaring*>> column_entries;
        columns.forEach([&](uint64_t mid, const roaring::Roaring& residual_source_indices) {
            if (!residual_source_indices.isEmpty()) {
                column_entries.emplace_back(mid, &residual_source_indices);
            }
        });
        vector<ResidualColumn> residual_columns(column_entries.size());
        GetGcnpCoverageFilterPool().ParallelFor(
            column_entries.size(), [&](size_t column_index) {
            const auto [mid, residual_source_indices] =
                column_entries[column_index];
            auto rhs = right.find(mid);
            if (rhs == right.end() || rhs->second.empty()) return;
            auto& residual = residual_columns[column_index];
            for (uint32_t source_index : *residual_source_indices) {
                residual.sources.push_back(sources[source_index].first);
            }
            residual.destinations = &rhs->second;
        }, false, rpq_final::kJoinThreads);

        for (auto& residual : residual_columns) {
            if (residual.destinations == nullptr || residual.sources.empty()) {
                continue;
            }
            GcnpFactorizedSharedBlock block;
            block.sources = std::move(residual.sources);
            block.destinations = *residual.destinations;
            factorized->shared_blocks.push_back(std::move(block));
        }
    }
    cerr << "[gcnp-union-workspace] sources=" << sources.size()
         << " nary_calls=" << union_scratch->nary_calls
         << " nary_input_rows=" << union_scratch->nary_input_rows
         << " nary_unique_inputs=" << union_scratch->nary_unique_inputs
         << " duplicate_aliases="
         << (union_scratch->nary_input_rows - union_scratch->nary_unique_inputs)
         << " native_input_capacity=" << union_scratch->native_inputs.capacity()
         << " shard_directory_entries=" << union_scratch->shards.shards.size()
         << " scope=compose threads=1" << '\n';
    if (profile) {
        cerr << "[gcnp-union-arity-profile]";
        for (size_t i = 0; i < union_scratch->arity_calls.size(); ++i)
            cerr << " calls_" << i << '=' << union_scratch->arity_calls[i]
                 << " ns_" << i << '=' << union_scratch->arity_ns[i];
        cerr << " arity3=three_or_more threads=1" << '\n';
    }
    return output;
}

static RelationRows ComposeRelationRowsExcluding(
    const RelationRows& left, const RelationRows& right,
    const RelationRows& coverage,
    const unordered_map<uint64_t, GcnpCoverageSignature>* coverage_signatures,
    GcnpExecutionStats* execution = nullptr,
    GcnpFactorizedResult* factorized = nullptr,
    bool factorized_only = false) {
    if (left.empty() || right.empty()) return RelationRows{};
    vector<uint64_t> sources;
    sources.reserve(left.size());
    for (const auto& [source, _] : left) sources.push_back(source);
    vector<RelationDstSet> filtered_rows(sources.size());
    vector<GcnpCoverageFilterStats> row_stats(sources.size());

    struct SharedCoverageGroup {
        const RelationDstSet* coverage = nullptr;
        vector<size_t> source_indices;
        RelationDstSet candidate_mids;
        RelationDstSet covered_mids;
        GcnpCoverageFilterStats test_stats;
    };
    auto coverage_fingerprint = [](const RelationDstSet& values) {
        uint64_t hash = values.cardinality();
        auto mix = [&hash](uint64_t value) {
            value ^= value >> 30;
            value *= 0xbf58476d1ce4e5b9ULL;
            value ^= value >> 27;
            value *= 0x94d049bb133111ebULL;
            value ^= value >> 31;
            hash ^= value + 0x9e3779b97f4a7c15ULL +
                (hash << 6) + (hash >> 2);
        };
        mix(values.bitmap.getSizeInBytes(true));
        if (!values.empty()) {
            mix(values.bitmap.minimum());
            mix(values.bitmap.maximum());
        }
        return hash;
    };
    vector<size_t> coverage_group_by_source(
        sources.size(), numeric_limits<size_t>::max());
    unordered_map<uint64_t, vector<size_t>> groups_by_fingerprint;
    vector<SharedCoverageGroup> coverage_groups;
    coverage_groups.reserve(sources.size());
    for (size_t source_index = 0; source_index < sources.size(); ++source_index) {
        auto cover = coverage.find(sources[source_index]);
        if (cover == coverage.end() || cover->second.empty()) continue;
        const uint64_t fingerprint = coverage_fingerprint(cover->second);
        auto& candidates = groups_by_fingerprint[fingerprint];
        size_t group_index = coverage_groups.size();
        for (size_t candidate : candidates) {
            if (coverage_groups[candidate].coverage->bitmap ==
                cover->second.bitmap) {
                group_index = candidate;
                break;
            }
        }
        if (group_index == coverage_groups.size()) {
            coverage_groups.push_back(SharedCoverageGroup{});
            coverage_groups.back().coverage = &cover->second;
            candidates.push_back(group_index);
        }
        coverage_groups[group_index].source_indices.push_back(source_index);
        coverage_group_by_source[source_index] = group_index;
    }

    for (auto& group : coverage_groups) {
        if (group.source_indices.size() <= 1) continue;
        for (size_t source_index : group.source_indices) {
            group.candidate_mids |= left.at(sources[source_index]);
        }
    }
    GetGcnpCoverageFilterPool().ParallelFor(
        coverage_groups.size(), [&](size_t group_index) {
        auto& group = coverage_groups[group_index];
        if (group.source_indices.size() <= 1) return;
        const GcnpCoverageSignature signature =
            GcnpMakeCoverageSignature(*group.coverage);
        for (uint64_t mid : group.candidate_mids) {
            auto rhs = right.find(mid);
            if (rhs == right.end() || rhs->second.empty()) continue;
            if (rhs->second.size() <= group.coverage->size() &&
                GcnpCoverageSampleMayContain(signature, rhs->second)) {
                ++group.test_stats.subset_tests;
                if (rhs->second.bitmap.isSubset(group.coverage->bitmap)) {
                    group.covered_mids.push_back(mid);
                }
            } else {
                ++group.test_stats.signature_rejections;
            }
        }
    });

    // Exact coverage checks run serially, before the serial GCNP join.
    GetGcnpCoverageFilterPool().ParallelFor(
        sources.size(), [&](size_t source_index) {
        const uint64_t source = sources[source_index];
        const auto left_it = left.find(source);
        if (left_it == left.end()) return;
        const RelationDstSet& mids = left_it->second;
        auto cover = coverage.find(source);
        if (cover == coverage.end() || cover->second.empty()) {
            filtered_rows[source_index] = mids;
            return;
        }
        const size_t group_index = coverage_group_by_source[source_index];
        if (group_index != numeric_limits<size_t>::max() &&
            coverage_groups[group_index].source_indices.size() > 1) {
            const RelationDstSet& covered_mids =
                coverage_groups[group_index].covered_mids;
            RelationDstSet kept;
            for (uint64_t mid : mids) {
                auto rhs = right.find(mid);
                if (rhs == right.end() || rhs->second.empty()) continue;
                ++row_stats[source_index].candidate_mids;
                if (covered_mids.bitmap.contains(mid)) {
                    ++row_stats[source_index].skipped_mids;
                    row_stats[source_index].skipped_rhs_pairs +=
                        rhs->second.size();
                } else {
                    kept.push_back(mid);
                }
            }
            if (!kept.empty()) {
                filtered_rows[source_index] = std::move(kept);
            }
            return;
        }
        GcnpCoverageSignature computed_signature;
        const GcnpCoverageSignature* signature = nullptr;
        if (coverage_signatures != nullptr) {
            auto found = coverage_signatures->find(source);
            if (found != coverage_signatures->end()) signature = &found->second;
        }
        if (signature == nullptr) {
            computed_signature = GcnpMakeCoverageSignature(cover->second);
            signature = &computed_signature;
        }
        RelationDstSet kept;
        for (uint64_t mid : mids) {
            auto rhs = right.find(mid);
            if (rhs == right.end() || rhs->second.empty()) continue;
            ++row_stats[source_index].candidate_mids;
            bool covered = false;
            if (rhs->second.size() <= cover->second.size() &&
                GcnpCoverageSampleMayContain(*signature, rhs->second)) {
                ++row_stats[source_index].subset_tests;
                covered = rhs->second.bitmap.isSubset(cover->second.bitmap);
            } else {
                ++row_stats[source_index].signature_rejections;
            }
            if (covered) {
                ++row_stats[source_index].skipped_mids;
                row_stats[source_index].skipped_rhs_pairs += rhs->second.size();
            } else {
                kept.push_back(mid);
            }
        }
        if (!kept.empty()) filtered_rows[source_index] = std::move(kept);
    });

    RelationRows filtered;
    filtered.reserve(left.size());
    GcnpCoverageFilterStats local;
    for (const auto& group : coverage_groups) {
        local.signature_rejections += group.test_stats.signature_rejections;
        local.subset_tests += group.test_stats.subset_tests;
    }
    for (size_t source_index = 0; source_index < sources.size(); ++source_index) {
        const auto& stats = row_stats[source_index];
        local.candidate_mids += stats.candidate_mids;
        local.signature_rejections += stats.signature_rejections;
        local.subset_tests += stats.subset_tests;
        local.skipped_mids += stats.skipped_mids;
        local.skipped_rhs_pairs += stats.skipped_rhs_pairs;
        if (!filtered_rows[source_index].empty()) {
            filtered.emplace(
                sources[source_index], std::move(filtered_rows[source_index]));
        }
    }
    auto& counters = GcnpCoverageFilterCounters();
    counters.candidate_mids.fetch_add(local.candidate_mids, memory_order_relaxed);
    counters.signature_rejections.fetch_add(local.signature_rejections, memory_order_relaxed);
    counters.subset_tests.fetch_add(local.subset_tests, memory_order_relaxed);
    counters.skipped_mids.fetch_add(local.skipped_mids, memory_order_relaxed);
    counters.skipped_rhs_pairs.fetch_add(local.skipped_rhs_pairs, memory_order_relaxed);
    return ComposeRelationRowsGcnp(
        filtered, right, nullptr, nullptr, execution, nullptr, nullptr,
        false, factorized, factorized_only);
}
