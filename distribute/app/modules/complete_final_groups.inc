// Retain the complete source-owned relation in its existing wire factorization.
// Every group represents all pairs sources x destinations, not a count reply.
// Exact cardinality of one serialized portable Roaring64Map, read from the
// container descriptive headers only (cardinality-1 per container). Returns
// false on malformed input. The bytes themselves are retained unchanged.
static bool CompleteFinalSerializedCardinality(
    const char* buf, size_t bytes, uint64_t& cardinality) {
    cardinality = 0;
    if (bytes < sizeof(uint64_t)) return false;
    uint64_t maps = 0;
    memcpy(&maps, buf, sizeof(maps));
    size_t off = sizeof(uint64_t);
    for (uint64_t m = 0; m < maps; ++m) {
        if (bytes - off < sizeof(uint32_t)) return false;
        off += sizeof(uint32_t);
        const char* inner = buf + off;
        const size_t remaining = bytes - off;
        const size_t inner_bytes = roaring::Roaring::serializedSizeInBytesSafe(inner, remaining);
        if (inner_bytes == 0 || inner_bytes > remaining || inner_bytes < 4) return false;
        uint32_t cookie = 0;
        memcpy(&cookie, inner, sizeof(cookie));
        size_t h = 4;
        int64_t containers = 0;
        if (cookie == 12346u) {
            int32_t n = 0;
            if (inner_bytes < 8) return false;
            memcpy(&n, inner + 4, sizeof(n));
            containers = n; h = 8;
        } else if ((cookie & 0xFFFFu) == 12347u) {
            containers = static_cast<int64_t>(cookie >> 16) + 1;
            h += static_cast<size_t>((containers + 7) / 8);
        } else return false;
        if (containers < 0 || h + static_cast<size_t>(containers) * 4 > inner_bytes) return false;
        for (int64_t c = 0; c < containers; ++c) {
            uint16_t card_minus_one = 0;
            memcpy(&card_minus_one, inner + h + static_cast<size_t>(c) * 4 + 2, sizeof(card_minus_one));
            cardinality += static_cast<uint64_t>(card_minus_one) + 1;
        }
        off += inner_bytes;
    }
    return off == bytes;
}

// Retain the complete source-owned relation in its existing wire factorization.
// Every group represents all pairs sources x destinations, not a count reply.
// Destinations stay in their exact serialized Roaring form; their cardinality
// is read from container headers instead of materializing every bitmap.
struct CompleteFinalGroup {
    RelationDstSet sources;
    string destinations;  // serialized portable Roaring64Map
    uint64_t destination_count = 0;
};

struct CompleteFinalGroups {
    vector<CompleteFinalGroup> groups;
    RelationDstSet sources;
    uint64_t rows = 0, pairs = 0;
    uint64_t verified_groups = 0;

    void Add(CompleteFinalGroup group) {
        if (group.sources.empty() || group.destination_count == 0) return;
        const uint64_t source_count = group.sources.cardinality();
        const uint64_t target_count = group.destination_count;
        if (source_count > numeric_limits<uint64_t>::max() - rows ||
            target_count > (numeric_limits<uint64_t>::max() - pairs) / source_count)
            throw overflow_error("final result cardinality overflow");
        rows += source_count;
        pairs += source_count * target_count;
        groups.push_back(std::move(group));
    }

    void ValidateSources() {
        // |union(S_i)| == sum |S_i| iff all S_i are disjoint.
        vector<const roaring::Roaring64Map*> inputs;
        inputs.reserve(groups.size());
        for (const auto& group : groups) inputs.push_back(&group.sources.bitmap.view());
        sources.bitmap = roaring::Roaring64Map::fastunion(inputs.size(), inputs.data());
        if (sources.cardinality() != rows)
            throw runtime_error("overlapping source-owned final result groups");
    }
};

static void ParseCompleteFinalGroups(
    const string& data, CompleteFinalGroups& relation, bool allow_chunks = true) {
    string raw, error;
    if (!UnwrapCompressedPayload(data, raw, error))
        throw runtime_error("incremental final decode: " + error);
    size_t off = 0;
    uint32_t magic = 0, version = 0, kind = 0, codec = 0;
    uint64_t count = 0;
    if (!IncReadU32(raw, off, magic) || !IncReadU32(raw, off, version) ||
        !IncReadU32(raw, off, kind) || !IncReadU32(raw, off, codec) ||
        !IncReadU64(raw, off, count) || magic != kIncrementalWireMagic ||
        version != kIncrementalWireVersion ||
        kind != static_cast<uint32_t>(IncrementalWireKind::RESULTS))
        throw runtime_error("bad complete final relation header");
    if (codec == static_cast<uint32_t>(IncrementalFinalRowsCodec::CHUNKS)) {
        if (!allow_chunks || count == 0 || count > 32)
            throw runtime_error("bad complete final chunk count");
        for (uint64_t i = 0; i < count; ++i) {
            uint64_t bytes = 0;
            if (!IncReadU64(raw, off, bytes) || bytes > raw.size() - off)
                throw runtime_error("bad complete final chunk length");
            ParseCompleteFinalGroups(raw.substr(off, static_cast<size_t>(bytes)), relation, false);
            off += static_cast<size_t>(bytes);
        }
    } else if (codec == static_cast<uint32_t>(IncrementalFinalRowsCodec::ROWS) ||
               codec == static_cast<uint32_t>(IncrementalFinalRowsCodec::PATTERN_GROUPS)) {
        if (count > (raw.size() - off) / 16)
            throw runtime_error("bad complete final group count");
        for (uint64_t i = 0; i < count; ++i) {
            CompleteFinalGroup group;
            if (codec == static_cast<uint32_t>(IncrementalFinalRowsCodec::ROWS)) {
                uint64_t source = 0;
                if (!IncReadU64(raw, off, source))
                    throw runtime_error("bad complete final source");
                group.sources.push_back(source);
            } else if (!IncReadSet(raw, off, group.sources)) {
                throw runtime_error("bad complete final source set");
            }
            uint64_t bytes = 0;
            if (!IncReadU64(raw, off, bytes) || bytes == 0 || bytes > raw.size() - off)
                throw runtime_error("bad complete final destination set");
            const char* dest = raw.data() + off;
            if (!CompleteFinalSerializedCardinality(dest, static_cast<size_t>(bytes), group.destination_count))
                throw runtime_error("bad complete final destination set");
            // Cross-check the header count against a full decode for a
            // bounded prefix of groups in every payload.
            if (i < 32) {
                const auto decoded = roaring::Roaring64Map::readSafe(dest, static_cast<size_t>(bytes));
                if (decoded.cardinality() != group.destination_count)
                    throw runtime_error("final destination header cardinality mismatch");
                ++relation.verified_groups;
            }
            group.destinations.assign(dest, static_cast<size_t>(bytes));
            off += static_cast<size_t>(bytes);
            relation.Add(std::move(group));
        }
    } else {
        throw runtime_error("unknown complete final result codec");
    }
    if (off != raw.size()) throw runtime_error("trailing complete final relation bytes");
}
