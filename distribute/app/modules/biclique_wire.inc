// Message-local Boolean matrix compression. No changes to scheduling or joins.
// Existing row/Roaring representation remains the fallback and size baseline.
static constexpr uint64_t kBicliqueRowsMarker = numeric_limits<uint64_t>::max();

struct BicliqueWireMetrics {
    atomic<uint64_t> attempts{0}, selected{0}, blocks{0}, shared_pairs{0};
    atomic<uint64_t> original_bytes{0}, encoded_bytes{0}, encode_us{0}, decode_us{0};
    atomic<uint64_t> hash_us{0}, plan_us{0}, pack_us{0}, parallel_batches{0};
};
static BicliqueWireMetrics biclique_wire_metrics;

static int BicliqueWireMode() {
    return rpq_final::kBicliqueWireMode;
}

using BicliqueRowViews = vector<pair<uint64_t, const RelationDstSet*>>;

static size_t BicliqueCompressionThreads() {
    return rpq_final::kCompressionThreads;
}

// Shared by all message lanes: never create N threads for every RPC.
// Only leaf compression work enters this pool, so waiting cannot recurse.
class BicliqueCompressionPool {
public:
    explicit BicliqueCompressionPool(size_t count) {
        try {
            for (size_t i = 0; i < count; ++i) workers_.emplace_back([this] {
                for (;;) {
                    function<void()> task;
                    {
                        unique_lock<mutex> lock(mu_);
                        cv_.wait(lock, [&] { return stop_ || !queue_.empty(); });
                        if (stop_ && queue_.empty()) return;
                        task = std::move(queue_.front());
                        queue_.pop_front();
                    }
                    task();
                }
            });
        } catch (...) { Stop(); throw; }
    }
    ~BicliqueCompressionPool() { Stop(); }
    future<void> Submit(function<void()> fn) {
        auto task = make_shared<packaged_task<void()>>(std::move(fn));
        auto result = task->get_future();
        { lock_guard<mutex> lock(mu_); queue_.push_back([task] { (*task)(); }); }
        cv_.notify_one();
        return result;
    }
private:
    void Stop() {
        { lock_guard<mutex> lock(mu_); stop_ = true; }
        cv_.notify_all();
        for (auto& worker : workers_) if (worker.joinable()) worker.join();
    }
    mutex mu_;
    condition_variable cv_;
    deque<function<void()>> queue_;
    vector<thread> workers_;
    bool stop_ = false;
};

static BicliqueCompressionPool& BicliqueSharedPool() {
    static BicliqueCompressionPool pool(BicliqueCompressionThreads());
    return pool;
}

template<class Function>
static void BicliqueParallel(size_t count, uint64_t bytes, const Function& fn) {
    const size_t lanes = min(BicliqueCompressionThreads(), count);
    if (lanes < 2 || count < 128 || bytes < 262144) { fn(0, count, 0); return; }
    auto& pool = BicliqueSharedPool();
    vector<future<void>> pending;
    pending.reserve(lanes);
    exception_ptr failure;
    try {
        for (size_t lane = 0; lane < lanes; ++lane) {
            const size_t begin = count * lane / lanes, end = count * (lane+1) / lanes;
            pending.push_back(pool.Submit([&, begin, end, lane] { fn(begin, end, lane); }));
        }
    } catch (...) { failure = current_exception(); }
    for (auto& task : pending) {
        try { task.get(); } catch (...) { if (!failure) failure = current_exception(); }
    }
    if (failure) rethrow_exception(failure);
    biclique_wire_metrics.parallel_batches.fetch_add(1, memory_order_relaxed);
}

static uint64_t BicliqueBitmapKey(const RelationDstSet& values) {
    string bytes(values.bitmap.getSizeInBytes(true), char(0));
    values.bitmap.write(bytes.data(), true);
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char byte : bytes) { hash ^= byte; hash *= 1099511628211ULL; }
    return hash;
}

static SharedBitmap64 BicliqueIntersection(const SharedBitmap64& left,
                                          const SharedBitmap64& right) {
    if (left.identity() == right.identity()) return left;
    return left.view().wireIntersection(right.view());
}

static SharedBitmap64 BicliqueDifference(const SharedBitmap64& left,
                                        const SharedBitmap64& right) {
    if (left.identity() == right.identity()) return {};
    if (right.isEmpty()) return left;
    return left.view().wireDifference(right.view());
}

// A compose stage sends the same immutable RHS storage to several helpers.
// Keep only its portable bytes/hash; message-local rectangle planning is unchanged.
// Access stays on the existing single matrix-message executor.
struct BicliqueImmutableWireCache {
    struct Entry {
        SharedBitmap64 owner;
        string bytes;
        uint64_t hash = 1469598103934665603ULL;
        bool hash_ready = false;
    };
    unordered_map<const void*, Entry> entries;
    uint64_t hits = 0, misses = 0, serialized_bytes = 0;
    struct PairKey {
        const void* left;
        const void* right;
        bool operator==(const PairKey& other) const {
            return left == other.left && right == other.right;
        }
    };
    struct PairHash {
        size_t operator()(const PairKey& key) const {
            const size_t left = std::hash<const void*>{}(key.left);
            const size_t right = std::hash<const void*>{}(key.right);
            return left ^ (right + size_t(0x9e3779b9) + (left << 6) + (left >> 2));
        }
    };
    struct PairParts {
        SharedBitmap64 left_owner, right_owner;
        SharedBitmap64 common, left_only, right_only;
        uint64_t common_bytes = 0, left_bytes = 0, right_bytes = 0;
    };
    unordered_map<PairKey, PairParts, PairHash> pair_parts;
    uint64_t pair_hits = 0, pair_misses = 0;
    unordered_map<PairKey, bool, PairHash> equality_results;
    uint64_t equality_hits = 0, equality_misses = 0;

    bool Equal(const SharedBitmap64& left, const SharedBitmap64& right) {
        if (left.identity() == right.identity()) return true;
        PairKey key{left.identity(), right.identity()};
        if (std::less<const void*>{}(key.right, key.left)) std::swap(key.left, key.right);
        auto found = equality_results.find(key);
        if (found != equality_results.end()) { ++equality_hits; return found->second; }
        // Get() has retained both immutable inputs in entries before grouping.
        const bool equal = left == right;
        equality_results.emplace(key, equal);
        ++equality_misses;
        return equal;
    }

    const PairParts& Partition(const SharedBitmap64& left, const SharedBitmap64& right) {
        const PairKey key{left.identity(), right.identity()};
        auto found = pair_parts.find(key);
        if (found != pair_parts.end()) { ++pair_hits; return found->second; }
        PairParts parts;
        // Retain both inputs so a released bitmap address cannot alias this key.
        parts.left_owner = left;
        parts.right_owner = right;
        parts.common = BicliqueIntersection(left, right);
        // The intersection is a subset of each input. Equal cardinality
        // therefore proves an empty residual without constructing ANDNOT.
        if (parts.common.cardinality() != left.cardinality())
            parts.left_only = BicliqueDifference(left, parts.common);
        if (parts.common.cardinality() != right.cardinality())
            parts.right_only = BicliqueDifference(right, parts.common);
        parts.common_bytes = parts.common.getSizeInBytes(true);
        if (!parts.left_only.isEmpty()) parts.left_bytes = parts.left_only.getSizeInBytes(true);
        if (!parts.right_only.isEmpty()) parts.right_bytes = parts.right_only.getSizeInBytes(true);
        ++pair_misses;
        return pair_parts.emplace(key, std::move(parts)).first->second;
    }

    Entry& GetBytes(const RelationDstSet& values) {
        auto found = entries.find(values.bitmap.identity());
        if (found != entries.end()) { ++hits; return found->second; }
        Entry entry;
        entry.owner = values.bitmap;
        entry.bytes.resize(values.bitmap.getSizeInBytes(true));
        values.bitmap.write(entry.bytes.data(), true);
        ++misses;
        serialized_bytes += entry.bytes.size();
        return entries.emplace(values.bitmap.identity(), std::move(entry)).first->second;
    }

    const Entry& Get(const RelationDstSet& values) {
        auto& entry = GetBytes(values);
        if (!entry.hash_ready) {
            for (unsigned char byte : entry.bytes) {
                entry.hash ^= byte;
                entry.hash *= 1099511628211ULL;
            }
            entry.hash_ready = true;
        }
        return entry;
    }

    void AppendKnown(string& out, const RelationDstSet& values) {
        // Pair residuals share immutable storage across rows and replies too.
        // Reuse their portable bytes without hashing output-only bitmaps.
        const auto& entry = GetBytes(values);
        IncAppendU64(out, entry.bytes.size());
        out.append(entry.bytes);
    }
};

static bool IncAppendBicliqueRows(string& out, const BicliqueRowViews& views,
    BicliqueImmutableWireCache* wire_cache = nullptr) {
    if (views.size() < 2) return false;
    const auto started = chrono::steady_clock::now();
    uint64_t original_bytes = 8;
    unordered_map<const void*, size_t> index_by_storage;
    vector<const RelationDstSet*> unique_targets;
    vector<uint64_t> unique_bytes;
    vector<size_t> target_indices(views.size());
    for (size_t i = 0; i < views.size(); ++i) {
        const auto* targets = views[i].second;
        auto [position, inserted] = index_by_storage.emplace(
            targets->bitmap.identity(), unique_targets.size());
        if (inserted) {
            unique_targets.push_back(targets);
            unique_bytes.push_back(targets->bitmap.getSizeInBytes(true));
        }
        target_indices[i] = position->second;
        original_bytes += 16 + unique_bytes[position->second];
    }
    vector<uint64_t> target_hashes(unique_targets.size());
    BicliqueParallel(unique_targets.size(), original_bytes, [&](size_t begin, size_t end, size_t) {
        for (size_t i = begin; i < end; ++i)
            if (!unique_targets[i]->empty()) target_hashes[i] = wire_cache
                ? wire_cache->Get(*unique_targets[i]).hash
                : BicliqueBitmapKey(*unique_targets[i]);
    });
    const auto hashed = chrono::steady_clock::now();
    if (IncrementalScalingTraceEnabled()) {
        cerr << "[biclique-hash-work] rows=" << views.size()
             << " immutable_storages=" << unique_targets.size()
             << " hash_us=" << chrono::duration_cast<chrono::microseconds>(
                    hashed - started).count() << " threads=1" << '\n';
    }
    struct Group {
        RelationDstSet sources;
        const RelationDstSet* targets = nullptr;
        vector<size_t> indices;
        uint64_t target_bytes = 0;
    };
    vector<Group> groups;
    unordered_map<uint64_t, vector<size_t>> hashes;
    groups.reserve(views.size());
    for (size_t i = 0; i < views.size(); ++i) {
        const auto& targets = *views[i].second;
        if (targets.empty()) continue;
        auto& candidates = hashes[target_hashes[target_indices[i]]];
        size_t found = groups.size();
        for (size_t g : candidates) {
            const bool equal = wire_cache
                ? wire_cache->Equal(groups[g].targets->bitmap, targets.bitmap)
                : groups[g].targets->bitmap == targets.bitmap;
            if (equal) { found = g; break; }
        }
        if (found == groups.size()) {
            candidates.push_back(found);
            groups.emplace_back();
            groups.back().targets = &targets;
            groups.back().target_bytes = unique_bytes[target_indices[i]];
        }
        groups[found].sources.push_back(views[i].first);
        groups[found].indices.push_back(i);
    }
    struct Block { RelationDstSet sources, targets; };
    vector<Block> blocks;
    RelationRows residual;
    vector<bool> paired(groups.size(), false);
    auto group_bytes = [&](const Group& g) -> uint64_t {
        const uint64_t row_bytes = g.indices.size() * (16 + g.target_bytes);
        const uint64_t block_bytes = 16 + g.sources.bitmap.getSizeInBytes(true) + g.target_bytes;
        return min(row_bytes, block_bytes);
    };
    {
        // One candidate per minimum target, one disjoint pairing pass.
        // The anchor only proposes a pair; exact bitmap intersection proves it.
        unordered_map<uint64_t, size_t> anchors;
        for (size_t i = 0; i < groups.size(); ++i) {
            auto& b = groups[i];
            const uint64_t anchor = *b.targets->begin();
            auto candidate = anchors.find(anchor);
            if (candidate == anchors.end()) { anchors.emplace(anchor, i); continue; }
            const size_t j = candidate->second;
            auto& a = groups[j];
            RelationDstSet common, left, right, sources;
            uint64_t common_bytes = 0, left_bytes = 0, right_bytes = 0;
            if (wire_cache) {
                const auto& parts = wire_cache->Partition(a.targets->bitmap, b.targets->bitmap);
                common.bitmap = parts.common;
                left.bitmap = parts.left_only;
                right.bitmap = parts.right_only;
                common_bytes = parts.common_bytes;
                left_bytes = parts.left_bytes;
                right_bytes = parts.right_bytes;
            } else {
                common.bitmap = BicliqueIntersection(a.targets->bitmap, b.targets->bitmap);
                if (common.cardinality() != a.targets->cardinality())
                    left.bitmap = BicliqueDifference(a.targets->bitmap, common.bitmap);
                if (common.cardinality() != b.targets->cardinality())
                    right.bitmap = BicliqueDifference(b.targets->bitmap, common.bitmap);
                common_bytes = common.bitmap.getSizeInBytes(true);
                if (!left.empty()) left_bytes = left.bitmap.getSizeInBytes(true);
                if (!right.empty()) right_bytes = right.bitmap.getSizeInBytes(true);
            }
            sources.bitmap = a.sources.bitmap | b.sources.bitmap;
            const uint64_t cost = 16 + sources.bitmap.getSizeInBytes(true) + common_bytes
                + (left.empty() ? 0 : a.indices.size() * (16 + left_bytes))
                + (right.empty() ? 0 : b.indices.size() * (16 + right_bytes));
            if (!common.empty() && cost < group_bytes(a) + group_bytes(b)) {
                blocks.push_back({std::move(sources), std::move(common)});
                if (!left.empty()) for (size_t k : a.indices) residual.emplace(views[k].first, left);
                if (!right.empty()) for (size_t k : b.indices) residual.emplace(views[k].first, right);
                paired[i] = paired[j] = true;
                anchors.erase(candidate);
            } else {
                candidate->second = i;
            }
        }
    }
    for (size_t i = 0; i < groups.size(); ++i) {
        if (paired[i]) continue;
        auto& g = groups[i];
        const uint64_t raw_cost = g.indices.size() * (16 + g.target_bytes);
        const uint64_t block_cost = 16 + g.sources.bitmap.getSizeInBytes(true) + g.target_bytes;
        if (g.indices.size() > 1 && block_cost < raw_cost) {
            blocks.push_back({std::move(g.sources), *g.targets});
        } else {
            for (size_t k : g.indices) residual.emplace(views[k].first, *views[k].second);
        }
    }
    // Empty rows are retained on wire just as in the legacy encoder.
    for (const auto& row : views) if (row.second->empty()) residual.emplace(row.first, *row.second);
    const auto planned = chrono::steady_clock::now();
    string packed;
    IncAppendU64(packed, kBicliqueRowsMarker);
    IncAppendU32(packed, 1);
    IncAppendU64(packed, blocks.size());
    uint64_t shared_pairs = 0;
    for (const auto& block : blocks)
        shared_pairs += block.sources.cardinality() * block.targets.cardinality();
    BicliqueRowViews remaining;
    remaining.reserve(residual.size());
    for (const auto& row : residual) remaining.emplace_back(row.first, &row.second);
    const size_t items = blocks.size() + remaining.size();
    vector<string> chunks(max<size_t>(1, min(BicliqueCompressionThreads(), items)));
    BicliqueParallel(items, original_bytes, [&](size_t begin, size_t end, size_t lane) {
        auto& chunk = chunks[lane];
        for (size_t i = begin; i < end; ++i) {
            if (i < blocks.size()) {
                IncAppendSet(chunk, blocks[i].sources);
                if (wire_cache) wire_cache->AppendKnown(chunk, blocks[i].targets);
                else IncAppendSet(chunk, blocks[i].targets);
            } else {
                if (i == blocks.size()) IncAppendU64(chunk, remaining.size());
                const auto& row = remaining[i-blocks.size()];
                IncAppendU64(chunk, row.first);
                if (wire_cache) wire_cache->AppendKnown(chunk, *row.second);
                else IncAppendSet(chunk, *row.second);
            }
        }
    });
    for (const auto& chunk : chunks) packed.append(chunk);
    if (remaining.empty()) IncAppendU64(packed, 0);
    const bool selected = !blocks.empty() && packed.size() < original_bytes;
    biclique_wire_metrics.attempts.fetch_add(1, memory_order_relaxed);
    biclique_wire_metrics.original_bytes.fetch_add(original_bytes, memory_order_relaxed);
    biclique_wire_metrics.encoded_bytes.fetch_add(selected ? packed.size() : original_bytes, memory_order_relaxed);
    if (selected) {
        out.append(packed);
        biclique_wire_metrics.selected.fetch_add(1, memory_order_relaxed);
        biclique_wire_metrics.blocks.fetch_add(blocks.size(), memory_order_relaxed);
        biclique_wire_metrics.shared_pairs.fetch_add(shared_pairs, memory_order_relaxed);
    }
    const auto finished = chrono::steady_clock::now();
    biclique_wire_metrics.encode_us.fetch_add(chrono::duration_cast<chrono::microseconds>(
        finished - started).count(), memory_order_relaxed);
    biclique_wire_metrics.hash_us.fetch_add(chrono::duration_cast<chrono::microseconds>(
        hashed - started).count(), memory_order_relaxed);
    biclique_wire_metrics.plan_us.fetch_add(chrono::duration_cast<chrono::microseconds>(
        planned - hashed).count(), memory_order_relaxed);
    biclique_wire_metrics.pack_us.fetch_add(chrono::duration_cast<chrono::microseconds>(
        finished - planned).count(), memory_order_relaxed);
    return selected;
}

static void IncAppendRows(string& out, const RelationRows& rows) {
    if (rows.size() >= 2) {
        BicliqueRowViews views;
        views.reserve(rows.size());
        for (const auto& row : rows) views.emplace_back(row.first, &row.second);
        if (IncAppendBicliqueRows(out, views)) return;
    }
    IncAppendRowsLegacy(out, rows);
}

static bool IncReadRows(const string& in, size_t& off, RelationRows& rows) {
    const size_t begin = off;
    uint64_t marker = 0;
    if (!IncReadU64(in, off, marker)) return false;
    if (marker != kBicliqueRowsMarker) { off = begin; return IncReadRowsLegacy(in, off, rows); }
    const auto started = chrono::steady_clock::now();
    uint32_t version = 0;
    uint64_t count = 0;
    if (!IncReadU32(in, off, version) || version != 1 || !IncReadU64(in, off, count)
        || count > (in.size() - off) / 16) return false;
    rows.clear();
    try {
        for (uint64_t i = 0; i < count; ++i) {
            RelationDstSet sources, targets;
            if (!IncReadSet(in, off, sources) || !IncReadSet(in, off, targets)
                || sources.empty() || targets.empty()) return false;
            for (uint64_t source : sources) rows[source] |= targets;
        }
        RelationRows residual;
        if (!IncReadRowsLegacy(in, off, residual)) return false;
        for (auto& row : residual) rows[row.first] |= row.second;
    } catch (const exception&) { return false; }
    biclique_wire_metrics.decode_us.fetch_add(chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - started).count(), memory_order_relaxed);
    return true;
}
