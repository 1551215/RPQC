// CSR topology avoids allocating a bitmap for every singleton SCC.
struct IncrementalOpenPlan {
    vector<uint64_t> vertices;
    vector<uint32_t> components;
    vector<uint64_t> member_offsets{0}, dag_offsets{0};
    vector<uint32_t> member_indices, dag_targets;
    mutex closure_mu;
    unordered_map<uint32_t, RelationDstSet> closure_cache;
    size_t ComponentCount() const { return member_offsets.size() - 1; }
    bool FindComponent(uint64_t vertex, uint32_t& component) const {
        auto it = lower_bound(vertices.begin(), vertices.end(), vertex);
        if (it == vertices.end() || *it != vertex) return false;
        component = components[it - vertices.begin()];
        return true;
    }
};

static shared_ptr<IncrementalOpenPlan> BuildIncrementalOpenPlan(const RelationRows& rows) {
    auto p = make_shared<IncrementalOpenPlan>();
    for (const auto& [src, dsts] : rows) {
        p->vertices.push_back(src);
        p->vertices.insert(p->vertices.end(), dsts.begin(), dsts.end());
    }
    auto& vertices = p->vertices;
    sort(vertices.begin(), vertices.end());
    vertices.erase(unique(vertices.begin(), vertices.end()), vertices.end());
    const size_t n = vertices.size();
    const uint32_t missing = numeric_limits<uint32_t>::max();
    if (n >= missing) throw runtime_error("open plan exceeds dense index limit");
    unordered_map<uint64_t, uint32_t> dense;
    dense.reserve(n);
    for (size_t v = 0; v < n; ++v) dense.emplace(vertices[v], static_cast<uint32_t>(v));
    vector<uint64_t> offsets(n + 1, 0);
    for (const auto& [src, dsts] : rows) offsets[dense.at(src) + 1] = dsts.size();
    for (size_t v = 0; v < n; ++v) offsets[v + 1] += offsets[v];
    vector<uint32_t> edges(offsets.back());
    for (const auto& [src, dsts] : rows) {
        uint64_t next = offsets[dense.at(src)];
        for (uint64_t dst : dsts) edges[next++] = dense.at(dst);
    }
    dense.clear(); dense.rehash(0);
    vector<uint32_t> index(n, missing), low(n), stack;
    vector<uint8_t> on_stack(n, 0);
    struct Frame { uint32_t vertex, parent; uint64_t next; };
    vector<Frame> frames;
    uint32_t next_index = 0, count = 0;
    p->components.resize(n);
    auto enter = [&](uint32_t v, uint32_t parent) {
        index[v] = low[v] = next_index++;
        on_stack[v] = 1; stack.push_back(v);
        frames.push_back({v, parent, offsets[v]});
    };
    for (size_t root = 0; root < n; ++root) {
        if (index[root] != missing) continue;
        enter(static_cast<uint32_t>(root), missing);
        while (!frames.empty()) {
            auto& frame = frames.back();
            const uint32_t v = frame.vertex;
            if (frame.next < offsets[v + 1]) {
                const uint32_t child = edges[frame.next++];
                if (index[child] == missing) enter(child, v);
                else if (on_stack[child]) low[v] = min(low[v], index[child]);
                continue;
            }
            const uint32_t parent = frame.parent;
            if (parent != missing) low[parent] = min(low[parent], low[v]);
            if (low[v] == index[v]) {
                while (true) {
                    uint32_t member = stack.back(); stack.pop_back();
                    on_stack[member] = 0; p->components[member] = count;
                    if (member == v) break;
                }
                ++count;
            }
            frames.pop_back();
        }
    }
    p->member_offsets.assign(static_cast<size_t>(count) + 1, 0);
    p->dag_offsets.assign(static_cast<size_t>(count) + 1, 0);
    for (uint32_t c : p->components) ++p->member_offsets[c + 1];
    for (size_t v = 0; v < n; ++v)
        for (uint64_t e = offsets[v]; e < offsets[v + 1]; ++e)
            if (p->components[v] != p->components[edges[e]]) ++p->dag_offsets[p->components[v] + 1];
    for (size_t c = 0; c < count; ++c) {
        p->member_offsets[c + 1] += p->member_offsets[c];
        p->dag_offsets[c + 1] += p->dag_offsets[c];
    }
    p->member_indices.resize(n); p->dag_targets.resize(p->dag_offsets.back());
    auto member_next = p->member_offsets, dag_next = p->dag_offsets;
    for (size_t v = 0; v < n; ++v) {
        uint32_t c = p->components[v];
        p->member_indices[member_next[c]++] = static_cast<uint32_t>(v);
        for (uint64_t e = offsets[v]; e < offsets[v + 1]; ++e) {
            uint32_t target = p->components[edges[e]];
            if (c != target) p->dag_targets[dag_next[c]++] = target;
        }
    }
    const auto original = p->dag_offsets;
    uint64_t write = 0;
    for (size_t c = 0; c < count; ++c) {
        auto first = p->dag_targets.begin() + original[c];
        auto last = p->dag_targets.begin() + original[c + 1];
        sort(first, last); last = unique(first, last);
        p->dag_offsets[c] = write;
        for (auto it = first; it != last; ++it) p->dag_targets[write++] = *it;
    }
    p->dag_offsets.back() = write; p->dag_targets.resize(write);
    return p;
}

static const RelationDstSet& IncrementalOpenComponentClosure(IncrementalOpenPlan& p, uint32_t component) {
    lock_guard<mutex> lock(p.closure_mu);
    if (component >= p.ComponentCount()) throw runtime_error("bad open SCC component id");
    auto cached = p.closure_cache.find(component);
    if (cached != p.closure_cache.end()) return cached->second;
    struct Frame { uint32_t component; uint64_t next; };
    vector<Frame> stack{{component, p.dag_offsets[component]}};
    while (!stack.empty()) {
        auto& frame = stack.back(); const uint32_t c = frame.component;
        if (frame.next < p.dag_offsets[c + 1]) {
            const uint32_t child = p.dag_targets[frame.next++];
            // Iterative Tarjan assigns children before their parents.
            if (child >= c) throw runtime_error("invalid open SCC condensation order");
            if (!p.closure_cache.count(child)) stack.push_back({child, p.dag_offsets[child]});
            continue;
        }
        RelationDstSet closure;
        for (uint64_t i = p.member_offsets[c]; i < p.member_offsets[c + 1]; ++i)
            closure.push_back(p.vertices[p.member_indices[i]]);
        for (uint64_t i = p.dag_offsets[c]; i < p.dag_offsets[c + 1]; ++i)
            closure.bitmap |= p.closure_cache.at(p.dag_targets[i]).bitmap;
        p.closure_cache.emplace(c, std::move(closure)); stack.pop_back();
    }
    return p.closure_cache.at(component);
}

static constexpr uint32_t kIncrementalOpenPlanMagic = 0x50434f49;
static constexpr uint32_t kIncrementalOpenPlanVersion = 2;
template<class T> static void AppendOpenArray(string& raw, const vector<T>& values) {
    IncAppendU64(raw, values.size());
    for (T v : values) {
        if constexpr (sizeof(T) == 8) IncAppendU64(raw, v);
        else IncAppendU32(raw, v);
    }
}
template<class T> static bool ReadOpenArray(const string& raw, size_t& offset, vector<T>& values) {
    uint64_t count = 0;
    if (!IncReadU64(raw, offset, count) || count > (raw.size() - offset) / sizeof(T)) return false;
    values.resize(static_cast<size_t>(count));
    for (auto& v : values) {
        if constexpr (sizeof(T) == 8) { if (!IncReadU64(raw, offset, v)) return false; }
        else { if (!IncReadU32(raw, offset, v)) return false; }
    }
    return true;
}
static string SerializeIncrementalOpenPlan(int query_id, int body_id, const IncrementalOpenPlan& p) {
    string raw;
    IncAppendU32(raw, kIncrementalOpenPlanMagic); IncAppendU32(raw, kIncrementalOpenPlanVersion);
    IncAppendU32(raw, static_cast<uint32_t>(query_id)); IncAppendU32(raw, static_cast<uint32_t>(body_id));
    AppendOpenArray(raw, p.vertices); AppendOpenArray(raw, p.components);
    AppendOpenArray(raw, p.member_offsets); AppendOpenArray(raw, p.member_indices);
    AppendOpenArray(raw, p.dag_offsets); AppendOpenArray(raw, p.dag_targets);
    return WrapCompressedPayload(raw);
}
static bool ParseIncrementalOpenPlan(const string& payload, int& query_id, int& body_id,
    shared_ptr<IncrementalOpenPlan>& plan, string& error) {
    string raw;
    if (!UnwrapCompressedPayload(payload, raw, error)) return false;
    size_t offset = 0; uint32_t magic = 0, version = 0, query = 0, body = 0;
    auto parsed = make_shared<IncrementalOpenPlan>();
    if (!IncReadU32(raw, offset, magic) || !IncReadU32(raw, offset, version) ||
        !IncReadU32(raw, offset, query) || !IncReadU32(raw, offset, body) ||
        magic != kIncrementalOpenPlanMagic || version != kIncrementalOpenPlanVersion ||
        !ReadOpenArray(raw, offset, parsed->vertices) || !ReadOpenArray(raw, offset, parsed->components) ||
        !ReadOpenArray(raw, offset, parsed->member_offsets) || !ReadOpenArray(raw, offset, parsed->member_indices) ||
        !ReadOpenArray(raw, offset, parsed->dag_offsets) || !ReadOpenArray(raw, offset, parsed->dag_targets) ||
        offset != raw.size()) { error = "bad compact open SCC plan"; return false; }
    const auto& p = *parsed;
    auto valid_offsets = [](const vector<uint64_t>& a, size_t size) {
        return !a.empty() && a.front() == 0 && a.back() == size && is_sorted(a.begin(), a.end());
    };
    if (p.vertices.size() >= numeric_limits<uint32_t>::max() || p.vertices.size() != p.components.size() ||
        p.vertices.size() != p.member_indices.size() || !is_sorted(p.vertices.begin(), p.vertices.end()) ||
        adjacent_find(p.vertices.begin(), p.vertices.end()) != p.vertices.end() ||
        !valid_offsets(p.member_offsets, p.member_indices.size()) || !valid_offsets(p.dag_offsets, p.dag_targets.size()) ||
        p.member_offsets.size() != p.dag_offsets.size()) {
        error = "invalid compact open SCC bounds"; return false;
    }
    vector<uint8_t> seen(p.vertices.size(), 0);
    for (size_t c = 0; c < p.ComponentCount(); ++c) {
        if (p.member_offsets[c] == p.member_offsets[c + 1]) { error = "empty open SCC"; return false; }
        for (uint64_t i = p.member_offsets[c]; i < p.member_offsets[c + 1]; ++i) {
            uint32_t v = p.member_indices[i];
            if (v >= seen.size() || seen[v] || p.components[v] != c) { error = "invalid SCC member"; return false; }
            seen[v] = 1;
        }
        for (uint64_t i = p.dag_offsets[c]; i < p.dag_offsets[c + 1]; ++i)
            if (p.dag_targets[i] >= c) { error = "invalid SCC edge order"; return false; }
    }
    query_id = static_cast<int>(query); body_id = static_cast<int>(body); plan = std::move(parsed);
    return true;
}
