// This file is included after the incremental row subscription primitives.
// It implements the dual-state bounded suffix used by R{m,n}:
//   R{m,n}(s) = R^m(s) o S[n-m],
//   S[0] = I, S[a+b] = S[a] union (R^a o S[b]).
// The state is local to one active range frontier, so composite macros only
// build suffix rows for mids that have actually been reached.
struct IncrementalPrefixSuffixKey {
    int depth = 0;
    uint64_t source = 0;
    bool operator==(const IncrementalPrefixSuffixKey& other) const {
        return depth == other.depth && source == other.source;
    }
};
struct IncrementalPrefixSuffixKeyHash {
    size_t operator()(const IncrementalPrefixSuffixKey& key) const {
        return hash<uint64_t>{}(key.source) ^ (hash<int>{}(key.depth) << 1);
    }
};
struct IncrementalPrefixSuffixEntry {
    bool started = false;
    bool complete = false;
    RelationDstSet values;
    vector<IncrementalDeltaCallback> waiters;
};
struct IncrementalPrefixSuffixCache {
    int query_id = 0;
    int body_id = 0;
    unordered_map<IncrementalPrefixSuffixKey, IncrementalPrefixSuffixEntry,
                  IncrementalPrefixSuffixKeyHash> entries;
};

// A suffix is independent of its parent range root.  Sharing this cache for
// one worker/query/body is essential for top-level range queries, where many
// B^m(source) rows often converge on the same mids.
static mutex g_incremental_prefix_suffix_cache_mu;
static unordered_map<IncrementalWorkerState*,
    unordered_map<int, unordered_map<int, shared_ptr<IncrementalPrefixSuffixCache>>>>
    g_incremental_prefix_suffix_caches;

static shared_ptr<IncrementalPrefixSuffixCache> IncrementalGetPrefixSuffixCache(
    IncrementalWorkerState& state, int query_id, int body_id) {
    lock_guard<mutex> lk(g_incremental_prefix_suffix_cache_mu);
    auto& cache = g_incremental_prefix_suffix_caches[&state][query_id][body_id];
    if (!cache) {
        cache = make_shared<IncrementalPrefixSuffixCache>();
        cache->query_id = query_id;
        cache->body_id = body_id;
    }
    return cache;
}

static void IncrementalClearPrefixSuffixCaches(IncrementalWorkerState& state, int query_id) {
    lock_guard<mutex> lk(g_incremental_prefix_suffix_cache_mu);
    auto state_it = g_incremental_prefix_suffix_caches.find(&state);
    if (state_it == g_incremental_prefix_suffix_caches.end()) return;
    state_it->second.erase(query_id);
    if (state_it->second.empty()) g_incremental_prefix_suffix_caches.erase(state_it);
}

static pair<int, int> IncrementalRangeAdditionSplit(int depth) {
    if (depth <= 1) return {0, 0};
    // Build dual states bottom-up on the binary basis:
    // S2=S1 U (P1 o S1), S4=S2 U (P2 o S2), and so on.  For a non-power
    // residual use r+2^h, where 2^h is the largest lower binary power.  The
    // residual must be left because S[a+b]=S[a] U (P[a] o S[b]) is asymmetric.
    // Thus S5=S1 U P1oS4 and S6=S2 U P2oS4, never S3+S3.
    int largest_lower_power = 1;
    while ((static_cast<long long>(largest_lower_power) << 1) < depth) {
        largest_lower_power <<= 1;
    }
    return {depth - largest_lower_power, largest_lower_power};
}

static void IncrementalSubscribePrefixSuffix(
    IncrementalWorkerState&, const shared_ptr<IncrementalPrefixSuffixCache>&,
    int, uint64_t, IncrementalDeltaCallback);

static void IncrementalPublishPrefixSuffix(
    IncrementalWorkerState& state, const shared_ptr<IncrementalPrefixSuffixCache>& cache,
    const IncrementalPrefixSuffixKey& key, const RelationDstSet& delta, bool complete) {
    RelationDstSet fresh;
    vector<IncrementalDeltaCallback> waiters;
    {
        auto& entry = cache->entries.at(key);
        fresh.bitmap = delta.bitmap - entry.values.bitmap;
        entry.values.bitmap |= fresh.bitmap;
        if (complete && !entry.complete) {
            entry.complete = true;
            waiters.swap(entry.waiters);
        } else if (!fresh.empty()) {
            waiters = entry.waiters;
        }
    }
    for (const auto& waiter : waiters) {
        if (!fresh.empty()) IncrementalInvoke(state, waiter, fresh, false);
        if (complete) IncrementalInvoke(state, waiter, RelationDstSet{}, true);
    }
}

static void IncrementalSubscribePrefixSuffix(
    IncrementalWorkerState& state, const shared_ptr<IncrementalPrefixSuffixCache>& cache,
    int depth, uint64_t source, IncrementalDeltaCallback done) {
    const IncrementalPrefixSuffixKey key{depth, source};
    RelationDstSet cached;
    bool complete = false;
    bool start = false;
    {
        auto& entry = cache->entries[key];
        cached = entry.values;
        complete = entry.complete;
        if (!complete) {
            entry.waiters.push_back(done);
            if (!entry.started) {
                entry.started = true;
                start = true;
            }
        }
    }
    if (!cached.empty()) IncrementalInvoke(state, done, cached, false);
    if (complete) {
        IncrementalInvoke(state, std::move(done), RelationDstSet{}, true);
        return;
    }
    if (!start) return;

    if (depth == 0) {
        RelationDstSet identity;
        identity.push_back(source);
        IncrementalPublishPrefixSuffix(state, cache, key, identity, true);
        return;
    }
    if (depth == 1) {
        RelationDstSet identity;
        identity.push_back(source);
        IncrementalPublishPrefixSuffix(state, cache, key, identity, false);
        IncrementalSubscribeRow(state,
            IncrementalRowKey{cache->query_id, cache->body_id, 1, source},
            [&state, cache, key](const RelationDstSet& delta, bool row_complete) {
                IncrementalPublishPrefixSuffix(state, cache, key, delta, row_complete);
            });
        return;
    }

    const auto split = IncrementalRangeAdditionSplit(depth);
    struct BuildContext {
        bool left_prefix_complete = false;
        bool exact_complete = false;
        bool complete = false;
        uint64_t pending_children = 0;
        unordered_set<uint64_t> children;
    };
    auto build = make_shared<BuildContext>();
    auto finish = make_shared<function<void()>>();
    *finish = [&state, cache, key, build]() {
        if (build->left_prefix_complete && build->exact_complete &&
            build->pending_children == 0 && !build->complete) {
            build->complete = true;
            IncrementalPublishPrefixSuffix(state, cache, key, RelationDstSet{}, true);
        }
    };
    IncrementalSubscribePrefixSuffix(state, cache, split.first, source,
        [&state, cache, key, build, finish](const RelationDstSet& delta, bool child_complete) {
            if (!delta.empty()) IncrementalPublishPrefixSuffix(state, cache, key, delta, false);
            if (child_complete) {
                build->left_prefix_complete = true;
                (*finish)();
            }
        });
    IncrementalSubscribeRow(state,
        IncrementalRowKey{cache->query_id, cache->body_id, split.first, source},
        [&state, cache, key, split, build, finish](const RelationDstSet& mids, bool exact_complete) {
            for (uint64_t mid : mids) {
                if (!build->children.insert(mid).second) continue;
                ++build->pending_children;
                IncrementalSubscribePrefixSuffix(state, cache, split.second, mid,
                    [&state, cache, key, build, finish](const RelationDstSet& delta, bool child_complete) {
                        if (!delta.empty()) IncrementalPublishPrefixSuffix(state, cache, key, delta, false);
                        if (child_complete) {
                            --build->pending_children;
                            (*finish)();
                        }
                    });
            }
            if (exact_complete) {
                build->exact_complete = true;
                (*finish)();
            }
        });
}
