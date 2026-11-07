#include "incremental_common.inc"

#include <array>

struct IncrementalWorkerState;
static void IncrementalTrackOrderedExecutorActivity(IncrementalWorkerState& state, bool started);
static void IncrementalTrackMatrixMessageActivity(IncrementalWorkerState& state, bool started);
static size_t IncrementalComputeThreadCount(int total_partitions);
static size_t IncrementalMatrixMessageThreadCount(int total_partitions);

class IncrementalExecutor {
public:
    explicit IncrementalExecutor(size_t count = 1) {
        count = max<size_t>(1, count);
        for (size_t i = 0; i < count; ++i) workers_.emplace_back([this]() { Run(); });
    }
    ~IncrementalExecutor() {
        { lock_guard<mutex> lk(mu_); stop_ = true; }
        cv_.notify_all();
        for (auto& worker : workers_) if (worker.joinable()) worker.join();
    }
    void Submit(function<void()> task) {
        { lock_guard<mutex> lk(mu_); if (discarding_) return; tasks_.push_back(std::move(task)); }
        cv_.notify_one();
    }
    // Formal data-plane work always wins over speculative cache warming.
    void SubmitLowPriority(function<void()> task) {
        { lock_guard<mutex> lk(mu_); if (discarding_) return; low_priority_tasks_.push_back(std::move(task)); }
        cv_.notify_one();
    }
    void BeginDiscard() {
        lock_guard<mutex> lk(mu_);
        discarding_ = true;
        tasks_.clear();
        low_priority_tasks_.clear();
    }
    void WaitDiscardedIdle() {
        unique_lock<mutex> lk(mu_);
        idle_cv_.wait(lk, [&] { return active_tasks_ == 0; });
        tasks_.clear();
        low_priority_tasks_.clear();
    }
    void Resume() {
        lock_guard<mutex> lk(mu_);
        discarding_ = false;
    }
    void WaitIdle() {
        unique_lock<mutex> lk(mu_);
        idle_cv_.wait(lk, [&] {
            return active_tasks_ == 0 && tasks_.empty() &&
                low_priority_tasks_.empty();
        });
    }
    void SetActivityHooks(function<void()> started, function<void()> finished) {
        lock_guard<mutex> lk(mu_);
        activity_started_ = std::move(started);
        activity_finished_ = std::move(finished);
    }
    pair<size_t, size_t> PendingAndActive() {
        lock_guard<mutex> lk(mu_);
        return {tasks_.size() + low_priority_tasks_.size(), active_tasks_};
    }
private:
    void Run() {
        while (true) {
            function<void()> task;
            function<void()> activity_started, activity_finished;
            { unique_lock<mutex> lk(mu_); cv_.wait(lk, [&] { return stop_ || !tasks_.empty() || !low_priority_tasks_.empty(); }); if (stop_ && tasks_.empty() && low_priority_tasks_.empty()) return; if (!tasks_.empty()) { task = std::move(tasks_.front()); tasks_.pop_front(); } else { task = std::move(low_priority_tasks_.front()); low_priority_tasks_.pop_front(); } ++active_tasks_; activity_started = activity_started_; activity_finished = activity_finished_; }
            if (activity_started) activity_started();
            try { task(); } catch (const exception& ex) { cerr << "[incremental-worker] task error: " << ex.what() << endl; }
            if (activity_finished) activity_finished();
            { lock_guard<mutex> lk(mu_); if (active_tasks_ > 0) --active_tasks_; if (active_tasks_ == 0 && tasks_.empty() && low_priority_tasks_.empty()) idle_cv_.notify_all(); }
        }
    }
    mutex mu_;
    condition_variable cv_;
    condition_variable idle_cv_;
    deque<function<void()>> tasks_;
    deque<function<void()>> low_priority_tasks_;
    vector<thread> workers_;
    function<void()> activity_started_;
    function<void()> activity_finished_;
    bool stop_ = false;
    bool discarding_ = false;
    size_t active_tasks_ = 0;
};

using IncrementalDeltaCallback = function<void(const RelationDstSet&, bool)>;
using IncrementalRowsCallback = function<void(const RelationRows&, bool)>;
static constexpr uint64_t kIncrementalEmitBatchPairs = 4096;
// DemandBatch emits a relation frontier in reasonably sized chunks.  This is
// a batching threshold, not a correctness or window-size parameter.
// A relation batch must be large enough for GCNP to observe repeated source
// patterns.  The previous 16K threshold split DBpedia frontiers into more
// than one hundred child batches before a power was complete.
// Keep an incremental frontier large enough that a destination block receives
// substantial GCNP work per update.  This remains a streaming threshold: the
// batch is still flushed immediately on completion, so it never creates a
// B^k-level barrier.  256K split DBpedia's dense powers into hundreds of
// updates per output shard and made repeated bitmap setup dominate the join.
static constexpr uint64_t kIncrementalDemandEmitPairs = 1048576;
// RSFM signatures are control-plane metadata.  Coalesce one direct B stream
// before notifying its parent so a burst of right deltas does not repeatedly
// activate/replay tiny left routes.
static constexpr uint64_t kIncrementalSignatureEmitPairs = 8192;
static constexpr size_t kIncrementalRequestBatchRows = 512;
static constexpr size_t kIncrementalBaseBatchEvents = 256;
static constexpr size_t kIncrementalBaseBatchBytes = 1U << 20;
static constexpr int kIncrementalBaseBatchDelayMs = 1;
static size_t IncrementalPositiveEnvSize(const char* name, size_t fallback) {
    const char* configured = getenv(name);
    if (configured == nullptr || *configured == '\0') return fallback;
    char* end = nullptr;
    const unsigned long long parsed = strtoull(configured, &end, 10);
    if (end == configured || *end != '\0' || parsed == 0) return fallback;
    return static_cast<size_t>(parsed);
}
static size_t IncrementalPeerDeltaBatchEvents(int total_partitions) {
    const size_t fallback = total_partitions <= 4 ? 512 :
        (total_partitions <= 6 ? 256 : 64);
    return IncrementalPositiveEnvSize("PEER_DELTA_BATCH_EVENTS", fallback);
}
static size_t IncrementalPeerDeltaBatchBytes(int total_partitions) {
    const size_t fallback = total_partitions <= 4 ? (16U << 20) :
        (total_partitions <= 6 ? (8U << 20) : (4U << 20));
    return IncrementalPositiveEnvSize("PEER_DELTA_BATCH_BYTES", fallback);
}
static int IncrementalPeerDeltaFlushDelayMs(int total_partitions) {
    const size_t fallback = total_partitions <= 4 ? 5 :
        (total_partitions <= 6 ? 2 : 1);
    return static_cast<int>(IncrementalPositiveEnvSize(
        "PEER_DELTA_FLUSH_DELAY_MS", fallback));
}
static constexpr uint64_t kIncrementalGcnpPlannerMaxMids = 256;
static constexpr size_t kIncrementalGcnpPlannerMinParents = 16;
static constexpr size_t kIncrementalGcnpPlannerMaxParents = 512;
static constexpr uint64_t kIncrementalGcnpPlannerMaxFrontierMids = 16384;
static constexpr size_t kIncrementalPerfSlots = 6;

#include "../../incremental_open_plan.inc"

static size_t IncrementalPerfSlot(int length) {
    switch (length) {
        case 1: return 0;
        case 2: return 1;
        case 4: return 2;
        case 8: return 3;
        case 16: return 4;
        default: return 5;
    }
}

struct IncrementalWaiter { int reply_pid = -1; uint64_t token = 0; };
struct IncrementalParentJoinBatch {
    mutex mu;
    IncrementalRowKey parent;
    uint64_t pending_children = 0;
    uint64_t child_count = 0;
    RelationDstSet values;
    RelationDstSet pending_delta;
};
struct IncrementalBinaryWaiter {
    IncrementalRowKey parent;
    shared_ptr<IncrementalParentJoinBatch> batch;
};
struct IncrementalRowState {
    enum class Status : uint8_t { NOT_STARTED, COMPUTING, COMPLETE } status = Status::NOT_STARTED;
    RelationDstSet values;
    RelationDstSet pending_delta;
    vector<IncrementalWaiter> waiters;
    // Internal exact-power composition does not need a captured callback or a
    // token-map entry. Store its parent row key directly.
    vector<IncrementalBinaryWaiter> binary_waiters;
    unordered_set<string> base_visited;
    uint64_t base_pending = 0;
    bool left_complete = false;
    uint64_t binary_pending = 0;
};
struct IncrementalCallback { IncrementalDeltaCallback fn; };
struct IncrementalGcnpJoinRequest {
    IncrementalRowKey parent;
    int right_len = 0;
    RelationDstSet mids;
};
struct IncrementalGcnpMetrics {
    uint64_t batches = 0;
    uint64_t source_rows = 0;
    uint64_t groups = 0;
    uint64_t reusable_groups = 0;
    uint64_t cacheable_patterns = 0;
    uint64_t cache_hits = 0;
    uint64_t direct_rhs_bytes = 0;
    uint64_t reused_rhs_bytes = 0;
};

// Choose a reusable source group solely from the left A(s,m) panel. The
// largest remaining row is still the seed, but a support-ordered common core
// may be a proper subset of that row. This avoids exact-pattern-only groups
// without source-pair comparisons or any dependence on RHS data.
static roaring::Roaring IncrementalGcnpSeedCorePrefixGroup(
    const vector<RelationDstSet>& source_mids,
    const unordered_map<uint64_t, roaring::Roaring>& columns,
    const roaring::Roaring& unassigned,
    uint32_t seed) {
    vector<pair<size_t, uint64_t>> ordered_mids;
    ordered_mids.reserve(source_mids[seed].cardinality());
    for (uint64_t mid : source_mids[seed]) {
        ordered_mids.emplace_back((columns.at(mid) & unassigned).cardinality(), mid);
    }
    sort(ordered_mids.begin(), ordered_mids.end(),
        [](const auto& lhs, const auto& rhs) {
            return lhs.first != rhs.first ? lhs.first > rhs.first : lhs.second < rhs.second;
        });

    roaring::Roaring prefix_sources = unassigned;
    roaring::Roaring best_sources;
    size_t best_score = 0;
    size_t best_group_size = 0;
    size_t core_size = 0;
    for (const auto& [_, mid] : ordered_mids) {
        prefix_sources &= columns.at(mid);
        if (prefix_sources.isEmpty()) break;
        ++core_size;
        const size_t group_size = prefix_sources.cardinality();
        const size_t score = group_size > 1 ? (group_size - 1) * core_size : 0;
        if (best_sources.isEmpty() || score > best_score ||
            (score == best_score && group_size > best_group_size)) {
            best_sources = prefix_sources;
            best_score = score;
            best_group_size = group_size;
        }
    }
    if (best_sources.isEmpty()) best_sources.add(seed);
    return best_sources;
}

// A matrix block is keyed by the demand batch that produces it. In middle
// mode it is stored at W_k and contains A[:,k], B[k,:], and partial C[k]. The
// same state type remains compatible with the legacy destination-block path.
struct IncrementalMatrixBlockKey {
    int parent_pid = -1;
    uint64_t parent_batch_id = 0;
    bool operator==(const IncrementalMatrixBlockKey& other) const {
        return parent_pid == other.parent_pid && parent_batch_id == other.parent_batch_id;
    }
};
struct IncrementalMatrixBlockKeyHash {
    size_t operator()(const IncrementalMatrixBlockKey& key) const {
        size_t h = hash<int>{}(key.parent_pid);
        h ^= hash<uint64_t>{}(key.parent_batch_id + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        return h;
    }
};

// The formal GCNP grouping depends only on the complete A(s,m) panel.  Once
// that panel is sealed, this plan stays immutable while B(m,d) arrives as a
// stream.  `group_targets` is the only mutable part: it accumulates the RHS
// union for one common-mid group and lets every RHS delta publish C deltas.
struct IncrementalGcnpDeltaPlan {
    vector<uint64_t> sources;
    vector<RelationDstSet> source_mids;
    vector<GcnpGroup> groups;
    unordered_map<uint64_t, vector<size_t>> common_groups_by_mid;
    // Keep the residual inverse index compressed.  A vector entry for every
    // (mid, source) pair made a sealed panel materialize hundreds of millions
    // of 32-bit values before any RHS delta could be consumed.
    unordered_map<uint64_t, roaring::Roaring> residual_sources_by_mid;
    vector<RelationDstSet> group_targets;
    mutex mu;
};

static shared_ptr<IncrementalGcnpDeltaPlan> IncrementalBuildGcnpDeltaPlan(
    const RelationRows& left, GcnpExecutionStats* stats = nullptr) {
    if (left.empty()) return make_shared<IncrementalGcnpDeltaPlan>();
    const auto begin = chrono::steady_clock::now();
    auto plan = make_shared<IncrementalGcnpDeltaPlan>();
    plan->sources.reserve(left.size());
    for (const auto& [source, _] : left) plan->sources.push_back(source);
    sort(plan->sources.begin(), plan->sources.end());
    plan->source_mids.resize(plan->sources.size());

    unordered_map<uint64_t, roaring::Roaring> columns;
    columns.reserve(left.size() * 2 + 1);
    for (uint32_t index = 0; index < plan->sources.size(); ++index) {
        plan->source_mids[index] = left.at(plan->sources[index]);
        for (uint64_t mid : plan->source_mids[index]) columns[mid].add(index);
    }
    if (stats != nullptr) {
        stats->prepare_us = static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - begin).count());
    }

    const auto group_begin = chrono::steady_clock::now();
    roaring::Roaring unassigned;
    priority_queue<pair<size_t, uint32_t>> pattern_heap;
    for (uint32_t index = 0; index < plan->sources.size(); ++index) {
        if (plan->source_mids[index].empty()) continue;
        unassigned.add(index);
        pattern_heap.emplace(plan->source_mids[index].size(), index);
    }
    while (!unassigned.isEmpty()) {
        while (!pattern_heap.empty() && !unassigned.contains(pattern_heap.top().second)) {
            pattern_heap.pop();
        }
        if (pattern_heap.empty()) break;
        const uint32_t seed = pattern_heap.top().second;
        pattern_heap.pop();
        roaring::Roaring group_sources = IncrementalGcnpSeedCorePrefixGroup(
            plan->source_mids, columns, unassigned, seed);
        RelationDstSet common_mids;
        bool initialized = false;
        for (uint32_t source_index : group_sources) {
            if (!initialized) {
                common_mids = plan->source_mids[source_index];
                initialized = true;
            } else {
                common_mids.bitmap &= plan->source_mids[source_index].bitmap;
            }
        }
        plan->groups.push_back({std::move(group_sources), std::move(common_mids)});
        unassigned -= plan->groups.back().sources;
    }

    // `columns` is already the complete mid -> source inverse index for the
    // sealed left panel.  Move it into the residual path, then subtract just
    // the sources covered by a common GCNP group.  This is equivalent to the
    // former per-source/per-mid expansion but preserves Roaring compression.
    plan->residual_sources_by_mid = std::move(columns);
    plan->group_targets.resize(plan->groups.size());
    for (size_t group_id = 0; group_id < plan->groups.size(); ++group_id) {
        const auto& group = plan->groups[group_id];
        const bool shared = group.sources.cardinality() > 1 && !group.common_mids.empty();
        if (shared) {
            for (uint64_t mid : group.common_mids) {
                plan->common_groups_by_mid[mid].push_back(group_id);
                auto residual = plan->residual_sources_by_mid.find(mid);
                if (residual != plan->residual_sources_by_mid.end()) {
                    residual->second -= group.sources;
                }
            }
        }
    }
    for (auto it = plan->residual_sources_by_mid.begin();
         it != plan->residual_sources_by_mid.end();) {
        if (it->second.isEmpty()) {
            it = plan->residual_sources_by_mid.erase(it);
        } else {
            ++it;
        }
    }
    if (stats != nullptr) {
        stats->group_extract_us = static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - group_begin).count());
        stats->groups = plan->groups.size();
        for (const auto& group : plan->groups) {
            const uint64_t uses = group.sources.cardinality();
            stats->grouped_source_uses += uses;
            if (uses > 1 && !group.common_mids.empty()) {
                ++stats->reusable_groups;
                stats->shared_source_uses += uses;
            }
        }
    }
    return plan;
}

static RelationRows IncrementalApplyGcnpDeltaPlan(
    IncrementalGcnpDeltaPlan& plan, const RelationRows& rhs_delta) {
    RelationRows candidates;
    if (rhs_delta.empty()) return candidates;
    lock_guard<mutex> lk(plan.mu);
    for (const auto& [mid, destinations] : rhs_delta) {
        if (destinations.empty()) continue;
        auto groups = plan.common_groups_by_mid.find(mid);
        if (groups != plan.common_groups_by_mid.end()) {
            for (size_t group_id : groups->second) {
                RelationDstSet fresh;
                fresh.bitmap = destinations.bitmap - plan.group_targets[group_id].bitmap;
                plan.group_targets[group_id].bitmap |= destinations.bitmap;
                if (fresh.empty()) continue;
                for (uint32_t source_index : plan.groups[group_id].sources) {
                    candidates[plan.sources[source_index]] |= fresh;
                }
            }
        }
        auto residuals = plan.residual_sources_by_mid.find(mid);
        if (residuals != plan.residual_sources_by_mid.end()) {
            for (uint32_t source_index : residuals->second) {
                candidates[plan.sources[source_index]] |= destinations;
            }
        }
    }
    return candidates;
}

// Prime a newly built plan with the RHS already resident at left-panel seal.
// This is deliberately bulk-oriented: the common pattern union and each
// source residual use the accepted fixed union kernel. Later RHS deltas
// use IncrementalApplyGcnpDeltaPlan and stay fine-grained.
static RelationRows IncrementalPrimeGcnpDeltaPlan(
    IncrementalGcnpDeltaPlan& plan, const RelationRows& right) {
    RelationRows candidates;
    if (right.empty()) return candidates;
    lock_guard<mutex> lk(plan.mu);
    candidates.reserve(plan.sources.size() * 2 + 1);
    auto union_scratch = make_unique<GcnpBitmapUnionScratch>();
    GcnpReadOnlyRightRows right_lookup(right);
    for (size_t group_id = 0; group_id < plan.groups.size(); ++group_id) {
        const auto& group = plan.groups[group_id];
        const bool shared = group.sources.cardinality() > 1 &&
            !group.common_mids.empty();
        RelationDstSet shared_targets;
        if (shared) {
            shared_targets = GcnpBuildPatternTargets(group.common_mids, right_lookup, *union_scratch);
            plan.group_targets[group_id] = shared_targets;
        }
        for (uint32_t source_index : group.sources) {
            RelationDstSet output;
            if (shared) output = shared_targets;
            vector<const SharedBitmap64*> residual_inputs;
            GcnpReadOnlyRightRows::Cursor cursor;
            for (uint64_t mid : plan.source_mids[source_index]) {
                if (shared && group.common_mids.bitmap.contains(mid)) continue;
                const auto* rhs = right_lookup.find(mid, cursor);
                if (rhs) residual_inputs.push_back(&rhs->bitmap);
            }
            if (shared && !shared_targets.empty())
                residual_inputs.insert(residual_inputs.begin(), &shared_targets.bitmap);
            output = GcnpUnionBitmapInputs(residual_inputs, *union_scratch);
            if (!output.empty()) candidates.emplace(
                plan.sources[source_index], std::move(output));
        }
    }
    return candidates;
}

struct IncrementalMatrixBlockState {
    RelationRows left_rows;
    RelationRows right_rows;
    // The S[a] shard used to suppress already-covered outputs while this
    // destination worker evaluates P[a] o S[b].
    RelationRows coverage_rows;
    unordered_map<uint64_t, GcnpCoverageSignature> coverage_signatures;
    RelationRows output_rows;
    unordered_map<uint64_t, GcnpCoverageSignature> output_signatures;
    bool retain_output_on_destination = false;
    // This is now a final-fetch visibility marker only. No intermediate
    // endpoint relation is returned to the parent/source owner.
    bool return_output_to_parent = false;
    bool resident_coverage_seeded = false;
    bool coverage_include_identity = false;
    int coverage_parent_pid = -1;
    uint64_t coverage_parent_batch_id = 0;
    int coverage_body_id = -1;
    int coverage_length = 0;
    RelationDstSet coverage_sources;
    uint64_t pending_input_updates = 0;
    unordered_set<uint64_t> completed_right_streams;
    uint64_t expected_right_streams = 0;
    unordered_set<uint64_t> completed_resident_left_streams;
    uint64_t expected_resident_left_streams = 0;
    uint64_t finalize_token = 0;
    bool input_sealed = false;
    bool finalize_queued = false;
    bool finalized = false;
    int right_parent_pid = -1;
    uint64_t right_parent_batch_id = 0;
    uint64_t expected_right_rows = 0;
    uint64_t expected_right_pairs = 0;
    RelationDstSet right_sources;
    int right_body_id = -1;
    int right_length = 0;
    bool right_include_identity = false;
    shared_ptr<GcnpPatternTargetMemo> speculative_pattern_memo;
    shared_ptr<atomic<bool>> speculative_cancel;
    bool speculative_queued = false;
    uint64_t speculative_generation = 0;
    uint64_t speculative_memoized_generation = 0;
    bool saw_left_input = false;
    uint64_t left_input_batches = 0;
    chrono::steady_clock::time_point first_left_input_at{};
    chrono::steady_clock::time_point left_panel_sealed_at{};
    chrono::steady_clock::time_point rhs_ready_at{};
    bool panel_wait_recorded = false;
    shared_ptr<IncrementalGcnpDeltaPlan> incremental_gcnp_plan;
    bool incremental_gcnp_plan_queued = false;
    uint64_t pending_middle_compute_tasks = 0;
    vector<IncrementalMatrixJoinUpdate> pending_middle_seals;
    // Synchronous middle mode accumulates both panels but never joins a
    // delta. The two seals are independent so an early source owner may
    // transfer completed rows while other owners are still working.
    bool synchronous_middle_left_sealed = false;
    bool synchronous_middle_right_sealed = false;
    bool synchronous_middle_join_queued = false;
    RelationRows synchronous_middle_pending_rhs_rows;
    bool synchronous_middle_rhs_apply_queued = false;
};
// Physical B^k[mid,l] payload retained by its destination owner W_l.  It is
// intentionally independent of a parent DemandBatch so RSFM can replay only
// its compact signature and left panels when the same B power is reused.
struct IncrementalRelationShardKey {
    int body_id = -1;
    int length = 0;
    bool operator==(const IncrementalRelationShardKey& other) const {
        return body_id == other.body_id && length == other.length;
    }
};
struct IncrementalRelationShardKeyHash {
    size_t operator()(const IncrementalRelationShardKey& key) const {
        size_t h = hash<int>{}(key.body_id);
        h ^= hash<int>{}(key.length + 0x9e3779b9 + static_cast<int>(h));
        return h;
    }
};
// A direct-right stream is owned by a physical C-output worker.  It becomes
// complete only after the upstream B stream ended and all local delta joins
// submitted by that stream have finished.
struct IncrementalDirectMatrixStreamKey {
    int parent_pid = -1;
    uint64_t parent_batch_id = 0;
    uint64_t stream_id = 0;
    bool operator==(const IncrementalDirectMatrixStreamKey& other) const {
        return parent_pid == other.parent_pid &&
               parent_batch_id == other.parent_batch_id &&
               stream_id == other.stream_id;
    }
};
struct IncrementalDirectMatrixStreamKeyHash {
    size_t operator()(const IncrementalDirectMatrixStreamKey& key) const {
        size_t h = hash<int>{}(key.parent_pid);
        h ^= hash<uint64_t>{}(key.parent_batch_id + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        h ^= hash<uint64_t>{}(key.stream_id + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2));
        return h;
    }
};
struct IncrementalDirectMatrixStreamState {
    uint64_t pending_updates = 0;
    bool upstream_complete = false;
};
struct IncrementalDirectSignatureStreamState {
    RelationRows announced;
    RelationRows pending;
};
struct IncrementalMiddlePresenceAccumulator {
    mutex mu;
    RelationDstSet mids;
};
// Compact control-plane view of a destination-resident relation. Endpoint
// sets never return to the parent; it only learns which source rows exist in
// each physical destination shard and their cardinalities.
struct IncrementalMatrixResidentDirectory {
    vector<RelationDstSet> sources_by_destination;
    vector<uint64_t> rows_by_destination;
    vector<uint64_t> pairs_by_destination;
};
struct IncrementalDemandBatch {
    uint64_t id = 0;
    int query_id = 0;
    int body_id = 0;
    int length = 0;
    RelationDstSet sources;
    RelationRows left_rows;
    RelationRows right_rows;
    RelationRows output_rows;
    RelationRows pending_emit;
    RelationRows pending_progress;
    uint64_t pending_progress_pairs = 0;
    RelationDstSet requested_mids;
    bool left_complete = false;
    uint64_t pending_right_batches = 0;
    uint64_t pending_shared_rows = 0;
    // A direct B stream completes only after the source owner has consumed
    // its compact routing signature and every destination block has acked.
    uint64_t pending_direct_right_streams = 0;
    unordered_map<uint64_t, uint64_t> direct_stream_pending_acks;
    unordered_set<uint64_t> direct_stream_signature_pending;
    uint64_t pending_matrix_joins = 0;
    // Middle-owner composition sends one update and one seal per middle
    // partition. This ledger makes completion independent of arrival order.
    unordered_set<int> middle_completed_partitions;
    bool matrix_dispatched = false;
    bool matrix_dispatching = false;
    bool cache_output_rows = true;
    bool generic_matrix_compose = false;
    // This batch is supplied by destination-resident continuation streams,
    // not by the controller's local RelationRows operands.  It owns only the
    // output handle/directory and completion tokens.
    bool resident_continuation_compose = false;
    unordered_set<int> resident_finalized_destinations;
    int matrix_right_parent_pid = -1;
    uint64_t matrix_right_parent_batch_id = 0;
    int matrix_right_body_id = -1;
    int matrix_right_length = 0;
    bool matrix_right_include_identity = false;
    // A generic matrix composition may provide an output coverage relation.
    // It is split by destination together with the RHS and consumed only by
    // the matching matrix blocks.
    RelationRows matrix_coverage_rows;
    // Immutable indexes built once when a generic compose batch starts. The
    // route path reuses these across all left/right delta callbacks.
    shared_ptr<unordered_map<int, RelationRows>> matrix_coverage_blocks;
    shared_ptr<unordered_map<int, unordered_map<uint64_t, GcnpCoverageSignature>>>
        matrix_coverage_signatures;
    int matrix_coverage_parent_pid = -1;
    uint64_t matrix_coverage_parent_batch_id = 0;
    int matrix_coverage_body_id = -1;
    int matrix_coverage_length = 0;
    bool matrix_coverage_include_identity = false;
    RelationDstSet matrix_coverage_sources;
    RelationDstSet matrix_right_sources;
    unordered_map<int, RelationDstSet> matrix_coverage_sources_by_destination;
    unordered_map<int, RelationDstSet> matrix_right_sources_by_destination;
    unordered_map<int, uint64_t> matrix_right_rows_by_destination;
    unordered_map<int, uint64_t> matrix_right_pairs_by_destination;
    shared_ptr<IncrementalMatrixResidentDirectory> matrix_output_directory;
    bool retain_output_on_destination = false;
    bool return_output_to_parent = false;
    bool flush_queued = false;
    bool complete = false;
    // Monotone RSFM signatures. Bit d means that this middle vertex has a
    // right-side value in destination shard d.
    unordered_map<uint64_t, uint64_t> rsfm_signature_by_middle;
    // Reverse index used when a new destination becomes active for a mid.
    // This avoids rescanning the complete source->mid relation on every
    // right-side delta.
    unordered_map<uint64_t, RelationDstSet> rsfm_sources_by_middle;
    // Source-side presence routing retains unmatched sides until the same
    // middle is present in both operands of this demand batch.
    unordered_map<uint64_t, RelationDstSet> middle_sources_by_middle;
    // Presence-only RHS index used by the synchronous middle data plane.
    // Values are intentionally not stored here; the RHS payload remains at
    // owner(mid) and is installed directly in the middle block.
    unordered_set<uint64_t> middle_rhs_present;
    RelationRows middle_sent_left;
    RelationRows middle_sent_right;
    bool middle_semijoin_enabled = true;
    // Route deltas are accumulated by physical C-block owner before they are
    // turned into matrix updates.  The input streams are monotone, so unioning
    // deltas here preserves the exact-once delivery invariant while avoiding
    // one RPC and one retained-block setup per small signature activation.
    unordered_map<int, RelationRows> rsfm_pending_left_by_destination;
    // Signature-triggered historical replay keeps the natural mid -> sources
    // layout until the destination worker expands it in parallel.
    unordered_map<int, RelationRows> rsfm_pending_transposed_left_by_destination;
    unordered_map<int, RelationRows> rsfm_pending_right_by_destination;
    bool rsfm_route_flush_queued = false;
    uint64_t pending_matrix_route_flushes = 0;
    uint64_t direct_right_streams_started = 0;
    bool matrix_seals_dispatched = false;
    bool middle_left_seals_dispatched = false;
    // Direct RHS shards are delivered eagerly, while their compact
    // destination signatures are accumulated. Once every requested signature
    // is complete, route the retained left frontier once with its final masks
    // instead of replaying it for every newly activated destination bit.
    bool rsfm_signatures_final = false;
    vector<IncrementalRowsCallback> callbacks;
    // Fresh output is exposed to the parent before this batch is sealed so it
    // can prefetch dependent RHS rows. These callbacks never publish COMPLETE;
    // the normal callbacks above remain the only completion authority.
    vector<IncrementalRowsCallback> progress_callbacks;
};
struct IncrementalOpenBodyPreparation {
    bool started = false;
    bool complete = false;
    bool bulk = false;
    vector<uint64_t> sources;
    uint64_t rows_complete = 0;
    RelationRows bulk_rows;
};
// A remote row is subscribed to once per requesting worker.  Local parents
// attach to this relay, receive an immediate replay, and then share future
// deltas.  Without this proxy, every parent continuation sends its own RPC
// subscription for the same (body, length, source) row.
struct IncrementalRemoteRelay {
    bool requested = false;
    bool complete = false;
    RelationDstSet values;
    vector<IncrementalDeltaCallback> callbacks;
    vector<IncrementalBinaryWaiter> binary_waiters;
};
struct IncrementalDelegatedMiddleJoin {
    IncrementalMatrixBlockKey block_key;
    int original_parent_pid = -1;
    uint64_t original_parent_batch_id = 0;
    uint64_t original_token = 0;
    int middle_partition = -1;
    uint64_t pending_tasks = 0;
    RelationRows candidates;
};
struct IncrementalGlobalExactMiddleStage {
    RelationRows left_rows;
    RelationRows right_rows;
    RelationRows coverage_rows;
    bool restore_coverage_output = false;
    RelationDstSet requested_sources;
    int base_body_id = -1;
    bool global_base_stage = false;
    bool global_virtual_stage = false;
    bool defer_factorized_materialization = false;
    roaring::Roaring rhs_sealed_senders;
    roaring::Roaring sealed_senders;
    bool join_queued = false;
};
struct IncrementalDirectBalanceStage {
    mutex mu;
    IncrementalGlobalExactMiddleStage inputs;
    vector<RelationRows> left_panels, coverage_panels;
    vector<RelationDstSet> rhs_keys_by_producer;
    RelationDstSet needed_rhs_keys;
    vector<RelationDstSet> requested_rhs_keys_by_producer;
    vector<uint64_t> rhs_replies_pending;
    vector<RelationRows> rhs_panels;
    chrono::steady_clock::time_point first_lhs_received;
    chrono::steady_clock::time_point rhs_request_started;
};
struct IncrementalSourceSetPendingGroup {
    RelationDstSet sources;
    vector<RelationDstSet> destinations;
};
struct IncrementalSourceSetGroupShard {
    mutex mu;
    unordered_map<uint64_t, vector<size_t>> candidates_by_fingerprint;
    vector<IncrementalSourceSetPendingGroup> groups;
};
struct IncrementalSourceSetAccumulator {
    explicit IncrementalSourceSetAccumulator(size_t shard_count) {
        shards.reserve(max<size_t>(1, shard_count));
        for (size_t index = 0; index < max<size_t>(1, shard_count); ++index) {
            shards.push_back(make_unique<IncrementalSourceSetGroupShard>());
        }
    }
    vector<unique_ptr<IncrementalSourceSetGroupShard>> shards;
};
struct IncrementalGlobalExactOutputStage {
    RelationRows rows;
    // Exact source-set groups are accumulated online as middle contributions
    // arrive, then materialized into normal source rows when the stage seals.
    shared_ptr<IncrementalSourceSetAccumulator> source_set_accumulator;
    roaring::Roaring completed_middles;
    uint64_t expected_middles = 0;
    function<void(RelationRows)> done;
};
struct IncrementalBalancedChunk {
    vector<uint64_t> sources;
    uint64_t work = 0;
    uint64_t input_bytes = 0;
};
struct IncrementalComposeInputOwners {
    shared_ptr<const RelationRows> left, right, coverage;
    bool restore_coverage_output = false;
    // Output rows are placed at IncrementalResultHome(source) instead of the
    // graph owner. Only for relations consumed solely as compose inputs.
    bool home_output = false;
};
struct IncrementalBalancedPanel {
    RelationRows left;
    RelationRows right;
    RelationRows coverage;
    IncrementalComposeInputOwners input_owners;
    bool restore_coverage_output = false;
    vector<IncrementalBalancedChunk> chunks;
    bool defer_materialization = false;
    int open_body_id = -1;
    RelationDstSet rhs_keys;
};
struct IncrementalBalanceReports {
    unordered_map<int, vector<pair<uint64_t, uint64_t>>> costs;
};
struct IncrementalResultSendDictionary {
    unordered_map<string, uint64_t> exact_ids;
    uint64_t next_dictionary_id = 1;
    uint64_t last_dictionary_id = 0;
    RelationDstSet last_destinations;
};
struct IncrementalSourceExactCache {
    RelationDstSet computed_sources;
    RelationRows rows;
};
struct IncrementalQueryState {
    IncrementalProgram program;
    unordered_map<IncrementalRowKey, IncrementalRowState, IncrementalRowKeyHash> rows;
    unordered_map<uint64_t, IncrementalCallback> callbacks;
    unordered_map<IncrementalRowKey, IncrementalRemoteRelay, IncrementalRowKeyHash> remote_relays;
    unordered_map<uint64_t, IncrementalRowKey> remote_tokens;
    unordered_map<uint64_t, IncrementalRowsCallback> remote_demand_callbacks;
    // A middle RHS-direct request returns only nonempty middle keys to the
    // source owner. This maps the compact presence stream token back to its
    // parent DemandBatch without materializing the RHS payload there.
    unordered_map<uint64_t, uint64_t> middle_presence_batches;
    // Direct B payloads bypass the parent.  This map routes destination
    // completion acks back to the DemandBatch that issued the request.
    unordered_map<uint64_t, uint64_t> direct_stream_batches;
    unordered_map<uint64_t, IncrementalDemandBatch> demand_batches;
    unordered_map<int, IncrementalOpenBodyPreparation> open_body_preparations;
    unordered_map<int, shared_ptr<IncrementalOpenPlan>> open_plans;
    // Matrix result acknowledgements are routed back to the DemandBatch that
    // submitted them.  The physical C block itself remains on W_l.
    unordered_map<uint64_t, uint64_t> matrix_join_tokens;
    unordered_map<IncrementalMatrixBlockKey, IncrementalMatrixBlockState,
                  IncrementalMatrixBlockKeyHash> matrix_blocks;
    unordered_map<uint64_t, IncrementalDelegatedMiddleJoin>
        delegated_middle_joins;
    unordered_map<uint64_t, IncrementalGlobalExactMiddleStage>
        global_exact_middle_stages;
    unordered_map<uint64_t, IncrementalGlobalExactOutputStage>
        global_exact_output_stages;
    unordered_map<uint64_t, IncrementalBalancedPanel> balance_panels;
    unordered_set<uint64_t> home_output_stages;
    unordered_map<uint64_t, IncrementalBalanceReports> balance_reports;
    unordered_map<uint64_t, vector<int>> balance_bucket_workers;
    unordered_map<uint64_t, IncrementalGlobalExactOutputStage> balance_reduction_stages;
    unordered_map<uint64_t, shared_ptr<const RelationRows>> balance_direct_rhs;
    unordered_map<uint64_t, shared_ptr<BicliqueImmutableWireCache>> balance_rhs_wire_cache;
    unordered_map<uint64_t, roaring::Roaring> balance_direct_requests;
    unordered_map<uint64_t, shared_ptr<IncrementalDirectBalanceStage>> balance_direct_stages;
    uint64_t balance_input_route_us = 0;
    uint64_t balance_input_merge_us = 0;
    uint64_t balance_rhs_select_us = 0, balance_rhs_pack_us = 0;
    uint64_t balance_rhs_queue_us = 0, balance_rhs_encode_us = 0;
    uint64_t balance_rhs_wrap_us = 0, balance_rhs_raw_bytes = 0;
    uint64_t balance_rhs_wire_bytes = 0;
    uint64_t balance_rhs_examined_rows = 0, balance_rhs_requests = 0;
    uint64_t owner_view_left_rows = 0, owner_view_coverage_rows = 0;
    uint64_t owner_view_rhs_inputs = 0, owner_view_final_moves = 0;
    array<uint64_t, 3> bounded_owner_us{};
    uint64_t helper_closure_restore_us = 0, helper_closure_restore_rows = 0;
    uint64_t async_accept_queue_us = 0, async_accept_run_us = 0, async_accept_messages = 0;
    uint64_t balance_rhs_requested_keys = 0, balance_rhs_keys_avoided = 0;
    uint64_t balance_rhs_empty_requests_avoided = 0;
    uint64_t balance_open_closure_us = 0;
    uint64_t balance_open_closure_rows = 0;
    uint64_t balance_open_closure_seeds = 0;
    uint64_t balance_planner_us = 0;
    uint64_t balance_join_us = 0;
    uint64_t balance_reduce_us = 0;
    uint64_t balance_dispatch_us = 0;
    uint64_t balance_coverage_us = 0;
    uint64_t balance_gather_us = 0;
    uint64_t balance_remote_tasks = 0;
    uint64_t balance_local_tasks = 0;
    // Peer sender strands are FIFO. A destination bitmap defined once for a
    // source owner can therefore be referenced by later result messages
    // without an announce/need round trip.
    vector<IncrementalResultSendDictionary> result_send_dictionaries;
    vector<unordered_map<uint64_t, RelationDstSet>>
        result_receive_dictionaries;
    // Complete P1 rows used by relation-granular exact/bounded stages.  This
    // cache avoids rebuilding a row-state machine when later dual-state levels
    // revisit the same source frontier.
    unordered_map<int, RelationRows> global_base_relation_cache;
    unordered_map<int, RelationDstSet> global_base_sources_scanned;
    unordered_map<int, unordered_map<int, IncrementalSourceExactCache>>
        global_source_exact_cache;
    unordered_map<IncrementalRelationShardKey, RelationRows,
                  IncrementalRelationShardKeyHash> destination_relation_shards;
    // Source-owner-side ledger: a B^k[mid,l] payload is installed at W_l once
    // per query, even when several DemandBatches reuse the same cached row.
    unordered_map<IncrementalRelationShardKey, unordered_map<int, RelationRows>,
                  IncrementalRelationShardKeyHash> delivered_relation_shards;
    unordered_map<IncrementalDirectMatrixStreamKey, IncrementalDirectMatrixStreamState,
                  IncrementalDirectMatrixStreamKeyHash> direct_matrix_streams;
    unordered_map<IncrementalDirectMatrixStreamKey, IncrementalDirectSignatureStreamState,
                  IncrementalDirectMatrixStreamKeyHash> direct_signature_streams;
    vector<IncrementalGcnpJoinRequest> gcnp_pending_joins;
    bool gcnp_flush_queued = false;
    IncrementalGcnpMetrics gcnp_metrics;
    uint64_t next_token = 1;
    uint64_t next_demand_batch_id = 1;
    RelationRows final_rows;
    // A top-level exact leaf already owns the final rows.  Keep its source
    // list and fetch directly from the exact-row cache instead of unioning
    // every final delta into a second RelationRows copy.
    bool direct_leaf_output = false;
    int direct_leaf_query_id = 0;
    int direct_leaf_body_id = -1;
    int direct_leaf_length = 0;
    vector<uint64_t> root_sources;
    uint64_t roots_total = 0;
    uint64_t roots_done = 0;
    bool roots_initialized = false;
    bool worker_query_started = false;
    bool worker_query_finished = false;
    chrono::steady_clock::time_point worker_query_started_at{};
    chrono::steady_clock::time_point worker_query_finished_at{};
    uint64_t compute_active_tasks = 0;
    uint64_t compute_wall_us = 0;
    chrono::steady_clock::time_point compute_interval_started_at{};
    uint64_t matrix_message_active_tasks = 0;
    uint64_t matrix_message_compute_us = 0;
    chrono::steady_clock::time_point matrix_message_interval_started_at{};
    uint64_t rows_started = 0;
    uint64_t row_cache_hits = 0;
    uint64_t remote_requests = 0;
    uint64_t remote_request_rpcs = 0;
    uint64_t remote_complete_rpcs = 0;
    uint64_t remote_stream_rpcs = 0;
    uint64_t delta_count = 0;
    // Low-overhead cumulative profiling. These counters are only read by the
    // status RPC and expose where an exact-power query spends its event work.
    uint64_t base_expand_tasks = 0;
    uint64_t base_expand_us = 0;
    uint64_t gcnp_input_mids = 0;
    uint64_t gcnp_planned_mids = 0;
    uint64_t gcnp_bypassed_mids = 0;
    uint64_t gcnp_wall_us = 0;
    uint64_t gcnp_complete_prepare_us = 0;
    uint64_t gcnp_complete_group_extract_us = 0;
    uint64_t gcnp_complete_pattern_cache_us = 0;
    uint64_t gcnp_complete_union_us = 0;
    uint64_t gcnp_complete_shared_target_us = 0;
    uint64_t gcnp_complete_source_materialize_us = 0;
    uint64_t gcnp_complete_groups = 0;
    uint64_t gcnp_complete_reusable_groups = 0;
    uint64_t gcnp_complete_grouped_source_uses = 0;
    uint64_t gcnp_complete_shared_source_uses = 0;
    uint64_t gcnp_complete_external_memo_hits = 0;
    uint64_t speculative_gcnp_tasks = 0;
    uint64_t speculative_gcnp_completed = 0;
    uint64_t speculative_gcnp_cancelled = 0;
    uint64_t speculative_gcnp_us = 0;
    uint64_t speculative_pattern_memo_stores = 0;
    uint64_t speculative_pattern_memo_hits = 0;
    uint64_t panel_wait_blocks = 0;
    uint64_t panel_left_input_batches = 0;
    uint64_t all_left_panels_sealed_elapsed_us = 0;
    uint64_t left_panel_wait_us = 0;
    uint64_t left_panel_wait_us_max = 0;
    uint64_t rhs_ready_before_left_blocks = 0;
    uint64_t rhs_ready_wait_for_left_us = 0;
    uint64_t rhs_ready_wait_for_left_us_max = 0;
    uint64_t left_sealed_before_rhs_blocks = 0;
    uint64_t left_wait_for_rhs_us = 0;
    uint64_t left_wait_for_rhs_us_max = 0;
    uint64_t join_batch_count = 0;
    uint64_t join_batch_children = 0;
    uint64_t join_batch_child_events = 0;
    uint64_t join_batch_publish_pairs = 0;
    uint64_t join_batch_wall_us = 0;
    uint64_t emit_merge_us = 0;
    uint64_t demand_batches_started = 0;
    uint64_t demand_batch_emits = 0;
    uint64_t demand_batch_pairs = 0;
    uint64_t demand_compose_calls = 0;
    uint64_t demand_compose_us = 0;
    uint64_t matrix_join_updates = 0;
    uint64_t matrix_join_result_pairs = 0;
    uint64_t matrix_join_us = 0;
    uint64_t factorized_result_messages = 0;
    uint64_t factorized_result_shared_blocks = 0;
    uint64_t factorized_result_dictionary_merges = 0;
    uint64_t factorized_result_wire_bytes_saved = 0;
    uint64_t factorized_result_coalesce_us = 0;
    uint64_t factorized_result_expand_us = 0;
    uint64_t factorized_result_union_us = 0;
    uint64_t factorized_result_encode_us = 0;
    uint64_t factorized_result_decode_us = 0;
    uint64_t result_commit_us = 0;
    uint64_t result_queue_wait_us = 0;
    uint64_t result_restore_us = 0, result_restore_max_lanes = 0;
    uint64_t factorized_result_blocks_after_coalesce = 0;
    uint64_t factorized_result_source_uses = 0;
    uint64_t factorized_source_set_blocks_before = 0;
    uint64_t factorized_source_set_blocks_after = 0;
    uint64_t factorized_source_set_reused_groups = 0;
    uint64_t factorized_source_set_blocks_merged = 0;
    uint64_t factorized_source_union_operations_avoided = 0;
    uint64_t factorized_source_set_group_us = 0;
    uint64_t factorized_source_set_union_us = 0;
    // Physical owners retain graph/state ownership. The opt-in stage balancer
    // records delegated stateless compute separately from retained state.
    uint64_t middle_scheduler_assigned_shards = 0;
    uint64_t middle_scheduler_submitted_work = 0;
    uint64_t middle_scheduler_completed_work = 0;
    uint64_t middle_scheduler_executed_work = 0;
    vector<uint64_t> middle_scheduler_outstanding_work;
    uint64_t range_coverage_pairs = 0;
    uint64_t pre_route_coverage_candidate_mids = 0;
    uint64_t pre_route_coverage_signature_rejections = 0;
    uint64_t pre_route_coverage_subset_tests = 0;
    uint64_t pre_route_coverage_skipped_mids = 0;
    uint64_t pre_route_coverage_skipped_rhs_pairs = 0;
    uint64_t pre_route_coverage_input_left_pairs = 0;
    uint64_t pre_route_coverage_output_left_pairs = 0;
    uint64_t rsfm_selected_left_pairs = 0;
    uint64_t rsfm_new_left_pairs = 0;
    uint64_t rsfm_replay_left_pairs = 0;
    uint64_t rsfm_right_pairs = 0;
    uint64_t rsfm_full_broadcast_pairs = 0;
    uint64_t rsfm_routed_messages = 0;
    uint64_t rsfm_signature_activations = 0;
    uint64_t rsfm_empty_left_dispatches = 0;
    uint64_t rsfm_resident_rhs_pairs_avoided = 0;
    uint64_t resident_rhs_expected_rows = 0;
    uint64_t resident_rhs_actual_rows = 0;
    uint64_t resident_rhs_expected_pairs = 0;
    uint64_t resident_rhs_actual_pairs = 0;
    uint64_t resident_rhs_mismatched_shards = 0;
    array<uint64_t, kIncrementalPerfSlots> emit_published_calls{};
    array<uint64_t, kIncrementalPerfSlots> emit_published_pairs{};
};
struct IncrementalWorkerState {
    int partition_id = 0;
    int total_partitions = 1;
    GraphPartition partition;
    PartRouter router;
    bool router_configured = false;
    // Some generated v2p files encode the exact cyclic assignment
    // owner(v)=v mod P.  Detecting that once at startup removes a binary
    // search from every routed right-side pair while retaining the general
    // v2p fallback for arbitrary graph partitions.
    bool owner_is_verified_modulo = false;
    string partition_root;
    vector<PeerInfo> peers;
    vector<unique_ptr<TcpClient>> clients;
    vector<unique_ptr<mutex>> client_mu;
    vector<unique_ptr<IncrementalExecutor>> peer_senders;
    vector<unique_ptr<mutex>> peer_delta_mu;
    vector<vector<IncrementalRowStreamEnvelope>> peer_delta_pending;
    vector<size_t> peer_delta_bytes;
    // Distinct peers use distinct mutexes. vector<bool> packs their flags
    // into shared words, so a clear for one peer can overwrite another
    // peer's scheduled bit and strand its final partial batch.
    vector<uint8_t> peer_delta_flush_scheduled;
    vector<unique_ptr<mutex>> peer_request_mu;
    vector<vector<IncrementalRowRequestEnvelope>> peer_request_pending;
    vector<uint8_t> peer_request_flush_scheduled;
    vector<unique_ptr<mutex>> peer_complete_mu;
    vector<vector<IncrementalRowCompleteEnvelope>> peer_complete_pending;
    vector<uint8_t> peer_complete_flush_scheduled;
    vector<unique_ptr<mutex>> peer_base_expand_mu;
    vector<vector<IncrementalBaseEvent>> peer_base_expand_pending;
    vector<size_t> peer_base_expand_bytes;
    vector<uint8_t> peer_base_expand_flush_scheduled;
    vector<unique_ptr<mutex>> peer_base_result_mu;
    vector<vector<IncrementalBaseResult>> peer_base_result_pending;
    vector<size_t> peer_base_result_bytes;
    vector<uint8_t> peer_base_result_flush_scheduled;
    vector<unique_ptr<mutex>> peer_result_dictionary_mu;
    mutex peer_init_mu;
    mutex query_lifecycle_mu;
    mutex query_handler_mu;
    condition_variable query_handler_cv;
    size_t active_query_handlers = 0;
    mutex mu;
    atomic<int> active_query_id{-1};
    atomic<bool> peer_failed{false};
    mutex peer_failure_mu;
    string peer_failure;
    atomic<uint64_t> peer_wire_bytes{0};
    atomic<uint64_t> peer_wire_messages{0};
    atomic<uint64_t> peer_e2e_us{0};
    atomic<uint64_t> peer_e2e_messages{0};
    atomic<bool> peer_communication_tracking{false};
    mutex peer_communication_mu;
    uint64_t peer_communication_active = 0;
    uint64_t peer_communication_wall_us = 0;
    chrono::steady_clock::time_point peer_communication_started_at{};
    uint64_t peer_communication_e2e_active = 0;
    uint64_t peer_communication_e2e_wall_us = 0;
    chrono::steady_clock::time_point peer_communication_e2e_started_at{};
    // Account actual serialized peer payload bytes by data-plane role.  The
    // RSFM replay is transported as a matrix left panel, so it is included in
    // LHS rather than inferred from its logical pair count.
    enum class PeerWireKind : size_t {
        BASE = 0,
        ROW,
        DEMAND,
        SIGNATURE,
        RHS,
        LHS,
        MATRIX_RESULT,
        OTHER,
        COUNT,
    };
    static constexpr size_t kPeerWireKindCount = static_cast<size_t>(PeerWireKind::COUNT);
    array<atomic<uint64_t>, kPeerWireKindCount> peer_wire_by_kind{};
    unordered_map<int, IncrementalQueryState> queries;
    // State transitions remain serialized for deterministic DELTA/COMPLETE
    // ordering. Graph/NFA edge expansion uses this same serial executor.
    IncrementalExecutor executor;
    // Only batches non-data-plane COMPLETE control messages.  It must not
    // delay the demand or delta critical path.
    IncrementalExecutor control_executor;
    IncrementalExecutor compute_executor;
    // Message-matrix fingerprinting and exact equality grouping use a
    // persistent pool so repeated compose stages do not create temporary
    // threads or consume GCNP/join compute slots.
    IncrementalExecutor factorization_executor;
    // Sender-side factorization is data-plane post-processing, but it must
    // not keep a compute_executor task occupied after GCNP/join has finished.
    IncrementalExecutor matrix_message_executor;
    // A speculative Roaring union is memory-bandwidth bound. One worker-wide
    // slot keeps cache warming from competing with every formal join slot.
    atomic<bool> speculative_gcnp_active{false};
    IncrementalWorkerState(int id, int total)
        : partition_id(id), total_partitions(total), partition(id, total),
          executor(1), control_executor(1),
          // Keep the main GCNP/join path strictly single-threaded. All
          // message queues below retain one consumer each; they do not
          // split a preparation operation across helper threads.
          compute_executor(1),
          factorization_executor(IncrementalMatrixMessageThreadCount(total)),
          matrix_message_executor(IncrementalMatrixMessageThreadCount(total)) {
        executor.SetActivityHooks(
            [this]() { IncrementalTrackOrderedExecutorActivity(*this, true); },
            [this]() { IncrementalTrackOrderedExecutorActivity(*this, false); });
        matrix_message_executor.SetActivityHooks(
            [this]() { IncrementalTrackMatrixMessageActivity(*this, true); },
            [this]() { IncrementalTrackMatrixMessageActivity(*this, false); });
        factorization_executor.SetActivityHooks(
            [this]() { IncrementalTrackMatrixMessageActivity(*this, true); },
            [this]() { IncrementalTrackMatrixMessageActivity(*this, false); });
    }
};

static void IncrementalRecordPeerFailure(
    IncrementalWorkerState& state, int query_id, int pid,
    MessageType type, string detail) {
    if (query_id < 0) return;
    replace(detail.begin(), detail.end(), '\n', ' ');
    replace(detail.begin(), detail.end(), '\r', ' ');
    lock_guard<mutex> lk(state.peer_failure_mu);
    if (state.active_query_id.load(memory_order_acquire) != query_id ||
        state.peer_failed.load(memory_order_relaxed)) return;
    state.peer_failure = "query=" + to_string(query_id) +
        " worker=" + to_string(state.partition_id) +
        " peer=" + to_string(pid) +
        " message=" + to_string(static_cast<uint32_t>(type)) +
        " error=" + detail.substr(0, 1024);
    state.peer_failed.store(true, memory_order_release);
    cerr << "[incremental-worker] query peer failure: " << state.peer_failure << endl;
}

class IncrementalQueryMessageGuard {
public:
    IncrementalQueryMessageGuard(IncrementalWorkerState& state, int query_id)
        : state_(&state) {
        lock_guard<mutex> lk(state.query_handler_mu);
        if (state.active_query_id.load(memory_order_acquire) != query_id) return;
        ++state.active_query_handlers;
        accepted_ = true;
    }
    ~IncrementalQueryMessageGuard() {
        if (!accepted_) return;
        lock_guard<mutex> lk(state_->query_handler_mu);
        if (--state_->active_query_handlers == 0) state_->query_handler_cv.notify_all();
    }
    explicit operator bool() const { return accepted_; }

private:
    IncrementalWorkerState* state_;
    bool accepted_ = false;
};

static int IncrementalOwner(const IncrementalWorkerState& state, uint64_t vertex) {
    if (state.owner_is_verified_modulo) {
        return static_cast<int>(vertex % static_cast<uint64_t>(state.total_partitions));
    }
    return PartitionOfVertex(state.router_configured ? &state.router : nullptr, vertex, state.total_partitions);
}

// Placement of intermediate (composed) relation rows. Base graph adjacency
// stays at IncrementalOwner(); composed rows are homed by a stable hash of the
// source so every stage's inputs are spread evenly over the p workers instead
// of following the METIS owner, whose per-stage source counts reach 3-10x the
// mean at P16. Only placement changes; rows, GCNP inputs and buckets do not.
static int IncrementalResultHome(const IncrementalWorkerState& state, uint64_t source) {
    return static_cast<int>(StableHash64(source ^ 0x5851f42d4c957f2dULL) %
        static_cast<uint64_t>(state.total_partitions));
}

// Count the union of payload-ready to receiver-acknowledged peer intervals.
static bool IncrementalBeginPeerCommunication(IncrementalWorkerState& state) {
    if (!state.peer_communication_tracking.load(memory_order_acquire)) return false;
    lock_guard<mutex> lk(state.peer_communication_mu);
    if (!state.peer_communication_tracking.load(memory_order_relaxed)) return false;
    if (state.peer_communication_active++ == 0)
        state.peer_communication_started_at = chrono::steady_clock::now();
    return true;
}

static void IncrementalEndPeerCommunication(IncrementalWorkerState& state) {
    lock_guard<mutex> lk(state.peer_communication_mu);
    if (state.peer_communication_active > 0 && --state.peer_communication_active == 0) {
        state.peer_communication_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - state.peer_communication_started_at).count());
    }
}

// Count the union from sender-side message construction/serialization through
// receiver parsing and ACK.  A message can finish serialization while another
// communication interval is already active, so retain the earliest start.
static bool IncrementalBeginPeerCommunicationE2E(
    IncrementalWorkerState& state,
    chrono::steady_clock::time_point message_started_at) {
    if (!state.peer_communication_tracking.load(memory_order_acquire)) return false;
    lock_guard<mutex> lk(state.peer_communication_mu);
    if (!state.peer_communication_tracking.load(memory_order_relaxed)) return false;
    if (state.peer_communication_e2e_active++ == 0) {
        state.peer_communication_e2e_started_at = message_started_at;
    } else if (message_started_at < state.peer_communication_e2e_started_at) {
        state.peer_communication_e2e_started_at = message_started_at;
    }
    return true;
}

static void IncrementalEndPeerCommunicationE2E(IncrementalWorkerState& state) {
    lock_guard<mutex> lk(state.peer_communication_mu);
    if (state.peer_communication_e2e_active > 0 &&
        --state.peer_communication_e2e_active == 0) {
        state.peer_communication_e2e_wall_us += static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() -
                state.peer_communication_e2e_started_at).count());
    }
}

static void IncrementalTrackOrderedExecutorActivity(IncrementalWorkerState& state, bool started) {
    lock_guard<mutex> lk(state.mu);
    const auto now = chrono::steady_clock::now();
    for (auto& [_, query] : state.queries) {
        if (!query.worker_query_started || query.worker_query_finished) continue;
        if (started) {
            if (query.compute_active_tasks++ == 0) query.compute_interval_started_at = now;
        } else if (query.compute_active_tasks > 0 && --query.compute_active_tasks == 0) {
            query.compute_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                now - query.compute_interval_started_at).count());
        }
    }
}

static void IncrementalTrackMatrixMessageActivity(
    IncrementalWorkerState& state, bool started) {
    lock_guard<mutex> lk(state.mu);
    const auto now = chrono::steady_clock::now();
    for (auto& [_, query] : state.queries) {
        if (!query.worker_query_started || query.worker_query_finished) continue;
        if (started) {
            if (query.matrix_message_active_tasks++ == 0) {
                query.matrix_message_interval_started_at = now;
            }
        } else if (query.matrix_message_active_tasks > 0 &&
                   --query.matrix_message_active_tasks == 0) {
            query.matrix_message_compute_us += static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    now - query.matrix_message_interval_started_at).count());
        }
    }
}

// Count the union of concurrent compute-task intervals instead of adding
// overlapping task durations from the compute pool.
static void IncrementalSubmitCompute(
    IncrementalWorkerState& state, int query_id, function<void()> task) {
    state.compute_executor.Submit([&state, query_id, task = std::move(task)]() mutable {
        bool tracked = false;
        {
            lock_guard<mutex> lk(state.mu);
            auto it = state.queries.find(query_id);
            if (it != state.queries.end() && it->second.worker_query_started) {
                auto& query = it->second;
                if (query.compute_active_tasks++ == 0)
                    query.compute_interval_started_at = chrono::steady_clock::now();
                tracked = true;
            }
        }
        try { task(); }
        catch (...) {
            lock_guard<mutex> lk(state.mu);
            auto it = state.queries.find(query_id);
            if (tracked && it != state.queries.end() && it->second.compute_active_tasks > 0 &&
                --it->second.compute_active_tasks == 0) {
                it->second.compute_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - it->second.compute_interval_started_at).count());
            }
            throw;
        }
        lock_guard<mutex> lk(state.mu);
        auto it = state.queries.find(query_id);
        if (tracked && it != state.queries.end() && it->second.compute_active_tasks > 0 &&
            --it->second.compute_active_tasks == 0) {
            it->second.compute_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - it->second.compute_interval_started_at).count());
        }
    });
}

// Keep join execution comparable with the single-node evaluator.
static size_t IncrementalComputeThreadCount(int total_partitions) {
    (void)total_partitions;
    return rpq_final::kJoinThreads;
}

// Message preparation has independent source-owner work and runs on a
// dedicated single-consumer queue, independent of the join queue.
static size_t IncrementalMatrixMessageThreadCount(int total_partitions) {
    (void)total_partitions;
    return rpq_final::kMessageThreads;
}

static bool IncrementalVerifyModuloV2P(const PartRouter& router, int partitions) {
    if (partitions <= 1 || router.strategy == "mod") return true;
    if (router.strategy != "v2p" || !router.v2p_loaded || router.v2p_keys.empty()) return false;
    // Missing v2p entries have different fallback semantics.  Only bypass the
    // table when it covers the complete zero-based vertex domain exactly.
    for (size_t index = 0; index < router.v2p_keys.size(); ++index) {
        const uint64_t vertex = router.v2p_keys[index];
        if (vertex != static_cast<uint64_t>(index) ||
            router.v2p_pids[index] != vertex % static_cast<uint64_t>(partitions)) {
            return false;
        }
    }
    return true;
}

static string IncrementalSerializeDirectLeafResults(const IncrementalQueryState& query) {
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    // Middle workers return partial C to the source-row owner. For a root
    // macro there is no destination-resident block to fetch, so the reduced
    // relation is kept in the query result accumulator.
    if (!query.final_rows.empty()) return SerializeIncrementalFinalRows(query.final_rows);
#endif
    RelationRows source_owned_rows;
    for (const auto& [_, block] : query.matrix_blocks) {
        if (block.retain_output_on_destination && !block.return_output_to_parent) {
            for (const auto& [source, values] : block.output_rows) {
                source_owned_rows[source] |= values;
            }
        }
    }
    // Matrix-backed roots leave their C[i,*] row blocks on W_i. Source sets
    // are disjoint by ownership, so coordinator-side merging is sufficient.
    if (!source_owned_rows.empty()) return SerializeIncrementalFinalRows(source_owned_rows);
    vector<uint64_t> sources = query.root_sources;
    sort(sources.begin(), sources.end());
    uint64_t nonempty_rows = 0;
    for (uint64_t source : sources) {
        const IncrementalRowKey key{query.direct_leaf_query_id, query.direct_leaf_body_id, query.direct_leaf_length, source};
        auto row = query.rows.find(key);
        if (row != query.rows.end() && !row->second.values.empty()) ++nonempty_rows;
    }
    string raw;
    IncAppendU64(raw, nonempty_rows);
    for (uint64_t source : sources) {
        const IncrementalRowKey key{query.direct_leaf_query_id, query.direct_leaf_body_id, query.direct_leaf_length, source};
        auto row = query.rows.find(key);
        if (row == query.rows.end() || row->second.values.empty()) continue;
        IncAppendU64(raw, source);
        IncAppendSet(raw, row->second.values);
    }
    string header;
    IncAppendU32(header, kIncrementalWireMagic);
    IncAppendU32(header, kIncrementalWireVersion);
    IncAppendU32(header, static_cast<uint32_t>(IncrementalWireKind::RESULTS));
    IncAppendU32(header, static_cast<uint32_t>(IncrementalFinalRowsCodec::ROWS));
    header.append(raw);
    return WrapStrongCompressedPayload(header);
}

// The final relation is source-owner partitioned.  A count response can
// therefore aggregate local exact cardinalities without shipping every
// source->destination bitmap back to the coordinator.
static uint64_t IncrementalCountDirectLeafResults(
    const IncrementalQueryState& query) {
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    if (!query.final_rows.empty()) return CountRelationPairs(query.final_rows);
#endif
    RelationRows source_owned_rows;
    for (const auto& [_, block] : query.matrix_blocks) {
        if (!block.retain_output_on_destination ||
            block.return_output_to_parent) continue;
        for (const auto& [source, values] : block.output_rows) {
            source_owned_rows[source] |= values;
        }
    }
    if (!source_owned_rows.empty()) return CountRelationPairs(source_owned_rows);

    uint64_t pairs = 0;
    for (uint64_t source : query.root_sources) {
        const IncrementalRowKey key{
            query.direct_leaf_query_id, query.direct_leaf_body_id,
            query.direct_leaf_length, source};
        auto row = query.rows.find(key);
        if (row != query.rows.end()) pairs += row->second.values.cardinality();
    }
    return pairs;
}
static void IncrementalEnsurePeers(IncrementalWorkerState& state) {
    lock_guard<mutex> init_lk(state.peer_init_mu);
    if (!state.clients.empty()) return;
    state.clients.resize(state.peers.size());
    state.client_mu.resize(state.peers.size());
    state.peer_senders.resize(state.peers.size());
    state.peer_delta_mu.resize(state.peers.size());
    state.peer_delta_pending.resize(state.peers.size());
    state.peer_delta_bytes.assign(state.peers.size(), 0);
    state.peer_delta_flush_scheduled.assign(state.peers.size(), false);
    state.peer_request_mu.resize(state.peers.size());
    state.peer_request_pending.resize(state.peers.size());
    state.peer_request_flush_scheduled.assign(state.peers.size(), false);
    state.peer_complete_mu.resize(state.peers.size());
    state.peer_complete_pending.resize(state.peers.size());
    state.peer_complete_flush_scheduled.assign(state.peers.size(), false);
    state.peer_base_expand_mu.resize(state.peers.size());
    state.peer_base_expand_pending.resize(state.peers.size());
    state.peer_base_expand_bytes.assign(state.peers.size(), 0);
    state.peer_base_expand_flush_scheduled.assign(state.peers.size(), false);
    state.peer_base_result_mu.resize(state.peers.size());
    state.peer_base_result_pending.resize(state.peers.size());
    state.peer_base_result_bytes.assign(state.peers.size(), 0);
    state.peer_base_result_flush_scheduled.assign(state.peers.size(), false);
    state.peer_result_dictionary_mu.resize(state.peers.size());
    for (size_t i = 0; i < state.peers.size(); ++i) {
        state.clients[i] = make_unique<TcpClient>(state.peers[i].host, state.peers[i].port);
        state.client_mu[i] = make_unique<mutex>();
        // One sender strand per peer preserves DELTA -> COMPLETE order while
        // keeping synchronous socket waits out of the state/event executor.
        state.peer_senders[i] = make_unique<IncrementalExecutor>(1);
        state.peer_delta_mu[i] = make_unique<mutex>();
        state.peer_request_mu[i] = make_unique<mutex>();
        state.peer_complete_mu[i] = make_unique<mutex>();
        state.peer_base_expand_mu[i] = make_unique<mutex>();
        state.peer_base_result_mu[i] = make_unique<mutex>();
        state.peer_result_dictionary_mu[i] = make_unique<mutex>();
    }
}
static Message IncrementalSendPeer(IncrementalWorkerState& state, int pid, const Message& request) {
    IncrementalEnsurePeers(state);
    if (pid < 0 || pid >= static_cast<int>(state.clients.size())) throw runtime_error("incremental bad peer");
    lock_guard<mutex> lk(*state.client_mu[static_cast<size_t>(pid)]);
    return state.clients[static_cast<size_t>(pid)]->SendRequest(request);
}
static IncrementalWorkerState::PeerWireKind IncrementalClassifyPeerWire(MessageType type) {
    using PeerWireKind = IncrementalWorkerState::PeerWireKind;
    switch (type) {
        case MessageType::INCREMENTAL_BASE_EXPAND:
        case MessageType::INCREMENTAL_BASE_EXPAND_RESULT:
        case MessageType::INCREMENTAL_BASE_EXPAND_BATCH:
        case MessageType::INCREMENTAL_BASE_EXPAND_RESULT_BATCH:
            return PeerWireKind::BASE;
        case MessageType::INCREMENTAL_REQUEST_ROW:
        case MessageType::INCREMENTAL_REQUEST_ROW_BATCH:
        case MessageType::INCREMENTAL_ROW_DELTA:
        case MessageType::INCREMENTAL_ROW_DELTA_BATCH:
        case MessageType::INCREMENTAL_ROW_COMPLETE:
        case MessageType::INCREMENTAL_ROW_COMPLETE_BATCH:
        case MessageType::INCREMENTAL_ROW_STREAM_BATCH:
            return PeerWireKind::ROW;
        case MessageType::INCREMENTAL_DEMAND_BATCH_REQUEST:
        case MessageType::INCREMENTAL_DEMAND_BATCH_STREAM:
            return PeerWireKind::DEMAND;
        case MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE:
            return PeerWireKind::LHS;
        case MessageType::INCREMENTAL_MATRIX_JOIN_RESULT:
            return PeerWireKind::MATRIX_RESULT;
        case MessageType::INCREMENTAL_MATRIX_RESIDENT_CONTINUE:
            return PeerWireKind::DEMAND;
        default:
            return PeerWireKind::OTHER;
    }
}
static void IncrementalEnqueuePeer(
    IncrementalWorkerState& state, int pid, Message request,
    IncrementalWorkerState::PeerWireKind kind = IncrementalWorkerState::PeerWireKind::OTHER) {
    const auto payload_ready_at = chrono::steady_clock::now();
    const bool tracked_communication = IncrementalBeginPeerCommunication(state);
    const bool tracked_communication_e2e = IncrementalBeginPeerCommunicationE2E(
        state, request.transport_build_started_at);
    IncrementalEnsurePeers(state);
    if (pid < 0 || pid >= static_cast<int>(state.peer_senders.size())) throw runtime_error("incremental bad peer");
    if (kind == IncrementalWorkerState::PeerWireKind::OTHER) kind = IncrementalClassifyPeerWire(request.type);
    const uint64_t bytes = static_cast<uint64_t>(request.data.size());
    const int query_id = state.active_query_id.load(memory_order_acquire);
    state.peer_wire_bytes.fetch_add(bytes, memory_order_relaxed);
    state.peer_wire_messages.fetch_add(1, memory_order_relaxed);
    state.peer_wire_by_kind[static_cast<size_t>(kind)].fetch_add(bytes, memory_order_relaxed);
    state.peer_senders[static_cast<size_t>(pid)]->Submit([&state, pid, query_id, request = std::move(request), payload_ready_at, tracked_communication, tracked_communication_e2e]() mutable {
        try {
            Message response = IncrementalSendPeer(state, pid, request);
            if (response.type != MessageType::RESPONSE_OK) {
                IncrementalRecordPeerFailure(state, query_id, pid, request.type,
                    "RPC rejected: " + response.GetField("error"));
            }
        } catch (const exception& ex) {
            IncrementalRecordPeerFailure(state, query_id, pid, request.type, ex.what());
        }
        state.peer_e2e_us.fetch_add(static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - payload_ready_at).count()), memory_order_relaxed);
        state.peer_e2e_messages.fetch_add(1, memory_order_relaxed);
        if (tracked_communication) IncrementalEndPeerCommunication(state);
        if (tracked_communication_e2e) IncrementalEndPeerCommunicationE2E(state);
    });
}
static void IncrementalFlushPeerRequests(IncrementalWorkerState& state, int pid) {
    IncrementalEnsurePeers(state);
    vector<IncrementalRowRequestEnvelope> requests;
    {
        lock_guard<mutex> lk(*state.peer_request_mu.at(static_cast<size_t>(pid)));
        requests.swap(state.peer_request_pending.at(static_cast<size_t>(pid)));
        state.peer_request_flush_scheduled.at(static_cast<size_t>(pid)) = false;
    }
    if (requests.empty()) return;
    {
        lock_guard<mutex> lk(state.mu);
        unordered_set<int> query_ids;
        for (const auto& request : requests) {
            query_ids.insert(request.key.query_id);
        }
        for (int query_id : query_ids) {
            auto query = state.queries.find(query_id);
            if (query != state.queries.end()) ++query->second.remote_request_rpcs;
        }
    }
    Message request(MessageType::INCREMENTAL_REQUEST_ROW_BATCH);
    request.data = SerializeIncrementalRowRequestBatch(requests);
    IncrementalEnqueuePeer(state, pid, std::move(request));
}
static void IncrementalQueuePeerRequest(
    IncrementalWorkerState& state, int pid, const IncrementalRowKey& key, uint64_t token) {
    IncrementalEnsurePeers(state);
    bool flush_now = false;
    bool schedule_flush = false;
    {
        lock_guard<mutex> lk(*state.peer_request_mu.at(static_cast<size_t>(pid)));
        auto& requests = state.peer_request_pending.at(static_cast<size_t>(pid));
        requests.push_back(IncrementalRowRequestEnvelope{key, state.partition_id, token});
        if (requests.size() >= kIncrementalRequestBatchRows) {
            flush_now = true;
        } else if (!state.peer_request_flush_scheduled.at(static_cast<size_t>(pid))) {
            state.peer_request_flush_scheduled.at(static_cast<size_t>(pid)) = true;
            schedule_flush = true;
        }
    }
    if (flush_now) IncrementalFlushPeerRequests(state, pid);
    else if (schedule_flush) {
        state.executor.Submit([&state, pid]() { IncrementalFlushPeerRequests(state, pid); });
    }
}
static void IncrementalFlushPeerDeltas(IncrementalWorkerState& state, int pid) {
    IncrementalEnsurePeers(state);
    vector<IncrementalRowStreamEnvelope> events;
    {
        lock_guard<mutex> lk(*state.peer_delta_mu.at(static_cast<size_t>(pid)));
        events.swap(state.peer_delta_pending.at(static_cast<size_t>(pid)));
        state.peer_delta_bytes.at(static_cast<size_t>(pid)) = 0;
        state.peer_delta_flush_scheduled.at(static_cast<size_t>(pid)) = false;
    }
    if (events.empty()) return;
    {
        lock_guard<mutex> lk(state.mu);
        unordered_set<int> query_ids;
        for (const auto& event : events) query_ids.insert(event.key.query_id);
        for (int query_id : query_ids) {
            auto query = state.queries.find(query_id);
            if (query != state.queries.end()) ++query->second.remote_stream_rpcs;
        }
    }
    Message request(MessageType::INCREMENTAL_ROW_STREAM_BATCH);
    request.data = SerializeIncrementalRowStreamBatch(events);
    IncrementalEnqueuePeer(state, pid, std::move(request));
}
static void IncrementalQueuePeerDelta(IncrementalWorkerState& state, int pid, const IncrementalRowKey& key, uint64_t token, const RelationDstSet& delta) {
    if (delta.empty()) return;
    IncrementalEnsurePeers(state);
    bool flush_now = false;
    bool schedule_flush = false;
    {
        lock_guard<mutex> lk(*state.peer_delta_mu.at(static_cast<size_t>(pid)));
        auto& events = state.peer_delta_pending.at(static_cast<size_t>(pid));
        events.push_back(IncrementalRowStreamEnvelope{false, key, token, delta});
        auto& bytes = state.peer_delta_bytes.at(static_cast<size_t>(pid));
        bytes += RelationDstSetBytes(delta) + 48;
        if (events.size() >= IncrementalPeerDeltaBatchEvents(state.total_partitions) ||
            bytes >= IncrementalPeerDeltaBatchBytes(state.total_partitions)) {
            flush_now = true;
        } else if (!state.peer_delta_flush_scheduled.at(static_cast<size_t>(pid))) {
            state.peer_delta_flush_scheduled.at(static_cast<size_t>(pid)) = true;
            schedule_flush = true;
        }
    }
    if (flush_now) IncrementalFlushPeerDeltas(state, pid);
    else if (schedule_flush) {
        state.control_executor.Submit([&state, pid]() {
            this_thread::sleep_for(chrono::milliseconds(
                IncrementalPeerDeltaFlushDelayMs(state.total_partitions)));
            state.executor.Submit([&state, pid]() { IncrementalFlushPeerDeltas(state, pid); });
        });
    }
}

static void IncrementalFlushPeerCompletes(IncrementalWorkerState& state, int pid) {
    IncrementalEnsurePeers(state);
    vector<IncrementalRowCompleteEnvelope> events;
    {
        lock_guard<mutex> lk(*state.peer_complete_mu.at(static_cast<size_t>(pid)));
        events.swap(state.peer_complete_pending.at(static_cast<size_t>(pid)));
        state.peer_complete_flush_scheduled.at(static_cast<size_t>(pid)) = false;
    }
    if (events.empty()) return;
    {
        lock_guard<mutex> lk(state.mu);
        unordered_set<int> query_ids;
        for (const auto& event : events) query_ids.insert(event.key.query_id);
        for (int query_id : query_ids) {
            auto query = state.queries.find(query_id);
            if (query != state.queries.end()) ++query->second.remote_complete_rpcs;
        }
    }
    Message request(MessageType::INCREMENTAL_ROW_COMPLETE_BATCH);
    request.data = SerializeIncrementalRowCompleteBatch(events);
    IncrementalEnqueuePeer(state, pid, std::move(request));
}

static void IncrementalQueuePeerComplete(
    IncrementalWorkerState& state, int pid, const IncrementalRowKey& key, uint64_t token) {
    IncrementalEnsurePeers(state);
    bool flush_now = false;
    bool schedule_flush = false;
    {
        lock_guard<mutex> lk(*state.peer_delta_mu.at(static_cast<size_t>(pid)));
        auto& events = state.peer_delta_pending.at(static_cast<size_t>(pid));
        events.push_back(IncrementalRowStreamEnvelope{true, key, token, RelationDstSet{}});
        auto& bytes = state.peer_delta_bytes.at(static_cast<size_t>(pid));
        bytes += 48;
        if (events.size() >= IncrementalPeerDeltaBatchEvents(state.total_partitions) ||
            bytes >= IncrementalPeerDeltaBatchBytes(state.total_partitions)) {
            flush_now = true;
        } else if (!state.peer_delta_flush_scheduled.at(static_cast<size_t>(pid))) {
            state.peer_delta_flush_scheduled.at(static_cast<size_t>(pid)) = true;
            schedule_flush = true;
        }
    }
    if (flush_now) IncrementalFlushPeerDeltas(state, pid);
    else if (schedule_flush) {
        state.control_executor.Submit([&state, pid]() {
            this_thread::sleep_for(chrono::milliseconds(
                IncrementalPeerDeltaFlushDelayMs(state.total_partitions)));
            state.executor.Submit([&state, pid]() { IncrementalFlushPeerDeltas(state, pid); });
        });
    }
}

static void IncrementalSubscribeRow(IncrementalWorkerState&, const IncrementalRowKey&, IncrementalDeltaCallback);
static void IncrementalSubscribeBinaryRight(IncrementalWorkerState&, const IncrementalRowKey&, const IncrementalRowKey&, shared_ptr<IncrementalParentJoinBatch> = nullptr);
static void IncrementalStartRow(IncrementalWorkerState&, const IncrementalRowKey&);
static void IncrementalEvalMacro(
    IncrementalWorkerState&, int, int, uint64_t, IncrementalDeltaCallback);
static void IncrementalProcessBaseExpand(IncrementalWorkerState&, const IncrementalBaseEvent&);
static void IncrementalHandleBaseResult(IncrementalWorkerState&, IncrementalBaseResult);
static void IncrementalOnBinaryRight(IncrementalWorkerState&, IncrementalRowKey, const RelationDstSet&, bool);
static void IncrementalDispatchBinaryWaiter(IncrementalWorkerState&, const IncrementalBinaryWaiter&, const RelationDstSet&, bool);

static void IncrementalInvoke(IncrementalWorkerState& state, IncrementalDeltaCallback callback, RelationDstSet delta, bool complete) {
    // All callers run on the single ordered state executor.  Re-enqueuing a
    // local continuation only allocates another task and delays the cascade;
    // invoke it here so a delta can flow B1 -> B2 -> ... immediately.
    callback(delta, complete);
}

static void IncrementalSendDelta(IncrementalWorkerState& state, int pid, const IncrementalRowKey& key, uint64_t token, const RelationDstSet& delta) {
    IncrementalQueuePeerDelta(state, pid, key, token, delta);
}
static void IncrementalSendComplete(IncrementalWorkerState& state, int pid, const IncrementalRowKey& key, uint64_t token) {
    IncrementalQueuePeerComplete(state, pid, key, token);
}

static void IncrementalEmitDelta(IncrementalWorkerState& state, const IncrementalRowKey& key, const RelationDstSet& candidates) {
    const auto merge_begin = chrono::steady_clock::now();
    RelationDstSet delta;
    vector<IncrementalWaiter> waiters;
    vector<IncrementalBinaryWaiter> binary_waiters;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(key.query_id);
        if (qit == state.queries.end()) return;
        auto rit = qit->second.rows.find(key);
        if (rit == qit->second.rows.end() || rit->second.status != IncrementalRowState::Status::COMPUTING) return;
        auto& row = rit->second;
        delta.bitmap = candidates.bitmap - row.values.bitmap;
        if (key.length > 1) delta.bitmap -= row.pending_delta.bitmap;
        if (delta.empty()) return;
        // High-level rows receive enormous streams of right-side deltas. Hold
        // fresh values in a Roaring microbatch, then perform one bulk union.
        // Base rows stay eager so dependency discovery starts immediately.
        if (key.length > 1) {
            row.pending_delta |= delta;
            if (row.pending_delta.cardinality() < kIncrementalEmitBatchPairs) return;
            delta = std::move(row.pending_delta);
            row.pending_delta = RelationDstSet{};
        }
        row.values.bitmap |= delta.bitmap;
        waiters = row.waiters;
        binary_waiters = row.binary_waiters;
        qit->second.delta_count++;
        const size_t slot = IncrementalPerfSlot(key.length);
        ++qit->second.emit_published_calls[slot];
        qit->second.emit_published_pairs[slot] += delta.cardinality();
        qit->second.emit_merge_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - merge_begin).count());
    }
    for (const auto& waiter : waiters) {
        if (waiter.reply_pid == state.partition_id) {
            IncrementalDeltaCallback callback;
            { lock_guard<mutex> lk(state.mu); auto qit = state.queries.find(key.query_id); if (qit != state.queries.end()) { auto it = qit->second.callbacks.find(waiter.token); if (it != qit->second.callbacks.end()) callback = it->second.fn; } }
            if (callback) IncrementalInvoke(state, callback, delta, false);
        } else {
            IncrementalSendDelta(state, waiter.reply_pid, key, waiter.token, delta);
        }
    }
    for (const auto& waiter : binary_waiters) IncrementalDispatchBinaryWaiter(state, waiter, delta, false);
}

static void IncrementalFlushPendingDelta(IncrementalWorkerState& state, const IncrementalRowKey& key) {
    const auto merge_begin = chrono::steady_clock::now();
    RelationDstSet delta;
    vector<IncrementalWaiter> waiters;
    vector<IncrementalBinaryWaiter> binary_waiters;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(key.query_id);
        if (qit == state.queries.end()) return;
        auto rit = qit->second.rows.find(key);
        if (rit == qit->second.rows.end() || rit->second.pending_delta.empty()) return;
        delta = std::move(rit->second.pending_delta);
        rit->second.pending_delta = RelationDstSet{};
        rit->second.values.bitmap |= delta.bitmap;
        waiters = rit->second.waiters;
        binary_waiters = rit->second.binary_waiters;
        qit->second.delta_count++;
        const size_t slot = IncrementalPerfSlot(key.length);
        ++qit->second.emit_published_calls[slot];
        qit->second.emit_published_pairs[slot] += delta.cardinality();
        qit->second.emit_merge_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - merge_begin).count());
    }
    for (const auto& waiter : waiters) {
        if (waiter.reply_pid == state.partition_id) {
            IncrementalDeltaCallback callback;
            { lock_guard<mutex> lk(state.mu); auto qit = state.queries.find(key.query_id); if (qit != state.queries.end()) { auto it = qit->second.callbacks.find(waiter.token); if (it != qit->second.callbacks.end()) callback = it->second.fn; } }
            if (callback) IncrementalInvoke(state, callback, delta, false);
        } else {
            IncrementalSendDelta(state, waiter.reply_pid, key, waiter.token, delta);
        }
    }
    for (const auto& waiter : binary_waiters) IncrementalDispatchBinaryWaiter(state, waiter, delta, false);
}

static void IncrementalCompleteRow(IncrementalWorkerState& state, const IncrementalRowKey& key) {
    // The final partial microbatch must be delivered before COMPLETE.
    IncrementalFlushPendingDelta(state, key);
    vector<IncrementalWaiter> waiters;
    vector<IncrementalBinaryWaiter> binary_waiters;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(key.query_id); if (qit == state.queries.end()) return;
        auto rit = qit->second.rows.find(key); if (rit == qit->second.rows.end() || rit->second.status == IncrementalRowState::Status::COMPLETE) return;
        rit->second.status = IncrementalRowState::Status::COMPLETE;
        if (qit->second.direct_leaf_output &&
            key.body_id == qit->second.direct_leaf_body_id &&
            key.length == qit->second.direct_leaf_length) {
            ++qit->second.roots_done;
        }
        waiters.swap(rit->second.waiters);
        binary_waiters.swap(rit->second.binary_waiters);
        rit->second.base_visited.clear();
        rit->second.base_visited.rehash(0);
        rit->second.base_pending = 0;
        rit->second.binary_pending = 0;
    }
    for (const auto& waiter : waiters) {
        if (waiter.reply_pid == state.partition_id) {
            IncrementalDeltaCallback callback;
            { lock_guard<mutex> lk(state.mu); auto qit = state.queries.find(key.query_id); if (qit != state.queries.end()) { auto it = qit->second.callbacks.find(waiter.token); if (it != qit->second.callbacks.end()) { callback = std::move(it->second.fn); qit->second.callbacks.erase(it); } } }
            if (callback) IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
        } else {
            IncrementalSendComplete(state, waiter.reply_pid, key, waiter.token);
        }
    }
    for (const auto& waiter : binary_waiters) IncrementalDispatchBinaryWaiter(state, waiter, RelationDstSet{}, true);
}

static void IncrementalFlushPeerBaseExpands(
    IncrementalWorkerState& state, int pid) {
    IncrementalEnsurePeers(state);
    vector<IncrementalBaseEvent> events;
    {
        lock_guard<mutex> lk(*state.peer_base_expand_mu.at(
            static_cast<size_t>(pid)));
        events.swap(state.peer_base_expand_pending.at(
            static_cast<size_t>(pid)));
        state.peer_base_expand_bytes.at(static_cast<size_t>(pid)) = 0;
        state.peer_base_expand_flush_scheduled.at(
            static_cast<size_t>(pid)) = false;
    }
    if (events.empty()) return;
    Message request(MessageType::INCREMENTAL_BASE_EXPAND_BATCH);
    request.data = SerializeIncrementalBaseExpandBatch(events);
    IncrementalEnqueuePeer(
        state, pid, std::move(request),
        IncrementalWorkerState::PeerWireKind::BASE);
}

static void IncrementalQueuePeerBaseExpand(
    IncrementalWorkerState& state, int pid, const IncrementalBaseEvent& event) {
    IncrementalEnsurePeers(state);
    bool flush_now = false;
    bool schedule_flush = false;
    {
        lock_guard<mutex> lk(*state.peer_base_expand_mu.at(
            static_cast<size_t>(pid)));
        auto& events = state.peer_base_expand_pending.at(
            static_cast<size_t>(pid));
        events.push_back(event);
        auto& bytes = state.peer_base_expand_bytes.at(
            static_cast<size_t>(pid));
        bytes += 36;
        if (events.size() >= kIncrementalBaseBatchEvents ||
            bytes >= kIncrementalBaseBatchBytes) {
            flush_now = true;
        } else if (!state.peer_base_expand_flush_scheduled.at(
                       static_cast<size_t>(pid))) {
            state.peer_base_expand_flush_scheduled.at(
                static_cast<size_t>(pid)) = true;
            schedule_flush = true;
        }
    }
    if (flush_now) {
        IncrementalFlushPeerBaseExpands(state, pid);
    } else if (schedule_flush) {
        state.control_executor.Submit([&state, pid]() {
            this_thread::sleep_for(
                chrono::milliseconds(kIncrementalBaseBatchDelayMs));
            IncrementalFlushPeerBaseExpands(state, pid);
        });
    }
}

static void IncrementalFlushPeerBaseResults(
    IncrementalWorkerState& state, int pid) {
    IncrementalEnsurePeers(state);
    vector<IncrementalBaseResult> results;
    {
        lock_guard<mutex> lk(*state.peer_base_result_mu.at(
            static_cast<size_t>(pid)));
        results.swap(state.peer_base_result_pending.at(
            static_cast<size_t>(pid)));
        state.peer_base_result_bytes.at(static_cast<size_t>(pid)) = 0;
        state.peer_base_result_flush_scheduled.at(
            static_cast<size_t>(pid)) = false;
    }
    if (results.empty()) return;
    Message request(MessageType::INCREMENTAL_BASE_EXPAND_RESULT_BATCH);
    request.data = SerializeIncrementalBaseResultBatch(results);
    IncrementalEnqueuePeer(
        state, pid, std::move(request),
        IncrementalWorkerState::PeerWireKind::BASE);
}

static void IncrementalQueuePeerBaseResult(
    IncrementalWorkerState& state, int pid, IncrementalBaseResult result) {
    IncrementalEnsurePeers(state);
    bool flush_now = false;
    bool schedule_flush = false;
    {
        lock_guard<mutex> lk(*state.peer_base_result_mu.at(
            static_cast<size_t>(pid)));
        auto& results = state.peer_base_result_pending.at(
            static_cast<size_t>(pid));
        auto& bytes = state.peer_base_result_bytes.at(
            static_cast<size_t>(pid));
        bytes += RelationDstSetBytes(result.accepted) +
            result.children.size() * 12 + 32;
        results.push_back(std::move(result));
        if (results.size() >= kIncrementalBaseBatchEvents ||
            bytes >= kIncrementalBaseBatchBytes) {
            flush_now = true;
        } else if (!state.peer_base_result_flush_scheduled.at(
                       static_cast<size_t>(pid))) {
            state.peer_base_result_flush_scheduled.at(
                static_cast<size_t>(pid)) = true;
            schedule_flush = true;
        }
    }
    if (flush_now) {
        IncrementalFlushPeerBaseResults(state, pid);
    } else if (schedule_flush) {
        state.control_executor.Submit([&state, pid]() {
            this_thread::sleep_for(
                chrono::milliseconds(kIncrementalBaseBatchDelayMs));
            IncrementalFlushPeerBaseResults(state, pid);
        });
    }
}

static void IncrementalDispatchBaseExpand(IncrementalWorkerState& state, const IncrementalBaseEvent& event) {
    const int owner = IncrementalOwner(state, event.vertex);
    if (owner == state.partition_id) { IncrementalSubmitCompute(state, event.key.query_id, [&state, event]() { IncrementalProcessBaseExpand(state, event); }); return; }
    IncrementalQueuePeerBaseExpand(state, owner, event);
}

#include "nfa_live_states.inc"

static void IncrementalProcessBaseExpand(IncrementalWorkerState& state, const IncrementalBaseEvent& event) {
    const auto base_begin = chrono::steady_clock::now();
    IncrementalBaseResult result; result.key = event.key;
    RegexQueryPattern pattern;
    { lock_guard<mutex> lk(state.mu); auto qit = state.queries.find(event.key.query_id); if (qit == state.queries.end()) return; pattern = qit->second.program.bodies.at(static_cast<size_t>(event.key.body_id)).pattern; }
    auto adj = state.partition.adjacency_map.find(event.vertex);
    if (adj != state.partition.adjacency_map.end() && event.state_id >= 0 && event.state_id < static_cast<int>(pattern.nfa.states.size())) {
        for (const auto& transition : pattern.nfa.states[static_cast<size_t>(event.state_id)].labeled) {
            auto edge = adj->second.find(transition.label); if (edge == adj->second.end()) continue;
            for (uint64_t dst : edge->second) for (int next : pattern.nfa.closure_by_state[static_cast<size_t>(transition.to)]) {
                if (next == pattern.nfa.accept) result.accepted.push_back(dst);
                else if (!IncrementalLiveNfaStatesEnabled() ||
                         IncrementalNfaStateConsumesLabel(pattern.nfa, next)) {
                    // Epsilon closure already contains every reachable live
                    // state. An inert member can only produce an empty reply.
                    result.children.emplace_back(dst, next);
                }
            }
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto query = state.queries.find(event.key.query_id);
        if (query != state.queries.end()) {
            ++query->second.base_expand_tasks;
            query->second.base_expand_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - base_begin).count());
        }
    }
    if (event.origin_pid == state.partition_id) {
        state.executor.Submit([&state, result = std::move(result)]() mutable { IncrementalHandleBaseResult(state, std::move(result)); });
        return;
    }
    IncrementalQueuePeerBaseResult(
        state, event.origin_pid, std::move(result));
}

static void IncrementalStartBaseRow(IncrementalWorkerState& state, const IncrementalRowKey& key) {
    int virtual_node = -1;
    {
        lock_guard<mutex> lk(state.mu);
        const auto& program = state.queries.at(key.query_id).program;
        if (key.body_id >= 0 &&
            static_cast<size_t>(key.body_id) < program.virtual_body_nodes.size()) {
            virtual_node =
                program.virtual_body_nodes[static_cast<size_t>(key.body_id)];
        }
    }
    if (virtual_node >= 0) {
        IncrementalEvalMacro(
            state, key.query_id, virtual_node, key.source,
            [&state, key](const RelationDstSet& delta, bool complete) {
                if (!delta.empty()) IncrementalEmitDelta(state, key, delta);
                if (complete) IncrementalCompleteRow(state, key);
            });
        return;
    }
    RegexQueryPattern pattern;
    { lock_guard<mutex> lk(state.mu); pattern = state.queries.at(key.query_id).program.bodies.at(static_cast<size_t>(key.body_id)).pattern; }
    vector<IncrementalBaseEvent> events;
    for (int sid : pattern.nfa.start_closure) {
        if (sid != pattern.nfa.accept && (!IncrementalLiveNfaStatesEnabled() ||
            IncrementalNfaStateConsumesLabel(pattern.nfa, sid))) {
            events.push_back(IncrementalBaseEvent{key, state.partition_id, key.source, sid});
        }
    }
    vector<IncrementalBaseEvent> unique_events;
    { lock_guard<mutex> lk(state.mu); auto& row = state.queries.at(key.query_id).rows.at(key); for (const auto& event : events) { string id = to_string(event.vertex) + ":" + to_string(event.state_id); if (row.base_visited.insert(std::move(id)).second) { ++row.base_pending; unique_events.push_back(event); } } }
    if (unique_events.empty()) { IncrementalCompleteRow(state, key); return; }
    for (const auto& event : unique_events) IncrementalDispatchBaseExpand(state, event);
}

static void IncrementalHandleBaseResult(IncrementalWorkerState& state, IncrementalBaseResult result) {
    vector<IncrementalBaseEvent> next_events;
    bool complete = false;
    { lock_guard<mutex> lk(state.mu); auto qit = state.queries.find(result.key.query_id); if (qit == state.queries.end()) return; auto rit = qit->second.rows.find(result.key); if (rit == qit->second.rows.end() || rit->second.status != IncrementalRowState::Status::COMPUTING) return; auto& row = rit->second; for (const auto& [vertex, sid] : result.children) { string id = to_string(vertex) + ":" + to_string(sid); if (row.base_visited.insert(std::move(id)).second) { ++row.base_pending; next_events.push_back(IncrementalBaseEvent{result.key, state.partition_id, vertex, sid}); } } if (row.base_pending > 0) --row.base_pending; complete = row.base_pending == 0; }
    IncrementalEmitDelta(state, result.key, result.accepted);
    for (const auto& event : next_events) IncrementalDispatchBaseExpand(state, event);
    if (complete) IncrementalCompleteRow(state, result.key);
}

static void IncrementalOnBinaryRight(IncrementalWorkerState& state, IncrementalRowKey parent, const RelationDstSet& delta, bool complete) {
    IncrementalEmitDelta(state, parent, delta);
    if (!complete) return;
    bool parent_complete = false;
    { lock_guard<mutex> lk(state.mu); auto& row = state.queries.at(parent.query_id).rows.at(parent); if (row.binary_pending > 0) --row.binary_pending; parent_complete = row.left_complete && row.binary_pending == 0; }
    if (parent_complete) IncrementalCompleteRow(state, parent);
}

static void IncrementalOnBinaryBatchComplete(
    IncrementalWorkerState& state, const IncrementalRowKey& parent,
    const RelationDstSet& delta, uint64_t completed_children) {
    IncrementalEmitDelta(state, parent, delta);
    bool parent_complete = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto& row = state.queries.at(parent.query_id).rows.at(parent);
        row.binary_pending = row.binary_pending > completed_children
            ? row.binary_pending - completed_children : 0;
        parent_complete = row.left_complete && row.binary_pending == 0;
    }
    if (parent_complete) IncrementalCompleteRow(state, parent);
}

static void IncrementalOnParentJoinBatchAggregate(
    IncrementalWorkerState& state, const shared_ptr<IncrementalParentJoinBatch>& batch,
    const RelationDstSet& delta, uint64_t completed_children, uint64_t logical_events) {
    const auto batch_begin = chrono::steady_clock::now();
    RelationDstSet publish;
    bool finished = false;
    {
        lock_guard<mutex> lk(batch->mu);
        if (!delta.empty()) {
            // The parent row is the unique publication boundary and already
            // performs exact set difference before forwarding a delta.  Doing
            // the same difference once per child here was redundant; retain a
            // local union only to coalesce the next parent publication.
            batch->pending_delta |= delta;
        }
        if (batch->pending_delta.cardinality() >= kIncrementalEmitBatchPairs) {
            publish = std::move(batch->pending_delta);
            batch->pending_delta = RelationDstSet{};
        }
        if (completed_children > 0 && batch->pending_children > 0) {
            const uint64_t consumed = min(batch->pending_children, completed_children);
            batch->pending_children -= consumed;
            finished = batch->pending_children == 0;
            if (finished && !batch->pending_delta.empty()) {
                publish = std::move(batch->pending_delta);
                batch->pending_delta = RelationDstSet{};
            }
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto query = state.queries.find(batch->parent.query_id);
        if (query != state.queries.end()) {
            query->second.join_batch_child_events += logical_events;
            query->second.join_batch_publish_pairs += publish.cardinality();
            query->second.join_batch_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - batch_begin).count());
        }
    }
    if (!publish.empty()) IncrementalEmitDelta(state, batch->parent, publish);
    if (finished) IncrementalOnBinaryBatchComplete(state, batch->parent, RelationDstSet{}, batch->child_count);
}

static void IncrementalOnParentJoinBatchChild(
    IncrementalWorkerState& state, const shared_ptr<IncrementalParentJoinBatch>& batch,
    const RelationDstSet& delta, bool complete) {
    IncrementalOnParentJoinBatchAggregate(state, batch, delta, complete ? 1 : 0, 1);
}

static void IncrementalDispatchBinaryWaiter(
    IncrementalWorkerState& state, const IncrementalBinaryWaiter& waiter,
    const RelationDstSet& delta, bool complete) {
    if (waiter.batch) IncrementalOnParentJoinBatchChild(state, waiter.batch, delta, complete);
    else IncrementalOnBinaryRight(state, waiter.parent, delta, complete);
}

static void IncrementalSubscribeBinaryBatch(
    IncrementalWorkerState& state, const IncrementalRowKey& parent, int right_len,
    const RelationDstSet& mids) {
    if (mids.empty()) return;
    auto batch = make_shared<IncrementalParentJoinBatch>();
    batch->parent = parent;
    batch->child_count = mids.cardinality();
    batch->pending_children = batch->child_count;

    struct CachedDelivery {
        IncrementalBinaryWaiter waiter;
        RelationDstSet values;
        bool complete = false;
    };
    vector<CachedDelivery> cached_deliveries;
    vector<pair<int, IncrementalRowRequestEnvelope>> remote_requests;
    vector<IncrementalRowKey> local_starts;
    cached_deliveries.reserve(static_cast<size_t>(min<uint64_t>(mids.cardinality(), 64)));
    remote_requests.reserve(static_cast<size_t>(min<uint64_t>(mids.cardinality(), 64)));
    local_starts.reserve(static_cast<size_t>(min<uint64_t>(mids.cardinality(), 64)));
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(parent.query_id);
        if (qit == state.queries.end()) return;
        auto& query = qit->second;
        ++query.join_batch_count;
        query.join_batch_children += batch->child_count;

        // Register an entire parent frontier while holding the state lock once.
        // The old path called IncrementalSubscribeBinaryRight once per mid,
        // which repeated this lock, relay lookup, and executor submission for
        // millions of child dependencies.
        for (uint64_t mid : mids) {
            IncrementalRowKey key{parent.query_id, parent.body_id, right_len, mid};
            IncrementalBinaryWaiter waiter{parent, batch};
            const int owner = IncrementalOwner(state, key.source);
            RelationDstSet cached;
            bool complete = false;
            if (owner != state.partition_id) {
                auto& relay = query.remote_relays[key];
                cached = relay.values;
                complete = relay.complete;
                if (!complete) {
                    relay.binary_waiters.push_back(waiter);
                    if (!relay.requested) {
                        relay.requested = true;
                        const uint64_t token = query.next_token++;
                        query.remote_tokens.emplace(token, key);
                        ++query.remote_requests;
                        remote_requests.emplace_back(owner, IncrementalRowRequestEnvelope{key, state.partition_id, token});
                    }
                }
            } else {
                auto& row = query.rows[key];
                cached = row.values;
                if (row.status == IncrementalRowState::Status::COMPLETE) {
                    complete = true;
                    ++query.row_cache_hits;
                } else {
                    row.binary_waiters.push_back(waiter);
                    if (row.status == IncrementalRowState::Status::NOT_STARTED) {
                        row.status = IncrementalRowState::Status::COMPUTING;
                        ++query.rows_started;
                        local_starts.push_back(key);
                    }
                }
            }
            if (!cached.empty() || complete) {
                cached_deliveries.push_back(CachedDelivery{std::move(waiter), std::move(cached), complete});
            }
        }
    }
    // Deliver cached replay after all registrations: a COMPLETE cannot make a
    // partially registered parent batch look finished.
    for (auto& delivery : cached_deliveries) {
        if (!delivery.values.empty()) {
            IncrementalDispatchBinaryWaiter(state, delivery.waiter, delivery.values, false);
        }
        if (delivery.complete) {
            IncrementalDispatchBinaryWaiter(state, delivery.waiter, RelationDstSet{}, true);
        }
    }
    for (const auto& [owner, request] : remote_requests) {
        IncrementalQueuePeerRequest(state, owner, request.key, request.token);
    }
    if (!local_starts.empty()) {
        state.executor.Submit([&state, starts = std::move(local_starts)]() mutable {
            for (const auto& key : starts) IncrementalStartRow(state, key);
        });
    }
}

static bool IncrementalReadCompleteLocalRow(
    IncrementalWorkerState& state, const IncrementalRowKey& key,
    RelationDstSet& values) {
    lock_guard<mutex> lk(state.mu);
    auto qit = state.queries.find(key.query_id);
    if (qit == state.queries.end()) return false;
    auto rit = qit->second.rows.find(key);
    if (rit == qit->second.rows.end() ||
        rit->second.status != IncrementalRowState::Status::COMPLETE) return false;
    values = rit->second.values;
    return true;
}

static void IncrementalFlushGcnpJoins(IncrementalWorkerState& state, int query_id) {
    const auto gcnp_begin = chrono::steady_clock::now();
    vector<IncrementalGcnpJoinRequest> requests;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        requests.swap(qit->second.gcnp_pending_joins);
        qit->second.gcnp_flush_queued = false;
        ++qit->second.gcnp_metrics.batches;
    }
    if (requests.empty()) return;
    uint64_t input_mids = 0;
    for (const auto& request : requests) input_mids += request.mids.cardinality();

    // GCNP's greedy common-pattern extraction now runs before right rows are
    // complete.  A group subscribes to every common right row once; its delta
    // union is then forwarded to all parent rows in the group immediately.
    // This is the streaming counterpart of the single-machine shared union.
    struct StreamGroup {
        mutex mu;
        vector<IncrementalRowKey> parents;
        uint64_t pending_children = 0;
        uint64_t children_per_parent = 0;
        RelationDstSet values;
    };
    uint64_t groups = 0;
    uint64_t reusable_groups = 0;
    uint64_t cache_hits = 0;
    unordered_map<uint64_t, vector<size_t>> classes;
    for (size_t index = 0; index < requests.size(); ++index) {
        const auto& request = requests[index];
        const uint64_t class_id = (static_cast<uint64_t>(static_cast<uint32_t>(request.parent.body_id)) << 32) |
                                  static_cast<uint32_t>(request.right_len);
        classes[class_id].push_back(index);
    }
    uint64_t planned_mids = 0;
    uint64_t bypassed_mids = 0;
    for (const auto& [_, class_indices] : classes) {
        uint64_t class_mids = 0;
        for (size_t request_index : class_indices) {
            class_mids += requests[request_index].mids.cardinality();
        }
        // GCNP is a shared-pattern optimizer, not a mandatory per-event
        // operation.  A tiny batch has too few parent rows to reuse a right
        // pattern; an enormous one builds a column index whose planning cost
        // dominates the sparse join.  In both cases the existing batched
        // continuation is the cheaper demand-preserving execution path.
        if (class_indices.size() < kIncrementalGcnpPlannerMinParents ||
            class_indices.size() > kIncrementalGcnpPlannerMaxParents ||
            class_mids > kIncrementalGcnpPlannerMaxFrontierMids) {
            for (size_t request_index : class_indices) {
                const auto& request = requests[request_index];
                IncrementalSubscribeBinaryBatch(state, request.parent, request.right_len, request.mids);
                bypassed_mids += request.mids.cardinality();
                ++groups;
            }
            continue;
        }
        vector<size_t> member_indices;
        member_indices.reserve(class_indices.size());
        for (size_t request_index : class_indices) {
            const auto& request = requests[request_index];
            const uint64_t mids = request.mids.cardinality();
            if (mids > kIncrementalGcnpPlannerMaxMids) {
                // A large, mostly unique frontier makes the greedy column
                // index more expensive than the join it is meant to save.
                // It still uses the same delta-aware batch continuation.
                IncrementalSubscribeBinaryBatch(state, request.parent, request.right_len, request.mids);
                bypassed_mids += mids;
                ++groups;
            } else {
                member_indices.push_back(request_index);
                planned_mids += mids;
            }
        }
        if (member_indices.empty()) continue;
        vector<RelationDstSet> usable(member_indices.size());
        unordered_map<uint64_t, roaring::Roaring> columns;
        columns.reserve(member_indices.size() * 4 + 1);
        for (uint32_t local_index = 0; local_index < member_indices.size(); ++local_index) {
            usable[local_index] = requests[member_indices[local_index]].mids;
            for (uint64_t mid : usable[local_index]) columns[mid].add(local_index);
        }
        roaring::Roaring unassigned;
        priority_queue<pair<uint64_t, uint32_t>> candidates;
        for (uint32_t index = 0; index < usable.size(); ++index) {
            if (!usable[index].empty()) {
                unassigned.add(index);
                candidates.emplace(usable[index].cardinality(), index);
            }
        }
        while (!unassigned.isEmpty()) {
            while (!candidates.empty() && !unassigned.contains(candidates.top().second)) candidates.pop();
            if (candidates.empty()) break;
            const uint32_t seed_index = candidates.top().second;
            candidates.pop();
            roaring::Roaring group_sources = IncrementalGcnpSeedCorePrefixGroup(
                usable, columns, unassigned, seed_index);
            RelationDstSet common_mids;
            bool initialized = false;
            for (uint32_t index : group_sources) {
                if (!initialized) { common_mids = usable[index]; initialized = true; }
                else common_mids.bitmap &= usable[index].bitmap;
            }
            const bool reusable = group_sources.cardinality() > 1 && !common_mids.empty();
            if (reusable) {
                ++reusable_groups;
                cache_hits += group_sources.cardinality() - 1;
                auto context = make_shared<StreamGroup>();
                context->pending_children = common_mids.cardinality();
                context->children_per_parent = common_mids.cardinality();
                for (uint32_t index : group_sources) context->parents.push_back(requests[member_indices[index]].parent);
                auto consume = [&state, context](const RelationDstSet& delta, bool complete) {
                    RelationDstSet fresh;
                    bool finished = false;
                    {
                        lock_guard<mutex> lk(context->mu);
                        fresh.bitmap = delta.bitmap - context->values.bitmap;
                        context->values.bitmap |= fresh.bitmap;
                        if (complete && context->pending_children > 0) {
                            --context->pending_children;
                            finished = context->pending_children == 0;
                        }
                    }
                    for (const auto& parent : context->parents) {
                        if (!fresh.empty()) IncrementalEmitDelta(state, parent, fresh);
                        if (finished) IncrementalOnBinaryBatchComplete(state, parent, RelationDstSet{}, context->children_per_parent);
                    }
                };
                const auto& representative = requests[member_indices[seed_index]];
                for (uint64_t mid : common_mids) {
                    IncrementalSubscribeRow(state, IncrementalRowKey{representative.parent.query_id, representative.parent.body_id, representative.right_len, mid}, consume);
                }
            } else {
                // A singleton has no reusable pattern. Keep its normal direct
                // parent-child subscriptions rather than dropping its mids.
                common_mids = RelationDstSet{};
            }
            for (uint32_t index : group_sources) {
                RelationDstSet remaining;
                remaining.bitmap = usable[index].bitmap - common_mids.bitmap;
                const auto& request = requests[member_indices[index]];
                IncrementalSubscribeBinaryBatch(state, request.parent, request.right_len, remaining);
            }
            unassigned -= group_sources;
            ++groups;
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        auto& metrics = query.gcnp_metrics;
        metrics.source_rows += requests.size();
        metrics.groups += groups;
        metrics.reusable_groups += reusable_groups;
        metrics.cacheable_patterns += reusable_groups;
        metrics.cache_hits += cache_hits;
        query.gcnp_input_mids += input_mids;
        query.gcnp_planned_mids += planned_mids;
        query.gcnp_bypassed_mids += bypassed_mids;
        query.gcnp_wall_us += static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - gcnp_begin).count());
    }
}

static void IncrementalEnqueueGcnpJoin(
    IncrementalWorkerState& state, IncrementalGcnpJoinRequest request) {
    bool queue_flush = false;
    const int query_id = request.parent.query_id;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        query.gcnp_pending_joins.push_back(std::move(request));
        if (!query.gcnp_flush_queued) {
            query.gcnp_flush_queued = true;
            queue_flush = true;
        }
    }
    if (queue_flush) {
        // The executor queue itself forms a microbatch while preserving the
        // critical delta path.  Delaying every first request by a timer made
        // large frontiers wait behind GCNP planning with little extra reuse.
        state.executor.Submit([&state, query_id]() {
            IncrementalFlushGcnpJoins(state, query_id);
        });
    }
}

static void IncrementalOnBinaryLeft(IncrementalWorkerState& state, IncrementalRowKey parent, int right_len, const RelationDstSet& delta, bool complete) {
    // A parent owns exactly one subscription to its left child, and every
    // child ROW_DELTA is already set-deduplicated.  Do not re-hash every mid
    // here: for high powers that redundant per-value set becomes the dominant
    // cost.  Keep the count only for COMPLETE accounting and hand the whole
    // delta batch to GCNP.
    if (!delta.empty()) {
        {
            lock_guard<mutex> lk(state.mu);
            state.queries.at(parent.query_id).rows.at(parent).binary_pending += delta.cardinality();
        }
        // Large frontiers cannot profit from the bounded single-threaded GCNP
        // planner.  Send them directly to the same batched delta join rather
        // than enqueueing, classifying, and immediately bypassing them later.
        if (delta.cardinality() > kIncrementalGcnpPlannerMaxMids) {
            {
                lock_guard<mutex> lk(state.mu);
                state.queries.at(parent.query_id).gcnp_bypassed_mids += delta.cardinality();
            }
            IncrementalSubscribeBinaryBatch(state, parent, right_len, delta);
        } else {
            IncrementalGcnpJoinRequest request;
            request.parent = parent;
            request.right_len = right_len;
            request.mids = delta;
            IncrementalEnqueueGcnpJoin(state, std::move(request));
        }
    }
    if (complete) {
        bool parent_complete = false;
        { lock_guard<mutex> lk(state.mu); auto& row = state.queries.at(parent.query_id).rows.at(parent); row.left_complete = true; parent_complete = row.binary_pending == 0; }
        if (parent_complete) IncrementalCompleteRow(state, parent);
    }
}

static pair<int, int> IncrementalExactBinarySplit(int length) {
    if (length <= 1) return {0, 0};
    int right = 1;
    while (right <= (length - 1) / 2) right *= 2;
    return {length - right, right};
}

static void IncrementalStartRow(IncrementalWorkerState& state, const IncrementalRowKey& key) {
    if (key.length == 0) { RelationDstSet identity; identity.push_back(key.source); IncrementalEmitDelta(state, key, identity); IncrementalCompleteRow(state, key); return; }
    if (key.length == 1) { IncrementalStartBaseRow(state, key); return; }
    const auto [left_len, right_len] = IncrementalExactBinarySplit(key.length);
    IncrementalRowKey left{key.query_id, key.body_id, left_len, key.source};
    IncrementalSubscribeRow(state, left, [&state, key, right_len](const RelationDstSet& delta, bool complete) { IncrementalOnBinaryLeft(state, key, right_len, delta, complete); });
}

static void IncrementalDeliverRelayDelta(IncrementalWorkerState& state, const IncrementalRowKey& key, uint64_t token, RelationDstSet candidates) {
    vector<IncrementalDeltaCallback> callbacks;
    vector<IncrementalBinaryWaiter> binary_waiters;
    RelationDstSet delta;
    {
        lock_guard<mutex> lk(state.mu);
        auto query_it = state.queries.find(key.query_id);
        if (query_it == state.queries.end()) return;
        auto& query = query_it->second;
        auto token_it = query.remote_tokens.find(token);
        if (token_it == query.remote_tokens.end()) return;
        auto relay_it = query.remote_relays.find(token_it->second);
        if (relay_it == query.remote_relays.end() || relay_it->second.complete) return;
        auto& relay = relay_it->second;
        delta.bitmap = candidates.bitmap - relay.values.bitmap;
        relay.values.bitmap |= delta.bitmap;
        if (!delta.empty()) { callbacks = relay.callbacks; binary_waiters = relay.binary_waiters; }
    }
    for (auto& callback : callbacks) IncrementalInvoke(state, callback, delta, false);
    for (const auto& waiter : binary_waiters) IncrementalDispatchBinaryWaiter(state, waiter, delta, false);
}

static void IncrementalCompleteRelay(IncrementalWorkerState& state, const IncrementalRowKey& key, uint64_t token) {
    vector<IncrementalDeltaCallback> callbacks;
    vector<IncrementalBinaryWaiter> binary_waiters;
    {
        lock_guard<mutex> lk(state.mu);
        auto query_it = state.queries.find(key.query_id);
        if (query_it == state.queries.end()) return;
        auto& query = query_it->second;
        auto token_it = query.remote_tokens.find(token);
        if (token_it == query.remote_tokens.end()) return;
        auto relay_it = query.remote_relays.find(token_it->second);
        query.remote_tokens.erase(token_it);
        if (relay_it == query.remote_relays.end() || relay_it->second.complete) return;
        relay_it->second.complete = true;
        callbacks.swap(relay_it->second.callbacks);
        binary_waiters.swap(relay_it->second.binary_waiters);
    }
    for (auto& callback : callbacks) IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
    for (const auto& waiter : binary_waiters) IncrementalDispatchBinaryWaiter(state, waiter, RelationDstSet{}, true);
}

struct IncrementalBatchContinuationEvent {
    shared_ptr<IncrementalParentJoinBatch> batch;
    RelationDstSet values;
    uint64_t completed_children = 0;
    uint64_t logical_events = 0;
};

static void IncrementalFlushBatchContinuations(
    IncrementalWorkerState& state,
    unordered_map<IncrementalParentJoinBatch*, IncrementalBatchContinuationEvent>& batched) {
    for (auto& [_, event] : batched) {
        IncrementalOnParentJoinBatchAggregate(
            state, event.batch, event.values, event.completed_children, event.logical_events);
    }
}

// The data plane already delivers a vector of row deltas.  Fuse all child
// deltas that target the same parent batch before touching its Roaring state.
// This keeps the per-row cache/relay semantics, but removes repeated callback
// dispatch and batch mutex acquisition within one received microbatch.
static void IncrementalDeliverRelayDeltaBatch(
    IncrementalWorkerState& state, vector<IncrementalRowDeltaEnvelope> events) {
    vector<pair<IncrementalDeltaCallback, RelationDstSet>> callbacks;
    vector<pair<IncrementalBinaryWaiter, RelationDstSet>> direct_waiters;
    unordered_map<IncrementalParentJoinBatch*, IncrementalBatchContinuationEvent> batched;
    {
        lock_guard<mutex> lk(state.mu);
        for (auto& event : events) {
            auto query_it = state.queries.find(event.key.query_id);
            if (query_it == state.queries.end()) continue;
            auto& query = query_it->second;
            auto token_it = query.remote_tokens.find(event.token);
            if (token_it == query.remote_tokens.end()) continue;
            auto relay_it = query.remote_relays.find(token_it->second);
            if (relay_it == query.remote_relays.end() || relay_it->second.complete) continue;
            auto& relay = relay_it->second;
            RelationDstSet delta;
            delta.bitmap = event.values.bitmap - relay.values.bitmap;
            if (delta.empty()) continue;
            relay.values.bitmap |= delta.bitmap;
            for (const auto& callback : relay.callbacks) callbacks.emplace_back(callback, delta);
            for (const auto& waiter : relay.binary_waiters) {
                if (!waiter.batch) {
                    direct_waiters.emplace_back(waiter, delta);
                    continue;
                }
                auto& aggregate = batched[waiter.batch.get()];
                if (!aggregate.batch) aggregate.batch = waiter.batch;
                aggregate.values.bitmap |= delta.bitmap;
                ++aggregate.logical_events;
            }
        }
    }
    for (auto& [callback, delta] : callbacks) IncrementalInvoke(state, callback, std::move(delta), false);
    for (auto& [waiter, delta] : direct_waiters) IncrementalDispatchBinaryWaiter(state, waiter, delta, false);
    IncrementalFlushBatchContinuations(state, batched);
}

static void IncrementalCompleteRelayBatch(
    IncrementalWorkerState& state, vector<IncrementalRowCompleteEnvelope> events) {
    vector<IncrementalDeltaCallback> callbacks;
    vector<IncrementalBinaryWaiter> direct_waiters;
    unordered_map<IncrementalParentJoinBatch*, IncrementalBatchContinuationEvent> batched;
    {
        lock_guard<mutex> lk(state.mu);
        for (const auto& event : events) {
            auto query_it = state.queries.find(event.key.query_id);
            if (query_it == state.queries.end()) continue;
            auto& query = query_it->second;
            auto token_it = query.remote_tokens.find(event.token);
            if (token_it == query.remote_tokens.end()) continue;
            auto relay_it = query.remote_relays.find(token_it->second);
            query.remote_tokens.erase(token_it);
            if (relay_it == query.remote_relays.end() || relay_it->second.complete) continue;
            auto& relay = relay_it->second;
            relay.complete = true;
            for (auto& callback : relay.callbacks) callbacks.push_back(std::move(callback));
            relay.callbacks.clear();
            for (auto& waiter : relay.binary_waiters) {
                if (!waiter.batch) {
                    direct_waiters.push_back(std::move(waiter));
                    continue;
                }
                auto& aggregate = batched[waiter.batch.get()];
                if (!aggregate.batch) aggregate.batch = waiter.batch;
                ++aggregate.completed_children;
                ++aggregate.logical_events;
            }
            relay.binary_waiters.clear();
        }
    }
    for (auto& callback : callbacks) IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
    for (const auto& waiter : direct_waiters) IncrementalDispatchBinaryWaiter(state, waiter, RelationDstSet{}, true);
    IncrementalFlushBatchContinuations(state, batched);
}

static void IncrementalDeliverRelayStreamBatch(
    IncrementalWorkerState& state, vector<IncrementalRowStreamEnvelope> events) {
    struct StreamAction {
        bool callback = false;
        bool complete = false;
        IncrementalDeltaCallback fn;
        IncrementalBinaryWaiter waiter;
        RelationDstSet values;
    };
    vector<StreamAction> actions;
    unordered_map<IncrementalParentJoinBatch*, IncrementalBatchContinuationEvent> batched;
    {
        lock_guard<mutex> lk(state.mu);
        for (auto& event : events) {
            auto query_it = state.queries.find(event.key.query_id);
            if (query_it == state.queries.end()) continue;
            auto& query = query_it->second;
            auto token_it = query.remote_tokens.find(event.token);
            if (token_it == query.remote_tokens.end()) continue;
            auto relay_it = query.remote_relays.find(token_it->second);
            if (relay_it == query.remote_relays.end()) continue;
            auto& relay = relay_it->second;
            if (event.complete) {
                query.remote_tokens.erase(token_it);
                if (relay.complete) continue;
                relay.complete = true;
                for (auto& callback : relay.callbacks) {
                    actions.push_back(StreamAction{true, true, std::move(callback), {}, {}});
                }
                relay.callbacks.clear();
                for (auto& waiter : relay.binary_waiters) {
                    if (!waiter.batch) {
                        actions.push_back(StreamAction{false, true, {}, std::move(waiter), {}});
                        continue;
                    }
                    auto& aggregate = batched[waiter.batch.get()];
                    if (!aggregate.batch) aggregate.batch = waiter.batch;
                    ++aggregate.completed_children;
                    ++aggregate.logical_events;
                }
                relay.binary_waiters.clear();
                continue;
            }
            if (relay.complete) continue;
            RelationDstSet delta;
            delta.bitmap = event.values.bitmap - relay.values.bitmap;
            if (delta.empty()) continue;
            relay.values.bitmap |= delta.bitmap;
            for (const auto& callback : relay.callbacks) {
                actions.push_back(StreamAction{true, false, callback, {}, delta});
            }
            for (const auto& waiter : relay.binary_waiters) {
                if (!waiter.batch) {
                    actions.push_back(StreamAction{false, false, {}, waiter, delta});
                    continue;
                }
                auto& aggregate = batched[waiter.batch.get()];
                if (!aggregate.batch) aggregate.batch = waiter.batch;
                aggregate.values.bitmap |= delta.bitmap;
                ++aggregate.logical_events;
            }
        }
    }
    // Direct continuations retain sender order.  Batch continuations are
    // associative set unions, so coalescing them across DELTA/COMPLETE
    // alternation is equivalent and avoids one mutex/update per child.
    for (auto& action : actions) {
        if (action.callback) {
            IncrementalInvoke(state, std::move(action.fn), std::move(action.values), action.complete);
        } else {
            IncrementalDispatchBinaryWaiter(state, action.waiter, action.values, action.complete);
        }
    }
    IncrementalFlushBatchContinuations(state, batched);
}

static void IncrementalSubscribeRow(IncrementalWorkerState& state, const IncrementalRowKey& key, IncrementalDeltaCallback callback) {
    const int owner = IncrementalOwner(state, key.source);
    if (owner != state.partition_id) {
        uint64_t token = 0;
        RelationDstSet cached;
        bool complete = false;
        bool request_remote = false;
        {
            lock_guard<mutex> lk(state.mu);
            auto& query = state.queries.at(key.query_id);
            auto& relay = query.remote_relays[key];
            cached = relay.values;
            complete = relay.complete;
            if (!complete) {
                relay.callbacks.push_back(callback);
                if (!relay.requested) {
                    relay.requested = true;
                    token = query.next_token++;
                    query.remote_tokens.emplace(token, key);
                    ++query.remote_requests;
                    request_remote = true;
                }
            }
        }
        if (!cached.empty()) IncrementalInvoke(state, callback, cached, false);
        if (complete) IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
        if (request_remote) {
            IncrementalQueuePeerRequest(state, owner, key, token);
        }
        return;
    }
    RelationDstSet cached;
    bool complete = false;
    bool start = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(key.query_id);
        auto& row = query.rows[key];
        cached = row.values;
        if (row.status == IncrementalRowState::Status::COMPLETE) { complete = true; ++query.row_cache_hits; }
        else {
            const uint64_t token = query.next_token++;
            query.callbacks.emplace(token, IncrementalCallback{callback});
            row.waiters.push_back(IncrementalWaiter{state.partition_id, token});
            if (row.status == IncrementalRowState::Status::NOT_STARTED) { row.status = IncrementalRowState::Status::COMPUTING; ++query.rows_started; start = true; }
        }
    }
    if (!cached.empty()) IncrementalInvoke(state, callback, cached, false);
    if (complete) IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
    if (start) state.executor.Submit([&state, key]() { IncrementalStartRow(state, key); });
}

static void IncrementalSubscribeBinaryRight(
    IncrementalWorkerState& state, const IncrementalRowKey& key,
    const IncrementalRowKey& parent, shared_ptr<IncrementalParentJoinBatch> batch) {
    const int owner = IncrementalOwner(state, key.source);
    if (owner != state.partition_id) {
        uint64_t token = 0;
        RelationDstSet cached;
        bool complete = false;
        bool request_remote = false;
        {
            lock_guard<mutex> lk(state.mu);
            auto& query = state.queries.at(key.query_id);
            auto& relay = query.remote_relays[key];
            cached = relay.values;
            complete = relay.complete;
            if (!complete) {
                relay.binary_waiters.push_back(IncrementalBinaryWaiter{parent, batch});
                if (!relay.requested) {
                    relay.requested = true;
                    token = query.next_token++;
                    query.remote_tokens.emplace(token, key);
                    ++query.remote_requests;
                    request_remote = true;
                }
            }
        }
        const IncrementalBinaryWaiter waiter{parent, std::move(batch)};
        if (!cached.empty()) IncrementalDispatchBinaryWaiter(state, waiter, cached, false);
        if (complete) IncrementalDispatchBinaryWaiter(state, waiter, RelationDstSet{}, true);
        if (request_remote) {
            IncrementalQueuePeerRequest(state, owner, key, token);
        }
        return;
    }
    RelationDstSet cached;
    bool complete = false;
    bool start = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(key.query_id);
        auto& row = query.rows[key];
        cached = row.values;
        if (row.status == IncrementalRowState::Status::COMPLETE) { complete = true; ++query.row_cache_hits; }
        else {
            row.binary_waiters.push_back(IncrementalBinaryWaiter{parent, batch});
            if (row.status == IncrementalRowState::Status::NOT_STARTED) {
                row.status = IncrementalRowState::Status::COMPUTING;
                ++query.rows_started;
                start = true;
            }
        }
    }
    const IncrementalBinaryWaiter waiter{parent, std::move(batch)};
    if (!cached.empty()) IncrementalDispatchBinaryWaiter(state, waiter, cached, false);
    if (complete) IncrementalDispatchBinaryWaiter(state, waiter, RelationDstSet{}, true);
    if (start) state.executor.Submit([&state, key]() { IncrementalStartRow(state, key); });
}

static void IncrementalRegisterRemoteWaiter(IncrementalWorkerState& state, IncrementalRowKey key, int reply_pid, uint64_t token) {
    RelationDstSet cached;
    bool complete = false;
    bool start = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(key.query_id);
        if (qit == state.queries.end()) return;
        auto& query = qit->second;
        auto& row = query.rows[key];
        cached = row.values;
        if (row.status == IncrementalRowState::Status::COMPLETE) { complete = true; ++query.row_cache_hits; }
        else {
            row.waiters.push_back(IncrementalWaiter{reply_pid, token});
            if (row.status == IncrementalRowState::Status::NOT_STARTED) { row.status = IncrementalRowState::Status::COMPUTING; ++query.rows_started; start = true; }
        }
    }
    if (!cached.empty()) IncrementalSendDelta(state, reply_pid, key, token, cached);
    if (complete) IncrementalSendComplete(state, reply_pid, key, token);
    if (start) IncrementalStartRow(state, key);
}

// A REQUEST_ROW_BATCH must remain a batch after it reaches the owner.  The
// previous handler decoded 512 requests and then performed 512 separate state
// locks and recursive starts, which recreated the control-plane granularity
// that request batching had removed on the sender.
static void IncrementalRegisterRemoteWaiterBatch(
    IncrementalWorkerState& state, vector<IncrementalRowRequestEnvelope> requests) {
    struct Replay {
        IncrementalRowKey key;
        int reply_pid = -1;
        uint64_t token = 0;
        RelationDstSet values;
        bool complete = false;
    };
    vector<Replay> replays;
    vector<IncrementalRowKey> starts;
    replays.reserve(requests.size());
    starts.reserve(requests.size());
    {
        lock_guard<mutex> lk(state.mu);
        for (const auto& request : requests) {
            auto qit = state.queries.find(request.key.query_id);
            if (qit == state.queries.end()) continue;
            auto& query = qit->second;
            auto& row = query.rows[request.key];
            Replay replay;
            replay.key = request.key;
            replay.reply_pid = request.reply_pid;
            replay.token = request.token;
            replay.values = row.values;
            if (row.status == IncrementalRowState::Status::COMPLETE) {
                replay.complete = true;
                ++query.row_cache_hits;
            } else {
                row.waiters.push_back(IncrementalWaiter{request.reply_pid, request.token});
                if (row.status == IncrementalRowState::Status::NOT_STARTED) {
                    row.status = IncrementalRowState::Status::COMPUTING;
                    ++query.rows_started;
                    starts.push_back(request.key);
                }
            }
            if (!replay.values.empty() || replay.complete) replays.push_back(std::move(replay));
        }
    }
    for (const auto& replay : replays) {
        if (!replay.values.empty()) {
            IncrementalSendDelta(state, replay.reply_pid, replay.key, replay.token, replay.values);
        }
        if (replay.complete) {
            IncrementalSendComplete(state, replay.reply_pid, replay.key, replay.token);
        }
    }
    if (!starts.empty()) {
        state.executor.Submit([&state, starts = std::move(starts)]() mutable {
            for (const auto& key : starts) IncrementalStartRow(state, key);
        });
    }
}

// -------------------------------------------------------------------------
// Frontier-level demand batches
//
// The legacy exact evaluator below this point uses row/mid continuations.
// DemandBatch deliberately does not: a child returns RelationRows and the
// parent composes a whole ready frontier at once.  Row states are still used
// for B^1 memoization/NFA expansion, while powers greater than one are
// scheduled as relation batches.

static void IncrementalDemandFlush(IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalDemandComplete(IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalDemandOnRight(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    RelationRows rows, bool complete);
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static void IncrementalDemandOnMiddlePresence(
    IncrementalWorkerState& state, int query_id, uint64_t token,
    RelationRows rows, RelationDstSet presence_mids, bool complete);
static void IncrementalSendMiddlePresence(
    IncrementalWorkerState& state,
    const IncrementalDemandBatchRequest& request,
    RelationDstSet presence_mids, bool complete);
static void IncrementalInstallMiddleRhsDirect(
    IncrementalWorkerState& state, int query_id, int parent_pid,
    uint64_t parent_batch_id, RelationRows rows);
#endif
static void IncrementalDemandEmit(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    const RelationRows& candidates);
static void IncrementalDemandMaybeComplete(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalDemandDispatchMatrixIfReady(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalDemandDispatchMatrixDelta(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    const RelationRows& left_delta, const RelationRows& right_delta);
static void IncrementalFlushRsfmRoutes(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalDispatchMatrixJoin(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id, int destination_pid,
    RelationRows left_rows, RelationRows right_rows, bool retain_output_on_destination,
    bool left_rows_transposed = false, int right_body_id = -1, int right_length = 0,
    RelationRows transposed_left_rows = {}, RelationRows coverage_rows = {},
    int coverage_parent_pid = -1, uint64_t coverage_parent_batch_id = 0,
    int coverage_body_id = -1, int coverage_length = 0,
    bool coverage_include_identity = false,
    bool return_output_to_parent = false,
    uint64_t expected_right_rows = 0,
    uint64_t expected_right_pairs = 0,
    bool right_include_identity = false);
static void IncrementalProcessMatrixJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static void IncrementalAcceptMatrixJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static void IncrementalAcceptGlobalExactStageUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static void IncrementalHandleGlobalExactStageResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result);
static void IncrementalMaybeFinalizeDestinationShard(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key);
static void IncrementalMaybeBuildIncrementalGcnpPlan(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key);
static void IncrementalPublishIncrementalGcnpDelta(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, RelationRows candidates);
static void IncrementalDispatchDestinationShardSeals(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    uint64_t expected_right_streams, bool left_only = false);
static void IncrementalMaybeDispatchDestinationShardSeals(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalCleanupGenericMatrixBlocks(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id);
static void IncrementalForwardDirectRightStream(
    IncrementalWorkerState& state, const IncrementalDemandBatchRequest& request,
    const RelationRows& rows, bool complete);
static void IncrementalDemandOnDirectSignature(
    IncrementalWorkerState& state, int query_id, uint64_t stream_id,
    RelationRows rows, bool complete);

static RelationRows IncrementalDifferenceRows(const RelationRows& candidates, const RelationRows& existing) {
    RelationRows out;
    out.reserve(candidates.size());
    for (const auto& [source, values] : candidates) {
        auto it = existing.find(source);
        if (it == existing.end()) {
            if (!values.empty()) out.emplace(source, values);
            continue;
        }
        RelationDstSet fresh;
        fresh.bitmap = values.bitmap - it->second.bitmap;
        if (!fresh.empty()) out.emplace(source, std::move(fresh));
    }
    return out;
}

static void IncrementalMergeRows(RelationRows& dst, const RelationRows& src) {
    for (const auto& [source, values] : src) {
        if (!values.empty()) dst[source] |= values;
    }
}

static void IncrementalMergeOwnedRows(RelationRows& dst, RelationRows& src) {
    for (auto& [source, values] : src) {
        if (values.empty()) continue;
        auto found = dst.find(source);
        if (found == dst.end()) dst.emplace(source, std::move(values));
        else found->second.bitmap |= values.bitmap;
    }
}

#if !defined(INCREMENTAL_SYNCHRONOUS_MATRIX_MODE)
#include "middle_presence_router.inc"
#else
#include "sync_message_management.inc"
#endif

static void IncrementalInvokeRows(
    IncrementalWorkerState& state, IncrementalRowsCallback callback,
    RelationRows rows, bool complete) {
    // Demand-batch state changes run on the ordered executor.  Calling the
    // parent directly preserves delta-before-complete ordering without a
    // second per-row task queue.
    callback(rows, complete);
}

static void IncrementalDemandEmit(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    const RelationRows& candidates) {
    if (candidates.empty()) return;
    bool queue_flush = false;
    RelationRows progress_delta;
    vector<IncrementalRowsCallback> progress_callbacks;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        RelationRows fresh = IncrementalDifferenceRows(candidates, batch.output_rows);
        if (fresh.empty()) return;
        const uint64_t fresh_pairs = CountRelationPairs(fresh);
        IncrementalMergeRows(batch.output_rows, fresh);
        IncrementalMergeRows(batch.pending_emit, fresh);
        if (!batch.progress_callbacks.empty()) {
            IncrementalMergeRows(batch.pending_progress, fresh);
            batch.pending_progress_pairs += fresh_pairs;
            if (batch.pending_progress_pairs >= kIncrementalEmitBatchPairs) {
                progress_delta.swap(batch.pending_progress);
                batch.pending_progress_pairs = 0;
                progress_callbacks = batch.progress_callbacks;
            }
        }
        qit->second.demand_batch_pairs += fresh_pairs;
 #if !defined(INCREMENTAL_SYNCHRONOUS_MATRIX_MODE)
        if (CountRelationPairs(batch.pending_emit) >= kIncrementalDemandEmitPairs && !batch.flush_queued) {
            batch.flush_queued = true;
            queue_flush = true;
        }
 #endif
    }
    for (const auto& callback : progress_callbacks) {
        IncrementalInvokeRows(state, callback, progress_delta, false);
    }
    if (queue_flush) {
        state.executor.Submit([&state, query_id, batch_id]() {
            IncrementalDemandFlush(state, query_id, batch_id);
        });
    }
}

static void IncrementalDemandFlush(IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    RelationRows delta;
    vector<IncrementalRowsCallback> callbacks;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        auto& batch = bit->second;
        batch.flush_queued = false;
        if (batch.pending_emit.empty()) return;
        delta.swap(batch.pending_emit);
        callbacks = batch.callbacks;
        ++qit->second.demand_batch_emits;
    }
    for (const auto& callback : callbacks) {
        IncrementalInvokeRows(state, callback, delta, false);
    }
}

static void IncrementalDemandMaybeComplete(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    bool ready = false;
    bool try_seal = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        auto& batch = bit->second;
        const bool inputs_ready = batch.left_complete && batch.pending_right_batches == 0 &&
                                  batch.pending_shared_rows == 0 &&
                                  batch.pending_direct_right_streams == 0;
        try_seal = (batch.length > 1 || batch.generic_matrix_compose) &&
                   !batch.resident_continuation_compose;
        // Every left/right delta is dispatched to destination blocks as soon
        // as it arrives.  A queued route flush is also an in-flight update:
        // COMPLETE must not publish this B^k batch before it is installed at
        // the matrix owners.
        ready = !batch.complete && !batch.matrix_dispatching && inputs_ready &&
                (!try_seal || batch.matrix_seals_dispatched) &&
                batch.pending_matrix_joins == 0 &&
                batch.pending_matrix_route_flushes == 0;
    }
    if (try_seal) IncrementalMaybeDispatchDestinationShardSeals(state, query_id, batch_id);
    if (ready) IncrementalDemandComplete(state, query_id, batch_id);
}

static void IncrementalPublishCompletedDemandRow(
    IncrementalWorkerState& state, const IncrementalRowKey& key,
    RelationDstSet values) {
    RelationDstSet delta;
    vector<IncrementalDeltaCallback> callbacks;
    vector<IncrementalWaiter> remote_waiters;
    vector<IncrementalBinaryWaiter> binary_waiters;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(key.query_id);
        if (qit == state.queries.end()) return;
        auto rit = qit->second.rows.find(key);
        if (rit == qit->second.rows.end() ||
            rit->second.status == IncrementalRowState::Status::COMPLETE) return;
        auto& row = rit->second;
        if (row.waiters.empty() && row.binary_waiters.empty()) {
            if (row.values.empty()) {
                row.values = std::move(values);
            } else {
                row.values |= values;
            }
            row.pending_delta = RelationDstSet{};
            row.status = IncrementalRowState::Status::COMPLETE;
            row.base_visited.clear();
            row.base_visited.rehash(0);
            row.base_pending = 0;
            row.binary_pending = 0;
            return;
        }
        delta.bitmap = values.bitmap - row.values.bitmap;
        row.values.bitmap |= delta.bitmap;
        row.pending_delta = RelationDstSet{};
        row.status = IncrementalRowState::Status::COMPLETE;
        for (const auto& waiter : row.waiters) {
            if (waiter.reply_pid != state.partition_id) {
                remote_waiters.push_back(waiter);
                continue;
            }
            auto callback = qit->second.callbacks.find(waiter.token);
            if (callback == qit->second.callbacks.end()) continue;
            callbacks.push_back(std::move(callback->second.fn));
            qit->second.callbacks.erase(callback);
        }
        binary_waiters.swap(row.binary_waiters);
        row.waiters.clear();
        row.base_visited.clear();
        row.base_visited.rehash(0);
        row.base_pending = 0;
        row.binary_pending = 0;
    }
    for (auto& callback : callbacks) {
        if (!delta.empty()) IncrementalInvoke(state, callback, delta, false);
        IncrementalInvoke(state, std::move(callback), RelationDstSet{}, true);
    }
    for (const auto& waiter : remote_waiters) {
        if (!delta.empty()) IncrementalSendDelta(
            state, waiter.reply_pid, key, waiter.token, delta);
        IncrementalSendComplete(state, waiter.reply_pid, key, waiter.token);
    }
    for (const auto& waiter : binary_waiters) {
        if (!delta.empty()) IncrementalDispatchBinaryWaiter(
            state, waiter, delta, false);
        IncrementalDispatchBinaryWaiter(
            state, waiter, RelationDstSet{}, true);
    }
}

static void IncrementalDemandComplete(IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    // Publish the final partial relation delta before the terminal event.
    IncrementalDemandFlush(state, query_id, batch_id);
    vector<IncrementalRowsCallback> callbacks;
    RelationRows cache_rows;
    int body_id = -1;
    int length = 0;
    RelationDstSet sources;
    bool publish_cache_rows = false;
    bool cleanup_generic_blocks = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        batch.complete = true;
        callbacks = batch.callbacks;
        body_id = batch.body_id;
        length = batch.length;
        sources = batch.sources;
        publish_cache_rows = batch.cache_output_rows;
        batch.pending_progress.clear();
        batch.pending_progress_pairs = 0;
        // All middle blocks are temporary in source-owner mode. Root/final
        // rows have already been returned to this parent before completion.
        cleanup_generic_blocks = batch.generic_matrix_compose;
        if (batch.cache_output_rows) {
            cache_rows = std::move(batch.output_rows);
        }
        if (batch.cache_output_rows) {
            for (uint64_t source : sources) {
                auto& row = qit->second.rows[
                    IncrementalRowKey{query_id, body_id, length, source}];
                if (row.status == IncrementalRowState::Status::NOT_STARTED) {
                    auto hit = cache_rows.find(source);
                    if (hit != cache_rows.end()) row.values = hit->second;
                    row.status = IncrementalRowState::Status::COMPLETE;
                }
            }
        }
        if (batch.generic_matrix_compose) {
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
            if (batch.retain_output_on_destination) {
                IncrementalMergeRows(qit->second.final_rows, batch.output_rows);
            }
#endif
            batch.left_rows.clear();
            batch.right_rows.clear();
            batch.matrix_coverage_rows.clear();
            batch.matrix_coverage_blocks.reset();
            batch.matrix_coverage_signatures.reset();
            batch.output_rows.clear();
            batch.pending_emit.clear();
        }
    }
    if (cleanup_generic_blocks) {
        IncrementalCleanupGenericMatrixBlocks(state, query_id, batch_id);
    }
    if (publish_cache_rows) {
        for (uint64_t source : sources) {
            const IncrementalRowKey key{query_id, body_id, length, source};
            auto row = cache_rows.find(source);
            IncrementalPublishCompletedDemandRow(
                state, key,
                row == cache_rows.end()
                    ? RelationDstSet{}
                    : std::move(row->second));
        }
    }
    for (const auto& callback : callbacks) {
        IncrementalInvokeRows(state, callback, RelationRows{}, true);
    }
}

static void IncrementalDemandOnSharedRow(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    uint64_t source, const RelationDstSet& values, bool complete) {
    if (!values.empty()) {
        RelationRows rows;
        rows.emplace(source, values);
        IncrementalDemandEmit(state, query_id, batch_id, rows);
    }
    if (!complete) return;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        if (bit->second.pending_shared_rows > 0) --bit->second.pending_shared_rows;
    }
    IncrementalDemandMaybeComplete(state, query_id, batch_id);
}

static void IncrementalStartDemandBatch(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, IncrementalRowsCallback callback,
    bool retain_output_on_destination = false,
    IncrementalRowsCallback progress_callback = {});

static void IncrementalDemandOnBaseRow(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    uint64_t source, const RelationDstSet& values, bool complete) {
    if (!values.empty()) {
        RelationRows delta;
        delta.emplace(source, values);
        IncrementalDemandEmit(state, query_id, batch_id, delta);
    }
    if (!complete) return;
    bool finished = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        auto& batch = bit->second;
        if (batch.pending_right_batches > 0) --batch.pending_right_batches;
        finished = batch.pending_right_batches == 0;
        if (finished) batch.left_complete = true;
    }
    if (finished) IncrementalDemandComplete(state, query_id, batch_id);
}

static void IncrementalDemandRequestRight(
    IncrementalWorkerState& state, int query_id, uint64_t parent_id,
    int body_id, int length, int owner, RelationDstSet sources) {
    if (sources.empty()) return;
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    uint64_t token = 0;
    bool middle_semijoin_enabled = true;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(parent_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        middle_semijoin_enabled = bit->second.middle_semijoin_enabled;
        ++bit->second.pending_right_batches;
        token = qit->second.next_token++;
        qit->second.middle_presence_batches.emplace(token, parent_id);
        if (owner != state.partition_id) {
            ++qit->second.remote_requests;
            ++qit->second.remote_request_rpcs;
        }
    }
    if (owner == state.partition_id) {
        auto presence = make_shared<IncrementalMiddlePresenceAccumulator>();
        IncrementalStartDemandBatch(
            state, query_id, body_id, length, std::move(sources),
            [&state, query_id, parent_id, token, presence,
             middle_semijoin_enabled](
                const RelationRows& rows, bool complete) {
                IncrementalInstallMiddleRhsDirect(
                    state, query_id, state.partition_id, parent_id, rows);
                if (!middle_semijoin_enabled) {
                    if (complete) {
                        IncrementalDemandOnMiddlePresence(
                            state, query_id, token, RelationRows{},
                            RelationDstSet{}, true);
                    }
                    return;
                }
                RelationDstSet completed_presence;
                {
                    lock_guard<mutex> lk(presence->mu);
                    for (const auto& [middle, destinations] : rows) {
                        if (!destinations.empty()) presence->mids.push_back(middle);
                    }
                    if (complete) completed_presence = std::move(presence->mids);
                }
                if (!complete) return;
                IncrementalDemandOnMiddlePresence(
                    state, query_id, token, RelationRows{},
                    std::move(completed_presence), true);
            });
        return;
    }
    IncrementalDemandBatchRequest request;
    request.query_id = query_id;
    request.body_id = body_id;
    request.length = length;
    request.reply_pid = state.partition_id;
    request.token = token;
    request.parent_batch_id = parent_id;
    request.middle_rhs_direct = true;
    request.sources = std::move(sources);
    Message message(MessageType::INCREMENTAL_DEMAND_BATCH_REQUEST);
    message.data = SerializeIncrementalDemandBatchRequest(request);
    IncrementalEnqueuePeer(state, owner, std::move(message));
#else
    uint64_t stream_id = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(parent_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& query = qit->second;
        stream_id = query.next_token++;
        bit->second.direct_stream_pending_acks.emplace(
            stream_id, static_cast<uint64_t>(state.total_partitions));
        bit->second.direct_stream_signature_pending.insert(stream_id);
        ++bit->second.pending_direct_right_streams;
        ++bit->second.direct_right_streams_started;
        query.direct_stream_batches.emplace(stream_id, parent_id);
        ++query.remote_requests;
        ++query.remote_request_rpcs;
    }
    IncrementalDemandBatchRequest request;
    request.query_id = query_id;
    request.body_id = body_id;
    request.length = length;
    request.reply_pid = state.partition_id;
    request.token = stream_id;
    request.direct_right_stream = true;
    request.parent_batch_id = parent_id;
    request.sources = std::move(sources);
    if (owner == state.partition_id) {
        IncrementalStartDemandBatch(
            state, query_id, body_id, length, std::move(request.sources),
            [&state, request](const RelationRows& rows, bool complete) {
                IncrementalForwardDirectRightStream(state, request, rows, complete);
            });
        return;
    }
    Message message(MessageType::INCREMENTAL_DEMAND_BATCH_REQUEST);
    message.data = SerializeIncrementalDemandBatchRequest(request);
    IncrementalEnqueuePeer(state, owner, std::move(message));
#endif
}

// Split B[j,*] by its destination owner.  The returned block for W_l is the
// physical B[j,l] relation that will be retained beside C[i,l] on W_l.
static unordered_map<int, RelationRows> IncrementalSplitRightByDestination(
    IncrementalWorkerState& state, const RelationRows& rows) {
    unordered_map<int, RelationRows> blocks;
    for (const auto& [mid, destinations] : rows) {
        unordered_map<int, RelationDstSet> per_owner;
        for (uint64_t destination : destinations) {
            per_owner[IncrementalOwner(state, destination)].push_back(destination);
        }
        for (auto& [owner, values] : per_owner) {
            if (!values.empty()) blocks[owner][mid] |= values;
        }
    }
    return blocks;
}

// Control-plane counterpart of IncrementalSplitRightByDestination.  It does
// not allocate or copy destination endpoint bitmaps: each RHS row becomes at
// most one bit per worker.  This must run before physical shard materializing
// so SIG_DELTA can start selected-left delivery while RHS payloads are built.
static RelationRows IncrementalBuildRightDestinationSignatures(
    IncrementalWorkerState& state, const RelationRows& rows) {
    RelationRows signatures;
    signatures.reserve(rows.size());
    for (const auto& [mid, destinations] : rows) {
        uint64_t mask = 0;
        for (uint64_t destination : destinations) {
            const int owner = IncrementalOwner(state, destination);
            if (owner >= 0 && owner < 64) mask |= (1ULL << owner);
        }
        while (mask != 0) {
            const uint64_t owner = static_cast<uint64_t>(__builtin_ctzll(mask));
            signatures[mid].push_back(owner);
            mask &= mask - 1;
        }
    }
    return signatures;
}

// A left A[i,j] row only participates in a destination block when its mid j
// exists in that block's B[j,l] keys.  This is the only relation data sent to
// a result worker once its right block has already been installed.
static RelationRows IncrementalFilterLeftForRightKeys(
    const RelationRows& left, const RelationRows& right) {
    RelationDstSet mids;
    for (const auto& [mid, _] : right) mids.push_back(mid);
    RelationRows filtered;
    filtered.reserve(left.size());
    for (const auto& [source, values] : left) {
        RelationDstSet match;
        match.bitmap = values.bitmap & mids.bitmap;
        if (!match.empty()) filtered.emplace(source, std::move(match));
    }
    return filtered;
}

static RelationRows IncrementalFilterLeftForRightSources(
    const RelationRows& left, const RelationDstSet& right_sources) {
    RelationRows filtered;
    filtered.reserve(left.size());
    for (const auto& [source, values] : left) {
        RelationDstSet match;
        match.bitmap = values.bitmap & right_sources.bitmap;
        if (!match.empty()) filtered.emplace(source, std::move(match));
    }
    return filtered;
}

static shared_ptr<IncrementalMatrixResidentDirectory>
IncrementalBuildResidentDirectory(
    IncrementalWorkerState& state, const RelationRows& rows) {
    auto directory = make_shared<IncrementalMatrixResidentDirectory>();
    directory->sources_by_destination.resize(
        static_cast<size_t>(state.total_partitions));
    directory->rows_by_destination.resize(
        static_cast<size_t>(state.total_partitions));
    directory->pairs_by_destination.resize(
        static_cast<size_t>(state.total_partitions));
    for (const auto& [source, destinations] : rows) {
        unordered_map<int, uint64_t> pairs_by_owner;
        for (uint64_t destination : destinations) {
            ++pairs_by_owner[IncrementalOwner(state, destination)];
        }
        for (const auto& [owner, pairs] : pairs_by_owner) {
            auto index = static_cast<size_t>(owner);
            directory->sources_by_destination[index].push_back(source);
            ++directory->rows_by_destination[index];
            directory->pairs_by_destination[index] += pairs;
        }
    }
    return directory;
}

// A destination C block can only emit rows for sources whose A[source, mid]
// has at least one matching B[mid, destination] key.  Coverage for every
// other source is dead payload: the block never looks it up.  Keep the
// coverage relation source-aligned with the already filtered left panel.
static RelationRows IncrementalFilterRowsForLeftSources(
    const RelationRows& rows, const RelationRows& filtered_left) {
    RelationRows filtered;
    filtered.reserve(filtered_left.size());
    for (const auto& [source, _] : filtered_left) {
        auto it = rows.find(source);
        if (it != rows.end() && !it->second.empty()) filtered.emplace(source, it->second);
    }
    return filtered;
}

static RelationRows IncrementalFilterRowsForSources(
    const RelationRows& rows, const RelationDstSet& sources) {
    if (sources.empty()) return RelationRows{};
    RelationRows filtered;
    filtered.reserve(min(rows.size(), static_cast<size_t>(sources.cardinality())));
    for (uint64_t source : sources) {
        auto it = rows.find(source);
        if (it != rows.end() && !it->second.empty()) {
            filtered.emplace(source, it->second);
        }
    }
    return filtered;
}

// Remove only source/mid pairs whose complete RHS row for this destination
// is already covered. The test is source-aware: one mid may be removable for
// one source and still required for another.
struct IncrementalPreRouteCoverageStats {
    uint64_t candidate_mids = 0;
    uint64_t signature_rejections = 0;
    uint64_t subset_tests = 0;
    uint64_t skipped_mids = 0;
    uint64_t skipped_rhs_pairs = 0;
    uint64_t input_left_pairs = 0;
    uint64_t output_left_pairs = 0;

    void Add(const IncrementalPreRouteCoverageStats& other) {
        candidate_mids += other.candidate_mids;
        signature_rejections += other.signature_rejections;
        subset_tests += other.subset_tests;
        skipped_mids += other.skipped_mids;
        skipped_rhs_pairs += other.skipped_rhs_pairs;
        input_left_pairs += other.input_left_pairs;
        output_left_pairs += other.output_left_pairs;
    }
};

static RelationRows IncrementalFilterLeftForCoveredRows(
    const RelationRows& left, const RelationRows& right,
    const RelationRows& coverage,
    IncrementalPreRouteCoverageStats* stats = nullptr) {
    if (left.empty() || right.empty() || coverage.empty()) return left;

    struct CoverageGroup {
        const RelationDstSet* coverage = nullptr;
        vector<size_t> source_indices;
        RelationDstSet candidate_mids;
        RelationDstSet covered_mids;
        IncrementalPreRouteCoverageStats physical_stats;
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

    vector<uint64_t> sources;
    sources.reserve(left.size());
    for (const auto& [source, _] : left) sources.push_back(source);
    vector<size_t> group_by_source(
        sources.size(), numeric_limits<size_t>::max());
    unordered_map<uint64_t, vector<size_t>> groups_by_fingerprint;
    vector<CoverageGroup> groups;
    groups.reserve(sources.size());
    for (size_t source_index = 0; source_index < sources.size(); ++source_index) {
        const uint64_t source = sources[source_index];
        auto cover_it = coverage.find(source);
        if (cover_it == coverage.end() || cover_it->second.empty()) {
            continue;
        }
        const uint64_t fingerprint = coverage_fingerprint(cover_it->second);
        auto& candidates = groups_by_fingerprint[fingerprint];
        size_t group_index = groups.size();
        for (size_t candidate : candidates) {
            if (groups[candidate].coverage->bitmap == cover_it->second.bitmap) {
                group_index = candidate;
                break;
            }
        }
        if (group_index == groups.size()) {
            groups.push_back(CoverageGroup{});
            groups.back().coverage = &cover_it->second;
            candidates.push_back(group_index);
        }
        groups[group_index].source_indices.push_back(source_index);
        groups[group_index].candidate_mids |= left.at(source);
        group_by_source[source_index] = group_index;
    }

    // Sources with an identical S_i(w) share the exact subset result for a
    // given middle row S_i(u). This preserves exact coverage semantics while
    // avoiding repeated scans of the same pair of Roaring bitmaps.
    GetGcnpCoverageFilterPool().ParallelFor(
        groups.size(), [&](size_t group_index) {
        auto& group = groups[group_index];
        const uint64_t cover_cardinality = group.coverage->cardinality();
        for (uint64_t mid : group.candidate_mids) {
            auto right_it = right.find(mid);
            if (right_it == right.end()) continue;
            const RelationDstSet& rhs = right_it->second;
            // The exact bitmap subset operation already checks the condition
            // established by each sampled contains/select pair. Avoid doing
            // both for every plausible RHS row; coverage remains exact.
            if (rhs.cardinality() <= cover_cardinality) {
                ++group.physical_stats.subset_tests;
                if (rhs.bitmap.isSubset(group.coverage->bitmap)) {
                    group.covered_mids.push_back(mid);
                }
            } else {
                ++group.physical_stats.signature_rejections;
            }
        }
    }, true);

    vector<RelationDstSet> filtered_rows(sources.size());
    vector<IncrementalPreRouteCoverageStats> row_stats(sources.size());
    RelationDstSet right_keys;
    for (const auto& [mid, _] : right) right_keys.push_back(mid);
    GetGcnpCoverageFilterPool().ParallelFor(sources.size(), [&](size_t index) {
        const auto left_it = left.find(sources[index]);
        const size_t group_index = group_by_source[index];
        if (left_it == left.end() ||
            group_index == numeric_limits<size_t>::max()) {
            if (left_it != left.end()) {
                filtered_rows[index] = left_it->second;
                row_stats[index].input_left_pairs = left_it->second.cardinality();
                row_stats[index].output_left_pairs = left_it->second.cardinality();
            }
            return;
        }
        const RelationDstSet& covered_mids = groups[group_index].covered_mids;
        row_stats[index].input_left_pairs = left_it->second.cardinality();
        RelationDstSet valid;
        valid.bitmap = left_it->second.bitmap & right_keys.bitmap;
        RelationDstSet removed;
        removed.bitmap = valid.bitmap & covered_mids.bitmap;
        RelationDstSet kept;
        kept.bitmap = valid.bitmap - covered_mids.bitmap;
        row_stats[index].candidate_mids = valid.cardinality();
        row_stats[index].skipped_mids = removed.cardinality();
        // Rebuild the retained row with native bitmap operations. Only the
        // removed mids need scalar visits for the existing RHS-pair counter.
        if (stats != nullptr) {
            for (uint64_t mid : removed) {
                row_stats[index].skipped_rhs_pairs +=
                    right.at(mid).cardinality();
            }
        }
        row_stats[index].output_left_pairs = kept.cardinality();
        if (!kept.empty()) filtered_rows[index] = std::move(kept);
    });

    RelationRows filtered;
    filtered.reserve(left.size());
    if (stats != nullptr) {
        for (const auto& group : groups) {
            stats->signature_rejections +=
                group.physical_stats.signature_rejections;
            stats->subset_tests += group.physical_stats.subset_tests;
        }
    }
    for (size_t index = 0; index < sources.size(); ++index) {
        if (stats != nullptr) stats->Add(row_stats[index]);
        if (!filtered_rows[index].empty()) {
            filtered.emplace(sources[index], std::move(filtered_rows[index]));
        }
    }
    return filtered;
}

// Coverage is only an optimization: omitted cover values can make the shifted
// branch emit duplicates, but the enclosing S[a] union remains exact.  Keep
// only values from RHS rows whose signature could be fully covered.  Any row
// that is truly contained by the original cover remains contained here, so a
// pre-join skip can never become unsound.
static RelationRows IncrementalRestrictCoverageToPotentialRows(
    const RelationRows& coverage, const RelationRows& left,
    const RelationRows& right) {
    unordered_map<uint64_t, GcnpCoverageSignature> right_signatures;
    right_signatures.reserve(right.size() * 2 + 1);
    for (const auto& [mid, values] : right) {
        right_signatures.emplace(mid, GcnpMakeCoverageSignature(values));
    }
    vector<uint64_t> sources;
    sources.reserve(left.size());
    for (const auto& [source, _] : left) sources.push_back(source);
    vector<RelationDstSet> restricted_rows(sources.size());
    GetGcnpCoverageFilterPool().ParallelFor(sources.size(), [&](size_t source_index) {
        const uint64_t source = sources[source_index];
        const auto left_it = left.find(source);
        if (left_it == left.end()) return;
        const RelationDstSet& mids = left_it->second;
        auto coverage_it = coverage.find(source);
        if (coverage_it == coverage.end()) return;
        const GcnpCoverageSignature cover_signature =
            GcnpMakeCoverageSignature(coverage_it->second);
        RelationDstSet potential_targets;
        for (uint64_t mid : mids) {
            auto right_it = right.find(mid);
            if (right_it == right.end()) continue;
            auto signature_it = right_signatures.find(mid);
            if (signature_it != right_signatures.end() &&
                GcnpSignatureMayContain(cover_signature, signature_it->second)) {
                potential_targets |= right_it->second;
            }
        }
        if (potential_targets.empty()) return;
        RelationDstSet relevant;
        relevant.bitmap = coverage_it->second.bitmap & potential_targets.bitmap;
        if (!relevant.empty()) restricted_rows[source_index] = std::move(relevant);
    });
    RelationRows restricted;
    restricted.reserve(left.size());
    for (size_t source_index = 0; source_index < sources.size(); ++source_index) {
        if (!restricted_rows[source_index].empty()) {
            restricted.emplace(sources[source_index], std::move(restricted_rows[source_index]));
        }
    }
    return restricted;
}

// Only destinations reachable through this source's participating RHS mids
// can be affected by the pre-join cover.  Restricting the cover here avoids
// sending S[a](source) endpoints that this particular C destination block
// will never inspect.
static void IncrementalMaybeFinishDirectStream(
    IncrementalWorkerState& state, int query_id, uint64_t stream_id) {
    uint64_t batch_id = 0;
    bool finished = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto stream_it = qit->second.direct_stream_batches.find(stream_id);
        if (stream_it == qit->second.direct_stream_batches.end()) return;
        batch_id = stream_it->second;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto ack_it = bit->second.direct_stream_pending_acks.find(stream_id);
        if (ack_it == bit->second.direct_stream_pending_acks.end() || ack_it->second != 0 ||
            bit->second.direct_stream_signature_pending.count(stream_id) != 0) return;
        bit->second.direct_stream_pending_acks.erase(ack_it);
        qit->second.direct_stream_batches.erase(stream_it);
        if (bit->second.pending_direct_right_streams > 0) {
            --bit->second.pending_direct_right_streams;
        }
        finished = true;
    }
    if (finished) IncrementalDemandMaybeComplete(state, query_id, batch_id);
}

static bool IncrementalConsumeDirectStreamAck(
    IncrementalWorkerState& state, int query_id, uint64_t stream_id) {
    bool known = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return false;
        auto stream_it = qit->second.direct_stream_batches.find(stream_id);
        if (stream_it == qit->second.direct_stream_batches.end()) return false;
        known = true;
        auto bit = qit->second.demand_batches.find(stream_it->second);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return true;
        auto ack_it = bit->second.direct_stream_pending_acks.find(stream_id);
        if (ack_it != bit->second.direct_stream_pending_acks.end() && ack_it->second > 0) {
            --ack_it->second;
        }
    }
    if (known) IncrementalMaybeFinishDirectStream(state, query_id, stream_id);
    return known;
}

static void IncrementalSendDirectStreamAck(
    IncrementalWorkerState& state, int query_id, int parent_pid, uint64_t stream_id) {
    if (parent_pid == state.partition_id) {
        state.executor.Submit([&state, query_id, stream_id]() {
            IncrementalConsumeDirectStreamAck(state, query_id, stream_id);
        });
        return;
    }
    IncrementalDemandBatchStream event;
    event.query_id = query_id;
    event.token = stream_id;
    event.complete = true;
    Message message(MessageType::INCREMENTAL_DEMAND_BATCH_STREAM);
    message.data = SerializeIncrementalDemandBatchStream(event);
    IncrementalEnqueuePeer(state, parent_pid, std::move(message));
}

static void IncrementalMaybeAckDirectMatrixStream(
    IncrementalWorkerState& state, int query_id, const IncrementalDirectMatrixStreamKey& key) {
    bool acknowledge = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto it = qit->second.direct_matrix_streams.find(key);
        if (it == qit->second.direct_matrix_streams.end() ||
            !it->second.upstream_complete || it->second.pending_updates != 0) return;
        qit->second.direct_matrix_streams.erase(it);
        auto& block = qit->second.matrix_blocks[
            IncrementalMatrixBlockKey{key.parent_pid, key.parent_batch_id}];
        block.completed_right_streams.insert(key.stream_id);
        if (block.expected_right_streams != 0 &&
            block.completed_right_streams.size() >= block.expected_right_streams &&
            block.rhs_ready_at == chrono::steady_clock::time_point{}) {
            block.rhs_ready_at = chrono::steady_clock::now();
        }
        acknowledge = true;
    }
    if (acknowledge) {
        IncrementalSendDirectStreamAck(state, query_id, key.parent_pid, key.stream_id);
        IncrementalMaybeFinalizeDestinationShard(
            state, query_id,
            IncrementalMatrixBlockKey{key.parent_pid, key.parent_batch_id});
    }
}

static void IncrementalMarkDirectMatrixStreamComplete(
    IncrementalWorkerState& state, int query_id, int parent_pid,
    uint64_t parent_batch_id, uint64_t stream_id,
    uint64_t expected_right_streams) {
    const IncrementalDirectMatrixStreamKey key{parent_pid, parent_batch_id, stream_id};
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        qit->second.direct_matrix_streams[key].upstream_complete = true;
        auto& block = qit->second.matrix_blocks[
            IncrementalMatrixBlockKey{parent_pid, parent_batch_id}];
        block.expected_right_streams = max(
            block.expected_right_streams, expected_right_streams);
    }
    IncrementalMaybeAckDirectMatrixStream(state, query_id, key);
}

static void IncrementalDirectMatrixTaskFinished(
    IncrementalWorkerState& state, int query_id, int parent_pid,
    uint64_t parent_batch_id, uint64_t stream_id) {
    const IncrementalDirectMatrixStreamKey key{parent_pid, parent_batch_id, stream_id};
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto it = qit->second.direct_matrix_streams.find(key);
        if (it == qit->second.direct_matrix_streams.end()) return;
        if (it->second.pending_updates > 0) --it->second.pending_updates;
    }
    IncrementalMaybeAckDirectMatrixStream(state, query_id, key);
}

static void IncrementalAcceptDirectMatrixJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    if (update.direct_stream_complete) {
        IncrementalMarkDirectMatrixStreamComplete(
            state, update.query_id, update.parent_pid, update.parent_batch_id,
            update.token, update.expected_right_streams);
        return;
    }
    IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
}

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static void IncrementalProcessMiddlePartitionJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static void IncrementalProcessMiddleComputeTask(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static void IncrementalRunBalancedTask(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update);
static bool IncrementalDirectBalanceEnabled();
static void IncrementalAcceptDirectBalanceTask(
    IncrementalWorkerState&, IncrementalMatrixJoinUpdate);
#endif

static void IncrementalAcceptMatrixJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    if (update.global_exact_stage && update.middle_compute_task) {
        if (IncrementalDirectBalanceEnabled() && update.middle_virtual_shard != 0) {
            IncrementalAcceptDirectBalanceTask(state, std::move(update));
            return;
        }
        IncrementalRunBalancedTask(state, std::move(update));
        return;
    }
    if (update.global_exact_stage) {
        IncrementalAcceptGlobalExactStageUpdate(state, std::move(update));
        return;
    }
    const IncrementalMatrixBlockKey block_key{
        update.parent_pid, update.parent_batch_id};
    if (update.resident_left_stream_complete) {
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(update.query_id);
            if (qit == state.queries.end()) return;
            auto& block = qit->second.matrix_blocks[block_key];
            block.retain_output_on_destination =
                block.retain_output_on_destination || update.retain_output_on_destination;
            block.expected_resident_left_streams = max(
                block.expected_resident_left_streams,
                update.expected_resident_left_streams);
            block.completed_resident_left_streams.insert(update.resident_left_stream_id);
            // Updates and this marker share one ordered peer sender.  The
            // pending counter also protects updates already queued locally.
            if (block.expected_resident_left_streams != 0 &&
                block.completed_resident_left_streams.size() >=
                    block.expected_resident_left_streams) {
                block.input_sealed = true;
                block.finalize_token = update.token;
                block.left_panel_sealed_at = chrono::steady_clock::now();
            }
        }
        IncrementalMaybeBuildIncrementalGcnpPlan(
            state, update.query_id, block_key);
        IncrementalMaybeFinalizeDestinationShard(state, update.query_id, block_key);
        return;
    }
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    if (update.middle_partition_join) {
        // Keep middle updates ordered with their seal. This is the only
        // ordering requirement of the middle data plane; the join itself is
        // still incremental and never recomputes old-old pairs.
        state.executor.Submit([&state, update = std::move(update)]() mutable {
            if (update.middle_compute_task) {
                IncrementalProcessMiddleComputeTask(state, std::move(update));
            } else {
                IncrementalProcessMiddlePartitionJoinUpdate(state, std::move(update));
            }
        });
        return;
    }
#endif
    if (update.finalize_shard) {
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(update.query_id);
            if (qit == state.queries.end()) return;
            auto& block = qit->second.matrix_blocks[block_key];
            block.input_sealed = true;
            block.left_panel_sealed_at = chrono::steady_clock::now();
            block.expected_right_streams = update.expected_right_streams;
            block.finalize_token = update.token;
            block.retain_output_on_destination =
                block.retain_output_on_destination || update.retain_output_on_destination;
            block.return_output_to_parent =
                block.return_output_to_parent || update.return_output_to_parent;
            if (update.right_body_id >= 0 && update.right_length > 0) {
                block.right_body_id = update.right_body_id;
                block.right_length = update.right_length;
                block.right_include_identity =
                    block.right_include_identity || update.right_include_identity;
            }
            if (update.right_parent_pid >= 0) {
                block.right_parent_pid = update.right_parent_pid;
                block.right_parent_batch_id = update.right_parent_batch_id;
            }
            block.expected_right_rows = max(
                block.expected_right_rows, update.expected_right_rows);
            block.expected_right_pairs = max(
                block.expected_right_pairs, update.expected_right_pairs);
            block.right_sources.bitmap |= update.right_sources.bitmap;
            block.coverage_include_identity =
                block.coverage_include_identity || update.coverage_include_identity;
            if (update.coverage_parent_pid >= 0) {
                block.coverage_parent_pid = update.coverage_parent_pid;
                block.coverage_parent_batch_id = update.coverage_parent_batch_id;
            }
            if (update.coverage_body_id >= 0) {
                block.coverage_body_id = update.coverage_body_id;
                block.coverage_length = update.coverage_length;
            }
            block.coverage_sources.bitmap |= update.coverage_sources.bitmap;
        }
        IncrementalMaybeBuildIncrementalGcnpPlan(
            state, update.query_id, block_key);
        IncrementalMaybeFinalizeDestinationShard(state, update.query_id, block_key);
        return;
    }
    const IncrementalDirectMatrixStreamKey stream_key{
        update.parent_pid, update.parent_batch_id, update.token};
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[block_key];
        if (!update.left_rows.empty() || !update.transposed_left_rows.empty()) {
            if (!block.saw_left_input) {
                block.saw_left_input = true;
                block.first_left_input_at = chrono::steady_clock::now();
            }
            ++block.left_input_batches;
        }
        if (block.speculative_cancel != nullptr) {
            block.speculative_cancel->store(true, memory_order_relaxed);
        }
        ++block.pending_input_updates;
        if (update.direct_right_stream) {
            ++qit->second.direct_matrix_streams[stream_key].pending_updates;
        }
    }
    const int task_query_id = update.query_id;
    IncrementalSubmitCompute(state, task_query_id, [&state, update = std::move(update)]() mutable {
        IncrementalProcessMatrixJoinUpdate(state, std::move(update));
    });
}

// Used by the local right-delta path, where the source owner already has the
// complete relation event. Signature-stream replay below instead preserves
// the transposed form until it reaches the destination compute worker.
static RelationRows IncrementalBuildLeftForRightKeys(
    const unordered_map<uint64_t, RelationDstSet>& sources_by_middle,
    const RelationRows& right) {
    RelationRows filtered;
    for (const auto& [mid, _] : right) {
        auto source_it = sources_by_middle.find(mid);
        if (source_it == sources_by_middle.end()) continue;
        for (uint64_t source : source_it->second) {
            filtered[source].push_back(mid);
        }
    }
    return filtered;
}

// A direct right request streams its physical RHS shards immediately.  The
// final signature closes a destination panel, but is not the first point at
// which left input may move: SIG_DELTA has already routed every known
// (source, mid) to each newly discovered destination.  Completion therefore
// only authorizes PANEL_END / exact GCNP planning.
static void IncrementalRsfmMaybeFinalizeRoutes(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    bool finalized = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        if (batch.rsfm_signatures_final || !batch.left_complete ||
            !batch.direct_stream_signature_pending.empty()) {
            return;
        }
        batch.rsfm_signatures_final = true;
        finalized = true;
    }
    if (finalized) {
        IncrementalMaybeDispatchDestinationShardSeals(state, query_id, batch_id);
    }
}

// The parent sees only this compact metadata stream.  It activates a left
// route exactly once for each (mid, destination-output-shard) pair; the real
// B[mid,*] payload never returns to the parent.
static void IncrementalDemandOnDirectSignature(
    IncrementalWorkerState& state, int query_id, uint64_t stream_id,
    RelationRows rows, bool complete) {
    uint64_t batch_id = 0;
    uint64_t signature_activations = 0;
    uint64_t replay_left_pairs = 0;
    bool queue_route_flush = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto stream_it = qit->second.direct_stream_batches.find(stream_id);
        if (stream_it == qit->second.direct_stream_batches.end()) return;
        batch_id = stream_it->second;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        unordered_map<int, RelationRows> activated_mids_by_destination;
        for (const auto& [mid, destination_ids] : rows) {
            for (uint64_t destination_u64 : destination_ids) {
                if (destination_u64 >= static_cast<uint64_t>(state.total_partitions) ||
                    destination_u64 >= 64) continue;
                const int destination_pid = static_cast<int>(destination_u64);
                const uint64_t destination_bit = 1ULL << destination_pid;
                uint64_t& signature = batch.rsfm_signature_by_middle[mid];
                if ((signature & destination_bit) != 0) continue;
                signature |= destination_bit;
                activated_mids_by_destination[destination_pid][mid].push_back(
                    destination_u64);
                ++signature_activations;
            }
        }
        // A new signature edge is the only time historic A(*, mid) must be
        // replayed.  The inverse index is monotone, so every logical pair is
        // delivered once to this destination and never to an old one again.
        for (const auto& [destination_pid, mids] : activated_mids_by_destination) {
            RelationRows replay = IncrementalBuildLeftForRightKeys(
                batch.rsfm_sources_by_middle, mids);
            if (replay.empty()) continue;
            replay_left_pairs += CountRelationPairs(replay);
            IncrementalMergeRows(
                batch.rsfm_pending_left_by_destination[destination_pid], replay);
        }
        if (replay_left_pairs != 0 && !batch.rsfm_route_flush_queued) {
            batch.rsfm_route_flush_queued = true;
            ++batch.pending_matrix_route_flushes;
            queue_route_flush = true;
        }
        qit->second.rsfm_signature_activations += signature_activations;
        qit->second.rsfm_replay_left_pairs += replay_left_pairs;
        qit->second.rsfm_selected_left_pairs += replay_left_pairs;
        if (complete) batch.direct_stream_signature_pending.erase(stream_id);
    }
    if (queue_route_flush) {
        state.executor.Submit([&state, query_id, batch_id]() {
            IncrementalFlushRsfmRoutes(state, query_id, batch_id);
        });
    }
    IncrementalRsfmMaybeFinalizeRoutes(state, query_id, batch_id);
    if (complete) IncrementalMaybeFinishDirectStream(state, query_id, stream_id);
}

static RelationRows IncrementalBufferDirectSignature(
    IncrementalWorkerState& state, const IncrementalDemandBatchRequest& request,
    const RelationRows& signatures) {
    RelationRows publish;
    if (signatures.empty()) return publish;
    const IncrementalDirectMatrixStreamKey key{
        request.reply_pid, request.parent_batch_id, request.token};
    lock_guard<mutex> lk(state.mu);
    auto qit = state.queries.find(request.query_id);
    if (qit == state.queries.end()) return publish;
    auto& stream = qit->second.direct_signature_streams[key];
    RelationRows known = stream.announced;
    IncrementalMergeRows(known, stream.pending);
    RelationRows fresh = IncrementalDifferenceRows(signatures, known);
    if (fresh.empty()) return publish;
    IncrementalMergeRows(stream.pending, fresh);
    if (CountRelationPairs(stream.pending) < kIncrementalSignatureEmitPairs) return publish;
    publish.swap(stream.pending);
    IncrementalMergeRows(stream.announced, publish);
    return publish;
}

static RelationRows IncrementalDrainDirectSignature(
    IncrementalWorkerState& state, const IncrementalDemandBatchRequest& request) {
    RelationRows pending;
    const IncrementalDirectMatrixStreamKey key{
        request.reply_pid, request.parent_batch_id, request.token};
    lock_guard<mutex> lk(state.mu);
    auto qit = state.queries.find(request.query_id);
    if (qit == state.queries.end()) return pending;
    auto it = qit->second.direct_signature_streams.find(key);
    if (it == qit->second.direct_signature_streams.end()) return pending;
    pending.swap(it->second.pending);
    IncrementalMergeRows(it->second.announced, pending);
    qit->second.direct_signature_streams.erase(it);
    return pending;
}

static void IncrementalSendDirectSignature(
    IncrementalWorkerState& state, const IncrementalDemandBatchRequest& request,
    RelationRows signatures, bool complete) {
    if (signatures.empty() && !complete) return;
    IncrementalDemandBatchStream event;
    event.query_id = request.query_id;
    event.token = request.token;
    event.direct_signature = true;
    event.complete = complete;
    event.rows = std::move(signatures);
    if (request.reply_pid == state.partition_id) {
        IncrementalDemandOnDirectSignature(
            state, event.query_id, event.token,
            std::move(event.rows), event.complete);
    } else {
        Message message(MessageType::INCREMENTAL_DEMAND_BATCH_STREAM);
        message.data = SerializeIncrementalDemandBatchStream(event);
        IncrementalEnqueuePeer(
            state, request.reply_pid, std::move(message),
            IncrementalWorkerState::PeerWireKind::SIGNATURE);
    }
}

static void IncrementalForwardDirectRightStream(
    IncrementalWorkerState& state, const IncrementalDemandBatchRequest& request,
    const RelationRows& rows, bool complete) {
    // The receiver needs the current stream watermark before the final left
    // panel seal. This makes a completed RHS prefix an explicit, safe input
    // snapshot for low-priority pattern-union warming.
    uint64_t expected_right_streams = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(request.query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(request.parent_batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        expected_right_streams = bit->second.direct_right_streams_started;
    }
    // Publish the compact signature before building physical destination
    // blocks. The latter copies large endpoint bitmaps and used to delay the
    // only information needed to route A(s, mid).
    RelationRows signatures = IncrementalBuildRightDestinationSignatures(state, rows);
    IncrementalSendDirectSignature(
        state, request, IncrementalBufferDirectSignature(state, request, signatures), false);
    if (complete) {
        IncrementalSendDirectSignature(
            state, request, IncrementalDrainDirectSignature(state, request), false);
        // The terminal signature is deliberately ahead of the physical RHS
        // stream-complete markers. It seals the left panel, not the RHS.
        IncrementalSendDirectSignature(state, request, RelationRows{}, true);
    }

    const auto right_blocks = IncrementalSplitRightByDestination(state, rows);
    if (!right_blocks.empty()) {
        for (const auto& [destination_pid, right_block] : right_blocks) {
            if (right_block.empty()) continue;
            // Signatures are emitted for every consuming parent, but the
            // physical B^k[mid,l] payload is destination-resident.  Suppress
            // a repeated installation when a completed right row is reused.
            RelationRows fresh_payload;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(request.query_id);
                if (qit == state.queries.end()) return;
                auto& delivered = qit->second.delivered_relation_shards[
                    IncrementalRelationShardKey{request.body_id, request.length}]
                    [destination_pid];
                fresh_payload = IncrementalDifferenceRows(right_block, delivered);
                IncrementalMergeRows(delivered, fresh_payload);
            }
            IncrementalMatrixJoinUpdate update;
            update.query_id = request.query_id;
            update.parent_pid = request.reply_pid;
            update.parent_batch_id = request.parent_batch_id;
            update.token = request.token;
            update.direct_right_stream = true;
            update.expected_right_streams = expected_right_streams;
            update.right_body_id = request.body_id;
            update.right_length = request.length;
            // The payload is installed in the destination-resident RHS
            // shard. The row-key directory belongs to this consumer block,
            // not to the delivery ledger: a later batch may reuse an
            // already delivered payload, but it still needs every requested
            // mid to project the shared resident shard.
            for (const auto& [mid, _] : right_block) {
                update.right_sources.push_back(mid);
            }
            update.expected_right_rows = right_block.size();
            update.expected_right_pairs = CountRelationPairs(right_block);
            update.right_rows = std::move(fresh_payload);
            if (destination_pid == state.partition_id) {
                IncrementalAcceptDirectMatrixJoinUpdate(state, std::move(update));
            } else {
                Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
                message.data = SerializeIncrementalMatrixJoinUpdate(update);
                IncrementalEnqueuePeer(
                    state, destination_pid, std::move(message),
                    IncrementalWorkerState::PeerWireKind::RHS);
            }
        }
    }

    if (!complete) return;
    // Every physical destination receives a marker, including empty shards.
    // That gives the parent an exact, fixed completion fan-in.
    for (int destination_pid = 0; destination_pid < state.total_partitions; ++destination_pid) {
        IncrementalMatrixJoinUpdate marker;
        marker.query_id = request.query_id;
        marker.parent_pid = request.reply_pid;
        marker.parent_batch_id = request.parent_batch_id;
        marker.token = request.token;
        marker.direct_right_stream = true;
        marker.direct_stream_complete = true;
        marker.expected_right_streams = expected_right_streams;
        if (destination_pid == state.partition_id) {
            IncrementalAcceptDirectMatrixJoinUpdate(state, std::move(marker));
        } else {
            Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
            message.data = SerializeIncrementalMatrixJoinUpdate(marker);
            IncrementalEnqueuePeer(
                state, destination_pid, std::move(message),
                IncrementalWorkerState::PeerWireKind::RHS);
        }
    }
}

static uint64_t IncrementalRelationPairs(const RelationRows& rows) {
    return CountRelationPairs(rows);
}

static void IncrementalHandleMatrixJoinResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result);

static void IncrementalSendDestinationShardResult(
    IncrementalWorkerState& state, int parent_pid,
    IncrementalMatrixJoinResult result) {
    if (parent_pid == state.partition_id) {
        state.executor.Submit([&state, result = std::move(result)]() mutable {
            IncrementalHandleMatrixJoinResult(state, std::move(result));
        });
        return;
    }
    const auto encode_begin = chrono::steady_clock::now();
    const int trace_query = result.query_id;
    const uint64_t trace_stage = result.global_stage_id;
    Message message(MessageType::INCREMENTAL_MATRIX_JOIN_RESULT);
    message.data = SerializeIncrementalMatrixJoinResult(result);
    if (IncrementalScalingTraceEnabled()) {
        ostringstream line;
        line << "[result-send-timing] worker=" << state.partition_id
             << " query=" << trace_query << " stage=" << trace_stage
             << " owner=" << parent_pid << " bytes=" << message.data.size()
             << " encode_us=" << chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - encode_begin).count()
             << " wall_end_us=" << IncrementalTraceWallUs();
        cerr << line.str() << '\n';
    }
    IncrementalEnqueuePeer(state, parent_pid, std::move(message));
}

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static uint64_t IncrementalEstimateMiddleJoinWork(
    const RelationRows& left, const RelationRows& right) {
    const uint64_t left_pairs = CountRelationPairs(left);
    const uint64_t right_pairs = CountRelationPairs(right);
    if (left_pairs == 0 || right_pairs == 0 || right.empty()) return 0;
    const uint64_t average_fanout =
        max<uint64_t>(1, (right_pairs + right.size() - 1) / right.size());
    if (left_pairs > numeric_limits<uint64_t>::max() / average_fanout) {
        return numeric_limits<uint64_t>::max();
    }
    return left_pairs * average_fanout;
}

struct IncrementalMiddleComputeInput {
    RelationRows left;
    RelationRows right;
    uint64_t work = 0;
    uint64_t transfer_pairs = 0;
    int compute_pid = -1;
};

// Split a physical middle update into independent, stateless compute chunks.
// The owner keeps the authoritative incremental state; helpers receive only
// these fragments and return candidate rows.  Splitting by mid preserves the
// matrix join boundary, while a very hot mid is further split by source so it
// can actually be stolen by more than one worker.
static vector<IncrementalMiddleComputeInput> IncrementalSplitMiddleComputeInput(
    const RelationRows& left, const RelationRows& right) {
    static constexpr uint64_t kChunkWork = 8ULL * 1024ULL * 1024ULL;
    unordered_map<uint64_t, vector<uint64_t>> sources_by_middle;
    sources_by_middle.reserve(right.size() * 2 + 1);
    for (const auto& [source, mids] : left) {
        for (uint64_t middle : mids) {
            if (right.find(middle) != right.end()) {
                sources_by_middle[middle].push_back(source);
            }
        }
    }

    vector<uint64_t> middles;
    middles.reserve(sources_by_middle.size());
    for (const auto& [middle, _] : sources_by_middle) middles.push_back(middle);
    sort(middles.begin(), middles.end(), [&](uint64_t lhs, uint64_t rhs) {
        const uint64_t lhs_work = static_cast<uint64_t>(
            sources_by_middle.at(lhs).size()) * right.at(lhs).cardinality();
        const uint64_t rhs_work = static_cast<uint64_t>(
            sources_by_middle.at(rhs).size()) * right.at(rhs).cardinality();
        return lhs_work != rhs_work ? lhs_work > rhs_work : lhs < rhs;
    });

    vector<IncrementalMiddleComputeInput> chunks;
    RelationRows current_left;
    RelationRows current_right;
    uint64_t current_work = 0;
    auto flush = [&]() {
        if (current_left.empty() || current_right.empty()) return;
        IncrementalMiddleComputeInput chunk;
        chunk.left = std::move(current_left);
        chunk.right = std::move(current_right);
        chunk.work = IncrementalEstimateMiddleJoinWork(chunk.left, chunk.right);
        chunk.transfer_pairs = CountRelationPairs(chunk.left) +
                               CountRelationPairs(chunk.right);
        chunks.push_back(std::move(chunk));
        current_left.clear();
        current_right.clear();
        current_work = 0;
    };

    for (uint64_t middle : middles) {
        const auto& sources = sources_by_middle.at(middle);
        const auto& rhs = right.at(middle);
        const uint64_t rhs_pairs = max<uint64_t>(1, rhs.cardinality());
        const uint64_t middle_work = static_cast<uint64_t>(sources.size()) * rhs_pairs;
        if (middle_work > kChunkWork && sources.size() > 1) {
            flush();
            const size_t sources_per_chunk = max<size_t>(
                1, static_cast<size_t>(kChunkWork / rhs_pairs));
            for (size_t begin = 0; begin < sources.size(); begin += sources_per_chunk) {
                const size_t end = min(sources.size(), begin + sources_per_chunk);
                IncrementalMiddleComputeInput chunk;
                for (size_t index = begin; index < end; ++index) {
                    chunk.left[sources[index]].push_back(middle);
                }
                chunk.right.emplace(middle, rhs);
                chunk.work = IncrementalEstimateMiddleJoinWork(
                    chunk.left, chunk.right);
                chunk.transfer_pairs = CountRelationPairs(chunk.left) +
                                       CountRelationPairs(chunk.right);
                chunks.push_back(std::move(chunk));
            }
            continue;
        }
        if (!current_left.empty() && current_work > 0 &&
            middle_work > kChunkWork - min(current_work, kChunkWork)) {
            flush();
        }
        for (uint64_t source : sources) current_left[source].push_back(middle);
        current_right.emplace(middle, rhs);
        current_work = min<uint64_t>(kChunkWork, current_work + middle_work);
    }
    flush();
    return chunks;
}

static int IncrementalReserveMiddleComputeWorker(
    IncrementalWorkerState& state, IncrementalQueryState& query,
    uint64_t work, uint64_t transfer_pairs, uint64_t tie_salt,
    bool batch_owner_is_overloaded, int preferred_compute_pid) {
    auto& outstanding = query.middle_scheduler_outstanding_work;
    outstanding.resize(static_cast<size_t>(state.total_partitions), 0);

    // Remote execution must save more join work than it adds in input
    // movement. Small bitmap unions stay at their physical middle owner.
    const uint64_t transfer_guard =
        transfer_pairs > numeric_limits<uint64_t>::max() / 4
            ? numeric_limits<uint64_t>::max() : transfer_pairs * 4;
    const bool remote_eligible = batch_owner_is_overloaded &&
        state.total_partitions > 1 &&
        work > transfer_guard;
    int selected = state.partition_id;
    if (remote_eligible) {
        if (preferred_compute_pid >= 0 &&
            preferred_compute_pid < state.total_partitions &&
            preferred_compute_pid != state.partition_id) {
            selected = preferred_compute_pid;
        } else {
        uint64_t best_score = numeric_limits<uint64_t>::max();
        const int start = static_cast<int>(tie_salt %
            static_cast<uint64_t>(state.total_partitions));
        for (int offset = 0; offset < state.total_partitions; ++offset) {
            const int candidate = (start + offset) % state.total_partitions;
            uint64_t score = outstanding[static_cast<size_t>(candidate)];
            if (candidate != state.partition_id) {
                score = score > numeric_limits<uint64_t>::max() - transfer_pairs
                    ? numeric_limits<uint64_t>::max() : score + transfer_pairs;
            }
            if (score < best_score) {
                best_score = score;
                selected = candidate;
            }
        }
        }
    }
    auto& selected_work = outstanding[static_cast<size_t>(selected)];
    selected_work = selected_work > numeric_limits<uint64_t>::max() - work
        ? numeric_limits<uint64_t>::max() : selected_work + work;
    query.middle_scheduler_submitted_work += work;
    if (selected != state.partition_id) ++query.middle_scheduler_assigned_shards;
    return selected;
}

static void IncrementalProcessMiddleComputeTask(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    GcnpExecutionStats stats;
    RelationRows candidates = ComposeRelationRowsGcnp(
        update.left_rows, update.right_rows, nullptr, nullptr, &stats);
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& query = qit->second;
        query.middle_scheduler_executed_work += update.scheduled_join_work;
        query.gcnp_wall_us += stats.prepare_us + stats.group_extract_us +
            stats.pattern_cache_us + stats.union_us + stats.shared_target_us +
            stats.source_materialize_us;
        query.gcnp_complete_prepare_us += stats.prepare_us;
        query.gcnp_complete_group_extract_us += stats.group_extract_us;
        query.gcnp_complete_pattern_cache_us += stats.pattern_cache_us;
        query.gcnp_complete_union_us += stats.union_us;
        query.gcnp_complete_shared_target_us += stats.shared_target_us;
        query.gcnp_complete_source_materialize_us += stats.source_materialize_us;
    }
    IncrementalMatrixJoinResult result;
    result.query_id = update.query_id;
    result.token = update.token;
    result.parent_batch_id = update.parent_batch_id;
    result.middle_compute_result = true;
    result.middle_partition = update.middle_partition;
    result.destination_pid = state.partition_id;
    result.scheduled_join_work = update.scheduled_join_work;
    result.rows = std::move(candidates);
    IncrementalSendDestinationShardResult(
        state, update.parent_pid, std::move(result));
}

#include "../../modules/synchronous_middle_streaming.inc"

// W_k owns the middle column/row pair for one binary composition.  Its state
// is monotone: a new left delta joins the resident right rows, and a new
// right delta joins the resident left rows.  The emitted partial relation is
// kept locally so overlapping paths through the same middle partition do not
// cross the wire twice.
static void IncrementalProcessMiddlePartitionJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    const IncrementalMatrixBlockKey key{
        update.parent_pid, update.parent_batch_id};
#if defined(INCREMENTAL_SYNCHRONOUS_MATRIX_MODE)
    IncrementalProcessSynchronousMiddlePartitionJoinUpdate(
        state, std::move(update));
    return;
#if 0
    const bool is_seal = update.middle_left_complete ||
                         update.middle_stream_complete;
    bool queue_complete_join = false;
    bool acknowledge_update = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[key];
        if (!is_seal) {
            IncrementalMergeRows(block.left_rows, update.left_rows);
            IncrementalMergeRows(block.right_rows, update.right_rows);
            // owner(mid) is only a rendezvous point for this compose. The
            // authoritative C(source, *) rows are returned to owner(source).
            block.retain_output_on_destination = false;
            acknowledge_update = !update.middle_rhs_direct;
        } else {
            if (update.middle_left_complete) {
                block.synchronous_middle_left_sealed = true;
                block.left_panel_sealed_at = chrono::steady_clock::now();
            }
            if (update.middle_stream_complete) {
                block.synchronous_middle_right_sealed = true;
                block.finalize_token = update.token;
            }
            if (block.synchronous_middle_left_sealed &&
                block.synchronous_middle_right_sealed &&
                !block.synchronous_middle_join_queued && !block.finalized) {
                block.synchronous_middle_join_queued = true;
                queue_complete_join = true;
            }
            // The left-only seal has its own token. The final seal token is
            // completed by the one-shot result.
            acknowledge_update = !update.middle_stream_complete;
        }
    }

    if (acknowledge_update) {
        IncrementalMatrixJoinResult acknowledgement;
        acknowledgement.query_id = update.query_id;
        acknowledgement.token = update.token;
        acknowledgement.parent_batch_id = update.parent_batch_id;
        acknowledgement.middle_partial = true;
        acknowledgement.middle_partition = update.middle_partition;
        acknowledgement.destination_pid = update.middle_partition;
        IncrementalSendDestinationShardResult(
            state, update.parent_pid, std::move(acknowledgement));
    }
    if (!queue_complete_join) return;

    const int task_query_id = update.query_id;
    const int parent_pid = update.parent_pid;
    const uint64_t parent_batch_id = update.parent_batch_id;
    const int middle_partition = update.middle_partition;
    IncrementalSubmitCompute(
        state, task_query_id,
        [&state, task_query_id, parent_pid, parent_batch_id,
         middle_partition, key]() mutable {
            RelationRows left;
            RelationRows right;
            uint64_t final_token = 0;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(task_query_id);
                if (qit == state.queries.end()) return;
                auto bit = qit->second.matrix_blocks.find(key);
                if (bit == qit->second.matrix_blocks.end() ||
                    bit->second.finalized ||
                    !bit->second.synchronous_middle_left_sealed ||
                    !bit->second.synchronous_middle_right_sealed) {
                    return;
                }
                left = std::move(bit->second.left_rows);
                right = std::move(bit->second.right_rows);
                final_token = bit->second.finalize_token;
            }

            const auto join_begin = chrono::steady_clock::now();
            GcnpExecutionStats stats;
            RelationRows candidates = ComposeRelationRowsGcnp(
                left, right, nullptr, nullptr, &stats);
            const uint64_t join_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - join_begin).count());
            const uint64_t left_pairs = CountRelationPairs(left);
            const uint64_t result_pairs = CountRelationPairs(candidates);
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(task_query_id);
                if (qit == state.queries.end()) return;
                auto bit = qit->second.matrix_blocks.find(key);
                if (bit == qit->second.matrix_blocks.end()) return;
                ++qit->second.matrix_join_updates;
                qit->second.matrix_join_result_pairs += result_pairs;
                qit->second.matrix_join_us += join_us;
                ++qit->second.gcnp_metrics.batches;
                qit->second.gcnp_metrics.source_rows += left.size();
                qit->second.gcnp_input_mids += left_pairs;
                qit->second.gcnp_planned_mids += left_pairs;
                qit->second.gcnp_wall_us += join_us;
                qit->second.gcnp_complete_prepare_us += stats.prepare_us;
                qit->second.gcnp_complete_group_extract_us += stats.group_extract_us;
                qit->second.gcnp_complete_pattern_cache_us += stats.pattern_cache_us;
                qit->second.gcnp_complete_union_us += stats.union_us;
                qit->second.gcnp_complete_shared_target_us += stats.shared_target_us;
                qit->second.gcnp_complete_source_materialize_us +=
                    stats.source_materialize_us;
                qit->second.gcnp_complete_groups += stats.groups;
                qit->second.gcnp_complete_reusable_groups += stats.reusable_groups;
                qit->second.gcnp_complete_grouped_source_uses +=
                    stats.grouped_source_uses;
                qit->second.gcnp_complete_shared_source_uses +=
                    stats.shared_source_uses;
                qit->second.gcnp_complete_external_memo_hits +=
                    stats.external_memo_hits;
                // No relation result survives on owner(mid).
                qit->second.matrix_blocks.erase(bit);
            }

            IncrementalMatrixJoinResult result;
            result.query_id = task_query_id;
            result.token = final_token;
            result.parent_batch_id = parent_batch_id;
            result.middle_partial = true;
            result.middle_complete = true;
            result.middle_partition = middle_partition;
            result.destination_pid = middle_partition;
            result.rows = std::move(candidates);
            IncrementalSendDestinationShardResult(
                state, parent_pid, std::move(result));
        });
    return;
#endif
#endif

    RelationRows fresh_left;
    RelationRows old_left;
    RelationRows fresh_right;
    RelationRows right_after;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[key];
        fresh_left = IncrementalDifferenceRows(update.left_rows, block.left_rows);
        fresh_right = IncrementalDifferenceRows(update.right_rows, block.right_rows);
        if (!fresh_right.empty()) old_left = block.left_rows;
        IncrementalMergeRows(block.left_rows, fresh_left);
        IncrementalMergeRows(block.right_rows, fresh_right);
        if (!fresh_left.empty()) right_after = block.right_rows;
        block.retain_output_on_destination =
            block.retain_output_on_destination || update.retain_output_on_destination;
    }

    // The common balanced case is byte-for-byte the fixed-owner algorithm:
    // no scheduler context, extra executor hop, or helper result round trip.
    if (!update.middle_allow_remote_compute) {
        GcnpExecutionStats stats;
        RelationRows candidates;
        if (!fresh_left.empty() && !right_after.empty()) {
            candidates = ComposeRelationRowsGcnp(
                fresh_left, right_after, nullptr, nullptr, &stats);
        }
        if (!old_left.empty() && !fresh_right.empty()) {
            RelationRows right_delta_candidates = ComposeRelationRowsGcnp(
                old_left, fresh_right, nullptr, nullptr, &stats);
            IncrementalMergeRows(candidates, right_delta_candidates);
        }
        RelationRows fresh_output;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(update.query_id);
            if (qit == state.queries.end()) return;
            auto& query = qit->second;
            auto& block = query.matrix_blocks[key];
            fresh_output = IncrementalDifferenceRows(candidates, block.output_rows);
            IncrementalMergeRows(block.output_rows, fresh_output);
            ++query.matrix_join_updates;
            query.matrix_join_result_pairs += CountRelationPairs(fresh_output);
            query.gcnp_wall_us += stats.prepare_us + stats.group_extract_us +
                stats.pattern_cache_us + stats.union_us + stats.shared_target_us +
                stats.source_materialize_us;
            query.gcnp_complete_prepare_us += stats.prepare_us;
            query.gcnp_complete_group_extract_us += stats.group_extract_us;
            query.gcnp_complete_pattern_cache_us += stats.pattern_cache_us;
            query.gcnp_complete_union_us += stats.union_us;
            query.gcnp_complete_shared_target_us += stats.shared_target_us;
            query.gcnp_complete_source_materialize_us += stats.source_materialize_us;
        }
        IncrementalMatrixJoinResult result;
        result.query_id = update.query_id;
        result.token = update.token;
        result.parent_batch_id = update.parent_batch_id;
        result.middle_partial = true;
        result.middle_partition = update.middle_partition;
        result.destination_pid = update.middle_partition;
        result.rows = std::move(fresh_output);
        IncrementalSendDestinationShardResult(
            state, update.parent_pid, std::move(result));
        return;
    }

    vector<IncrementalMiddleComputeInput> tasks;
    auto append_split_tasks = [&](const RelationRows& left, const RelationRows& right) {
        auto chunks = IncrementalSplitMiddleComputeInput(left, right);
        tasks.insert(tasks.end(),
                     make_move_iterator(chunks.begin()),
                     make_move_iterator(chunks.end()));
    };
    if (!fresh_left.empty() && !right_after.empty()) {
        append_split_tasks(fresh_left, right_after);
    }
    if (!old_left.empty() && !fresh_right.empty()) {
        append_split_tasks(old_left, fresh_right);
    }

    if (tasks.empty()) {
        IncrementalMatrixJoinResult result;
        result.query_id = update.query_id;
        result.token = update.token;
        result.parent_batch_id = update.parent_batch_id;
        result.middle_partial = true;
        result.middle_partition = update.middle_partition;
        result.destination_pid = update.middle_partition;
        IncrementalSendDestinationShardResult(
            state, update.parent_pid, std::move(result));
        return;
    }

    uint64_t delegated_token = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& query = qit->second;
        delegated_token = query.next_token++;
        uint64_t salt = delegated_token ^ update.parent_batch_id;
        for (auto& task : tasks) {
            task.compute_pid = IncrementalReserveMiddleComputeWorker(
                state, query, task.work, task.transfer_pairs, salt++,
                update.middle_allow_remote_compute,
                update.middle_preferred_compute_pid);
        }
        IncrementalDelegatedMiddleJoin context;
        context.block_key = key;
        context.original_parent_pid = update.parent_pid;
        context.original_parent_batch_id = update.parent_batch_id;
        context.original_token = update.token;
        context.middle_partition = update.middle_partition;
        context.pending_tasks = tasks.size();
        query.delegated_middle_joins.emplace(delegated_token, std::move(context));
        query.matrix_blocks[key].pending_middle_compute_tasks += tasks.size();
    }
    for (auto& task : tasks) {
        IncrementalMatrixJoinUpdate compute;
        compute.query_id = update.query_id;
        compute.parent_pid = state.partition_id;
        compute.parent_batch_id = update.parent_batch_id;
        compute.token = delegated_token;
        compute.middle_partition_join = true;
        compute.middle_compute_task = true;
        compute.middle_partition = update.middle_partition;
        compute.scheduled_join_work = task.work;
        compute.left_rows = std::move(task.left);
        compute.right_rows = std::move(task.right);
        if (task.compute_pid == state.partition_id) {
            IncrementalAcceptMatrixJoinUpdate(state, std::move(compute));
        } else {
            Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
            message.data = SerializeIncrementalMatrixJoinUpdate(compute);
            IncrementalEnqueuePeer(
                state, task.compute_pid, std::move(message),
                IncrementalWorkerState::PeerWireKind::LHS);
        }
    }
}
#endif

static RelationRows IncrementalSnapshotDirectRhsLocked(
    IncrementalWorkerState& state, IncrementalQueryState& query,
    const IncrementalMatrixBlockState& block) {
    if (block.right_parent_pid >= 0) return {};
    if (block.right_body_id >= 0 && block.right_length > 0) {
        auto resident = query.destination_relation_shards.find(
            IncrementalRelationShardKey{block.right_body_id, block.right_length});
        if (resident == query.destination_relation_shards.end()) return {};
        RelationRows right = IncrementalFilterRowsForSources(
            resident->second, block.right_sources);
        if (block.right_include_identity) {
            for (uint64_t source : block.right_sources) {
                if (IncrementalOwner(state, source) == state.partition_id) {
                    right[source].push_back(source);
                }
            }
        }
        return right;
    }
    return block.right_rows;
}

// Push a C delta through the normal DemandBatch callback path immediately.
// Completion remains a separate final-shard result, so downstream relation
// composition can begin without observing an incomplete relation as closed.
static void IncrementalPublishIncrementalGcnpDelta(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, RelationRows candidates) {
    if (candidates.empty()) return;
    RelationRows fresh;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end() || bit->second.finalized) return;
        fresh = IncrementalDifferenceRows(candidates, bit->second.output_rows);
        if (fresh.empty()) return;
        IncrementalMergeRows(bit->second.output_rows, fresh);
        for (const auto& [source, values] : fresh) {
            GcnpAddCoverageSignature(bit->second.output_signatures[source], values);
        }
        ++qit->second.matrix_join_updates;
        qit->second.matrix_join_result_pairs += CountRelationPairs(fresh);
    }
    IncrementalMatrixJoinResult delta;
    delta.query_id = query_id;
    delta.parent_batch_id = key.parent_batch_id;
    delta.direct_right_stream = true;
    delta.rows = std::move(fresh);
    IncrementalSendDestinationShardResult(state, key.parent_pid, std::move(delta));
}

static void IncrementalMaybeBuildIncrementalGcnpPlan(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key) {
    bool queue = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end()) return;
        auto& block = bit->second;
        // Coverage is source-specific and keeps the legacy final compose.
        // A parent-resident RHS is likewise immutable only at its own final
        // barrier, so the streamed plan applies to direct physical RHS only.
        if (!block.input_sealed || block.pending_input_updates != 0 ||
            block.finalized || block.incremental_gcnp_plan != nullptr ||
            block.incremental_gcnp_plan_queued || block.right_parent_pid >= 0 ||
            !block.coverage_rows.empty() || block.coverage_parent_pid >= 0 ||
            block.coverage_body_id >= 0 || block.coverage_include_identity ||
            block.left_rows.empty()) {
            return;
        }
        block.incremental_gcnp_plan_queued = true;
        queue = true;
    }
    if (!queue) return;
    IncrementalSubmitCompute(state, query_id, [&state, query_id, key]() {
        RelationRows left;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.matrix_blocks.find(key);
            if (bit == qit->second.matrix_blocks.end()) return;
            if (bit->second.finalized || !bit->second.input_sealed ||
                bit->second.pending_input_updates != 0) {
                bit->second.incremental_gcnp_plan_queued = false;
                return;
            }
            left = bit->second.left_rows;
        }
        GcnpExecutionStats stats;
        auto plan = IncrementalBuildGcnpDeltaPlan(left, &stats);
        RelationRows current_right;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.matrix_blocks.find(key);
            if (bit == qit->second.matrix_blocks.end()) return;
            auto& block = bit->second;
            block.incremental_gcnp_plan_queued = false;
            if (block.finalized || block.incremental_gcnp_plan != nullptr) return;
            block.incremental_gcnp_plan = plan;
            current_right = IncrementalSnapshotDirectRhsLocked(state, qit->second, block);
            ++qit->second.gcnp_metrics.batches;
            qit->second.gcnp_metrics.source_rows += left.size();
            qit->second.gcnp_complete_prepare_us += stats.prepare_us;
            qit->second.gcnp_complete_group_extract_us += stats.group_extract_us;
            qit->second.gcnp_complete_groups += stats.groups;
            qit->second.gcnp_complete_reusable_groups += stats.reusable_groups;
            qit->second.gcnp_complete_grouped_source_uses += stats.grouped_source_uses;
            qit->second.gcnp_complete_shared_source_uses += stats.shared_source_uses;
        }
        IncrementalPublishIncrementalGcnpDelta(
            state, query_id, key,
            IncrementalPrimeGcnpDeltaPlan(*plan, current_right));
        IncrementalMaybeFinalizeDestinationShard(state, query_id, key);
    });
}

static void IncrementalMaybeFinalizeDestinationShard(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key) {
    bool queue = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end()) return;
        auto& block = bit->second;
        if (!block.input_sealed || block.finalized || block.finalize_queued ||
            block.pending_input_updates != 0 ||
            block.completed_right_streams.size() < block.expected_right_streams ||
            block.completed_resident_left_streams.size() <
                block.expected_resident_left_streams) {
            return;
        }
        const bool can_stream_incrementally =
            block.right_parent_pid < 0 && !block.left_rows.empty() &&
            block.coverage_rows.empty() && block.coverage_parent_pid < 0 &&
            block.coverage_body_id < 0 && !block.coverage_include_identity;
        if (can_stream_incrementally &&
            (block.incremental_gcnp_plan_queued ||
             block.incremental_gcnp_plan == nullptr)) {
            return;
        }
        if (block.speculative_cancel != nullptr) {
            block.speculative_cancel->store(true, memory_order_relaxed);
        }
        if (!block.panel_wait_recorded && block.saw_left_input) {
            const auto now = chrono::steady_clock::now();
            if (block.left_panel_sealed_at == chrono::steady_clock::time_point{}) {
                block.left_panel_sealed_at = now;
            }
            const uint64_t panel_wait_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    block.left_panel_sealed_at - block.first_left_input_at).count());
            ++qit->second.panel_wait_blocks;
            qit->second.panel_left_input_batches += block.left_input_batches;
            if (qit->second.worker_query_started) {
                qit->second.all_left_panels_sealed_elapsed_us = max(
                    qit->second.all_left_panels_sealed_elapsed_us,
                    static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
                        block.left_panel_sealed_at -
                        qit->second.worker_query_started_at).count()));
            }
            qit->second.left_panel_wait_us += panel_wait_us;
            qit->second.left_panel_wait_us_max = max(
                qit->second.left_panel_wait_us_max, panel_wait_us);
            if (block.rhs_ready_at != chrono::steady_clock::time_point{}) {
                if (block.rhs_ready_at <= block.left_panel_sealed_at) {
                    const uint64_t rhs_wait_us = static_cast<uint64_t>(
                        chrono::duration_cast<chrono::microseconds>(
                            block.left_panel_sealed_at - block.rhs_ready_at).count());
                    ++qit->second.rhs_ready_before_left_blocks;
                    qit->second.rhs_ready_wait_for_left_us += rhs_wait_us;
                    qit->second.rhs_ready_wait_for_left_us_max = max(
                        qit->second.rhs_ready_wait_for_left_us_max, rhs_wait_us);
                } else {
                    const uint64_t lhs_wait_us = static_cast<uint64_t>(
                        chrono::duration_cast<chrono::microseconds>(
                            block.rhs_ready_at - block.left_panel_sealed_at).count());
                    ++qit->second.left_sealed_before_rhs_blocks;
                    qit->second.left_wait_for_rhs_us += lhs_wait_us;
                    qit->second.left_wait_for_rhs_us_max = max(
                        qit->second.left_wait_for_rhs_us_max, lhs_wait_us);
                }
            }
            block.panel_wait_recorded = true;
        }
        block.finalize_queued = true;
        queue = true;
    }
    if (!queue) return;

    IncrementalSubmitCompute(state, query_id, [&state, query_id, key]() {
        RelationRows left;
        RelationRows right;
        RelationRows coverage;
        unordered_map<uint64_t, GcnpCoverageSignature> coverage_signatures;
        shared_ptr<GcnpPatternTargetMemo> speculative_pattern_memo;
        shared_ptr<IncrementalGcnpDeltaPlan> incremental_gcnp_plan;
        bool retain_output_on_destination = false;
        uint64_t finalize_token = 0;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.matrix_blocks.find(key);
            if (bit == qit->second.matrix_blocks.end()) return;
            auto& block = bit->second;
            if (!block.input_sealed || block.finalized ||
                block.pending_input_updates != 0 ||
                block.completed_right_streams.size() < block.expected_right_streams ||
                block.completed_resident_left_streams.size() <
                    block.expected_resident_left_streams) {
                block.finalize_queued = false;
                return;
            }
            if (!block.resident_coverage_seeded && block.coverage_parent_pid >= 0) {
                auto resident_coverage = qit->second.matrix_blocks.find(
                    IncrementalMatrixBlockKey{
                        block.coverage_parent_pid,
                        block.coverage_parent_batch_id});
                if (resident_coverage != qit->second.matrix_blocks.end() &&
                    resident_coverage->second.finalized) {
                    block.coverage_rows = IncrementalFilterRowsForSources(
                        resident_coverage->second.output_rows,
                        block.coverage_sources);
                    for (const auto& [source, values] : block.coverage_rows) {
                        GcnpAddCoverageSignature(
                            block.coverage_signatures[source], values);
                    }
                    block.output_rows = block.coverage_rows;
                    block.output_signatures = block.coverage_signatures;
                }
                block.resident_coverage_seeded = true;
            }
            if (!block.resident_coverage_seeded && block.coverage_body_id >= 0) {
                auto resident_coverage = qit->second.destination_relation_shards.find(
                    IncrementalRelationShardKey{
                        block.coverage_body_id, block.coverage_length});
                if (resident_coverage !=
                    qit->second.destination_relation_shards.end()) {
                    block.coverage_rows = IncrementalFilterRowsForSources(
                        resident_coverage->second, block.coverage_sources);
                    for (const auto& [source, values] : block.coverage_rows) {
                        GcnpAddCoverageSignature(
                            block.coverage_signatures[source], values);
                    }
                    block.output_rows = block.coverage_rows;
                    block.output_signatures = block.coverage_signatures;
                }
                block.resident_coverage_seeded = true;
            }
            left = std::move(block.left_rows);
            if (block.right_parent_pid >= 0) {
                auto resident = qit->second.matrix_blocks.find(
                    IncrementalMatrixBlockKey{
                        block.right_parent_pid, block.right_parent_batch_id});
                if (resident != qit->second.matrix_blocks.end() &&
                    resident->second.finalized) {
                    right = IncrementalFilterRowsForSources(
                        resident->second.output_rows, block.right_sources);
                }
            } else if (block.right_body_id >= 0 && block.right_length > 0) {
                auto resident = qit->second.destination_relation_shards.find(
                    IncrementalRelationShardKey{block.right_body_id, block.right_length});
                if (resident != qit->second.destination_relation_shards.end()) {
                    right = IncrementalFilterRowsForSources(
                        resident->second, block.right_sources);
                }
                if (block.right_include_identity) {
                    for (uint64_t source : block.right_sources) {
                        if (IncrementalOwner(state, source) == state.partition_id) {
                            right[source].push_back(source);
                        }
                    }
                }
            } else {
                right = std::move(block.right_rows);
            }
            if (block.coverage_include_identity) {
                for (uint64_t source : block.coverage_sources) {
                    if (IncrementalOwner(state, source) == state.partition_id) {
                        block.coverage_rows[source].push_back(source);
                        block.output_rows[source].push_back(source);
                        RelationDstSet identity;
                        identity.push_back(source);
                        GcnpAddCoverageSignature(
                            block.coverage_signatures[source], identity);
                        GcnpAddCoverageSignature(
                            block.output_signatures[source], identity);
                    }
                }
            }
            coverage = block.coverage_rows;
            coverage_signatures = block.coverage_signatures;
            speculative_pattern_memo = block.speculative_pattern_memo;
            incremental_gcnp_plan = block.incremental_gcnp_plan;
            retain_output_on_destination = block.retain_output_on_destination;
            finalize_token = block.finalize_token;
        }

        const uint64_t input_mids = CountRelationPairs(left);
        const uint64_t actual_right_rows = right.size();
        const uint64_t actual_right_pairs = CountRelationPairs(right);
        const auto join_begin = chrono::steady_clock::now();
        // For a resident coverage handle the parent has no endpoint copy and
        // therefore cannot pre-filter. The destination owner has both S[a]
        // and S[b], so it applies source-aware full-row coverage immediately
        // before the single complete-shard GCNP plan.
        GcnpExecutionStats gcnp_execution;
        RelationRows candidates;
        if (incremental_gcnp_plan == nullptr) {
            candidates = coverage.empty()
                ? ComposeRelationRowsGcnp(
                    left, right, nullptr, nullptr, &gcnp_execution,
                    speculative_pattern_memo.get())
                : ComposeRelationRowsExcluding(
                    left, right, coverage, &coverage_signatures, &gcnp_execution);
        }
        const uint64_t join_us = static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - join_begin).count());

        RelationRows fresh_output;
        RelationDstSet output_sources;
        uint64_t output_row_count = 0;
        uint64_t output_pair_count = 0;
        uint64_t fresh_output_pair_count = 0;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.matrix_blocks.find(key);
            if (bit == qit->second.matrix_blocks.end()) return;
            auto& block = bit->second;
            // A complete destination panel runs exactly once.  With no
            // dual-state coverage, a retained result block is empty here, so
            // candidates are already the exact output.  Move them directly
            // into the resident shard instead of materializing a full
            // difference bitmap and then unioning it into another full copy.
            const bool can_adopt_candidates = coverage.empty() &&
                block.retain_output_on_destination && block.output_rows.empty();
            if (can_adopt_candidates) {
                block.output_rows = std::move(candidates);
                fresh_output_pair_count = CountRelationPairs(block.output_rows);
                for (const auto& [source, values] : block.output_rows) {
                    GcnpAddCoverageSignature(block.output_signatures[source], values);
                }
            } else {
                fresh_output = IncrementalDifferenceRows(candidates, block.output_rows);
                IncrementalMergeRows(block.output_rows, fresh_output);
                fresh_output_pair_count = CountRelationPairs(fresh_output);
                for (const auto& [source, values] : fresh_output) {
                    GcnpAddCoverageSignature(block.output_signatures[source], values);
                }
            }
            block.finalized = true;
            block.finalize_queued = false;
            output_row_count = block.output_rows.size();
            output_pair_count = CountRelationPairs(block.output_rows);
            for (const auto& [source, _] : block.output_rows) {
                output_sources.push_back(source);
            }
            ++qit->second.matrix_join_updates;
            qit->second.matrix_join_result_pairs += fresh_output_pair_count;
            qit->second.matrix_join_us += join_us;
            ++qit->second.gcnp_metrics.batches;
            qit->second.gcnp_metrics.source_rows += left.size();
            qit->second.gcnp_input_mids += input_mids;
            qit->second.gcnp_planned_mids += input_mids;
            qit->second.gcnp_wall_us += join_us;
            qit->second.gcnp_complete_prepare_us += gcnp_execution.prepare_us;
            qit->second.gcnp_complete_group_extract_us +=
                gcnp_execution.group_extract_us;
            qit->second.gcnp_complete_pattern_cache_us +=
                gcnp_execution.pattern_cache_us;
            qit->second.gcnp_complete_union_us += gcnp_execution.union_us;
            qit->second.gcnp_complete_shared_target_us +=
                gcnp_execution.shared_target_us;
            qit->second.gcnp_complete_source_materialize_us +=
                gcnp_execution.source_materialize_us;
            qit->second.gcnp_complete_groups += gcnp_execution.groups;
            qit->second.gcnp_complete_reusable_groups +=
                gcnp_execution.reusable_groups;
            qit->second.gcnp_complete_grouped_source_uses +=
                gcnp_execution.grouped_source_uses;
            qit->second.gcnp_complete_shared_source_uses +=
                gcnp_execution.shared_source_uses;
            qit->second.gcnp_complete_external_memo_hits +=
                gcnp_execution.external_memo_hits;
            if (block.expected_right_rows != 0 || block.expected_right_pairs != 0) {
                qit->second.resident_rhs_expected_rows += block.expected_right_rows;
                qit->second.resident_rhs_actual_rows += actual_right_rows;
                qit->second.resident_rhs_expected_pairs += block.expected_right_pairs;
                qit->second.resident_rhs_actual_pairs += actual_right_pairs;
                if (block.expected_right_rows != actual_right_rows ||
                    block.expected_right_pairs != actual_right_pairs) {
                    ++qit->second.resident_rhs_mismatched_shards;
                    cerr << "[bounded-resident-rhs-mismatch] worker="
                         << state.partition_id
                         << " parent_pid=" << key.parent_pid
                         << " parent_batch=" << key.parent_batch_id
                         << " right_parent_pid=" << block.right_parent_pid
                         << " right_parent_batch=" << block.right_parent_batch_id
                         << " right_body=" << block.right_body_id
                         << " right_length=" << block.right_length
                         << " expected_rows=" << block.expected_right_rows
                         << " actual_rows=" << actual_right_rows
                         << " expected_pairs=" << block.expected_right_pairs
                         << " actual_pairs=" << actual_right_pairs << endl;
                }
            }
        }

        IncrementalMatrixJoinResult result;
        result.query_id = query_id;
        result.token = finalize_token;
        result.parent_batch_id = key.parent_batch_id;
        result.destination_pid = state.partition_id;
        result.output_sources = std::move(output_sources);
        result.output_rows = output_row_count;
        result.output_pairs = output_pair_count;
        // A retained block returns only its compact directory. Intermediate
        // endpoints never travel back to the parent/source owner.
        if (!retain_output_on_destination) {
            result.rows = std::move(fresh_output);
        }
        IncrementalSendDestinationShardResult(
            state, key.parent_pid, std::move(result));
    });
}


static void IncrementalProcessMatrixJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    if (update.cleanup_only) {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit != state.queries.end()) {
            qit->second.matrix_blocks.erase(
                IncrementalMatrixBlockKey{update.parent_pid, update.parent_batch_id});
        }
        return;
    }
    {
    if (update.left_rows_transposed) {
        RelationRows expanded;
        for (const auto& [mid, sources] : update.left_rows) {
            for (uint64_t source : sources) expanded[source].push_back(mid);
        }
        update.left_rows = std::move(expanded);
    }
    for (const auto& [mid, sources] : update.transposed_left_rows) {
        for (uint64_t source : sources) update.left_rows[source].push_back(mid);
    }
    update.transposed_left_rows.clear();

    const IncrementalMatrixBlockKey key{
        update.parent_pid, update.parent_batch_id};
    shared_ptr<IncrementalGcnpDeltaPlan> incremental_plan;
    RelationRows fresh_rhs_delta;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[key];
        block.retain_output_on_destination =
            block.retain_output_on_destination || update.retain_output_on_destination;
        block.return_output_to_parent =
            block.return_output_to_parent || update.return_output_to_parent;
        block.coverage_include_identity =
            block.coverage_include_identity || update.coverage_include_identity;
        if (update.coverage_parent_pid >= 0) {
            block.coverage_parent_pid = update.coverage_parent_pid;
            block.coverage_parent_batch_id = update.coverage_parent_batch_id;
        }
        if (update.coverage_body_id >= 0) {
            block.coverage_body_id = update.coverage_body_id;
            block.coverage_length = update.coverage_length;
        }
        block.coverage_sources.bitmap |= update.coverage_sources.bitmap;
        if (update.right_body_id >= 0 && update.right_length > 0) {
            block.right_body_id = update.right_body_id;
            block.right_length = update.right_length;
            block.right_include_identity =
                block.right_include_identity || update.right_include_identity;
        }
        if (update.right_parent_pid >= 0) {
            block.right_parent_pid = update.right_parent_pid;
            block.right_parent_batch_id = update.right_parent_batch_id;
        }
        block.expected_right_rows = max(
            block.expected_right_rows, update.expected_right_rows);
        block.expected_right_pairs = max(
            block.expected_right_pairs, update.expected_right_pairs);
        block.right_sources.bitmap |= update.right_sources.bitmap;
        IncrementalMergeRows(block.left_rows, update.left_rows);
        if (update.right_body_id >= 0 && update.right_length > 0) {
            auto& resident = qit->second.destination_relation_shards[
                IncrementalRelationShardKey{
                    update.right_body_id, update.right_length}];
            fresh_rhs_delta = IncrementalDifferenceRows(update.right_rows, resident);
            IncrementalMergeRows(resident, fresh_rhs_delta);
        } else {
            fresh_rhs_delta = IncrementalDifferenceRows(update.right_rows, block.right_rows);
            IncrementalMergeRows(block.right_rows, fresh_rhs_delta);
        }
        if (!update.coverage_rows.empty()) {
            RelationRows fresh_coverage = IncrementalDifferenceRows(
                update.coverage_rows, block.coverage_rows);
            IncrementalMergeRows(block.coverage_rows, fresh_coverage);
            for (const auto& [source, values] : fresh_coverage) {
                GcnpAddCoverageSignature(block.coverage_signatures[source], values);
            }
            if (update.retain_output_on_destination &&
                update.return_output_to_parent) {
                IncrementalMergeRows(block.output_rows, fresh_coverage);
                for (const auto& [source, values] : fresh_coverage) {
                    GcnpAddCoverageSignature(
                        block.output_signatures[source], values);
                }
            }
            block.resident_coverage_seeded = true;
        }
        // Resident coverage is projected at finalization, after the seal has
        // supplied the complete source bitmap for this suffix instance.
        if (block.pending_input_updates > 0) --block.pending_input_updates;
        ++block.speculative_generation;
        incremental_plan = block.incremental_gcnp_plan;
    }

    if (incremental_plan != nullptr && !fresh_rhs_delta.empty()) {
        IncrementalPublishIncrementalGcnpDelta(
            state, update.query_id, key,
            IncrementalApplyGcnpDeltaPlan(*incremental_plan, fresh_rhs_delta));
    }

    if (update.direct_right_stream) {
        IncrementalDirectMatrixTaskFinished(
            state, update.query_id, update.parent_pid,
            update.parent_batch_id, update.token);
    } else {
        IncrementalMatrixJoinResult ack;
        ack.query_id = update.query_id;
        ack.token = update.token;
        ack.parent_batch_id = update.parent_batch_id;
        IncrementalSendDestinationShardResult(
            state, update.parent_pid, std::move(ack));
    }
    IncrementalMaybeBuildIncrementalGcnpPlan(state, update.query_id, key);
    IncrementalMaybeFinalizeDestinationShard(state, update.query_id, key);
    return;
    }

#if 0
    // Legacy delta-at-a-time matrix kernel. Kept disabled in this experimental
    // branch so the complete-shard GCNP path above is the sole executor.
    // A route flush can carry fresh source -> mid rows together with a
    // compact signature replay encoded as mid -> sources.  Expand the latter
    // at its destination and join their union in one matrix task.  This keeps
    // the compressed replay wire format while avoiding a second task/result
    // round trip for the same C shard.
    if (update.left_rows_transposed) {
        RelationRows rows;
        rows.reserve(update.left_rows.size());
        for (const auto& [mid, sources] : update.left_rows) {
            for (uint64_t source : sources) rows[source].push_back(mid);
        }
        update.left_rows = std::move(rows);
    }
    if (!update.transposed_left_rows.empty()) {
        for (const auto& [mid, sources] : update.transposed_left_rows) {
            for (uint64_t source : sources) update.left_rows[source].push_back(mid);
        }
        update.transposed_left_rows.clear();
    }
    RelationRows fresh_left;
    RelationRows fresh_right;
    RelationRows old_left;
    RelationRows right_with_fresh;
    RelationRows coverage_rows;
    unordered_map<uint64_t, GcnpCoverageSignature> coverage_signatures;
    struct ResidentWakeup {
        IncrementalMatrixBlockKey key;
        RelationRows left_rows;
        bool retain_output_on_destination = false;
        bool return_output_to_parent = false;
    };
    vector<ResidentWakeup> resident_wakeups;
    IncrementalMatrixBlockKey key{update.parent_pid, update.parent_batch_id};
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[key];
        block.retain_output_on_destination =
            block.retain_output_on_destination || update.retain_output_on_destination;
        block.return_output_to_parent =
            block.return_output_to_parent || update.return_output_to_parent;
        if (update.right_body_id >= 0 && update.right_length > 0) {
            block.right_body_id = update.right_body_id;
            block.right_length = update.right_length;
        }
        fresh_left = IncrementalDifferenceRows(update.left_rows, block.left_rows);
        old_left = block.left_rows;
        IncrementalMergeRows(block.left_rows, fresh_left);
        if (update.right_body_id >= 0 && update.right_length > 0) {
            auto& resident = qit->second.destination_relation_shards[
                IncrementalRelationShardKey{update.right_body_id, update.right_length}];
            fresh_right = IncrementalDifferenceRows(update.right_rows, resident);
            IncrementalMergeRows(resident, fresh_right);
            right_with_fresh = resident;
            // A resident RHS can be installed after another parent block has
            // already received its RSFM-selected lhs. Wake all such blocks on
            // this RHS delta; otherwise a payload-suppressed replay could
            // leave that earlier lhs permanently unmatched.
            if (!fresh_right.empty()) {
                for (const auto& [other_key, other] : qit->second.matrix_blocks) {
                    if ((other_key == key) || other.right_body_id != update.right_body_id ||
                        other.right_length != update.right_length || other.left_rows.empty()) {
                        continue;
                    }
                    resident_wakeups.push_back(ResidentWakeup{
                        other_key, other.left_rows, other.retain_output_on_destination,
                        other.return_output_to_parent});
                }
            }
        } else {
            fresh_right = IncrementalDifferenceRows(update.right_rows, block.right_rows);
            right_with_fresh = block.right_rows;
            IncrementalMergeRows(right_with_fresh, fresh_right);
            IncrementalMergeRows(block.right_rows, fresh_right);
        }
        if (!update.coverage_rows.empty()) {
            RelationRows fresh_coverage = IncrementalDifferenceRows(
                update.coverage_rows, block.coverage_rows);
            if (!fresh_coverage.empty()) {
                qit->second.range_coverage_pairs += CountRelationPairs(fresh_coverage);
                IncrementalMergeRows(block.coverage_rows, fresh_coverage);
                for (const auto& [source, values] : fresh_coverage) {
                    GcnpAddCoverageSignature(block.coverage_signatures[source], values);
                }
                if (update.retain_output_on_destination &&
                    update.return_output_to_parent) {
                    IncrementalMergeRows(block.output_rows, fresh_coverage);
                    for (const auto& [source, values] : fresh_coverage) {
                        GcnpAddCoverageSignature(block.output_signatures[source], values);
                    }
                }
            }
        }
        if (update.coverage_parent_pid >= 0 && !block.resident_coverage_seeded) {
            const IncrementalMatrixBlockKey coverage_key{
                update.coverage_parent_pid, update.coverage_parent_batch_id};
            auto coverage_block = qit->second.matrix_blocks.find(coverage_key);
            if (coverage_block != qit->second.matrix_blocks.end()) {
                block.coverage_rows = coverage_block->second.output_rows;
                block.coverage_signatures = coverage_block->second.output_signatures;
                // The retained result represents S[a+b], not merely the
                // shifted branch. Seed it with S[a] before adding fresh join
                // output, while replies still contain shifted deltas only.
                block.output_rows = coverage_block->second.output_rows;
                block.output_signatures = coverage_block->second.output_signatures;
            }
            block.resident_coverage_seeded = true;
        }
        if (update.coverage_body_id >= 0 && !block.resident_coverage_seeded) {
            const IncrementalRelationShardKey coverage_key{
                update.coverage_body_id, update.coverage_length};
            auto coverage_shard = qit->second.destination_relation_shards.find(coverage_key);
            if (coverage_shard != qit->second.destination_relation_shards.end()) {
                block.coverage_rows = coverage_shard->second;
            }
            if (update.coverage_include_identity) {
                for (const auto& [source, _] : block.left_rows) {
                    if (IncrementalOwner(state, source) == state.partition_id) {
                        block.coverage_rows[source].push_back(source);
                    }
                }
            }
            for (const auto& [source, values] : block.coverage_rows) {
                GcnpAddCoverageSignature(block.coverage_signatures[source], values);
            }
            block.output_rows = block.coverage_rows;
            block.output_signatures = block.coverage_signatures;
            block.resident_coverage_seeded = true;
        }
        // The coverage is fixed for this generic composition. Copy it before
        // releasing state.mu so the GCNP kernel sees a stable filter.
        coverage_rows = block.coverage_rows;
        coverage_signatures = block.coverage_signatures;
    }

    const auto join_begin = chrono::steady_clock::now();
    RelationRows candidates;
    if (!fresh_left.empty() && !right_with_fresh.empty()) {
        candidates = coverage_rows.empty()
            ? ComposeRelationRows(fresh_left, right_with_fresh)
            : ComposeRelationRowsExcluding(
                fresh_left, right_with_fresh, coverage_rows, &coverage_signatures);
    }
    if (!old_left.empty() && !fresh_right.empty()) {
        RelationRows right_delta_candidates = coverage_rows.empty()
            ? ComposeRelationRows(old_left, fresh_right)
            : ComposeRelationRowsExcluding(
                old_left, fresh_right, coverage_rows, &coverage_signatures);
        IncrementalMergeRows(candidates, right_delta_candidates);
    }
    const uint64_t join_us = static_cast<uint64_t>(chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - join_begin).count());

    RelationRows fresh_output;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end()) return;
        fresh_output = IncrementalDifferenceRows(candidates, bit->second.output_rows);
        IncrementalMergeRows(bit->second.output_rows, fresh_output);
        for (const auto& [source, values] : fresh_output) {
            GcnpAddCoverageSignature(bit->second.output_signatures[source], values);
        }
        ++qit->second.matrix_join_updates;
        qit->second.matrix_join_result_pairs += CountRelationPairs(fresh_output);
        qit->second.matrix_join_us += join_us;
    }

    IncrementalMatrixJoinResult result;
    result.query_id = update.query_id;
    result.token = update.token;
    result.parent_batch_id = update.parent_batch_id;
    result.direct_right_stream = update.direct_right_stream;
    // The root's C blocks are already on their result owners.  An empty
    // response is still an acknowledgement for the parent completion barrier.
    if (!update.retain_output_on_destination || update.return_output_to_parent) {
        result.rows = std::move(fresh_output);
    }
    if (update.parent_pid == state.partition_id) {
        // Matrix blocks are independent compute work.  Complete their parent
        // token on the ordered executor so demand DELTA/COMPLETE callbacks
        // keep the same ordering guarantees as the original evaluator.
        state.executor.Submit([&state, result = std::move(result)]() mutable {
            IncrementalHandleMatrixJoinResult(state, std::move(result));
        });
    } else {
        Message message(MessageType::INCREMENTAL_MATRIX_JOIN_RESULT);
        message.data = SerializeIncrementalMatrixJoinResult(result);
        IncrementalEnqueuePeer(state, update.parent_pid, std::move(message));
    }

    // Each wakeup belongs to a different parent C block, so its output
    // deduplication and completion route are independent of the stream that
    // happened to install the physical RHS payload.
    for (const auto& wakeup : resident_wakeups) {
        RelationRows wakeup_candidates = ComposeRelationRows(wakeup.left_rows, fresh_right);
        RelationRows wakeup_output;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(update.query_id);
            if (qit == state.queries.end()) continue;
            auto bit = qit->second.matrix_blocks.find(wakeup.key);
            if (bit == qit->second.matrix_blocks.end()) continue;
            wakeup_output = IncrementalDifferenceRows(wakeup_candidates, bit->second.output_rows);
            IncrementalMergeRows(bit->second.output_rows, wakeup_output);
            ++qit->second.matrix_join_updates;
            qit->second.matrix_join_result_pairs += CountRelationPairs(wakeup_output);
        }
        IncrementalMatrixJoinResult wakeup_result;
        wakeup_result.query_id = update.query_id;
        wakeup_result.parent_batch_id = wakeup.key.parent_batch_id;
        // This is a resident-RHS delta notification, not an acknowledgement
        // of a specific left-panel token.
        wakeup_result.direct_right_stream = true;
        if (!wakeup.retain_output_on_destination || wakeup.return_output_to_parent) {
            wakeup_result.rows = std::move(wakeup_output);
        }
        if (wakeup.key.parent_pid == state.partition_id) {
            IncrementalHandleMatrixJoinResult(state, std::move(wakeup_result));
        } else {
            Message message(MessageType::INCREMENTAL_MATRIX_JOIN_RESULT);
            message.data = SerializeIncrementalMatrixJoinResult(wakeup_result);
            IncrementalEnqueuePeer(state, wakeup.key.parent_pid, std::move(message));
        }
    }
    if (update.direct_right_stream) {
        IncrementalDirectMatrixTaskFinished(
            state, update.query_id, update.parent_pid,
            update.parent_batch_id, update.token);
    }
}
#endif
}

static void IncrementalDispatchMatrixJoin(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id, int destination_pid,
    RelationRows left_rows, RelationRows right_rows, bool retain_output_on_destination,
    bool left_rows_transposed, int right_body_id, int right_length,
    RelationRows transposed_left_rows, RelationRows coverage_rows,
    int coverage_parent_pid, uint64_t coverage_parent_batch_id,
    int coverage_body_id, int coverage_length, bool coverage_include_identity,
    bool return_output_to_parent, uint64_t expected_right_rows,
    uint64_t expected_right_pairs, bool right_include_identity) {
    if (left_rows.empty() && transposed_left_rows.empty() && right_rows.empty() &&
        coverage_rows.empty()) return;
    uint64_t token = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        token = qit->second.next_token++;
        qit->second.matrix_join_tokens.emplace(token, batch_id);
        ++bit->second.pending_matrix_joins;
    }
    IncrementalMatrixJoinUpdate update;
    update.query_id = query_id;
    update.parent_pid = state.partition_id;
    update.parent_batch_id = batch_id;
    update.token = token;
    update.retain_output_on_destination = retain_output_on_destination;
    update.return_output_to_parent = return_output_to_parent;
    update.left_rows_transposed = left_rows_transposed;
    update.right_body_id = right_body_id;
    update.right_length = right_length;
    update.right_include_identity = right_include_identity;
    update.expected_right_rows = expected_right_rows;
    update.expected_right_pairs = expected_right_pairs;
    update.coverage_parent_pid = coverage_parent_pid;
    update.coverage_parent_batch_id = coverage_parent_batch_id;
    update.coverage_body_id = coverage_body_id;
    update.coverage_length = coverage_length;
    update.coverage_include_identity = coverage_include_identity;
    update.left_rows = std::move(left_rows);
    update.transposed_left_rows = std::move(transposed_left_rows);
    update.right_rows = std::move(right_rows);
    update.coverage_rows = std::move(coverage_rows);
    if (destination_pid == state.partition_id) {
        IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
    } else {
        Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
        message.data = SerializeIncrementalMatrixJoinUpdate(update);
        IncrementalEnqueuePeer(state, destination_pid, std::move(message));
    }
}

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static void IncrementalDispatchMiddlePartitionJoin(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    int middle_pid, RelationRows left_rows, RelationRows right_rows,
    bool allow_remote_compute, int preferred_compute_pid,
    RelationRows coverage_rows = {}) {
    if (left_rows.empty() && right_rows.empty() && coverage_rows.empty()) return;
    uint64_t token = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        token = qit->second.next_token++;
        qit->second.matrix_join_tokens.emplace(token, batch_id);
        ++bit->second.pending_matrix_joins;
    }
    IncrementalMatrixJoinUpdate update;
    update.query_id = query_id;
    update.parent_pid = state.partition_id;
    update.parent_batch_id = batch_id;
    update.token = token;
    update.middle_partition_join = true;
    update.middle_partition = middle_pid;
    update.middle_allow_remote_compute = allow_remote_compute;
    update.middle_preferred_compute_pid = preferred_compute_pid;
    update.left_rows = std::move(left_rows);
    update.right_rows = std::move(right_rows);
    update.coverage_rows = std::move(coverage_rows);
    if (middle_pid == state.partition_id) {
        IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
    } else {
        Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
        message.data = SerializeIncrementalMatrixJoinUpdate(update);
        IncrementalEnqueuePeer(
            state, middle_pid, std::move(message),
            IncrementalWorkerState::PeerWireKind::LHS);
    }
}
#endif

static void IncrementalDispatchDestinationShardSeals(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    uint64_t expected_right_streams, bool left_only) {
    vector<IncrementalMatrixJoinUpdate> seals;
    seals.reserve(static_cast<size_t>(state.total_partitions));
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        for (int destination_pid = 0; destination_pid < state.total_partitions;
             ++destination_pid) {
            const uint64_t token = qit->second.next_token++;
            qit->second.matrix_join_tokens.emplace(token, batch_id);
            ++bit->second.pending_matrix_joins;
            IncrementalMatrixJoinUpdate seal;
            seal.query_id = query_id;
            seal.parent_pid = state.partition_id;
            seal.parent_batch_id = batch_id;
            seal.token = token;
            seal.finalize_shard = true;
            seal.expected_right_streams = expected_right_streams;
            seal.retain_output_on_destination = bit->second.retain_output_on_destination;
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
            seal.middle_partition_join = true;
            seal.middle_left_complete = true;
            seal.middle_stream_complete = !left_only;
            seal.middle_partition = destination_pid;
            seal.finalize_shard = false;
#endif
            seal.return_output_to_parent = bit->second.return_output_to_parent;
            if (bit->second.generic_matrix_compose) {
                seal.right_parent_pid = bit->second.matrix_right_parent_pid;
                seal.right_parent_batch_id = bit->second.matrix_right_parent_batch_id;
                auto right_sources =
                    bit->second.matrix_right_sources_by_destination.find(
                        destination_pid);
                if (right_sources !=
                    bit->second.matrix_right_sources_by_destination.end()) {
                    seal.right_sources = right_sources->second;
                }
                auto right_rows =
                    bit->second.matrix_right_rows_by_destination.find(
                        destination_pid);
                if (right_rows !=
                    bit->second.matrix_right_rows_by_destination.end()) {
                    seal.expected_right_rows = right_rows->second;
                }
                auto right_pairs =
                    bit->second.matrix_right_pairs_by_destination.find(
                        destination_pid);
                if (right_pairs !=
                    bit->second.matrix_right_pairs_by_destination.end()) {
                    seal.expected_right_pairs = right_pairs->second;
                }
                seal.right_body_id = bit->second.matrix_right_body_id;
                seal.right_length = bit->second.matrix_right_length;
                seal.right_include_identity =
                    bit->second.matrix_right_include_identity;
                seal.coverage_parent_pid = bit->second.matrix_coverage_parent_pid;
                seal.coverage_parent_batch_id = bit->second.matrix_coverage_parent_batch_id;
                seal.coverage_body_id = bit->second.matrix_coverage_body_id;
                seal.coverage_length = bit->second.matrix_coverage_length;
                seal.coverage_include_identity = bit->second.matrix_coverage_include_identity;
                auto coverage_sources =
                    bit->second.matrix_coverage_sources_by_destination.find(
                        destination_pid);
                if (coverage_sources !=
                    bit->second.matrix_coverage_sources_by_destination.end()) {
                    seal.coverage_sources = coverage_sources->second;
                }
            } else {
                seal.right_body_id = bit->second.body_id;
                seal.right_length =
                    IncrementalExactBinarySplit(bit->second.length).second;
            }
            seals.push_back(std::move(seal));
        }
    }
    for (int destination_pid = 0; destination_pid < state.total_partitions;
         ++destination_pid) {
        auto seal = std::move(seals[static_cast<size_t>(destination_pid)]);
        if (destination_pid == state.partition_id) {
            IncrementalAcceptMatrixJoinUpdate(state, std::move(seal));
        } else {
            Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
            message.data = SerializeIncrementalMatrixJoinUpdate(seal);
            IncrementalEnqueuePeer(
                state, destination_pid, std::move(message),
                IncrementalWorkerState::PeerWireKind::LHS);
        }
    }
}

static void IncrementalMaybeDispatchDestinationShardSeals(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    bool dispatch_left_seal = false;
    bool dispatch_final_seal = false;
    uint64_t expected_right_streams = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
        const bool input_closed = batch.left_complete && !batch.matrix_dispatching &&
            batch.pending_right_batches == 0 &&
            batch.pending_direct_right_streams == 0;
        if (batch.left_complete && !batch.middle_left_seals_dispatched &&
            !batch.matrix_dispatching &&
            batch.pending_matrix_route_flushes == 0) {
            batch.middle_left_seals_dispatched = true;
            dispatch_left_seal = true;
        }
#else
        const bool input_closed = batch.generic_matrix_compose
            ? batch.left_complete && !batch.matrix_dispatching
            : batch.left_complete && batch.rsfm_signatures_final;
#endif
        if (!batch.matrix_seals_dispatched && input_closed &&
            !batch.rsfm_route_flush_queued &&
            batch.pending_matrix_route_flushes == 0) {
            batch.matrix_seals_dispatched = true;
            expected_right_streams = batch.direct_right_streams_started;
            dispatch_final_seal = true;
        }
    }
    if (dispatch_left_seal) {
        IncrementalDispatchDestinationShardSeals(
            state, query_id, batch_id, 0, true);
    }
    if (dispatch_final_seal) {
        IncrementalDispatchDestinationShardSeals(
            state, query_id, batch_id, expected_right_streams);
    }
}

static void IncrementalCleanupGenericMatrixBlocks(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    for (int destination_pid = 0; destination_pid < state.total_partitions; ++destination_pid) {
        IncrementalMatrixJoinUpdate cleanup;
        cleanup.query_id = query_id;
        cleanup.parent_pid = state.partition_id;
        cleanup.parent_batch_id = batch_id;
        cleanup.cleanup_only = true;
        if (destination_pid == state.partition_id) {
            IncrementalProcessMatrixJoinUpdate(state, std::move(cleanup));
        } else {
            Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
            message.data = SerializeIncrementalMatrixJoinUpdate(cleanup);
            IncrementalEnqueuePeer(state, destination_pid, std::move(message));
        }
    }
}

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static void IncrementalHandleDelegatedMiddleResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result) {
    IncrementalDelegatedMiddleJoin completed;
    RelationRows fresh_output;
    bool ready = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(result.query_id);
        if (qit == state.queries.end()) return;
        auto& query = qit->second;
        auto context_it = query.delegated_middle_joins.find(result.token);
        if (context_it == query.delegated_middle_joins.end()) return;
        auto& context = context_it->second;
        IncrementalMergeRows(context.candidates, result.rows);
        if (context.pending_tasks > 0) --context.pending_tasks;
        auto block_it = query.matrix_blocks.find(context.block_key);
        if (block_it != query.matrix_blocks.end() &&
            block_it->second.pending_middle_compute_tasks > 0) {
            --block_it->second.pending_middle_compute_tasks;
        }
        if (result.destination_pid >= 0 &&
            result.destination_pid < state.total_partitions) {
            auto& outstanding = query.middle_scheduler_outstanding_work;
            const size_t index = static_cast<size_t>(result.destination_pid);
            if (index < outstanding.size()) {
                outstanding[index] = outstanding[index] > result.scheduled_join_work
                    ? outstanding[index] - result.scheduled_join_work : 0;
            }
        }
        query.middle_scheduler_completed_work += result.scheduled_join_work;
        if (context.pending_tasks == 0) {
            completed = std::move(context);
            auto& block = query.matrix_blocks[completed.block_key];
            fresh_output = IncrementalDifferenceRows(
                completed.candidates, block.output_rows);
            IncrementalMergeRows(block.output_rows, fresh_output);
            ++query.matrix_join_updates;
            query.matrix_join_result_pairs += CountRelationPairs(fresh_output);
            query.delegated_middle_joins.erase(context_it);
            ready = true;
        }
    }
    if (!ready) return;
    IncrementalMatrixJoinResult parent_result;
    parent_result.query_id = result.query_id;
    parent_result.token = completed.original_token;
    parent_result.parent_batch_id = completed.original_parent_batch_id;
    parent_result.middle_partial = true;
    parent_result.middle_partition = completed.middle_partition;
    parent_result.destination_pid = completed.middle_partition;
    parent_result.rows = std::move(fresh_output);
    IncrementalSendDestinationShardResult(
        state, completed.original_parent_pid, std::move(parent_result));
}
#endif

static void IncrementalHandleMatrixJoinResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result) {
    if (result.global_exact_stage) {
        IncrementalHandleGlobalExactStageResult(state, std::move(result));
        return;
    }
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    if (result.middle_compute_result) {
        IncrementalHandleDelegatedMiddleResult(state, std::move(result));
        return;
    }
    if (result.middle_partial) {
        RelationRows candidates;
        bool found = false;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(result.query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.demand_batches.find(result.parent_batch_id);
            if (bit == qit->second.demand_batches.end()) return;
            auto& batch = bit->second;
            candidates = std::move(result.rows);
            if (!batch.matrix_coverage_rows.empty()) {
                candidates = IncrementalDifferenceRows(
                    candidates, batch.matrix_coverage_rows);
            }
            if (result.middle_complete && result.middle_partition >= 0 &&
                batch.middle_completed_partitions.insert(result.middle_partition).second) {
                // The seal has its own matrix token. Its completion is
                // accounted for below, so this set is only the per-middle
                // completion ledger.
            }
            auto token_it = qit->second.matrix_join_tokens.find(result.token);
            if (token_it != qit->second.matrix_join_tokens.end()) {
                qit->second.matrix_join_tokens.erase(token_it);
                if (batch.pending_matrix_joins > 0) --batch.pending_matrix_joins;
            }
            found = true;
        }
        if (found && !candidates.empty()) {
            // IncrementalDemandEmit performs the single parent-side union and
            // suppresses duplicates produced by different middle partitions.
            IncrementalDemandEmit(state, result.query_id,
                                  result.parent_batch_id, candidates);
        }
        IncrementalDemandMaybeComplete(
            state, result.query_id, result.parent_batch_id);
        return;
    }
#endif
    if (result.direct_right_stream) {
        IncrementalDemandEmit(state, result.query_id, result.parent_batch_id, result.rows);
        return;
    }
    uint64_t batch_id = 0;
    bool found = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(result.query_id);
        if (qit == state.queries.end()) return;
        auto token_it = qit->second.matrix_join_tokens.find(result.token);
        if (token_it == qit->second.matrix_join_tokens.end()) {
            // Resident continuation panel updates use token zero and their
            // acknowledgements are intentionally not part of the parent
            // completion count.  Only a finalized C shard has a destination
            // directory and is counted exactly once here.
            auto bit = qit->second.demand_batches.find(result.parent_batch_id);
            if (bit == qit->second.demand_batches.end() ||
                !bit->second.resident_continuation_compose ||
                result.destination_pid < 0 ||
                !bit->second.resident_finalized_destinations.insert(
                    result.destination_pid).second) {
                return;
            }
            batch_id = result.parent_batch_id;
        } else {
            batch_id = token_it->second;
            qit->second.matrix_join_tokens.erase(token_it);
        }
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        if (bit->second.matrix_output_directory && result.destination_pid >= 0 &&
            result.destination_pid < state.total_partitions) {
            auto& directory = *bit->second.matrix_output_directory;
            const auto index = static_cast<size_t>(result.destination_pid);
            if (directory.sources_by_destination.size() <
                static_cast<size_t>(state.total_partitions)) {
                directory.sources_by_destination.resize(
                    static_cast<size_t>(state.total_partitions));
                directory.rows_by_destination.resize(
                    static_cast<size_t>(state.total_partitions));
                directory.pairs_by_destination.resize(
                    static_cast<size_t>(state.total_partitions));
            }
            directory.sources_by_destination[index] =
                std::move(result.output_sources);
            directory.rows_by_destination[index] = result.output_rows;
            directory.pairs_by_destination[index] = result.output_pairs;
        }
        if (bit->second.pending_matrix_joins > 0) --bit->second.pending_matrix_joins;
        found = true;
    }
    if (!found) return;
    IncrementalDemandEmit(state, result.query_id, batch_id, result.rows);
    IncrementalDemandMaybeComplete(state, result.query_id, batch_id);
}

// The old matrix experiment waited until a batch had received every A and B
// row, then materialized one source-owner join.  That reintroduced a binary
// barrier.  Here each arriving relation delta is sent immediately to the
// destination owner of its output block.  Crucially, an update carries only
// its new delta: every output owner retains its own A[i,j] and B[j,l] blocks,
// and IncrementalProcessMatrixJoinUpdate joins the fresh side with the
// locally retained opposite side.  Sending a complete opposite relation with
// every delta made the previous attempt repeatedly serialize and recompute
// the same blocks.
static void IncrementalDemandDispatchMatrixDelta(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    const RelationRows& left_delta, const RelationRows& right_delta) {
    if (left_delta.empty() && right_delta.empty()) return;

    bool generic_matrix_compose = false;
    bool retain_output_on_destination = false;
    int generic_right_parent_pid = -1;
    uint64_t generic_right_parent_batch_id = 0;
    int generic_right_body_id = -1;
    int generic_right_length = 0;
    shared_ptr<unordered_map<int, RelationRows>> generic_coverage_blocks;
    shared_ptr<unordered_map<int, unordered_map<uint64_t, GcnpCoverageSignature>>>
        generic_coverage_signatures;
    int generic_coverage_parent_pid = -1;
    uint64_t generic_coverage_parent_batch_id = 0;
    int generic_coverage_body_id = -1;
    int generic_coverage_length = 0;
    bool generic_coverage_include_identity = false;
    bool return_output_to_parent = false;
    unordered_map<int, RelationDstSet> generic_right_sources_by_destination;
    unordered_map<int, uint64_t> generic_right_rows_by_destination;
    unordered_map<int, uint64_t> generic_right_pairs_by_destination;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        generic_matrix_compose = bit->second.generic_matrix_compose;
        retain_output_on_destination = bit->second.retain_output_on_destination;
        generic_right_parent_pid = bit->second.matrix_right_parent_pid;
        generic_right_parent_batch_id = bit->second.matrix_right_parent_batch_id;
        generic_right_body_id = bit->second.matrix_right_body_id;
        generic_right_length = bit->second.matrix_right_length;
        generic_coverage_blocks = bit->second.matrix_coverage_blocks;
        generic_coverage_signatures = bit->second.matrix_coverage_signatures;
        generic_coverage_parent_pid = bit->second.matrix_coverage_parent_pid;
        generic_coverage_parent_batch_id = bit->second.matrix_coverage_parent_batch_id;
        generic_coverage_body_id = bit->second.matrix_coverage_body_id;
        generic_coverage_length = bit->second.matrix_coverage_length;
        generic_coverage_include_identity = bit->second.matrix_coverage_include_identity;
        return_output_to_parent = bit->second.return_output_to_parent;
        generic_right_sources_by_destination =
            bit->second.matrix_right_sources_by_destination;
        generic_right_rows_by_destination =
            bit->second.matrix_right_rows_by_destination;
        generic_right_pairs_by_destination =
            bit->second.matrix_right_pairs_by_destination;
    }
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
    // Both materialized macro operands and streaming exact-power deltas use
    // the same middle-owner data plane.
    // Middle partitioning changes only the binary join data plane. A source
    // row is split by its middle keys and the RHS is split by its row key;
    // both fragments meet at W_k. The source worker remains the parent/result
    // owner and receives only partial C deltas.
        RelationRows presence_left;
        RelationRows presence_right;
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            auto bit = qit->second.demand_batches.find(batch_id);
            if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
            IncrementalMergeRows(bit->second.left_rows, left_delta);
            IncrementalMergeRows(bit->second.right_rows, right_delta);
#if defined(INCREMENTAL_SYNCHRONOUS_MATRIX_MODE)
            // Keep both operands source-owned until the same middle key is
            // known to exist on the RHS. A left delta with no matching RHS is
            // retained in the batch, not sent to a middle worker.
            if (bit->second.middle_semijoin_enabled) {
                SyncMessageBuildRoute(
                    bit->second, left_delta, right_delta,
                    presence_left, presence_right);
            } else {
                presence_left = left_delta;
                presence_right = right_delta;
            }
#else
            MiddlePresenceRecordLeft(bit->second, left_delta);
            MiddlePresenceBuildRoute(
                bit->second, left_delta, right_delta,
                presence_left, presence_right);
#endif
        }

        unordered_map<int, RelationRows> left_by_middle;
        unordered_map<int, RelationRows> right_by_middle;
        unordered_map<int, uint64_t> join_work_by_middle;
        uint64_t total_join_work = 0;
        for (const auto& [source, mids] : presence_left) {
            for (uint64_t middle : mids) {
                const int middle_pid = IncrementalOwner(state, middle);
                left_by_middle[middle_pid][source].push_back(middle);
            }
        }
        for (const auto& [middle, destinations] : presence_right) {
            if (!destinations.empty()) {
                const int middle_pid = IncrementalOwner(state, middle);
                right_by_middle[middle_pid][middle] = destinations;
            }
        }
        // Input volume is a poor proxy for join cost: one middle row can have
        // a much larger RHS fanout than another row with the same wire size.
        // Estimate the actual work as sum_mid(|A(:,mid)| * |B(mid,:)|).
        for (const auto& [middle_pid, left_rows] : left_by_middle) {
            auto right_it = right_by_middle.find(middle_pid);
            if (right_it == right_by_middle.end()) continue;
            uint64_t work = 0;
            for (const auto& [source, mids] : left_rows) {
                (void)source;
                for (uint64_t middle : mids) {
                    auto rhs_it = right_it->second.find(middle);
                    if (rhs_it == right_it->second.end()) continue;
                    const uint64_t fanout = rhs_it->second.cardinality();
                    if (fanout != 0 && work >
                        numeric_limits<uint64_t>::max() - fanout) {
                        work = numeric_limits<uint64_t>::max();
                    } else {
                        work += fanout;
                    }
                }
            }
            join_work_by_middle[middle_pid] = work;
            total_join_work = total_join_work >
                numeric_limits<uint64_t>::max() - work
                ? numeric_limits<uint64_t>::max()
                : total_join_work + work;
        }
        unordered_set<int> middle_partitions;
        for (const auto& [middle_pid, _] : left_by_middle) middle_partitions.insert(middle_pid);
        for (const auto& [middle_pid, _] : right_by_middle) middle_partitions.insert(middle_pid);
        for (int middle_pid : middle_partitions) {
            RelationRows left;
            RelationRows right;
            auto left_it = left_by_middle.find(middle_pid);
            if (left_it != left_by_middle.end()) left = std::move(left_it->second);
            auto right_it = right_by_middle.find(middle_pid);
            if (right_it != right_by_middle.end()) right = std::move(right_it->second);
            const uint64_t owner_work = join_work_by_middle[middle_pid];
            const uint64_t active_owners = middle_partitions.size();
            const uint64_t average_work = active_owners == 0 ? 0 :
                total_join_work / active_owners;
 #if defined(INCREMENTAL_SYNCHRONOUS_MATRIX_MODE)
            const bool allow_remote_compute = false;
 #else
            const bool allow_remote_compute = active_owners >= 3 &&
                owner_work > 0 && average_work > 0 &&
                owner_work > average_work + average_work / 2;
 #endif
            int preferred_compute_pid = -1;
            if (allow_remote_compute) {
                uint64_t smallest_work = numeric_limits<uint64_t>::max();
                for (int candidate_pid : middle_partitions) {
                    if (candidate_pid == middle_pid) continue;
                    const uint64_t candidate_work =
                        join_work_by_middle[candidate_pid];
                    if (candidate_work < smallest_work ||
                        (candidate_work == smallest_work &&
                         candidate_pid < preferred_compute_pid)) {
                        smallest_work = candidate_work;
                        preferred_compute_pid = candidate_pid;
                    }
                }
            }
            IncrementalDispatchMiddlePartitionJoin(
                state, query_id, batch_id, middle_pid,
                std::move(left), std::move(right), allow_remote_compute,
                preferred_compute_pid);
        }
        return;
#endif
    // Generic matrix composition receives fully materialized operands. Route
    // it directly to the destination blocks, while exact B^k batches below
    // retain the streaming RSFM signature and delivery machinery.
    if (generic_matrix_compose) {
#if !defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
        uint64_t selected_left_pairs = 0;
        uint64_t routed_messages = 0;
        uint64_t resident_rhs_pairs_avoided = 0;
        IncrementalPreRouteCoverageStats pre_route_coverage;
        unordered_set<int> dispatched_destinations;
        if (!left_delta.empty() && !right_delta.empty()) {
            const auto right_blocks = IncrementalSplitRightByDestination(state, right_delta);
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit != state.queries.end()) {
                    auto bit = qit->second.demand_batches.find(batch_id);
                    if (bit != qit->second.demand_batches.end()) {
                        for (const auto& [destination_pid, right_block] :
                             right_blocks) {
                            auto& sources = bit->second
                                .matrix_right_sources_by_destination[
                                    destination_pid];
                            for (const auto& [source, _] : right_block) {
                                sources.push_back(source);
                            }
                        }
                    }
                }
            }
            for (const auto& [destination_pid, right_block] : right_blocks) {
                RelationRows matching = IncrementalFilterLeftForRightKeys(left_delta, right_block);
                if (matching.empty()) continue;
                RelationRows coverage;
                RelationRows destination_coverage;
                const unordered_map<uint64_t, GcnpCoverageSignature>* cover_signatures = nullptr;
                if (generic_coverage_blocks) {
                    const auto coverage_it = generic_coverage_blocks->find(destination_pid);
                    if (coverage_it != generic_coverage_blocks->end()) {
                        destination_coverage = coverage_it->second;
                        coverage = IncrementalFilterRowsForLeftSources(
                            coverage_it->second, matching);
                        if (generic_coverage_signatures) {
                            const auto signature_it =
                                generic_coverage_signatures->find(destination_pid);
                            if (signature_it != generic_coverage_signatures->end()) {
                                cover_signatures = &signature_it->second;
                            }
                        }
                    }
                }
                if (cover_signatures != nullptr) {
                    IncrementalPreRouteCoverageStats block_stats;
                    matching = IncrementalFilterLeftForCoveredRows(
                        matching, right_block, coverage, &block_stats);
                    pre_route_coverage.Add(block_stats);
                }
                if (matching.empty()) continue;
                selected_left_pairs += CountRelationPairs(matching);
                ++routed_messages;
                dispatched_destinations.insert(destination_pid);
                RelationRows update_coverage;
                if (generic_coverage_parent_pid < 0) {
                    update_coverage = std::move(destination_coverage);
                }
                RelationRows update_right;
                if (generic_right_parent_pid >= 0) {
                    resident_rhs_pairs_avoided += CountRelationPairs(right_block);
                } else {
                    update_right = right_block;
                }
                IncrementalDispatchMatrixJoin(
                    state, query_id, batch_id, destination_pid,
                    std::move(matching), std::move(update_right),
                    retain_output_on_destination, false,
                    generic_right_body_id, generic_right_length, RelationRows{},
                    std::move(update_coverage), generic_coverage_parent_pid,
                    generic_coverage_parent_batch_id, generic_coverage_body_id,
                    generic_coverage_length, generic_coverage_include_identity,
                    return_output_to_parent, right_block.size(),
                    CountRelationPairs(right_block));
            }
        }
        // A resident RHS exposes only a compact source-row directory to the
        // parent. Use those row keys for selective routing; endpoint payloads
        // remain in the destination block that owns them.
        if (!left_delta.empty() && right_delta.empty() &&
            (generic_right_parent_pid >= 0 || generic_right_body_id >= 0)) {
            for (const auto& [destination_pid, right_sources] :
                 generic_right_sources_by_destination) {
                RelationRows matching = IncrementalFilterLeftForRightSources(
                    left_delta, right_sources);
                if (matching.empty()) continue;
                selected_left_pairs += CountRelationPairs(matching);
                resident_rhs_pairs_avoided +=
                    generic_right_pairs_by_destination[destination_pid];
                ++routed_messages;
                dispatched_destinations.insert(destination_pid);
                IncrementalDispatchMatrixJoin(
                    state, query_id, batch_id, destination_pid,
                    std::move(matching), RelationRows{},
                    retain_output_on_destination, false,
                    generic_right_body_id, generic_right_length,
                    RelationRows{}, RelationRows{},
                    generic_coverage_parent_pid,
                    generic_coverage_parent_batch_id,
                    generic_coverage_body_id, generic_coverage_length,
                    generic_coverage_include_identity,
                    return_output_to_parent,
                    generic_right_rows_by_destination[destination_pid],
                    generic_right_pairs_by_destination[destination_pid]);
            }
        }
        // S[a+b] is a union. A destination can own S[a] rows even when its
        // shifted P[a] o S[b] branch has no matching left panel. Preserve
        // those rows in the retained block with a coverage-only update;
        // otherwise a later resident-RHS reuse observes an incomplete S.
        if (generic_coverage_parent_pid < 0 && generic_coverage_blocks) {
            for (const auto& [destination_pid, coverage] :
                 *generic_coverage_blocks) {
                if (coverage.empty() ||
                    dispatched_destinations.count(destination_pid) != 0) {
                    continue;
                }
                IncrementalDispatchMatrixJoin(
                    state, query_id, batch_id, destination_pid,
                    RelationRows{}, RelationRows{},
                    retain_output_on_destination, false,
                    generic_right_body_id, generic_right_length,
                    RelationRows{}, coverage,
                    generic_coverage_parent_pid,
                    generic_coverage_parent_batch_id,
                    generic_coverage_body_id, generic_coverage_length,
                    generic_coverage_include_identity,
                    return_output_to_parent, 0, 0);
                ++routed_messages;
            }
        }
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit != state.queries.end()) {
                auto& query = qit->second;
                query.pre_route_coverage_candidate_mids +=
                    pre_route_coverage.candidate_mids;
                query.pre_route_coverage_signature_rejections +=
                    pre_route_coverage.signature_rejections;
                query.pre_route_coverage_subset_tests +=
                    pre_route_coverage.subset_tests;
                query.pre_route_coverage_skipped_mids +=
                    pre_route_coverage.skipped_mids;
                query.pre_route_coverage_skipped_rhs_pairs +=
                    pre_route_coverage.skipped_rhs_pairs;
                query.pre_route_coverage_input_left_pairs +=
                    pre_route_coverage.input_left_pairs;
                query.pre_route_coverage_output_left_pairs +=
                    pre_route_coverage.output_left_pairs;
                query.rsfm_selected_left_pairs += selected_left_pairs;
                query.rsfm_new_left_pairs += selected_left_pairs;
                query.rsfm_routed_messages += routed_messages;
                query.rsfm_resident_rhs_pairs_avoided += resident_rhs_pairs_avoided;
                query.rsfm_full_broadcast_pairs +=
                    CountRelationPairs(left_delta) *
                    static_cast<uint64_t>(state.total_partitions);
            }
        }
        return;
#endif
    }

    const uint64_t theoretical_broadcast_pairs =
        CountRelationPairs(left_delta) * static_cast<uint64_t>(state.total_partitions);

    bool queue_route_flush = false;
    uint64_t new_left_pairs = 0;
    uint64_t replay_left_pairs = 0;
    uint64_t right_pairs = 0;
    uint64_t signature_activations = 0;
    unordered_map<int, RelationRows> routed_left;
    unordered_map<int, RelationRows> replay_left;
    unordered_map<int, RelationRows> routed_right;
    const auto right_blocks = IncrementalSplitRightByDestination(state, right_delta);
    IncrementalDemandBatch* batch_ptr = nullptr;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        batch_ptr = &bit->second;
    }
    // Demand callbacks are serialized by state.executor. Only that executor
    // mutates these RSFM fields; matrix compute workers operate exclusively on
    // matrix_blocks. Keep state.mu available while the hot bitmap work runs.
    auto& batch = *batch_ptr;
    {
        // Right deltas reveal the exact destination signature for each mid.
        // Only a newly activated (mid, destination) edge needs historical
        // replay.  Replaying active destinations again was the main cost of
        // the first delivery implementation.
        for (const auto& [destination_pid, right_block] : right_blocks) {
            if (right_block.empty()) continue;
            routed_right[destination_pid] = right_block;
            RelationRows newly_active_right;
            for (const auto& [mid, _] : right_block) {
                if (destination_pid < 64) {
                    const uint64_t bit = 1ULL << destination_pid;
                    uint64_t& signature = batch.rsfm_signature_by_middle[mid];
                    if ((signature & bit) == 0) {
                        newly_active_right[mid] = right_block.at(mid);
                        ++signature_activations;
                    }
                    signature |= bit;
                }
            }
            if (!newly_active_right.empty()) {
                replay_left[destination_pid] = IncrementalBuildLeftForRightKeys(
                    batch.rsfm_sources_by_middle, newly_active_right);
            }
        }

        // Route newly arrived A immediately against the signature prefix
        // known so far. A later SIG_DELTA replays this mid only to the newly
        // activated destination through IncrementalDemandOnDirectSignature.
        for (const auto& [source, mids] : left_delta) {
            for (uint64_t mid : mids) {
                auto sig_it = batch.rsfm_signature_by_middle.find(mid);
                if (sig_it == batch.rsfm_signature_by_middle.end()) continue;
                uint64_t mask = sig_it->second;
                while (mask != 0) {
                    const int destination_pid = static_cast<int>(__builtin_ctzll(mask));
                    routed_left[destination_pid][source].push_back(mid);
                    mask &= mask - 1;
                }
            }
        }

        for (const auto& [_, rows] : routed_left) new_left_pairs += IncrementalRelationPairs(rows);
        for (const auto& [_, rows] : replay_left) replay_left_pairs += IncrementalRelationPairs(rows);
        for (const auto& [_, rows] : routed_right) right_pairs += IncrementalRelationPairs(rows);

        // The left frontier is monotone and every signature bit is activated
        // once.  Therefore each current left pair is routed once to each
        // already-active destination, and a historical pair is replayed once
        // when a new destination bit appears.  No delivered relation needs to
        // be materialized or diffed to preserve exact-once delivery.
        for (auto& [destination_pid, replay] : replay_left) {
            IncrementalMergeRows(routed_left[destination_pid], replay);
        }

    }

    // The receiver-side block state is monotone.  Coalescing deltas here is
    // therefore semantically identical to sending them one-at-a-time, but
    // ensures a burst of signature activations produces one matrix update per
    // destination rather than a burst of tiny RPCs and join tasks.
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        for (auto& [destination_pid, rows] : routed_left) {
            IncrementalMergeRows(batch.rsfm_pending_left_by_destination[destination_pid], rows);
        }
        for (auto& [destination_pid, rows] : routed_right) {
            IncrementalMergeRows(batch.rsfm_pending_right_by_destination[destination_pid], rows);
        }
        if ((!routed_left.empty() || !routed_right.empty()) && !batch.rsfm_route_flush_queued) {
            batch.rsfm_route_flush_queued = true;
            ++batch.pending_matrix_route_flushes;
            queue_route_flush = true;
        }
        auto& query = qit->second;
        query.rsfm_new_left_pairs += new_left_pairs;
        query.rsfm_replay_left_pairs += replay_left_pairs;
        query.rsfm_selected_left_pairs += new_left_pairs + replay_left_pairs;
        query.rsfm_right_pairs += right_pairs;
        query.rsfm_signature_activations += signature_activations;
        query.rsfm_full_broadcast_pairs += theoretical_broadcast_pairs;
    }
    if (queue_route_flush) {
        state.executor.Submit([&state, query_id, batch_id]() {
            IncrementalFlushRsfmRoutes(state, query_id, batch_id);
        });
    }
}

static void IncrementalFlushRsfmRoutes(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    unordered_map<int, RelationRows> routed_left;
    unordered_map<int, RelationRows> routed_left_transposed;
    unordered_map<int, RelationRows> routed_right;
    bool retain_output_on_destination = false;
    int right_body_id = -1;
    int right_length = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        batch.rsfm_route_flush_queued = false;
        if (batch.pending_matrix_route_flushes > 0) --batch.pending_matrix_route_flushes;
        routed_left.swap(batch.rsfm_pending_left_by_destination);
        routed_left_transposed.swap(batch.rsfm_pending_transposed_left_by_destination);
        routed_right.swap(batch.rsfm_pending_right_by_destination);
        retain_output_on_destination = batch.retain_output_on_destination;
        if (batch.generic_matrix_compose) {
            right_body_id = batch.matrix_right_body_id;
            right_length = batch.matrix_right_length;
        } else {
            right_body_id = batch.body_id;
            right_length = IncrementalExactBinarySplit(batch.length).second;
        }
    }

    unordered_set<int> destinations;
    for (const auto& [destination_pid, _] : routed_left) destinations.insert(destination_pid);
    for (const auto& [destination_pid, _] : routed_left_transposed) destinations.insert(destination_pid);
    for (const auto& [destination_pid, _] : routed_right) destinations.insert(destination_pid);
    uint64_t empty_left_dispatches = 0;
    for (int destination_pid : destinations) {
        const auto left_it = routed_left.find(destination_pid);
        const auto transposed_left_it = routed_left_transposed.find(destination_pid);
        const auto right_it = routed_right.find(destination_pid);
        const RelationRows empty;
        const RelationRows& left = left_it == routed_left.end() ? empty : left_it->second;
        const RelationRows& transposed_left =
            transposed_left_it == routed_left_transposed.end() ? empty : transposed_left_it->second;
        const RelationRows& right = right_it == routed_right.end() ? empty : right_it->second;
        if (left.empty() && transposed_left.empty() && right.empty()) continue;
        if (left.empty() && transposed_left.empty()) ++empty_left_dispatches;
        IncrementalDispatchMatrixJoin(
            state, query_id, batch_id, destination_pid,
            left, right, retain_output_on_destination, false,
            right_body_id, right_length, transposed_left);
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit != state.queries.end()) {
            qit->second.rsfm_routed_messages += static_cast<uint64_t>(destinations.size());
            qit->second.rsfm_empty_left_dispatches += empty_left_dispatches;
        }
    }
    IncrementalMaybeDispatchDestinationShardSeals(state, query_id, batch_id);
    IncrementalDemandMaybeComplete(state, query_id, batch_id);
}

// Kept as a no-op transition point for the zero-output path.  Normal binary
// work is dispatched from left/right DELTA handlers above.
static void IncrementalDemandDispatchMatrixIfReady(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    IncrementalDemandMaybeComplete(state, query_id, batch_id);
}

static void IncrementalDemandOnLeft(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    RelationRows rows, bool complete) {
    RelationRows fresh_left;
    unordered_map<int, RelationDstSet> by_owner;
    int body_id = -1;
    int right_len = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        fresh_left = IncrementalDifferenceRows(rows, batch.left_rows);
        IncrementalMergeRows(batch.left_rows, fresh_left);
#if !defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
        for (const auto& [source, mids] : fresh_left) {
            for (uint64_t mid : mids) {
                batch.rsfm_sources_by_middle[mid].push_back(source);
            }
        }
#endif
        body_id = batch.body_id;
        right_len = IncrementalExactBinarySplit(batch.length).second;
        for (const auto& [_, mids] : fresh_left) {
            for (uint64_t mid : mids) {
                if (batch.requested_mids.bitmap.addChecked(mid)) {
                    by_owner[IncrementalOwner(state, mid)].push_back(mid);
                }
            }
        }
        if (complete) batch.left_complete = true;
    }
    for (auto& [owner, mids] : by_owner) {
        IncrementalDemandRequestRight(state, query_id, batch_id, body_id, right_len, owner, std::move(mids));
    }
    // Install every A delta at output owners even when this parent has not
    // received B yet.  The destination block retains it until its matching
    // B[j,l] delta arrives.
    if (!fresh_left.empty()) {
        IncrementalDemandDispatchMatrixDelta(
            state, query_id, batch_id, fresh_left, RelationRows{});
    }
    if (complete) {
#if !defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
        IncrementalRsfmMaybeFinalizeRoutes(state, query_id, batch_id);
#endif
        IncrementalDemandMaybeComplete(state, query_id, batch_id);
    }
}

static void IncrementalDemandOnRight(
    IncrementalWorkerState& state, int query_id, uint64_t batch_id,
    RelationRows rows, bool complete) {
    RelationRows fresh_right;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        auto& batch = bit->second;
        fresh_right = IncrementalDifferenceRows(rows, batch.right_rows);
        IncrementalMergeRows(batch.right_rows, fresh_right);
        if (complete && batch.pending_right_batches > 0) --batch.pending_right_batches;
    }
    // Symmetric to the A side: a B delta may arrive first and waits in its
    // destination block until an A delta is installed there.
    if (!fresh_right.empty()) {
        IncrementalDemandDispatchMatrixDelta(
            state, query_id, batch_id, RelationRows{}, fresh_right);
    }
    if (complete) IncrementalDemandMaybeComplete(state, query_id, batch_id);
}

static void IncrementalRunDemandBatch(IncrementalWorkerState& state, int query_id, uint64_t batch_id) {
    int body_id = -1;
    int length = 0;
    RelationDstSet sources;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.demand_batches.find(batch_id);
        if (bit == qit->second.demand_batches.end()) return;
        body_id = bit->second.body_id;
        length = bit->second.length;
        sources = bit->second.sources;
    }
    if (sources.empty()) {
        {
            lock_guard<mutex> lk(state.mu);
            auto& batch = state.queries.at(query_id).demand_batches.at(batch_id);
            batch.left_complete = true;
        }
        IncrementalDemandMaybeComplete(state, query_id, batch_id);
        return;
    }
    if (length == 1) {
        {
            lock_guard<mutex> lk(state.mu);
            // Reuse pending_right_batches as the number of outstanding base
            // rows; it is zero before this batch starts and never mixes with
            // binary right-child batches for length one.
            state.queries.at(query_id).demand_batches.at(batch_id).pending_right_batches = sources.cardinality();
        }
        for (uint64_t source : sources) {
            IncrementalSubscribeRow(state, IncrementalRowKey{query_id, body_id, 1, source},
                [&state, query_id, batch_id, source](const RelationDstSet& values, bool complete) {
                    IncrementalDemandOnBaseRow(state, query_id, batch_id, source, values, complete);
                });
        }
        return;
    }
    const int left_len = IncrementalExactBinarySplit(length).first;
    IncrementalStartDemandBatch(
        state, query_id, body_id, left_len, std::move(sources),
        [&state, query_id, batch_id](const RelationRows& rows, bool complete) {
            IncrementalDemandOnLeft(state, query_id, batch_id, rows, complete);
        }, false,
        [&state, query_id, batch_id](const RelationRows& rows, bool) {
            IncrementalDemandOnLeft(state, query_id, batch_id, rows, false);
        });
}

static void IncrementalStartDemandBatch(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, IncrementalRowsCallback callback,
    bool retain_output_on_destination,
    IncrementalRowsCallback progress_callback) {
    uint64_t batch_id = 0;
    RelationRows cached_rows;
    vector<uint64_t> shared_sources;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        // A row remains the cache/memoization unit.  Batch scheduling only
        // receives the sources that are still missing; completed exact rows
        // are replayed as one RelationRows delta immediately below.  Without
        // this admission step, overlapping parent frontiers repeatedly start
        // the same B^k(source) batch and recreate the old explosion at a
        // coarser level.
        RelationDstSet missing_sources;
        for (uint64_t source : sources) {
            const IncrementalRowKey key{query_id, body_id, length, source};
            auto row = query.rows.find(key);
            if (row != query.rows.end() &&
                row->second.status == IncrementalRowState::Status::COMPLETE) {
                ++query.row_cache_hits;
                if (!row->second.values.empty()) {
                    cached_rows.emplace(source, row->second.values);
                }
            } else {
                missing_sources.push_back(source);
            }
        }
        if (length > 1 && !missing_sources.empty()) {
            RelationDstSet owned_sources;
            for (uint64_t source : missing_sources) {
                const IncrementalRowKey key{
                    query_id, body_id, length, source};
                auto row = query.rows.find(key);
                if (row != query.rows.end() &&
                    row->second.status ==
                        IncrementalRowState::Status::COMPUTING) {
                    shared_sources.push_back(source);
                    continue;
                }
                auto& owned = query.rows[key];
                if (owned.status ==
                    IncrementalRowState::Status::NOT_STARTED) {
                    owned.status = IncrementalRowState::Status::COMPUTING;
                    ++query.rows_started;
                }
                owned_sources.push_back(source);
            }
            missing_sources = std::move(owned_sources);
        }
        batch_id = query.next_demand_batch_id++;
        IncrementalDemandBatch batch;
        batch.id = batch_id;
        batch.query_id = query_id;
        batch.body_id = body_id;
        batch.length = length;
        batch.middle_semijoin_enabled =
            !IncrementalProgramHasOpenRange(query.program);
        batch.retain_output_on_destination = retain_output_on_destination;
        batch.sources = std::move(missing_sources);
        batch.pending_shared_rows = shared_sources.size();
        batch.callbacks.push_back(std::move(callback));
        if (progress_callback) {
            batch.progress_callbacks.push_back(std::move(progress_callback));
        }
        query.demand_batches.emplace(batch_id, std::move(batch));
        ++query.demand_batches_started;
    }
    if (!cached_rows.empty()) IncrementalDemandEmit(state, query_id, batch_id, cached_rows);
    for (uint64_t source : shared_sources) {
        IncrementalSubscribeRow(
            state, IncrementalRowKey{query_id, body_id, length, source},
            [&state, query_id, batch_id, source](
                const RelationDstSet& values, bool complete) {
                IncrementalDemandOnSharedRow(
                    state, query_id, batch_id, source, values, complete);
            });
    }
    state.executor.Submit([&state, query_id, batch_id]() {
        IncrementalRunDemandBatch(state, query_id, batch_id);
    });
}

static uint64_t IncrementalStartGenericMatrixCompose(
    IncrementalWorkerState& state, int query_id,
    RelationRows left_rows, RelationRows right_rows,
    IncrementalRowsCallback callback, bool retain_output_on_destination,
    RelationRows coverage_rows = {}, int coverage_parent_pid = -1,
    uint64_t coverage_parent_batch_id = 0,
    int coverage_body_id = -1, int coverage_length = 0,
    bool coverage_include_identity = false,
    bool return_output_to_parent = false,
    shared_ptr<uint64_t> assigned_batch_id = {},
    int right_parent_pid = -1, uint64_t right_parent_batch_id = 0,
    int right_body_id = -1, int right_length = 0,
    shared_ptr<IncrementalMatrixResidentDirectory> output_directory = {},
    shared_ptr<IncrementalMatrixResidentDirectory> right_directory = {},
    shared_ptr<IncrementalMatrixResidentDirectory> coverage_directory = {},
    bool right_include_identity = false) {
    uint64_t batch_id = 0;
    RelationDstSet coverage_sources;
    for (const auto& [source, _] : coverage_rows) {
        coverage_sources.push_back(source);
    }
    RelationDstSet right_sources;
    for (const auto& [source, _] : right_rows) {
        right_sources.push_back(source);
    }
    auto coverage_blocks = make_shared<unordered_map<int, RelationRows>>(
        IncrementalSplitRightByDestination(state, coverage_rows));
    unordered_map<int, RelationDstSet> coverage_sources_by_destination;
    for (const auto& [destination_pid, rows] : *coverage_blocks) {
        auto& sources = coverage_sources_by_destination[destination_pid];
        for (const auto& [source, _] : rows) sources.push_back(source);
    }
    if (coverage_directory) {
        for (size_t index = 0;
             index < coverage_directory->sources_by_destination.size(); ++index) {
            const auto& sources = coverage_directory->sources_by_destination[index];
            if (sources.empty()) continue;
            coverage_sources.bitmap |= sources.bitmap;
            coverage_sources_by_destination[static_cast<int>(index)].bitmap |=
                sources.bitmap;
        }
    }
    unordered_map<int, RelationDstSet> right_sources_by_destination;
    unordered_map<int, uint64_t> right_rows_by_destination;
    unordered_map<int, uint64_t> right_pairs_by_destination;
    if (right_directory) {
        for (size_t index = 0;
             index < right_directory->sources_by_destination.size(); ++index) {
            const auto& sources = right_directory->sources_by_destination[index];
            if (!sources.empty()) {
                right_sources.bitmap |= sources.bitmap;
                right_sources_by_destination[static_cast<int>(index)] = sources;
            }
            if (index < right_directory->rows_by_destination.size()) {
                right_rows_by_destination[static_cast<int>(index)] =
                    right_directory->rows_by_destination[index];
            }
            if (index < right_directory->pairs_by_destination.size()) {
                right_pairs_by_destination[static_cast<int>(index)] =
                    right_directory->pairs_by_destination[index];
            }
        }
    }
    auto coverage_signatures = make_shared<
        unordered_map<int, unordered_map<uint64_t, GcnpCoverageSignature>>>();
    coverage_signatures->reserve(coverage_blocks->size() * 2 + 1);
    for (const auto& [destination_pid, rows] : *coverage_blocks) {
        auto& signatures = (*coverage_signatures)[destination_pid];
        signatures.reserve(rows.size() * 2 + 1);
        for (const auto& [source, values] : rows) {
            signatures.emplace(source, GcnpMakeCoverageSignature(values));
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        batch_id = query.next_demand_batch_id++;
        IncrementalDemandBatch batch;
        batch.id = batch_id;
        batch.query_id = query_id;
        batch.body_id = -1;
        batch.length = 0;
        batch.left_complete = true;
        batch.matrix_dispatching = true;
        batch.cache_output_rows = false;
        batch.generic_matrix_compose = true;
        batch.matrix_right_parent_pid = right_parent_pid;
        batch.matrix_right_parent_batch_id = right_parent_batch_id;
        batch.matrix_right_body_id = right_body_id;
        batch.matrix_right_length = right_length;
        batch.matrix_right_include_identity = right_include_identity;
#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
        batch.matrix_coverage_rows = std::move(coverage_rows);
#else
        batch.matrix_coverage_rows.clear();
#endif
        batch.matrix_coverage_blocks = std::move(coverage_blocks);
        batch.matrix_coverage_signatures = std::move(coverage_signatures);
        batch.matrix_coverage_parent_pid = coverage_parent_pid;
        batch.matrix_coverage_parent_batch_id = coverage_parent_batch_id;
        batch.matrix_coverage_body_id = coverage_body_id;
        batch.matrix_coverage_length = coverage_length;
        batch.matrix_coverage_include_identity = coverage_include_identity;
        batch.matrix_coverage_sources = std::move(coverage_sources);
        batch.matrix_right_sources = std::move(right_sources);
        batch.matrix_coverage_sources_by_destination =
            std::move(coverage_sources_by_destination);
        batch.matrix_right_sources_by_destination =
            std::move(right_sources_by_destination);
        batch.matrix_right_rows_by_destination =
            std::move(right_rows_by_destination);
        batch.matrix_right_pairs_by_destination =
            std::move(right_pairs_by_destination);
        batch.matrix_output_directory = std::move(output_directory);
        batch.retain_output_on_destination = retain_output_on_destination;
        batch.return_output_to_parent = return_output_to_parent;
        batch.callbacks.push_back(std::move(callback));
        query.demand_batches.emplace(batch_id, std::move(batch));
        ++query.demand_batches_started;
    }
    if (assigned_batch_id) *assigned_batch_id = batch_id;
    IncrementalDemandDispatchMatrixDelta(
        state, query_id, batch_id, left_rows, right_rows);
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit != state.queries.end()) {
            auto bit = qit->second.demand_batches.find(batch_id);
            if (bit != qit->second.demand_batches.end()) bit->second.matrix_dispatching = false;
        }
    }
    IncrementalDemandMaybeComplete(state, query_id, batch_id);
    return batch_id;
}

// Allocate a destination-resident C handle without materializing either
// operand at the parent.  Every physical input shard receives a continuation
// request.  It will later send a stream-close marker to every C shard, which
// is the complete-panel proof required by the one-shot GCNP kernel.
static uint64_t IncrementalStartResidentMacroCompose(
    IncrementalWorkerState& state, int query_id, int input_parent_pid,
    uint64_t input_parent_batch_id, int right_node_id,
    IncrementalRowsCallback callback,
    shared_ptr<pair<int, uint64_t>> assigned_handle,
    shared_ptr<IncrementalMatrixResidentDirectory> output_directory,
    bool visible_in_final_fetch = true) {
    uint64_t batch_id = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        batch_id = query.next_demand_batch_id++;
        IncrementalDemandBatch batch;
        batch.id = batch_id;
        batch.query_id = query_id;
        batch.body_id = -1;
        batch.length = 0;
        batch.left_complete = true;
        batch.cache_output_rows = false;
        batch.generic_matrix_compose = true;
        batch.resident_continuation_compose = true;
        batch.retain_output_on_destination = true;
        // Preserve the legacy inverse visibility bit used by final fetching.
        batch.return_output_to_parent = !visible_in_final_fetch;
        batch.matrix_output_directory = std::move(output_directory);
        batch.callbacks.push_back(std::move(callback));
        batch.pending_matrix_joins = static_cast<uint64_t>(state.total_partitions);
        query.demand_batches.emplace(batch_id, std::move(batch));
        ++query.demand_batches_started;
    }
    assigned_handle->first = state.partition_id;
    assigned_handle->second = batch_id;
    // All input workers must participate, including empty shards: an empty
    // stream is still part of the complete-panel proof for every C shard.
    for (int input_pid = 0; input_pid < state.total_partitions; ++input_pid) {
        IncrementalMatrixResidentContinue request;
        request.query_id = query_id;
        request.input_parent_pid = input_parent_pid;
        request.input_parent_batch_id = input_parent_batch_id;
        request.output_parent_pid = state.partition_id;
        request.output_parent_batch_id = batch_id;
        request.right_node_id = right_node_id;
        request.stream_id = static_cast<uint64_t>(input_pid);
        request.expected_streams = static_cast<uint64_t>(state.total_partitions);
        request.retain_output_on_destination = true;
        Message message(MessageType::INCREMENTAL_MATRIX_RESIDENT_CONTINUE);
        message.data = SerializeIncrementalMatrixResidentContinue(request);
        IncrementalEnqueuePeer(state, input_pid, std::move(message));
    }
    return batch_id;
}

static void IncrementalHandleDemandBatchRequest(
    IncrementalWorkerState& state, IncrementalDemandBatchRequest request) {
    for (uint64_t source : request.sources) {
        if (IncrementalOwner(state, source) != state.partition_id) {
            cerr << "[incremental-demand] batch request contains non-owner source" << endl;
            return;
        }
    }
    const int query_id = request.query_id;
    if (request.middle_rhs_direct) {
        // The request is handled by owner(mid), which is exactly the middle
        // worker for this binary composition. Keep RHS payload on that
        // worker and acknowledge only after its local middle update returns.
        bool middle_semijoin_enabled = true;
        {
            lock_guard<mutex> lk(state.mu);
            auto query = state.queries.find(request.query_id);
            if (query == state.queries.end()) return;
            middle_semijoin_enabled =
                !IncrementalProgramHasOpenRange(query->second.program);
        }
        auto presence = make_shared<IncrementalMiddlePresenceAccumulator>();
        IncrementalStartDemandBatch(
            state, request.query_id, request.body_id, request.length,
            std::move(request.sources),
            [&state, request, presence, middle_semijoin_enabled](
                const RelationRows& rows, bool complete) {
                IncrementalInstallMiddleRhsDirect(
                    state, request.query_id, request.reply_pid,
                    request.parent_batch_id, rows);
                if (!middle_semijoin_enabled) {
                    if (complete) {
                        IncrementalSendMiddlePresence(
                            state, request, RelationDstSet{}, true);
                    }
                    return;
                }
                RelationDstSet completed_presence;
                {
                    lock_guard<mutex> lk(presence->mu);
                    for (const auto& [middle, destinations] : rows) {
                        if (!destinations.empty()) presence->mids.push_back(middle);
                    }
                    if (complete) completed_presence = std::move(presence->mids);
                }
                if (!complete) return;
                IncrementalSendMiddlePresence(
                    state, request, std::move(completed_presence), true);
            });
        return;
    }
    if (request.direct_right_stream) {
        IncrementalStartDemandBatch(
            state, request.query_id, request.body_id, request.length,
            std::move(request.sources),
            [&state, request](const RelationRows& rows, bool complete) {
                IncrementalForwardDirectRightStream(state, request, rows, complete);
            });
        return;
    }
    IncrementalStartDemandBatch(
        state, request.query_id, request.body_id, request.length,
        std::move(request.sources),
        [&state, query_id, reply_pid = request.reply_pid, token = request.token](const RelationRows& rows, bool complete) {
            IncrementalDemandBatchStream event;
            event.query_id = query_id;
            event.token = token;
            event.complete = complete;
            event.rows = rows;
            Message message(MessageType::INCREMENTAL_DEMAND_BATCH_STREAM);
            message.data = SerializeIncrementalDemandBatchStream(event);
            IncrementalEnqueuePeer(state, reply_pid, std::move(message));
        });
}

static void IncrementalHandleDemandBatchStream(
    IncrementalWorkerState& state, IncrementalDemandBatchStream event) {
    if (event.middle_presence) {
        IncrementalDemandOnMiddlePresence(
            state, event.query_id, event.token,
            std::move(event.rows), std::move(event.middle_presence_mids),
            event.complete);
        return;
    }
    if (event.direct_signature) {
        IncrementalDemandOnDirectSignature(
            state, event.query_id, event.token, std::move(event.rows), event.complete);
        return;
    }
    // Direct stream ACKs use the existing stream envelope but have no row
    // payload. Handle them before legacy remote-demand callbacks.
    if (event.complete && event.rows.empty() &&
        IncrementalConsumeDirectStreamAck(state, event.query_id, event.token)) {
        return;
    }
    IncrementalRowsCallback callback;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(event.query_id);
        if (qit == state.queries.end()) return;
        auto it = qit->second.remote_demand_callbacks.find(event.token);
        if (it == qit->second.remote_demand_callbacks.end()) return;
        callback = it->second;
        if (event.complete) qit->second.remote_demand_callbacks.erase(it);
    }
    IncrementalInvokeRows(state, std::move(callback), std::move(event.rows), event.complete);
}

// Range leaves are evaluated against the same exact-row cache as exact
// leaves. Bounded ranges union cached B^k(source) rows. Open ranges first
// obtain B^l(source), then grow only newly discovered mids through cached
// B^1(mid) rows until the local closure is stable.
#include "../../incremental_range_dual_state.inc"
#include "../../incremental_open_scc.inc"
#ifndef INCREMENTAL_RANGE_ENABLE_COVERAGE_FILTER
#define INCREMENTAL_RANGE_ENABLE_COVERAGE_FILTER 1
#endif
#if defined(INCREMENTAL_RANGE_ENABLE_COVERAGE_FILTER) && \
    !defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
#define INCREMENTAL_RANGE_COVERAGE_FILTER 1
#define INCREMENTAL_RANGE_RESIDENT_COVER 1
#define INCREMENTAL_RANGE_RESIDENT_RHS 1
#endif
static void IncrementalEvaluateGlobalExactPowerRelation(
    IncrementalWorkerState&, int, int, int, RelationDstSet, uint64_t,
    function<void(RelationRows)>);
static void IncrementalEvaluateGlobalExactRelation(
    IncrementalWorkerState&, int, int, int, RelationDstSet, uint64_t,
    function<void(RelationRows)>);
static void IncrementalEvaluateGlobalBoundedRelation(
    IncrementalWorkerState&, int, int, int, int, RelationDstSet, uint64_t,
    function<void(RelationRows)>);
static void IncrementalStartGlobalVirtualTransition(
    IncrementalWorkerState&, int, uint64_t, const RelationRows&,
    const RelationRows&, function<void(RelationRows)>);
static bool IncrementalDirectBalanceEnabled();
static void IncrementalStartBalancedOpenClosure(
    IncrementalWorkerState&, int, int, uint64_t, RelationRows,
    function<void(RelationRows)>);
#include "../incremental_matrix_macro.inc"
#include "synchronous_exact_global.inc"
#include "synchronous_bounded_global.inc"
#if defined(INCREMENTAL_RANGE_ENABLE_COVERAGE_FILTER) && \
    !defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
#undef INCREMENTAL_RANGE_RESIDENT_RHS
#undef INCREMENTAL_RANGE_RESIDENT_COVER
#undef INCREMENTAL_RANGE_COVERAGE_FILTER
#endif

static void IncrementalSendResidentContinuationUpdate(
    IncrementalWorkerState& state, const IncrementalMatrixResidentContinue& request,
    int destination_pid, RelationRows left_rows, RelationRows right_rows) {
    if (left_rows.empty() || right_rows.empty()) return;
    IncrementalMatrixJoinUpdate update;
    update.query_id = request.query_id;
    update.parent_pid = request.output_parent_pid;
    update.parent_batch_id = request.output_parent_batch_id;
    // Ordinary data updates are acknowledged with token zero.  The output
    // parent counts only the final per-destination completion result.
    update.token = 0;
    update.retain_output_on_destination = request.retain_output_on_destination;
    update.left_rows = std::move(left_rows);
    update.right_rows = std::move(right_rows);
    if (destination_pid == state.partition_id) {
        IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
    } else {
        Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
        message.data = SerializeIncrementalMatrixJoinUpdate(update);
        IncrementalEnqueuePeer(state, destination_pid, std::move(message));
    }
}

static void IncrementalSendResidentContinuationSeal(
    IncrementalWorkerState& state, const IncrementalMatrixResidentContinue& request,
    int destination_pid) {
    IncrementalMatrixJoinUpdate seal;
    seal.query_id = request.query_id;
    seal.parent_pid = request.output_parent_pid;
    seal.parent_batch_id = request.output_parent_batch_id;
    seal.token = 0;
    seal.retain_output_on_destination = request.retain_output_on_destination;
    seal.resident_left_stream_complete = true;
    seal.resident_left_stream_id = request.stream_id;
    seal.expected_resident_left_streams = request.expected_streams;
    if (destination_pid == state.partition_id) {
        IncrementalAcceptMatrixJoinUpdate(state, std::move(seal));
    } else {
        Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
        message.data = SerializeIncrementalMatrixJoinUpdate(seal);
        IncrementalEnqueuePeer(state, destination_pid, std::move(message));
    }
}

// A continuation executes where the input A(s,m) shard already resides.  It
// never sends A back to the output parent: the local worker derives its `m`
// frontier, evaluates the right macro for that frontier, and routes only the
// matching panel directly to each C(s,d) owner.
static void IncrementalHandleResidentMatrixContinue(
    IncrementalWorkerState& state, IncrementalMatrixResidentContinue request) {
    RelationRows left_rows;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(request.query_id);
        if (qit == state.queries.end()) return;
        auto block = qit->second.matrix_blocks.find(
            IncrementalMatrixBlockKey{request.input_parent_pid,
                                      request.input_parent_batch_id});
        if (block != qit->second.matrix_blocks.end() && block->second.finalized) {
            left_rows = block->second.output_rows;
        }
    }
    RelationDstSet mids = IncrementalMatrixCollectDestinations(left_rows);
    IncrementalMatrixEvalMacroBatch(
        state, request.query_id, request.right_node_id, std::move(mids), false,
        [&state, request = std::move(request), left_rows = std::move(left_rows)](
            RelationRows right_rows) mutable {
            const auto right_blocks =
                IncrementalSplitRightByDestination(state, right_rows);
            for (const auto& [destination_pid, right_block] : right_blocks) {
                RelationRows matching =
                    IncrementalFilterLeftForRightKeys(left_rows, right_block);
                IncrementalSendResidentContinuationUpdate(
                    state, request, destination_pid, std::move(matching), right_block);
            }
            for (int destination_pid = 0;
                 destination_pid < state.total_partitions; ++destination_pid) {
                IncrementalSendResidentContinuationSeal(
                    state, request, destination_pid);
            }
    });
}

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
static void IncrementalDemandOnMiddlePresence(
    IncrementalWorkerState& state, int query_id, uint64_t token,
    RelationRows rows, RelationDstSet presence_mids, bool complete) {
    RelationRows left_to_release;
    uint64_t parent_batch_id = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto presence_it = qit->second.middle_presence_batches.find(token);
        if (presence_it == qit->second.middle_presence_batches.end()) return;
        parent_batch_id = presence_it->second;
        auto bit = qit->second.demand_batches.find(parent_batch_id);
        if (bit == qit->second.demand_batches.end() || bit->second.complete) return;
        // The RHS owner sends only non-empty middle keys. Release the
        // corresponding retained A(*,mid) rows; the physical RHS payload is
        // already installed at owner(mid).
        left_to_release = SyncMessageBuildLeftCandidatesForPresence(
            bit->second, presence_mids);
        if (complete) qit->second.middle_presence_batches.erase(presence_it);
    }
    if (!left_to_release.empty()) {
        IncrementalDemandDispatchMatrixDelta(
            state, query_id, parent_batch_id, left_to_release, RelationRows{});
    }
    if (complete) {
        // This is the ordered completion acknowledgement for the RHS batch.
        IncrementalDemandOnRight(
            state, query_id, parent_batch_id, RelationRows{}, true);
    }
}

static void IncrementalSendMiddlePresence(
    IncrementalWorkerState& state,
    const IncrementalDemandBatchRequest& request,
    RelationDstSet presence_mids, bool complete) {
    // Return one complete compressed key bitmap, never the RHS endpoint
    // payload. RHS rows are still installed incrementally at owner(mid).
    IncrementalDemandBatchStream event;
    event.query_id = request.query_id;
    event.token = request.token;
    event.complete = complete;
    event.middle_presence = true;
    event.middle_presence_mids = std::move(presence_mids);
    event.rows.clear();
    if (request.reply_pid == state.partition_id) {
        IncrementalDemandOnMiddlePresence(
            state, event.query_id, event.token,
            std::move(event.rows), std::move(event.middle_presence_mids),
            event.complete);
    } else {
        Message message(MessageType::INCREMENTAL_DEMAND_BATCH_STREAM);
        message.data = SerializeIncrementalDemandBatchStream(event);
        IncrementalEnqueuePeer(state, request.reply_pid, std::move(message));
    }
}
#endif

#if defined(INCREMENTAL_MIDDLE_PARTITION_MODE)
// The RHS is evaluated by its own source/mid owner. In middle-partition mode
// that worker is also the physical join target, so install the delta directly
// in its middle block. The source owner receives only the ordinary empty
// completion event from the demand callback below.
static void IncrementalInstallMiddleRhsDirect(
    IncrementalWorkerState& state, int query_id, int parent_pid,
    uint64_t parent_batch_id, RelationRows rows) {
    if (rows.empty()) return;
    IncrementalMatrixJoinUpdate update;
    update.query_id = query_id;
    update.parent_pid = parent_pid;
    update.parent_batch_id = parent_batch_id;
    update.middle_partition_join = true;
    update.middle_rhs_direct = true;
    update.middle_partition = state.partition_id;
    update.right_rows = std::move(rows);
    // This function runs on the RHS owner's ordered executor. Calling the
    // middle handler directly makes the completion callback a true barrier:
    // the source cannot close the batch before this RHS delta is installed.
    IncrementalProcessMiddlePartitionJoinUpdate(state, std::move(update));
}
#endif

static void IncrementalEvalRangeLeaf(
    IncrementalWorkerState& state, int query_id, int body_id, int min_rep,
    int max_rep, uint64_t source, IncrementalDeltaCallback done) {
    if (max_rep >= 0) {
        struct Context {
            RelationDstSet values;
            uint64_t pending = 0;
            bool prefix_complete = false;
            bool completed = false;
            IncrementalDeltaCallback done;
        };
        auto context = make_shared<Context>();
        context->done = std::move(done);
        auto cache = IncrementalGetPrefixSuffixCache(state, query_id, body_id);
        auto finish = make_shared<function<void()>>();
        *finish = [&state, context]() {
            if (context->prefix_complete && context->pending == 0 && !context->completed) {
                context->completed = true;
                IncrementalInvoke(state, context->done, RelationDstSet{}, true);
            }
        };
        IncrementalSubscribeRow(
            state, IncrementalRowKey{query_id, body_id, min_rep, source},
            [&state, cache, context, finish, max_rep, min_rep](const RelationDstSet& mids, bool prefix_complete) {
                for (uint64_t mid : mids) {
                    ++context->pending;
                    IncrementalSubscribePrefixSuffix(
                        state, cache, max_rep - min_rep, mid,
                        [&state, context, finish](const RelationDstSet& delta, bool complete) {
                            RelationDstSet fresh;
                            fresh.bitmap = delta.bitmap - context->values.bitmap;
                            context->values.bitmap |= fresh.bitmap;
                            if (!fresh.empty()) IncrementalInvoke(state, context->done, fresh, false);
                            if (complete) { --context->pending; (*finish)(); }
                        });
                }
                if (prefix_complete) { context->prefix_complete = true; (*finish)(); }
            });
        return;
    }
    IncrementalEvalOpenLeafScc(state, query_id, body_id, min_rep, source, std::move(done));
    return;

    struct Context {
        RelationDstSet values;
        RelationDstSet expanded;
        uint64_t pending_children = 0;
        bool prefix_complete = false;
        bool completed = false;
        IncrementalDeltaCallback done;
    };
    auto context = make_shared<Context>();
    context->done = std::move(done);
    auto finish = make_shared<function<void()>>();
    *finish = [&state, context]() {
        if (context->prefix_complete && context->pending_children == 0 && !context->completed) {
            context->completed = true;
            IncrementalInvoke(state, context->done, RelationDstSet{}, true);
        }
    };
    auto expand = make_shared<function<void(const RelationDstSet&)>>();
    *expand = [&state, query_id, body_id, context, expand, finish](const RelationDstSet& mids) {
        for (uint64_t mid : mids) {
            if (!context->expanded.bitmap.addChecked(mid)) continue;
            ++context->pending_children;
            IncrementalSubscribeRow(
                state, IncrementalRowKey{query_id, body_id, 1, mid},
                [&state, context, expand, finish](const RelationDstSet& delta, bool complete) {
                    RelationDstSet fresh;
                    fresh.bitmap = delta.bitmap - context->values.bitmap;
                    context->values.bitmap |= fresh.bitmap;
                    if (!fresh.empty()) IncrementalInvoke(state, context->done, fresh, false);
                    if (!delta.empty()) (*expand)(delta);
                    if (complete && context->pending_children > 0) --context->pending_children;
                    (*finish)();
                });
        }
    };
    IncrementalSubscribeRow(
        state, IncrementalRowKey{query_id, body_id, min_rep, source},
        [&state, context, expand, finish](const RelationDstSet& delta, bool complete) {
            RelationDstSet fresh;
            fresh.bitmap = delta.bitmap - context->values.bitmap;
            context->values.bitmap |= fresh.bitmap;
            if (!fresh.empty()) IncrementalInvoke(state, context->done, fresh, false);
            if (!delta.empty()) (*expand)(delta);
            if (complete) context->prefix_complete = true;
            (*finish)();
        });
}

static void IncrementalEvalMacro(IncrementalWorkerState& state, int query_id, int node_id, uint64_t source, IncrementalDeltaCallback done) {
    IncrementalMacroNode node;
    { lock_guard<mutex> lk(state.mu); node = state.queries.at(query_id).program.nodes.at(static_cast<size_t>(node_id)); }
    if (node.kind == IncrementalMacroKind::LEAF ||
        node.kind == IncrementalMacroKind::REPETITION) {
        if (node.min_rep == node.max_rep) {
            IncrementalSubscribeRow(state, IncrementalRowKey{query_id, node.body_id, node.min_rep, source}, std::move(done));
        } else {
            IncrementalEvalRangeLeaf(state, query_id, node.body_id, node.min_rep, node.max_rep, source, std::move(done));
        }
        return;
    }
    if (node.kind == IncrementalMacroKind::CHOICE) {
        struct ChoiceContext { mutex mu; int pending = 2; RelationDstSet values; IncrementalDeltaCallback done; };
        auto context = make_shared<ChoiceContext>(); context->done = std::move(done);
        auto child = [&state, context](const RelationDstSet& delta, bool complete) {
            RelationDstSet fresh; bool finished = false;
            { lock_guard<mutex> lk(context->mu); fresh.bitmap = delta.bitmap - context->values.bitmap; context->values.bitmap |= fresh.bitmap; if (complete) finished = --context->pending == 0; }
            if (!fresh.empty()) IncrementalInvoke(state, context->done, fresh, false);
            if (finished) IncrementalInvoke(state, context->done, RelationDstSet{}, true);
        };
        IncrementalEvalMacro(state, query_id, node.left, source, child);
        IncrementalEvalMacro(state, query_id, node.right, source, child);
        return;
    }
    struct SequenceContext { mutex mu; bool left_complete = false; uint64_t pending = 0; unordered_set<uint64_t> children; RelationDstSet values; IncrementalDeltaCallback done; };
    auto context = make_shared<SequenceContext>(); context->done = std::move(done);
    auto left = [&state, query_id, node, context](const RelationDstSet& delta, bool complete) {
        vector<uint64_t> new_mids;
        for (uint64_t mid : delta) { lock_guard<mutex> lk(context->mu); if (context->children.insert(mid).second) { ++context->pending; new_mids.push_back(mid); } }
        for (uint64_t mid : new_mids) IncrementalEvalMacro(state, query_id, node.right, mid, [&state, context](const RelationDstSet& child_delta, bool child_complete) {
            RelationDstSet fresh; bool finished = false;
            { lock_guard<mutex> lk(context->mu); fresh.bitmap = child_delta.bitmap - context->values.bitmap; context->values.bitmap |= fresh.bitmap; if (child_complete) { --context->pending; finished = context->left_complete && context->pending == 0; } }
            if (!fresh.empty()) IncrementalInvoke(state, context->done, fresh, false);
            if (finished) IncrementalInvoke(state, context->done, RelationDstSet{}, true);
        });
        if (complete) { bool finished = false; { lock_guard<mutex> lk(context->mu); context->left_complete = true; finished = context->pending == 0; } if (finished) IncrementalInvoke(state, context->done, RelationDstSet{}, true); }
    };
    IncrementalEvalMacro(state, query_id, node.left, source, left);
}

static void IncrementalCompleteRoot(IncrementalWorkerState& state, int query_id, uint64_t source, const RelationDstSet& delta, bool complete) {
    lock_guard<mutex> lk(state.mu); auto& query = state.queries.at(query_id); if (!delta.empty()) query.final_rows[source] |= delta; if (complete) ++query.roots_done;
}

static void IncrementalStartDirectLeafRoot(IncrementalWorkerState& state, const IncrementalRowKey& key) {
    bool start = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(key.query_id);
        auto& row = query.rows[key];
        if (row.status == IncrementalRowState::Status::COMPLETE) {
            ++query.row_cache_hits;
            ++query.roots_done;
            return;
        }
        if (row.status == IncrementalRowState::Status::NOT_STARTED) {
            row.status = IncrementalRowState::Status::COMPUTING;
            ++query.rows_started;
            start = true;
        }
    }
    if (start) state.executor.Submit([&state, key]() { IncrementalStartRow(state, key); });
}

static void IncrementalStartRootsAsync(IncrementalWorkerState& state, int query_id) {
    vector<uint64_t> sources; int root = -1; bool direct_leaf = false; bool exact_direct_leaf = false; bool bounded_direct_leaf = false; int body_id = -1, length = 0, max_length = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        if (!query.worker_query_started) {
            query.worker_query_started = true;
            query.worker_query_finished = false;
            query.worker_query_started_at = chrono::steady_clock::now();
        }
        unordered_set<string> labels;
        for (const auto& label : IncrementalMacroStartLabels(query.program, query.program.root)) labels.insert(label);
        auto start = state.partition.get_start_point(labels);
        sources.assign(start.begin(), start.end());
        query.roots_total = sources.size();
        query.roots_done = 0;
        query.roots_initialized = true;
        query.root_sources = sources;
        root = query.program.root;
        const auto& root_node = query.program.nodes.at(static_cast<size_t>(root));
        direct_leaf = root_node.kind == IncrementalMacroKind::LEAF ||
            root_node.kind == IncrementalMacroKind::REPETITION;
        exact_direct_leaf = direct_leaf && root_node.min_rep == root_node.max_rep;
        bounded_direct_leaf = direct_leaf && root_node.max_rep >= 0 &&
            root_node.min_rep != root_node.max_rep;
        // Exact leaf roots use one local DemandBatch.  `roots_total` becomes
        // one batch completion rather than one completion callback per row.
        query.direct_leaf_output = true;
        if (direct_leaf) {
            body_id = root_node.body_id;
            length = root_node.min_rep;
            max_length = root_node.max_rep;
            query.direct_leaf_query_id = query_id;
            query.direct_leaf_body_id = body_id;
            query.direct_leaf_length = length;
            query.direct_leaf_output = root_node.min_rep == root_node.max_rep && length > 1;
        }
    }
    if (exact_direct_leaf && body_id >= 0 && length >= 0) {
        RelationDstSet root_sources;
        for (uint64_t source : sources) root_sources.push_back(source);
        {
            lock_guard<mutex> lk(state.mu);
            auto& query = state.queries.at(query_id);
            query.roots_total = 1;
            query.roots_done = 0;
        }
        IncrementalStartGlobalExactRoot(
            state, query_id, body_id, length, std::move(root_sources));
        return;
    }
    if (bounded_direct_leaf && body_id >= 0 && length >= 0 &&
        max_length >= length) {
        RelationDstSet root_sources;
        for (uint64_t source : sources) root_sources.push_back(source);
        {
            lock_guard<mutex> lk(state.mu);
            auto& query = state.queries.at(query_id);
            query.roots_total = 1;
            query.roots_done = 0;
            query.direct_leaf_output = true;
            query.direct_leaf_body_id = -1;
            query.direct_leaf_length = -1;
        }
        IncrementalStartGlobalBoundedRoot(
            state, query_id, body_id, length, max_length,
            std::move(root_sources));
        return;
    }
    RelationDstSet root_sources;
    for (uint64_t source : sources) root_sources.push_back(source);
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        query.roots_total = 1;
        query.roots_done = 0;
        query.direct_leaf_output = true;
        query.direct_leaf_body_id = -1;
        query.direct_leaf_length = -1;
    }
    IncrementalMatrixEvalMacroBatch(
        state, query_id, root, std::move(root_sources),
        !IncrementalDirectBalanceEnabled(),
        [&state, query_id, store_source_rows = IncrementalDirectBalanceEnabled()](RelationRows rows) {
            if (store_source_rows) {
                // A root has no subsequent resident continuation. Its result
                // already returned to its stable result home, so an identity
                // join would only rescan and redistribute the completed rows.
                for (const auto& [source, _] : rows) {
                    if (IncrementalResultHome(state, source) != state.partition_id) {
                        throw runtime_error("balanced root final row has wrong home");
                    }
                }
            }
            lock_guard<mutex> lk(state.mu);
            if (store_source_rows) {
                state.queries.at(query_id).final_rows = std::move(rows);
            }
            ++state.queries.at(query_id).roots_done;
        });
}

static void IncrementalDiscardPreviousQueryRuntime(IncrementalWorkerState& state) {
    {
        lock_guard<mutex> lk(state.query_handler_mu);
        state.active_query_id.store(-1, memory_order_release);
    }
    state.peer_communication_tracking.store(false, memory_order_release);

    state.executor.BeginDiscard();
    state.control_executor.BeginDiscard();
    state.compute_executor.BeginDiscard();
    state.matrix_message_executor.BeginDiscard();
    {
        unique_lock<mutex> lk(state.query_handler_mu);
        state.query_handler_cv.wait(lk, [&state] {
            return state.active_query_handlers == 0;
        });
    }
    // Ordered result handlers can wait on the factorization pool. Drain the
    // parents first; discarding their child tasks would strand the barrier.
    state.executor.WaitDiscardedIdle();
    state.control_executor.WaitDiscardedIdle();
    state.compute_executor.WaitDiscardedIdle();
    state.matrix_message_executor.WaitDiscardedIdle();
    state.factorization_executor.BeginDiscard();
    state.factorization_executor.WaitDiscardedIdle();

    for (size_t pid = 0; pid < state.peer_delta_pending.size(); ++pid) {
        lock_guard<mutex> lk(*state.peer_delta_mu[pid]);
        state.peer_delta_pending[pid].clear();
        state.peer_delta_bytes[pid] = 0;
        state.peer_delta_flush_scheduled[pid] = false;
    }
    for (size_t pid = 0; pid < state.peer_request_pending.size(); ++pid) {
        lock_guard<mutex> lk(*state.peer_request_mu[pid]);
        state.peer_request_pending[pid].clear();
        state.peer_request_flush_scheduled[pid] = false;
    }
    for (size_t pid = 0; pid < state.peer_complete_pending.size(); ++pid) {
        lock_guard<mutex> lk(*state.peer_complete_mu[pid]);
        state.peer_complete_pending[pid].clear();
        state.peer_complete_flush_scheduled[pid] = false;
    }
    for (size_t pid = 0; pid < state.peer_base_expand_pending.size(); ++pid) {
        lock_guard<mutex> lk(*state.peer_base_expand_mu[pid]);
        state.peer_base_expand_pending[pid].clear();
        state.peer_base_expand_bytes[pid] = 0;
        state.peer_base_expand_flush_scheduled[pid] = false;
    }
    for (size_t pid = 0; pid < state.peer_base_result_pending.size(); ++pid) {
        lock_guard<mutex> lk(*state.peer_base_result_mu[pid]);
        state.peer_base_result_pending[pid].clear();
        state.peer_base_result_bytes[pid] = 0;
        state.peer_base_result_flush_scheduled[pid] = false;
    }
    for (auto& sender : state.peer_senders) {
        if (sender) sender->WaitIdle();
    }
    {
        lock_guard<mutex> lk(state.peer_failure_mu);
        state.peer_failure.clear();
        state.peer_failed.store(false, memory_order_release);
    }

    vector<int> old_query_ids;
    {
        lock_guard<mutex> lk(state.mu);
        old_query_ids.reserve(state.queries.size());
        for (const auto& [old_query_id, _] : state.queries) {
            old_query_ids.push_back(old_query_id);
        }
        state.queries.clear();
        state.speculative_gcnp_active.store(false, memory_order_relaxed);
    }
    for (int old_query_id : old_query_ids) {
        IncrementalClearPrefixSuffixCaches(state, old_query_id);
    }

    state.executor.Resume();
    state.control_executor.Resume();
    state.compute_executor.Resume();
    state.matrix_message_executor.Resume();
    state.factorization_executor.Resume();
}

static Message HandleIncrementalInitQuery(IncrementalWorkerState& state, const Message& request) {
    const int query_id = request.GetFieldInt("query_id");
    if (request.GetField("activate_only") == "1") {
        lock_guard<mutex> lifecycle_lk(state.query_lifecycle_mu);
        {
            lock_guard<mutex> lk(state.mu);
            if (state.queries.find(query_id) == state.queries.end()) {
                Message out(MessageType::RESPONSE_ERROR);
                out.AddField("error", "query not initialized");
                return out;
            }
        }
        lock_guard<mutex> lk(state.query_handler_mu);
        if (state.active_query_id.load(memory_order_acquire) != -1) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", "another query is already active");
            return out;
        }
        state.active_query_id.store(query_id, memory_order_release);
        return Message(MessageType::RESPONSE_OK);
    }
    const string expression = request.GetField("expression");
    const vector<PeerInfo> requested_peers = ParsePeers(request.GetField("workers"));
    IncrementalProgram program;
    string error;
    if (!CompileIncrementalProgram(expression, program, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    if (static_cast<int>(requested_peers.size()) != state.total_partitions) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", "workers count mismatch");
        return out;
    }

    lock_guard<mutex> lifecycle_lk(state.query_lifecycle_mu);
    if (!state.peers.empty()) {
        for (size_t i = 0; i < requested_peers.size(); ++i) {
            if (state.peers[i].host != requested_peers[i].host ||
                state.peers[i].port != requested_peers[i].port) {
                Message out(MessageType::RESPONSE_ERROR);
                out.AddField("error", "workers changed while peer transport is active");
                return out;
            }
        }
    }

    IncrementalDiscardPreviousQueryRuntime(state);
    ResetGcnpCoverageFilterStats();
    state.peer_wire_bytes.store(0, memory_order_relaxed);
    state.peer_wire_messages.store(0, memory_order_relaxed);
    state.peer_e2e_us.store(0, memory_order_relaxed);
    state.peer_e2e_messages.store(0, memory_order_relaxed);
    {
        lock_guard<mutex> timing_lk(state.peer_communication_mu);
        state.peer_communication_active = 0;
        state.peer_communication_wall_us = 0;
        state.peer_communication_e2e_active = 0;
        state.peer_communication_e2e_wall_us = 0;
    }
    for (auto& counter : state.peer_wire_by_kind) counter.store(0, memory_order_relaxed);
    {
        lock_guard<mutex> lk(state.mu);
        if (state.peers.empty()) state.peers = requested_peers;
        auto& query = state.queries[query_id];
        query = IncrementalQueryState{};
        query.program = std::move(program);
    }
    return Message(MessageType::RESPONSE_OK);
}
#include "../../incremental_open_worker_control.inc"
static Message HandleIncrementalStartRoot(IncrementalWorkerState& state, const Message& request) {
    const int query_id = request.GetFieldInt("query_id"); IncrementalQueryMessageGuard guard(state, query_id); if (!guard) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", "query not active"); return out; } { lock_guard<mutex> lk(state.mu); if (state.queries.find(query_id) == state.queries.end()) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", "query not initialized"); return out; } auto& query = state.queries.at(query_id); query.roots_initialized = false; query.worker_query_started = true; query.worker_query_finished = false; query.worker_query_started_at = chrono::steady_clock::now(); query.compute_active_tasks = 0; query.compute_wall_us = 0; query.matrix_message_active_tasks = 0; query.matrix_message_compute_us = 0; state.peer_communication_tracking.store(true, memory_order_release); }
    state.executor.Submit([&state, query_id]() { IncrementalStartRootsAsync(state, query_id); }); return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRequestRow(IncrementalWorkerState& state, const Message& request) {
    IncrementalRowKey key; int reply_pid = -1; uint64_t token = 0; string error; if (!ParseIncrementalRowRequest(request.data, key, reply_pid, token, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; }
    IncrementalQueryMessageGuard guard(state, key.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    if (IncrementalOwner(state, key.source) != state.partition_id) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", "row request sent to non-owner"); return out; }
    { lock_guard<mutex> lk(state.mu); if (state.queries.find(key.query_id) == state.queries.end()) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", "unknown query"); return out; } }
    state.executor.Submit([&state, key, reply_pid, token]() { IncrementalRegisterRemoteWaiter(state, key, reply_pid, token); });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRequestRowBatch(IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalRowRequestEnvelope> requests;
    string error;
    if (!ParseIncrementalRowRequestBatch(request.data, requests, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    const int active_query_id = state.active_query_id.load(memory_order_acquire);
    requests.erase(remove_if(requests.begin(), requests.end(),
        [active_query_id](const auto& entry) {
            return entry.key.query_id != active_query_id;
        }), requests.end());
    if (requests.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    for (const auto& entry : requests) {
        if (IncrementalOwner(state, entry.key.source) != state.partition_id) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", "row request batch sent to non-owner");
            return out;
        }
    }
    state.executor.Submit([&state, requests = std::move(requests)]() mutable {
        IncrementalRegisterRemoteWaiterBatch(state, std::move(requests));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRowDelta(IncrementalWorkerState& state, const Message& request) {
    IncrementalRowKey key; uint64_t token = 0; RelationDstSet delta; string error; if (!ParseIncrementalRowDelta(request.data, key, token, delta, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; }
    IncrementalQueryMessageGuard guard(state, key.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, key, token, delta = std::move(delta)]() mutable { IncrementalDeliverRelayDelta(state, key, token, std::move(delta)); });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRowDeltaBatch(IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalRowDeltaEnvelope> events; string error;
    if (!ParseIncrementalRowDeltaBatch(request.data, events, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; }
    const int active_query_id = state.active_query_id.load(memory_order_acquire);
    events.erase(remove_if(events.begin(), events.end(),
        [active_query_id](const auto& event) {
            return event.key.query_id != active_query_id;
        }), events.end());
    if (events.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, events = std::move(events)]() mutable {
        IncrementalDeliverRelayDeltaBatch(state, std::move(events));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRowComplete(IncrementalWorkerState& state, const Message& request) {
    IncrementalRowKey key; uint64_t token = 0; string error; if (!ParseIncrementalRowComplete(request.data, key, token, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; }
    IncrementalQueryMessageGuard guard(state, key.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, key, token]() { IncrementalCompleteRelay(state, key, token); });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRowCompleteBatch(IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalRowCompleteEnvelope> events;
    string error;
    if (!ParseIncrementalRowCompleteBatch(request.data, events, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    const int active_query_id = state.active_query_id.load(memory_order_acquire);
    events.erase(remove_if(events.begin(), events.end(),
        [active_query_id](const auto& event) {
            return event.key.query_id != active_query_id;
        }), events.end());
    if (events.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, events = std::move(events)]() mutable {
        IncrementalCompleteRelayBatch(state, std::move(events));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalRowStreamBatch(IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalRowStreamEnvelope> events;
    string error;
    if (!ParseIncrementalRowStreamBatch(request.data, events, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    const int active_query_id = state.active_query_id.load(memory_order_acquire);
    events.erase(remove_if(events.begin(), events.end(),
        [active_query_id](const auto& event) {
            return event.key.query_id != active_query_id;
        }), events.end());
    if (events.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, events = std::move(events)]() mutable {
        IncrementalDeliverRelayStreamBatch(state, std::move(events));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalDemandBatchRequest(IncrementalWorkerState& state, const Message& request) {
    IncrementalDemandBatchRequest demand;
    string error;
    if (!ParseIncrementalDemandBatchRequest(request.data, demand, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    IncrementalQueryMessageGuard guard(state, demand.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    {
        lock_guard<mutex> lk(state.mu);
        if (state.queries.find(demand.query_id) == state.queries.end()) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", "unknown query");
            return out;
        }
    }
    state.executor.Submit([&state, demand = std::move(demand)]() mutable {
        IncrementalHandleDemandBatchRequest(state, std::move(demand));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalDemandBatchStream(IncrementalWorkerState& state, const Message& request) {
    IncrementalDemandBatchStream event;
    string error;
    if (!ParseIncrementalDemandBatchStream(request.data, event, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    IncrementalQueryMessageGuard guard(state, event.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit([&state, event = std::move(event)]() mutable {
        IncrementalHandleDemandBatchStream(state, std::move(event));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalMatrixJoinUpdate(IncrementalWorkerState& state, const Message& request) {
    const auto trace_begin = chrono::steady_clock::now();
    IncrementalMatrixJoinUpdate update;
    string error;
    if (!ParseIncrementalMatrixJoinUpdate(request.data, update, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    IncrementalQueryMessageGuard guard(state, update.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    const auto trace_decoded = chrono::steady_clock::now();
    const int trace_query = update.query_id;
    const uint64_t trace_stage = update.global_stage_id, trace_tag = update.middle_virtual_shard;
    const int trace_sender = update.global_sender_pid;

    if (update.direct_stream_complete) {
        IncrementalAcceptDirectMatrixJoinUpdate(state, std::move(update));
    } else {
        IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
    }
    if (IncrementalScalingTraceEnabled() && trace_tag != 0) {
        ostringstream line;
        line << "[scaling-receive] worker=" << state.partition_id
             << " query=" << trace_query << " stage=" << trace_stage
             << " tag=" << trace_tag << " sender=" << trace_sender
             << " bytes=" << request.data.size()
             << " decode_us=" << chrono::duration_cast<chrono::microseconds>(trace_decoded-trace_begin).count()
             << " accept_us=" << chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now()-trace_decoded).count();
        cerr << line.str() << '\n';
    }
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalMatrixJoinResult(IncrementalWorkerState& state, const Message& request) {
    IncrementalMatrixJoinResult result;
    string error;
    const auto decode_begin = chrono::steady_clock::now();
    if (!ParseIncrementalMatrixJoinResult(request.data, result, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    IncrementalQueryMessageGuard guard(state, result.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    if (result.factorized_rows && result.global_exact_stage) {
        const uint64_t decode_us = static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - decode_begin).count());
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(result.query_id);
        if (qit != state.queries.end()) {
            qit->second.factorized_result_decode_us += decode_us;
        }
    }
    const auto queued_at = chrono::steady_clock::now();
    state.executor.Submit([&state, result = std::move(result), queued_at]() mutable {
        {
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(result.query_id);
            if (qit != state.queries.end()) {
                qit->second.result_queue_wait_us +=
                    chrono::duration_cast<chrono::microseconds>(
                        chrono::steady_clock::now() - queued_at).count();
            }
        }
        IncrementalHandleMatrixJoinResult(state, std::move(result));
    });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalMatrixResidentContinueRequest(
    IncrementalWorkerState& state, const Message& request) {
    IncrementalMatrixResidentContinue continuation;
    string error;
    if (!ParseIncrementalMatrixResidentContinue(request.data, continuation, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    IncrementalQueryMessageGuard guard(state, continuation.query_id); if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit(
        [&state, continuation = std::move(continuation)]() mutable {
            IncrementalHandleResidentMatrixContinue(state, std::move(continuation));
        });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalBaseExpand(IncrementalWorkerState& state, const Message& request) { IncrementalBaseEvent event; string error; if (!ParseIncrementalBaseExpand(request.data, event, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; } IncrementalQueryMessageGuard guard(state, event.key.query_id); if (!guard) return Message(MessageType::RESPONSE_OK); IncrementalSubmitCompute(state, event.key.query_id, [&state, event]() { IncrementalProcessBaseExpand(state, event); }); return Message(MessageType::RESPONSE_OK); }
static Message HandleIncrementalBaseResult(IncrementalWorkerState& state, const Message& request) { IncrementalBaseResult result; string error; if (!ParseIncrementalBaseResult(request.data, result, error)) { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", error); return out; } IncrementalQueryMessageGuard guard(state, result.key.query_id); if (!guard) return Message(MessageType::RESPONSE_OK); state.executor.Submit([&state, result = std::move(result)]() mutable { IncrementalHandleBaseResult(state, std::move(result)); }); return Message(MessageType::RESPONSE_OK); }
static Message HandleIncrementalBaseExpandBatch(
    IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalBaseEvent> events;
    string error;
    if (!ParseIncrementalBaseExpandBatch(request.data, events, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    const int active_query_id =
        state.active_query_id.load(memory_order_acquire);
    events.erase(remove_if(events.begin(), events.end(),
        [active_query_id](const IncrementalBaseEvent& event) {
            return event.key.query_id != active_query_id;
        }), events.end());
    if (events.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    IncrementalSubmitCompute(
        state, active_query_id,
        [&state, events = std::move(events)]() mutable {
            for (const auto& event : events) {
                IncrementalProcessBaseExpand(state, event);
            }
        });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalBaseResultBatch(
    IncrementalWorkerState& state, const Message& request) {
    vector<IncrementalBaseResult> results;
    string error;
    if (!ParseIncrementalBaseResultBatch(request.data, results, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    const int active_query_id =
        state.active_query_id.load(memory_order_acquire);
    results.erase(remove_if(results.begin(), results.end(),
        [active_query_id](const IncrementalBaseResult& result) {
            return result.key.query_id != active_query_id;
        }), results.end());
    if (results.empty()) return Message(MessageType::RESPONSE_OK);
    IncrementalQueryMessageGuard guard(state, active_query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    state.executor.Submit(
        [&state, results = std::move(results)]() mutable {
            for (auto& result : results) {
                IncrementalHandleBaseResult(state, std::move(result));
            }
        });
    return Message(MessageType::RESPONSE_OK);
}
static Message HandleIncrementalStatus(IncrementalWorkerState& state, const Message& request) {
    const int query_id = request.GetFieldInt("query_id");
    Message out(MessageType::RESPONSE_OK);
    const bool diagnostics = request.GetFieldInt("diagnostics") != 0;
    if (diagnostics) {
        IncrementalEnsurePeers(state);
        auto add_executor = [&](const string& name, IncrementalExecutor& executor) {
            const auto counts = executor.PendingAndActive();
            out.AddField("debug_" + name + "_queued", static_cast<uint64_t>(counts.first));
            out.AddField("debug_" + name + "_active", static_cast<uint64_t>(counts.second));
        };
        add_executor("ordered", state.executor);
        add_executor("control", state.control_executor);
        add_executor("compute", state.compute_executor);
        for (size_t peer = 0; peer < state.peers.size(); ++peer) {
            const string prefix = "debug_peer_" + to_string(peer);
            {
                lock_guard<mutex> lk(*state.peer_base_expand_mu[peer]);
                out.AddField(prefix + "_base_expand_pending",
                    static_cast<uint64_t>(state.peer_base_expand_pending[peer].size()));
                out.AddField(prefix + "_base_expand_scheduled",
                    state.peer_base_expand_flush_scheduled[peer] ? 1 : 0);
            }
            {
                lock_guard<mutex> lk(*state.peer_base_result_mu[peer]);
                out.AddField(prefix + "_base_result_pending",
                    static_cast<uint64_t>(state.peer_base_result_pending[peer].size()));
                out.AddField(prefix + "_base_result_scheduled",
                    state.peer_base_result_flush_scheduled[peer] ? 1 : 0);
            }
            add_executor("sender_" + to_string(peer), *state.peer_senders[peer]);
        }
    }
    lock_guard<mutex> lk(state.mu);
    auto qit = state.queries.find(query_id);
    if (qit == state.queries.end()) { out.AddField("error", "unknown query"); return out; }
    auto& query = qit->second;
    if (diagnostics) {
        uint64_t computing_rows = 0, pending_base_events = 0;
        string pending_rows;
        for (const auto& [key, row] : query.rows) {
            if (row.status != IncrementalRowState::Status::COMPUTING) continue;
            ++computing_rows;
            pending_base_events += row.base_pending;
            if (computing_rows <= 64) {
                if (!pending_rows.empty()) pending_rows += ',';
                pending_rows += to_string(key.body_id) + "/" + to_string(key.length) +
                    "/" + to_string(key.source) + "/" + to_string(row.base_pending);
            }
        }
        out.AddField("debug_computing_rows", computing_rows);
        out.AddField("debug_pending_base_events", pending_base_events);
        out.AddField("debug_pending_rows_body_length_source_events", pending_rows);
    }
    const bool done = query.roots_initialized && query.roots_done == query.roots_total;
    if (done && query.worker_query_started && !query.worker_query_finished) {
        query.worker_query_finished = true;
        query.worker_query_finished_at = chrono::steady_clock::now();
        state.peer_communication_tracking.store(false, memory_order_release);
    }
    const auto worker_query_end = query.worker_query_finished ? query.worker_query_finished_at : chrono::steady_clock::now();
    const uint64_t worker_query_wall_us = query.worker_query_started ? static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(worker_query_end - query.worker_query_started_at).count()) : 0;
    out.AddField("roots_initialized", query.roots_initialized ? 1 : 0);
    out.AddField("roots_total", query.roots_total);
    out.AddField("roots_done", query.roots_done);
    out.AddField("done", done ? 1 : 0);
    out.AddField("worker_query_wall_us", worker_query_wall_us);
    // Completion polls stay cheap even after this worker's roots finish: it
    // can still serve other workers. The coordinator requests full accounting
    // explicitly once every worker has completed its roots.
    if (request.GetFieldInt("fast_poll") != 0 && !diagnostics) {
        return out;
    }
    const auto coverage_filter_stats = ReadGcnpCoverageFilterStats();
    out.AddField("result_commit_us", query.result_commit_us);
    out.AddField("result_queue_wait_us", query.result_queue_wait_us);
    out.AddField("result_restore_us", query.result_restore_us);
    out.AddField("result_restore_max_lanes", query.result_restore_max_lanes);
    out.AddField("balance_planner_us", query.balance_planner_us);
    out.AddField("balance_join_us", query.balance_join_us);
    out.AddField("owner_view_left_rows", query.owner_view_left_rows);
    out.AddField("owner_view_coverage_rows", query.owner_view_coverage_rows);
    out.AddField("owner_view_rhs_inputs", query.owner_view_rhs_inputs);
    out.AddField("owner_view_final_moves", query.owner_view_final_moves);
    out.AddField("bounded_destinations_us", query.bounded_owner_us[0]);
    out.AddField("bounded_closure_merge_us", query.bounded_owner_us[1]);
    out.AddField("bounded_closure_copy_us", query.bounded_owner_us[2]);
    out.AddField("helper_closure_restore_us", query.helper_closure_restore_us);
    out.AddField("helper_closure_restore_rows", query.helper_closure_restore_rows);
    out.AddField("async_accept_queue_us", query.async_accept_queue_us);
    out.AddField("async_accept_run_us", query.async_accept_run_us);
    out.AddField("async_accept_messages", query.async_accept_messages);
    out.AddField("balance_reduce_us", query.balance_reduce_us);
    out.AddField("balance_dispatch_us", query.balance_dispatch_us);
    out.AddField("balance_coverage_us", query.balance_coverage_us);
    out.AddField("balance_gather_us", query.balance_gather_us);
    out.AddField("balance_input_route_us", query.balance_input_route_us);
    out.AddField("balance_open_closure_us", query.balance_open_closure_us);
    out.AddField("balance_open_closure_rows", query.balance_open_closure_rows);
    out.AddField("balance_open_closure_seeds", query.balance_open_closure_seeds);
    out.AddField("balance_input_merge_us", query.balance_input_merge_us);
    out.AddField("balance_rhs_select_us", query.balance_rhs_select_us);
    out.AddField("balance_rhs_pack_us", query.balance_rhs_pack_us);
    out.AddField("balance_rhs_queue_us", query.balance_rhs_queue_us);
    out.AddField("balance_rhs_encode_us", query.balance_rhs_encode_us);
    out.AddField("balance_rhs_wrap_us", query.balance_rhs_wrap_us);
    out.AddField("balance_rhs_raw_bytes", query.balance_rhs_raw_bytes);
    out.AddField("balance_rhs_wire_bytes", query.balance_rhs_wire_bytes);
    out.AddField("balance_rhs_examined_rows", query.balance_rhs_examined_rows);
    out.AddField("balance_rhs_requests", query.balance_rhs_requests);
    out.AddField("balance_rhs_requested_keys", query.balance_rhs_requested_keys);
    out.AddField("balance_rhs_keys_avoided", query.balance_rhs_keys_avoided);
    out.AddField("balance_rhs_empty_requests_avoided", query.balance_rhs_empty_requests_avoided);
    out.AddField("balance_remote_tasks", query.balance_remote_tasks);
    out.AddField("balance_local_tasks", query.balance_local_tasks);
    const uint64_t compute_wall_us = query.compute_wall_us +
        (query.compute_active_tasks == 0 ? 0 : static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(worker_query_end - query.compute_interval_started_at).count()));
    out.AddField("worker_compute_wall_us", min(compute_wall_us, worker_query_wall_us));
    uint64_t matrix_message_compute_us = query.matrix_message_compute_us;
    if (query.matrix_message_active_tasks != 0) {
        matrix_message_compute_us += static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                worker_query_end - query.matrix_message_interval_started_at).count());
    }
    out.AddField("matrix_message_compute_us",
                 min(matrix_message_compute_us, worker_query_wall_us));
    out.AddField("inclusive_compute_wall_us",
                 min(compute_wall_us + matrix_message_compute_us,
                     worker_query_wall_us));
    uint64_t communication_wall_us = 0;
    { lock_guard<mutex> timing_lk(state.peer_communication_mu); communication_wall_us = state.peer_communication_wall_us +
        (state.peer_communication_active == 0 ? 0 : static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(worker_query_end - state.peer_communication_started_at).count())); }
    out.AddField("worker_communication_wall_us", min(communication_wall_us, worker_query_wall_us));
    uint64_t communication_e2e_wall_us = 0;
    { lock_guard<mutex> timing_lk(state.peer_communication_mu); communication_e2e_wall_us =
        state.peer_communication_e2e_wall_us +
        (state.peer_communication_e2e_active == 0 ? 0 : static_cast<uint64_t>(
            chrono::duration_cast<chrono::microseconds>(
                worker_query_end - state.peer_communication_e2e_started_at).count())); }
    out.AddField("worker_communication_e2e_union_us",
                 min(communication_e2e_wall_us, worker_query_wall_us));
    out.AddField("biclique_attempts", biclique_wire_metrics.attempts.load());
    out.AddField("biclique_selected", biclique_wire_metrics.selected.load());
    out.AddField("biclique_blocks", biclique_wire_metrics.blocks.load());
    out.AddField("biclique_shared_pairs", biclique_wire_metrics.shared_pairs.load());
    out.AddField("biclique_original_bytes", biclique_wire_metrics.original_bytes.load());
    out.AddField("biclique_encoded_bytes", biclique_wire_metrics.encoded_bytes.load());
    out.AddField("biclique_encode_us", biclique_wire_metrics.encode_us.load());
    out.AddField("biclique_decode_us", biclique_wire_metrics.decode_us.load());
    out.AddField("biclique_hash_us", biclique_wire_metrics.hash_us.load());
    out.AddField("biclique_plan_us", biclique_wire_metrics.plan_us.load());
    out.AddField("biclique_pack_us", biclique_wire_metrics.pack_us.load());
    out.AddField("biclique_parallel_batches", biclique_wire_metrics.parallel_batches.load());
    out.AddField("rows_started", query.rows_started);
    out.AddField("row_cache_hits", query.row_cache_hits);
    out.AddField("remote_requests", query.remote_requests);
    out.AddField("remote_request_rpcs", query.remote_request_rpcs);
    out.AddField("remote_complete_rpcs", query.remote_complete_rpcs);
    out.AddField("remote_stream_rpcs", query.remote_stream_rpcs);
    out.AddField("delta_count", query.delta_count);
    out.AddField("base_expand_tasks", query.base_expand_tasks);
    out.AddField("base_expand_us", query.base_expand_us);
    out.AddField("gcnp_batches", query.gcnp_metrics.batches);
    out.AddField("gcnp_source_rows", query.gcnp_metrics.source_rows);
    out.AddField("gcnp_groups", query.gcnp_metrics.groups);
    out.AddField("gcnp_reusable_groups", query.gcnp_metrics.reusable_groups);
    out.AddField("gcnp_cache_hits", query.gcnp_metrics.cache_hits);
    out.AddField("gcnp_input_mids", query.gcnp_input_mids);
    out.AddField("gcnp_planned_mids", query.gcnp_planned_mids);
    out.AddField("gcnp_bypassed_mids", query.gcnp_bypassed_mids);
    out.AddField("gcnp_wall_us", query.gcnp_wall_us);
    out.AddField("gcnp_complete_prepare_us", query.gcnp_complete_prepare_us);
    out.AddField("gcnp_complete_group_extract_us", query.gcnp_complete_group_extract_us);
    out.AddField("gcnp_complete_pattern_cache_us", query.gcnp_complete_pattern_cache_us);
    out.AddField("gcnp_complete_union_us", query.gcnp_complete_union_us);
    out.AddField("gcnp_complete_shared_target_us", query.gcnp_complete_shared_target_us);
    out.AddField("gcnp_complete_source_materialize_us", query.gcnp_complete_source_materialize_us);
    out.AddField("gcnp_complete_groups", query.gcnp_complete_groups);
    out.AddField("gcnp_complete_reusable_groups", query.gcnp_complete_reusable_groups);
    out.AddField("gcnp_complete_grouped_source_uses", query.gcnp_complete_grouped_source_uses);
    out.AddField("gcnp_complete_shared_source_uses", query.gcnp_complete_shared_source_uses);
    out.AddField("gcnp_complete_external_memo_hits", query.gcnp_complete_external_memo_hits);
    out.AddField("speculative_gcnp_tasks", query.speculative_gcnp_tasks);
    out.AddField("speculative_gcnp_completed", query.speculative_gcnp_completed);
    out.AddField("speculative_gcnp_cancelled", query.speculative_gcnp_cancelled);
    out.AddField("speculative_gcnp_us", query.speculative_gcnp_us);
    out.AddField("speculative_pattern_memo_stores", query.speculative_pattern_memo_stores);
    out.AddField("speculative_pattern_memo_hits", query.speculative_pattern_memo_hits);
    out.AddField("panel_wait_blocks", query.panel_wait_blocks);
    out.AddField("panel_left_input_batches", query.panel_left_input_batches);
    out.AddField("all_left_panels_sealed_elapsed_us",
                 query.all_left_panels_sealed_elapsed_us);
    out.AddField("left_panel_wait_us", query.left_panel_wait_us);
    out.AddField("left_panel_wait_us_max", query.left_panel_wait_us_max);
    out.AddField("rhs_ready_before_left_blocks", query.rhs_ready_before_left_blocks);
    out.AddField("rhs_ready_wait_for_left_us", query.rhs_ready_wait_for_left_us);
    out.AddField("rhs_ready_wait_for_left_us_max", query.rhs_ready_wait_for_left_us_max);
    out.AddField("left_sealed_before_rhs_blocks", query.left_sealed_before_rhs_blocks);
    out.AddField("left_wait_for_rhs_us", query.left_wait_for_rhs_us);
    out.AddField("left_wait_for_rhs_us_max", query.left_wait_for_rhs_us_max);
    out.AddField("coverage_filter_candidate_mids", coverage_filter_stats.candidate_mids);
    out.AddField("coverage_filter_signature_rejections", coverage_filter_stats.signature_rejections);
    out.AddField("coverage_filter_subset_tests", coverage_filter_stats.subset_tests);
    out.AddField("coverage_filter_skipped_mids", coverage_filter_stats.skipped_mids);
    out.AddField("coverage_filter_skipped_rhs_pairs", coverage_filter_stats.skipped_rhs_pairs);
    out.AddField("join_batch_count", query.join_batch_count);
    out.AddField("join_batch_children", query.join_batch_children);
    out.AddField("join_batch_child_events", query.join_batch_child_events);
    out.AddField("join_batch_publish_pairs", query.join_batch_publish_pairs);
    out.AddField("join_batch_wall_us", query.join_batch_wall_us);
    out.AddField("emit_merge_us", query.emit_merge_us);
    uint64_t demand_open = 0;
    uint64_t demand_pending_right = 0;
    uint64_t demand_pending_matrix = 0;
    uint64_t demand_left_done = 0;
    for (const auto& [_, batch] : query.demand_batches) {
        if (!batch.complete) ++demand_open;
        demand_pending_right += batch.pending_right_batches;
        demand_pending_matrix += batch.pending_matrix_joins;
        if (batch.left_complete) ++demand_left_done;
    }
    out.AddField("demand_batches_started", query.demand_batches_started);
    out.AddField("demand_batches_open", demand_open);
    out.AddField("demand_pending_right", demand_pending_right);
    out.AddField("demand_pending_matrix", demand_pending_matrix);
    out.AddField("demand_left_done", demand_left_done);
    out.AddField("demand_batch_emits", query.demand_batch_emits);
    out.AddField("demand_batch_pairs", query.demand_batch_pairs);
    out.AddField("demand_compose_calls", query.demand_compose_calls);
    out.AddField("demand_compose_us", query.demand_compose_us);
    out.AddField("matrix_join_updates", query.matrix_join_updates);
    out.AddField("matrix_join_result_pairs", query.matrix_join_result_pairs);
    out.AddField("matrix_join_us", query.matrix_join_us);
    out.AddField("factorized_result_messages",
                 query.factorized_result_messages);
    out.AddField("factorized_result_shared_blocks",
                 query.factorized_result_shared_blocks);
    out.AddField("factorized_result_dictionary_merges",
                 query.factorized_result_dictionary_merges);
    out.AddField("factorized_result_wire_bytes_saved",
                 query.factorized_result_wire_bytes_saved);
    out.AddField("factorized_result_coalesce_us",
                 query.factorized_result_coalesce_us);
    out.AddField("factorized_result_expand_us",
                 query.factorized_result_expand_us);
    out.AddField("factorized_result_union_us",
                 query.factorized_result_union_us);
    out.AddField("factorized_result_encode_us",
                 query.factorized_result_encode_us);
    out.AddField("factorized_result_decode_us",
                 query.factorized_result_decode_us);
    out.AddField("factorized_result_blocks_after_coalesce",
                 query.factorized_result_blocks_after_coalesce);
    out.AddField("factorized_result_source_uses",
                 query.factorized_result_source_uses);
    out.AddField("factorized_source_set_blocks_before",
                 query.factorized_source_set_blocks_before);
    out.AddField("factorized_source_set_blocks_after",
                 query.factorized_source_set_blocks_after);
    out.AddField("factorized_source_set_reused_groups",
                 query.factorized_source_set_reused_groups);
    out.AddField("factorized_source_set_blocks_merged",
                 query.factorized_source_set_blocks_merged);
    out.AddField("factorized_source_union_operations_avoided",
                 query.factorized_source_union_operations_avoided);
    out.AddField("factorized_source_set_group_us",
                 query.factorized_source_set_group_us);
    out.AddField("factorized_source_set_union_us",
                 query.factorized_source_set_union_us);
    out.AddField("middle_scheduler_assigned_shards",
                 query.middle_scheduler_assigned_shards);
    out.AddField("middle_scheduler_submitted_work",
                 query.middle_scheduler_submitted_work);
    out.AddField("middle_scheduler_completed_work",
                 query.middle_scheduler_completed_work);
    out.AddField("middle_scheduler_executed_work",
                 query.middle_scheduler_executed_work);
    out.AddField("range_coverage_pairs", query.range_coverage_pairs);
    out.AddField("pre_route_coverage_candidate_mids", query.pre_route_coverage_candidate_mids);
    out.AddField("pre_route_coverage_signature_rejections", query.pre_route_coverage_signature_rejections);
    out.AddField("pre_route_coverage_subset_tests", query.pre_route_coverage_subset_tests);
    out.AddField("pre_route_coverage_skipped_mids", query.pre_route_coverage_skipped_mids);
    out.AddField("pre_route_coverage_skipped_rhs_pairs", query.pre_route_coverage_skipped_rhs_pairs);
    out.AddField("pre_route_coverage_input_left_pairs", query.pre_route_coverage_input_left_pairs);
    out.AddField("pre_route_coverage_output_left_pairs", query.pre_route_coverage_output_left_pairs);
    out.AddField("rsfm_selected_left_pairs", query.rsfm_selected_left_pairs);
    out.AddField("rsfm_new_left_pairs", query.rsfm_new_left_pairs);
    out.AddField("rsfm_replay_left_pairs", query.rsfm_replay_left_pairs);
    out.AddField("rsfm_right_pairs", query.rsfm_right_pairs);
    out.AddField("rsfm_full_broadcast_pairs", query.rsfm_full_broadcast_pairs);
    out.AddField("rsfm_routed_messages", query.rsfm_routed_messages);
    out.AddField("rsfm_signature_activations", query.rsfm_signature_activations);
    out.AddField("rsfm_empty_left_dispatches", query.rsfm_empty_left_dispatches);
    out.AddField("rsfm_resident_rhs_pairs_avoided", query.rsfm_resident_rhs_pairs_avoided);
    out.AddField("resident_rhs_expected_rows", query.resident_rhs_expected_rows);
    out.AddField("resident_rhs_actual_rows", query.resident_rhs_actual_rows);
    out.AddField("resident_rhs_expected_pairs", query.resident_rhs_expected_pairs);
    out.AddField("resident_rhs_actual_pairs", query.resident_rhs_actual_pairs);
    out.AddField("resident_rhs_mismatched_shards", query.resident_rhs_mismatched_shards);
    out.AddField("peer_wire_bytes", state.peer_wire_bytes.load(memory_order_relaxed));
    out.AddField("peer_wire_messages", state.peer_wire_messages.load(memory_order_relaxed));
    out.AddField("peer_e2e_us", state.peer_e2e_us.load(memory_order_relaxed));
    out.AddField("peer_e2e_messages", state.peer_e2e_messages.load(memory_order_relaxed));
    out.AddField("peer_base_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::BASE)].load(memory_order_relaxed));
    out.AddField("peer_row_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::ROW)].load(memory_order_relaxed));
    out.AddField("peer_demand_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::DEMAND)].load(memory_order_relaxed));
    out.AddField("peer_signature_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::SIGNATURE)].load(memory_order_relaxed));
    out.AddField("peer_rhs_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::RHS)].load(memory_order_relaxed));
    out.AddField("peer_lhs_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::LHS)].load(memory_order_relaxed));
    out.AddField("peer_matrix_result_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::MATRIX_RESULT)].load(memory_order_relaxed));
    out.AddField("peer_other_wire_bytes", state.peer_wire_by_kind[static_cast<size_t>(IncrementalWorkerState::PeerWireKind::OTHER)].load(memory_order_relaxed));
    static constexpr array<int, kIncrementalPerfSlots> lengths{1, 2, 4, 8, 16, 32};
    for (size_t slot = 0; slot < lengths.size(); ++slot) {
        out.AddField("emit_calls_l" + to_string(lengths[slot]), query.emit_published_calls[slot]);
        out.AddField("emit_pairs_l" + to_string(lengths[slot]), query.emit_published_pairs[slot]);
    }
    return out;
}
static Message HandleIncrementalFetchResults(IncrementalWorkerState& state, const Message& request) {
    const auto trace_begin = chrono::steady_clock::now();
    const int query_id = request.GetFieldInt("query_id");
    const bool summary_only = request.GetFieldInt("summary_only") != 0;
    string data;
    uint64_t result_pairs = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", "unknown query");
            return out;
        }
        if (summary_only) {
            result_pairs = qit->second.direct_leaf_output
                ? IncrementalCountDirectLeafResults(qit->second)
                : CountRelationPairs(qit->second.final_rows);
        } else if (qit->second.direct_leaf_output) {
            data = IncrementalSerializeDirectLeafResults(qit->second);
        } else {
            data = SerializeIncrementalFinalRows(qit->second.final_rows);
        }
    }
    Message out(MessageType::RESPONSE_DATA);
    if (summary_only) out.AddField("result_pairs", result_pairs);
    else out.data = std::move(data);
    if (IncrementalScalingTraceEnabled()) {
        ostringstream line;
        line << "[scaling-final-worker] worker=" << state.partition_id
             << " query=" << query_id << " bytes=" << out.data.size()
             << " elapsed_us=" << chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now()-trace_begin).count();
        cerr << line.str() << '\n';
    }
    return out;
}
