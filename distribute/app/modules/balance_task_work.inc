// Exactly p compute sets; physical graph ownership is unchanged.
static size_t IncrementalBalanceBucketCount(int partitions) {
    if (partitions <= 0) throw invalid_argument("balance requires positive partitions");
    return static_cast<size_t>(partitions);
}

// Before GCNP extraction, each source-mid incidence predicts one RHS row union.
static uint64_t IncrementalEstimateUnionRowWork(const RelationDstSet& mids) {
    return mids.cardinality();
}

struct IncrementalPrefixBalancePlan {
    vector<uint64_t> offsets;
    uint64_t total_work = 0, target_work = 0;
};

static IncrementalPrefixBalancePlan IncrementalPlanPrefixWork(
    const vector<uint64_t>& work) {
    IncrementalBalanceBucketCount(static_cast<int>(work.size()));
    IncrementalPrefixBalancePlan plan;
    plan.offsets.reserve(work.size());
    for (uint64_t cost : work) {
        plan.offsets.push_back(plan.total_work);
        if (cost > numeric_limits<uint64_t>::max() - plan.total_work)
            throw overflow_error("prefix work total overflow");
        plan.total_work += cost;
    }
    plan.target_work = plan.total_work / work.size() + (plan.total_work % work.size() != 0);
    return plan;
}

struct IncrementalPrefixAssignment {
    uint64_t offset = 0, target_work = 0, total_work = 0;
    uint64_t local_work = 0, local_sources = 0;
    // Exchanged rhs_rows work of every owner for this stage (owner-first).
    vector<uint64_t> owner_work;
};

// Owner-first surplus migration. Every worker computes the same plan from the
// exchanged per-owner work: T = ceil(total / p); an owner keeps its sources
// (stable-hash order) while its local prefix is below T; the remaining prefix
// positions [T, w_i) are mapped, in owner order, onto the deficits
// max(0, T - w_j) laid out in helper order. Exactly p buckets, whole-source
// tasks and rhs_rows units are unchanged; each helper receives at most
// T + one source's work, the same bound as weighted prefix stripes.
struct IncrementalOwnerFirstPlan {
    uint64_t target = 0, excess_offset = 0;
    vector<uint64_t> deficit_end;  // cumulative deficit capacity by helper
};

static IncrementalOwnerFirstPlan IncrementalPlanOwnerFirst(
    const vector<uint64_t>& work, int self) {
    IncrementalBalanceBucketCount(static_cast<int>(work.size()));
    uint64_t total = 0;
    for (uint64_t w : work) {
        if (w > numeric_limits<uint64_t>::max() - total) throw overflow_error("owner-first total overflow");
        total += w;
    }
    IncrementalOwnerFirstPlan plan;
    plan.target = total / work.size() + (total % work.size() != 0);
    uint64_t deficit = 0;
    plan.deficit_end.reserve(work.size());
    for (size_t j = 0; j < work.size(); ++j) {
        if (static_cast<int>(j) < self && work[j] > plan.target)
            plan.excess_offset += work[j] - plan.target;
        deficit += work[j] < plan.target ? plan.target - work[j] : 0;
        plan.deficit_end.push_back(deficit);
    }
    return plan;
}

static int IncrementalOwnerFirstHelper(
    const IncrementalOwnerFirstPlan& plan, uint64_t local_prefix, int self) {
    if (plan.target == 0 || local_prefix < plan.target) return self;
    const uint64_t position = plan.excess_offset + (local_prefix - plan.target);
    const auto it = upper_bound(plan.deficit_end.begin(), plan.deficit_end.end(), position);
    if (it == plan.deficit_end.end())
        throw runtime_error("owner-first excess exceeds deficit capacity");
    return static_cast<int>(it - plan.deficit_end.begin());
}

static int IncrementalPrefixWorker(uint64_t offset, uint64_t local_prefix,
    uint64_t target_work, int partitions, int origin) {
    IncrementalBalanceBucketCount(partitions);
    if (origin < 0 || origin >= partitions) throw invalid_argument("bad prefix origin");
    if (target_work == 0) return origin;
    if (local_prefix > numeric_limits<uint64_t>::max() - offset)
        throw overflow_error("prefix position overflow");
    return static_cast<int>(min<uint64_t>(partitions - 1, (offset + local_prefix) / target_work));
}

// Only scheduling metadata uses this fixed-width envelope. Matrix relation
// messages, their compression, and the RPC framing are unchanged.
struct IncrementalSourceBalanceHeader {
    uint32_t op = 0;
    int query_id = 0, origin = 0;
    uint64_t stage_id = 0, count = 0;
};

static Message IncrementalMakeSourceBalanceMessage(
    uint32_t op, int query_id, uint64_t stage_id, int origin,
    const vector<pair<uint64_t, uint64_t>>& values) {
    Message message(MessageType::INCREMENTAL_BALANCE_CONTROL);
    message.data.reserve(32 + values.size() * 16);
    AppendRawU32(message.data, 0x31504c53U); // SLP1
    AppendRawU32(message.data, op);
    AppendRawU32(message.data, static_cast<uint32_t>(query_id));
    AppendRawU32(message.data, static_cast<uint32_t>(origin));
    AppendRawU64(message.data, stage_id);
    AppendRawU64(message.data, values.size());
    for (const auto& [source, value] : values) {
        AppendRawU64(message.data, source);
        AppendRawU64(message.data, value);
    }
    return message;
}

static IncrementalSourceBalanceHeader IncrementalReadSourceBalanceHeader(
    const Message& message) {
    size_t offset = 0;
    uint32_t magic = 0, query = 0, origin = 0;
    IncrementalSourceBalanceHeader header;
    if (!ReadRawU32(message.data, offset, magic) || magic != 0x31504c53U ||
        !ReadRawU32(message.data, offset, header.op) ||
        !ReadRawU32(message.data, offset, query) ||
        !ReadRawU32(message.data, offset, origin) ||
        !ReadRawU64(message.data, offset, header.stage_id) ||
        !ReadRawU64(message.data, offset, header.count) ||
        (header.op != 1 && header.op != 2 && header.op != 3) ||
        query > static_cast<uint32_t>(numeric_limits<int>::max()) ||
        origin > static_cast<uint32_t>(numeric_limits<int>::max()) ||
        header.count != (message.data.size() - offset) / 16 ||
        (message.data.size() - offset) % 16 != 0)
        throw runtime_error("bad source-work balance envelope");
    header.query_id = static_cast<int>(query);
    header.origin = static_cast<int>(origin);
    return header;
}

static vector<pair<uint64_t, uint64_t>> IncrementalReadSourceBalanceValues(
    const Message& message, const IncrementalSourceBalanceHeader& header) {
    vector<pair<uint64_t, uint64_t>> values;
    values.reserve(static_cast<size_t>(header.count));
    size_t offset = 32;
    for (uint64_t i = 0; i < header.count; ++i) {
        uint64_t source, value;
        if (!ReadRawU64(message.data, offset, source) ||
            !ReadRawU64(message.data, offset, value))
            throw runtime_error("truncated source-work balance values");
        values.emplace_back(source, value);
    }
    return values;
}
