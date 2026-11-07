static constexpr size_t kFinalSourceLanes = rpq_final::kFinalThreads;

static vector<RelationRows> PartitionFinalRows(RelationRows rows) {
    vector<RelationRows> shards(kFinalSourceLanes);
    if constexpr (kFinalSourceLanes == 1) {
        // Serial collection already has its only source lane. Keep its nodes.
        shards.front() = std::move(rows);
        return shards;
    }
    for (auto& shard : shards) shard.reserve(rows.size() / kFinalSourceLanes + 1);
    while (!rows.empty()) {
        auto node = rows.extract(rows.begin());
        const size_t lane = hash<uint64_t>{}(node.key()) % kFinalSourceLanes;
        shards[lane].insert(std::move(node));
    }
    return shards;
}

struct ParallelFinalRows {
    vector<RelationRows> shards;
    uint64_t rows = 0, pairs = 0;
};

// Equal sources always enter the same lane, so duplicate pairs are still unioned.
static ParallelFinalRows ReduceFinalRows(vector<vector<RelationRows>> incoming) {
    ParallelFinalRows result;
    result.shards.resize(kFinalSourceLanes);
    vector<future<pair<uint64_t, uint64_t>>> reducers;
    for (size_t lane = 0; lane < kFinalSourceLanes; ++lane) {
        reducers.push_back(async(launch::deferred, [&, lane] {
            auto& target = result.shards[lane];
            size_t capacity = 0;
            for (const auto& worker : incoming) capacity += worker[lane].size();
            if (!incoming.empty()) target = std::move(incoming.front()[lane]);
            target.reserve(capacity);
            for (auto& worker : incoming) {
                auto& source = worker[lane];
                target.merge(source);
                for (const auto& [vertex, values] : source) target.at(vertex) |= values;
                source.clear();
            }
            return make_pair(static_cast<uint64_t>(target.size()), CountRelationPairs(target));
        }));
    }
    for (auto& reducer : reducers) {
        auto counts = reducer.get();
        result.rows += counts.first;
        result.pairs += counts.second;
    }
    return result;
}
