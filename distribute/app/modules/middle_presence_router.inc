// Source-side presence routing for middle-partition composition.

static void MiddlePresenceRecordLeft(
    IncrementalDemandBatch& batch, const RelationRows& fresh_left) {
    for (const auto& [source, mids] : fresh_left) {
        (void)source;
        for (uint64_t middle : mids) {
            batch.middle_sources_by_middle[middle].push_back(source);
        }
    }
}

static void MiddlePresenceBuildRoute(
    IncrementalDemandBatch& batch,
    const RelationRows& fresh_left,
    const RelationRows& fresh_right,
    RelationRows& routed_left,
    RelationRows& routed_right) {
    unordered_set<uint64_t> touched_mids;
    for (const auto& [source, mids] : fresh_left) {
        (void)source;
        for (uint64_t middle : mids) touched_mids.insert(middle);
    }
    for (const auto& [middle, destinations] : fresh_right) {
        if (!destinations.empty()) touched_mids.insert(middle);
    }

    RelationRows left_candidates;
    RelationRows right_candidates;
    for (uint64_t middle : touched_mids) {
        auto left_it = batch.middle_sources_by_middle.find(middle);
        auto right_it = batch.right_rows.find(middle);
        if (left_it == batch.middle_sources_by_middle.end() ||
            right_it == batch.right_rows.end() ||
            right_it->second.empty()) {
            continue;
        }
        for (uint64_t source : left_it->second) {
            left_candidates[source].push_back(middle);
        }
        right_candidates[middle] = right_it->second;
    }

    routed_left = IncrementalDifferenceRows(
        left_candidates, batch.middle_sent_left);
    routed_right = IncrementalDifferenceRows(
        right_candidates, batch.middle_sent_right);
    IncrementalMergeRows(batch.middle_sent_left, routed_left);
    IncrementalMergeRows(batch.middle_sent_right, routed_right);
}

// Synchronous middle composition keeps the RHS payload at owner(mid), so the
// source owner receives only a presence stream. Route the retained left rows
// for newly usable mids without pretending the compact marker is RHS data.
static void MiddlePresenceBuildLeftRouteFromKeys(
    IncrementalDemandBatch& batch, const RelationRows& presence_rows,
    RelationRows& routed_left) {
    for (const auto& [middle, _] : presence_rows) {
        batch.middle_rhs_present.insert(middle);
        auto left_it = batch.middle_sources_by_middle.find(middle);
        if (left_it == batch.middle_sources_by_middle.end()) continue;
        for (uint64_t source : left_it->second) {
            routed_left[source].push_back(middle);
        }
    }
    routed_left = IncrementalDifferenceRows(routed_left, batch.middle_sent_left);
    IncrementalMergeRows(batch.middle_sent_left, routed_left);
}

static void MiddlePresenceBuildLeftRouteForKnownKeys(
    IncrementalDemandBatch& batch, const RelationRows& fresh_left,
    RelationRows& routed_left) {
    for (const auto& [source, mids] : fresh_left) {
        for (uint64_t middle : mids) {
            if (batch.middle_rhs_present.count(middle) == 0) continue;
            routed_left[source].push_back(middle);
        }
    }
    routed_left = IncrementalDifferenceRows(routed_left, batch.middle_sent_left);
    IncrementalMergeRows(batch.middle_sent_left, routed_left);
}
