// Message admission for the synchronous middle-owner data plane.
//
// The relation itself remains source-owned.  This file only maintains a
// per-compose admission ledger:
//   left_seen[m]       sources that have A(s,m)
//   rhs_present[m]     m for which B(m,*) is known to be non-empty
//   left_sent/right_sent exact-once physical delivery ledgers
//
// A left pair is admitted only after both sides are known.  If the left side
// arrives first it stays in the source owner's batch and is released when the
// compact RHS presence event arrives.  No endpoint payload is sent merely to
// announce presence.

static void SyncMessageRecordLeft(
    IncrementalDemandBatch& batch, const RelationRows& fresh_left) {
    for (const auto& [source, mids] : fresh_left) {
        for (uint64_t middle : mids) {
            batch.middle_sources_by_middle[middle].bitmap.add(source);
        }
    }
}

static void SyncMessageMarkRhsPresent(
    IncrementalDemandBatch& batch, const RelationDstSet& mids) {
    for (uint64_t middle : mids) batch.middle_rhs_present.insert(middle);
}

static RelationRows SyncMessageBuildLeftCandidatesForPresence(
    IncrementalDemandBatch& batch, const RelationDstSet& mids) {
    RelationRows candidates;
    for (uint64_t middle : mids) {
        batch.middle_rhs_present.insert(middle);
        auto sources = batch.middle_sources_by_middle.find(middle);
        if (sources == batch.middle_sources_by_middle.end()) continue;
        for (uint64_t source : sources->second) {
            candidates[source].push_back(middle);
        }
    }
    return candidates;
}

static void SyncMessageBuildRoute(
    IncrementalDemandBatch& batch,
    const RelationRows& fresh_left,
    const RelationRows& fresh_right,
    RelationRows& routed_left,
    RelationRows& routed_right) {
    SyncMessageRecordLeft(batch, fresh_left);
    for (const auto& [middle, destinations] : fresh_right) {
        if (!destinations.empty()) batch.middle_rhs_present.insert(middle);
    }

    RelationRows left_candidates;
    RelationRows right_candidates;
    RelationDstSet touched;
    for (const auto& [source, mids] : fresh_left) {
        (void)source;
        touched.bitmap |= mids.bitmap;
    }
    for (const auto& [middle, destinations] : fresh_right) {
        if (!destinations.empty()) touched.push_back(middle);
    }

    for (uint64_t middle : touched) {
        if (batch.middle_rhs_present.count(middle) == 0) continue;
        auto sources = batch.middle_sources_by_middle.find(middle);
        if (sources != batch.middle_sources_by_middle.end()) {
            for (uint64_t source : sources->second) {
                left_candidates[source].push_back(middle);
            }
        }
        auto rhs = batch.right_rows.find(middle);
        if (rhs != batch.right_rows.end() && !rhs->second.empty()) {
            right_candidates.emplace(middle, rhs->second);
        }
    }

    routed_left = IncrementalDifferenceRows(left_candidates, batch.middle_sent_left);
    routed_right = IncrementalDifferenceRows(right_candidates, batch.middle_sent_right);
    IncrementalMergeRows(batch.middle_sent_left, routed_left);
    IncrementalMergeRows(batch.middle_sent_right, routed_right);
}

