static void IncrementalDispatchDirectBalancePanel(
    IncrementalWorkerState&, int, uint64_t, IncrementalPrefixAssignment);

// Collect exchanged per-owner stage work; when all p reports (including this
// worker's own, recorded only after its panel is published) are present,
// dispatch the local panel with the deterministic owner-first plan.
static void IncrementalRecordStageWork(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id, int origin,
    uint64_t work, uint64_t sources,
    chrono::steady_clock::time_point begin = chrono::steady_clock::now()) {
    if (origin < 0 || origin >= state.total_partitions)
        throw runtime_error("bad owner-first work origin");
    IncrementalPrefixAssignment assignment;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto& reports = qit->second.balance_reports;
        auto& pending = reports[stage_id].costs;
        if (!pending.emplace(origin, vector<pair<uint64_t, uint64_t>>{{work, sources}}).second)
            throw runtime_error("duplicate owner-first work report");
        if (pending.size() != static_cast<size_t>(state.total_partitions)) return;
        assignment.owner_work.resize(state.total_partitions);
        for (int owner = 0; owner < state.total_partitions; ++owner) {
            const auto& value = pending.at(owner).at(0);
            assignment.owner_work[owner] = value.first;
            if (owner == state.partition_id) {
                assignment.local_work = value.first;
                assignment.local_sources = value.second;
            }
        }
        reports.erase(stage_id);
    }
    for (uint64_t w : assignment.owner_work) assignment.total_work += w;
    assignment.target_work = assignment.total_work / state.total_partitions +
        (assignment.total_work % state.total_partitions != 0);
    const auto queued = chrono::steady_clock::now();
    state.matrix_message_executor.Submit(
        [&state, query_id, stage_id, assignment = std::move(assignment), begin, queued]() mutable {
            try {
                if (IncrementalScalingTraceEnabled()) {
                    const auto dispatched = chrono::steady_clock::now();
                    ostringstream line;
                    line << "[local-stage-dispatch] worker=" << state.partition_id
                         << " query=" << query_id << " stage=" << stage_id
                         << " prepare_us=" << chrono::duration_cast<chrono::microseconds>(queued-begin).count()
                         << " queue_us=" << chrono::duration_cast<chrono::microseconds>(dispatched-queued).count()
                         << " allocation_rpcs=0 local_work=" << assignment.local_work;
                    cerr << line.str() << '\n';
                }
                IncrementalDispatchDirectBalancePanel(state, query_id, stage_id, std::move(assignment));
            } catch (const exception& error) {
                IncrementalRecordPeerFailure(state, query_id, state.partition_id,
                    MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
            }
        });
}

// Schedule complete source rows before routing to physical middle owners.
// Graph/RHS ownership stays fixed; helpers pull only their required RHS rows.
static void IncrementalStartDirectBalanceCompose(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id,
    const RelationRows& left, const RelationRows& right,
    function<void(RelationRows)> done, const RelationRows* coverage,
    int open_body_id, IncrementalComposeInputOwners owners) {
    const auto begin = chrono::steady_clock::now();
    const bool home_output = owners.home_output;
    IncrementalBalancedPanel panel;
    if (!IncrementalOwnerInputViewsEnabled()) owners = {};
    panel.restore_coverage_output = owners.restore_coverage_output;
    if (!owners.left) panel.left = left;
    panel.input_owners.left = std::move(owners.left);
    panel.input_owners.coverage = std::move(owners.coverage);
    panel.open_body_id = open_body_id;
    if (coverage != nullptr && !panel.input_owners.coverage) panel.coverage = *coverage;
    uint64_t total_work = 0;
    for (const auto& [source, mids] : left) {
        // Every source has one result home, independent of graph ownership.
        if (IncrementalResultHome(state, source) != state.partition_id) {
            throw runtime_error("direct balance requires home-placed LHS");
        }
        const uint64_t work = IncrementalEstimateUnionRowWork(mids);
        if (work > numeric_limits<uint64_t>::max() - total_work)
            throw overflow_error("local prefix work overflow");
        total_work += work;
    }
    // Advertise actual producer keys, not owner(mid): bounded identities can
    // occur outside that owner, and every matching producer must contribute.
    if (IncrementalScalingTraceEnabled()) {
        ostringstream line;
        line << "[stage-timeline] ev=owner_start worker=" << state.partition_id
             << " query=" << query_id << " stage=" << stage_id
             << " wall_us=" << IncrementalTraceWallUs() << " rows=" << left.size();
        cerr << line.str() << '\n';
    }
    const bool retained_rhs = static_cast<bool>(owners.right);
    auto rhs = owners.right ? std::move(owners.right) : make_shared<const RelationRows>(right);
    if (open_body_id < 0) {
        for (const auto& [mid, targets] : right) {
            if (!targets.empty()) panel.rhs_keys.push_back(mid);
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        if (panel.input_owners.left) query.owner_view_left_rows += left.size();
        if (panel.input_owners.coverage) query.owner_view_coverage_rows += coverage->size();
        if (retained_rhs) ++query.owner_view_rhs_inputs;
        auto& output = query.global_exact_output_stages[stage_id];
        output = IncrementalGlobalExactOutputStage{};
        output.expected_middles = state.total_partitions;
        if (home_output) query.home_output_stages.insert(stage_id);
        // Empty intersections no longer send an RHS request. Keep producer
        // inputs until every helper has returned its output to this owner:
        // each such result causally follows that helper's complete RHS input.
        // Queued encoders retain their own shared owners independently.
        output.done = [&state, query_id, stage_id, done = std::move(done)](
            RelationRows rows) mutable {
            {
                lock_guard<mutex> lk(state.mu);
                auto query = state.queries.find(query_id);
                if (query == state.queries.end()) return;
                query->second.balance_direct_rhs.erase(stage_id);
                query->second.balance_rhs_wire_cache.erase(stage_id);
                query->second.balance_direct_requests.erase(stage_id);
            }
            if (done) done(std::move(rows));
        };
        if (!query.balance_panels.emplace(stage_id, std::move(panel)).second ||
            (open_body_id < 0 &&
             !query.balance_direct_rhs.emplace(stage_id, std::move(rhs)).second)) {
            throw runtime_error("duplicate direct balance stage");
        }
        if (open_body_id < 0)
            query.balance_rhs_wire_cache.emplace(stage_id, make_shared<BicliqueImmutableWireCache>());
        query.balance_input_route_us += chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - begin).count();
    }
    // Owner-first balancing needs every owner's rhs_rows work for this stage.
    // Exchange one (work, sources) pair with every peer; each worker then
    // computes the identical plan. The panel is published above first.
    Message report = IncrementalMakeSourceBalanceMessage(
        3, query_id, stage_id, state.partition_id, {{total_work, left.size()}});
    for (int peer = 0; peer < state.total_partitions; ++peer)
        if (peer != state.partition_id) IncrementalEnqueuePeer(state, peer, report);
    IncrementalRecordStageWork(state, query_id, stage_id, state.partition_id,
        total_work, left.size(), begin);
}

static void IncrementalStartBalancedOpenClosure(
    IncrementalWorkerState& state, int query_id, int body_id,
    uint64_t stage_id, RelationRows prefix, function<void(RelationRows)> done) {
    // Only the temporary source-frontier task moves. The replicated SCC plan
    // is immutable except for its existing worker-local lazy closure cache.
    IncrementalStartDirectBalanceCompose(
        state, query_id, stage_id, prefix, RelationRows{},
        std::move(done), nullptr, body_id);
}

static void IncrementalDispatchDirectBalancePanel(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id,
    IncrementalPrefixAssignment assignment) {
    const auto dispatch_begin = chrono::steady_clock::now();
    IncrementalBalancedPanel panel;
    {
        lock_guard<mutex> lk(state.mu);
        auto& panels = state.queries.at(query_id).balance_panels;
        auto found = panels.find(stage_id);
        if (found == panels.end()) throw runtime_error("missing direct panel");
        panel = std::move(found->second);
        panels.erase(found);
    }
    const auto& source_rows = panel.input_owners.left ? *panel.input_owners.left : panel.left;
    if (assignment.local_sources != source_rows.size())
        throw runtime_error("prefix assignment source count mismatch");
    uint64_t local_prefix = 0;
    vector<uint64_t> routed_work(state.total_partitions, 0);
    // Spread each producer's original partition across the same p buckets.
    // Row weights and indivisible source tasks stay unchanged. Rotating the
    // local weighted stripes avoids aligning all producers' boundary rows.
    const uint64_t local_target = assignment.local_work / state.total_partitions +
        (assignment.local_work % state.total_partitions != 0);
    if (assignment.owner_work.size() != static_cast<size_t>(state.total_partitions) ||
        assignment.owner_work[state.partition_id] != assignment.local_work)
        throw runtime_error("owner-first work vector mismatch");
    const auto owner_plan = IncrementalPlanOwnerFirst(assignment.owner_work, state.partition_id);
    auto destination = [&](const RelationDstSet& mids) {
        const auto work = IncrementalEstimateUnionRowWork(mids);
        if (local_prefix > assignment.local_work || work > assignment.local_work - local_prefix)
            throw runtime_error("prefix assignment changed local work");
        const int worker = IncrementalOwnerFirstHelper(owner_plan, local_prefix, state.partition_id);
        local_prefix += work;
        routed_work[worker] += work;
        return worker;
    };
    vector<IncrementalMatrixJoinUpdate> updates(state.total_partitions);
    // Prefix weights and p buckets are unchanged. Enumerate sources by a
    // stable permutation so hash-table/partition locality does not concentrate
    // rows with similar bitmap shapes in the same contiguous prefix range.
    vector<uint64_t> ordered_sources;
    ordered_sources.reserve(source_rows.size());
    for (const auto& entry : source_rows) ordered_sources.push_back(entry.first);
    sort(ordered_sources.begin(), ordered_sources.end(), [](uint64_t a, uint64_t b) {
        const auto ha = StableHash64(a), hb = StableHash64(b);
        return ha != hb ? ha < hb : a < b;
    });
    if (panel.input_owners.left) {
      const auto& cover_rows = panel.input_owners.coverage ? *panel.input_owners.coverage : panel.coverage;
      for (uint64_t source : ordered_sources) {
        const auto& mids = panel.input_owners.left->at(source);
        const int helper = destination(mids);
        auto& update = updates.at(helper);
        update.left_row_views.emplace_back(source, &mids);
        auto cover = cover_rows.find(source);
        if (cover != cover_rows.end()) {
            if (panel.input_owners.coverage)
                update.coverage_row_views.emplace_back(source, &cover->second);
            else update.coverage_rows.emplace(source, std::move(panel.coverage.at(source)));
        }
      }
    } else {
      for (uint64_t source : ordered_sources) {
        auto& mids = panel.left.at(source);
        const int helper = destination(mids);
        auto& update = updates.at(helper);
        update.left_rows.emplace(source, std::move(mids));
        auto cover = panel.coverage.find(source);
        if (cover != panel.coverage.end())
            update.coverage_rows.emplace(source, std::move(cover->second));
      }
    }
    if (local_prefix != assignment.local_work)
        throw runtime_error("prefix assignment lost local work");
    const auto dispatch_us = chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - dispatch_begin).count();
    {
        lock_guard<mutex> lk(state.mu);
        state.queries.at(query_id).balance_dispatch_us += dispatch_us;
    }
    ostringstream diagnostic;
    diagnostic << "[weighted-prefix-route] worker=" << state.partition_id
        << " stage=" << stage_id << " sources=" << source_rows.size()
        << " offset=" << assignment.offset << " local_work=" << local_prefix
        << " target_work=" << assignment.target_work << " dispatch_us=" << dispatch_us
        << " assignment=owner_first_surplus target=" << owner_plan.target << " local_target=" << local_target
        << " worker_work=";
    for (size_t i = 0; i < routed_work.size(); ++i)
        diagnostic << (i ? "," : "") << i << ":" << routed_work[i];
    diagnostic << '\n';
    cerr << diagnostic.str();
    for (int helper = 0; helper < state.total_partitions; ++helper) {
        auto& update = updates[helper];
        update.restore_coverage_output = panel.restore_coverage_output;
        update.left_view_owner = panel.input_owners.left;
        update.coverage_view_owner = panel.input_owners.coverage;
        update.query_id = query_id;
        update.global_exact_stage = true;
        update.middle_compute_task = true;
        update.middle_virtual_shard = panel.open_body_id == -2 ? 5 : panel.open_body_id < 0 ? 1 : 4;
        update.right_body_id = panel.open_body_id;
        update.global_stage_id = stage_id;
        update.global_sender_pid = state.partition_id;
        update.middle_partition = state.partition_id;
        update.right_sources = panel.rhs_keys;
        {
            lock_guard<mutex> lk(state.mu);
            if (helper != state.partition_id &&
                (!update.left_rows.empty() || !update.left_row_views.empty())) {
                ++state.queries.at(query_id).balance_remote_tasks;
            }
        }
        const auto lhs_queued = chrono::steady_clock::now();
        state.matrix_message_executor.Submit(
            [&state, helper, query_id, stage_id, lhs_queued, update = std::move(update)]() mutable {
                try {
                    const bool trace = IncrementalScalingTraceEnabled();
                    const size_t rows = update.left_rows.size() + update.left_row_views.size();
                    const auto lhs_begin = chrono::steady_clock::now();
                    IncrementalMatrixEncodingTiming encoding;
                    IncrementalSendGlobalExactUpdate(state, helper, std::move(update),
                        trace ? &encoding : nullptr);
                    if (trace) {
                        ostringstream line;
                        line << "[lhs-panel-timing] worker=" << state.partition_id
                             << " query=" << query_id << " stage=" << stage_id
                             << " helper=" << helper << " rows=" << rows
                             << " queue_us=" << chrono::duration_cast<chrono::microseconds>(lhs_begin - lhs_queued).count()
                             << " total_us=" << chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - lhs_begin).count()
                             << " encode_us=" << encoding.encode_us << " wrap_us=" << encoding.wrap_us
                             << " raw_bytes=" << encoding.raw_bytes << " wire_bytes=" << encoding.wire_bytes
                             << " wall_end_us=" << IncrementalTraceWallUs();
                        cerr << line.str() << '\n';
                    }
                } catch (const exception& error) {
                    IncrementalRecordPeerFailure(state, query_id, helper,
                        MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
                }
            });
    }
}

static void IncrementalAcceptDirectBalanceTask(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    const int query_id = update.query_id;
    const uint64_t stage_id = update.global_stage_id;
    const int sender = update.global_sender_pid;
    if (sender < 0 || sender >= state.total_partitions) {
        throw runtime_error("invalid direct balance sender");
    }
    if (update.middle_virtual_shard == 2) {
        shared_ptr<const RelationRows> rhs;
        shared_ptr<BicliqueImmutableWireCache> wire_cache;
        {
            lock_guard<mutex> lk(state.mu);
            auto& query = state.queries.at(query_id);
            rhs = query.balance_direct_rhs.at(stage_id);
            wire_cache = query.balance_rhs_wire_cache.at(stage_id);
            // Helpers request disjoint key deltas as LHS panels arrive.
            // Retain immutable RHS/cache until every helper's output seals;
            // one early reply no longer means that helper has all its keys.
            if (update.right_sources.empty())
                throw runtime_error("empty pipelined RHS request");
        }
        const auto queued = chrono::steady_clock::now();
        state.matrix_message_executor.Submit(
            [&state, query_id, stage_id, sender, rhs, wire_cache, queued,
             keys = std::move(update.right_sources)]() mutable {
                try {
                    const auto select_begin = chrono::steady_clock::now();
                    IncrementalMatrixJoinUpdate reply;
                    reply.query_id = query_id;
                    reply.global_exact_stage = true;
                    reply.middle_compute_task = true;
                    reply.middle_virtual_shard = 3;
                    reply.global_stage_id = stage_id;
                    reply.global_sender_pid = state.partition_id;
                    const bool local = sender == state.partition_id;
                    if (!local) reply.right_wire_cache = wire_cache;
                    auto select_row = [&](uint64_t mid, const RelationDstSet& targets) {
                        if (local) reply.right_rows.emplace(mid, targets);
                        else reply.right_row_views.emplace_back(mid, &targets);
                    };
                    const bool scan_local = rhs->size() < keys.cardinality();
                    if (scan_local) {
                        for (const auto& [mid, targets] : *rhs) {
                            if (!targets.empty() && keys.bitmap.contains(mid)) {
                                select_row(mid, targets);
                            }
                        }
                    } else {
                        for (uint64_t mid : keys) {
                            auto found = rhs->find(mid);
                            if (found != rhs->end() && !found->second.empty()) {
                                select_row(mid, found->second);
                            }
                        }
                    }
                    const auto selected = chrono::steady_clock::now();
                    IncrementalMatrixEncodingTiming encoding;
                    IncrementalSendGlobalExactUpdate(state, sender, std::move(reply), &encoding);
                    const auto packed = chrono::steady_clock::now();
                    if (IncrementalScalingTraceEnabled()) {
                        ostringstream line;
                        line << "[rhs-request-timing] query=" << query_id
                             << " stage=" << stage_id << " producer=" << state.partition_id
                             << " helper=" << sender << " keys=" << keys.cardinality()
                             << " queue_us=" << chrono::duration_cast<chrono::microseconds>(
                                    select_begin - queued).count()
                             << " select_us=" << chrono::duration_cast<chrono::microseconds>(
                                    selected - select_begin).count()
                             << " encode_us=" << encoding.encode_us
                             << " wrap_us=" << encoding.wrap_us
                             << " prepare_enqueue_us=" << chrono::duration_cast<chrono::microseconds>(
                                    packed - selected).count()
                             << " raw_bytes=" << encoding.raw_bytes
                             << " wire_bytes=" << encoding.wire_bytes;
                        cerr << line.str() << '\n';
                    }
                    if (!local && IncrementalScalingTraceEnabled()) {
                        cerr << "[rhs-wire-cache] stage=" << stage_id
                             << " entries=" << wire_cache->entries.size()
                             << " hits=" << wire_cache->hits
                             << " misses=" << wire_cache->misses
                             << " bytes=" << wire_cache->serialized_bytes
                             << " pair_entries=" << wire_cache->pair_parts.size()
                             << " pair_hits=" << wire_cache->pair_hits
                             << " pair_misses=" << wire_cache->pair_misses
                             << " equality_hits=" << wire_cache->equality_hits
                             << " equality_misses=" << wire_cache->equality_misses
                             << '\n';
                    }
                    lock_guard<mutex> lk(state.mu);
                    auto& query = state.queries.at(query_id);
                    query.balance_rhs_select_us += chrono::duration_cast<chrono::microseconds>(
                        selected - select_begin).count();
                    query.balance_rhs_pack_us += chrono::duration_cast<chrono::microseconds>(
                        packed - selected).count();
                    query.balance_rhs_queue_us += chrono::duration_cast<chrono::microseconds>(
                        select_begin - queued).count();
                    query.balance_rhs_encode_us += encoding.encode_us;
                    query.balance_rhs_wrap_us += encoding.wrap_us;
                    query.balance_rhs_raw_bytes += encoding.raw_bytes;
                    query.balance_rhs_wire_bytes += encoding.wire_bytes;
                    query.balance_rhs_examined_rows += scan_local ? rhs->size() : keys.cardinality();
                    ++query.balance_rhs_requests;
                } catch (const exception& error) {
                    IncrementalRecordPeerFailure(state, query_id, sender,
                        MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
                }
            });
        return;
    }
    bool request_rhs = false, execute = false;
    vector<RelationDstSet> requested_by_producer;
    uint64_t requested_keys = 0, avoided_keys = 0, empty_requests_avoided = 0;
    IncrementalMatrixJoinUpdate combined;
    const auto begin = chrono::steady_clock::now();
    uint64_t trace_wait_us = 0, trace_apply_us = 0, trace_need_us = 0, trace_directory_us = 0;
    uint64_t trace_collect_us = 0;
    uint64_t trace_sources = 0, trace_rhs = 0, trace_needed = 0;
    uint64_t trace_lhs_ready_us = 0, trace_rhs_ready_us = 0;
    shared_ptr<IncrementalDirectBalanceStage> pending;
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        auto& entry = query.balance_direct_stages[stage_id];
        if (!entry) entry = make_shared<IncrementalDirectBalanceStage>();
        pending = entry;
    }
    {
        // Large panel merges and required-key unions must not block unrelated
        // GCNP tasks, output owners, status RPCs, or other compose stages.
        lock_guard<mutex> lk(pending->mu);
        const auto acquired = chrono::steady_clock::now();
        trace_wait_us = chrono::duration_cast<chrono::microseconds>(acquired-begin).count();
        auto& stage = pending->inputs;
        const bool open_closure = update.middle_virtual_shard == 4;
        const bool virtual_transition = update.middle_virtual_shard == 5;
        const bool left_contribution = update.middle_virtual_shard == 1 || open_closure || virtual_transition;
        if (pending->rhs_keys_by_producer.empty()) {
            pending->rhs_keys_by_producer.resize(state.total_partitions);
            pending->requested_rhs_keys_by_producer.resize(state.total_partitions);
            pending->rhs_replies_pending.resize(state.total_partitions, 0);
        }
        auto& seals = left_contribution
            ? stage.sealed_senders : stage.rhs_sealed_senders;
        if ((update.middle_virtual_shard != 1 && update.middle_virtual_shard != 3 &&
             !open_closure && !virtual_transition) ||
            (left_contribution && seals.contains(static_cast<uint32_t>(sender)))) {
            throw runtime_error("bad or duplicate direct balance contribution");
        }
        if (left_contribution) {
            if (pending->first_lhs_received == chrono::steady_clock::time_point{})
                pending->first_lhs_received = chrono::steady_clock::now();
            stage.restore_coverage_output = stage.restore_coverage_output || update.restore_coverage_output;
            if (!open_closure) {
                pending->rhs_keys_by_producer.at(sender) = std::move(update.right_sources);
                unordered_set<const void*> merged_storage;
                vector<const roaring::Roaring64Map*> inputs;
                inputs.reserve(update.left_rows.size());
                merged_storage.reserve(update.left_rows.size());
                for (const auto& [source, mids] : update.left_rows) {
                    if (!mids.empty() && merged_storage.insert(mids.bitmap.identity()).second)
                        inputs.push_back(&mids.bitmap.view());
                }
                pending->needed_rhs_keys.bitmap |= roaring::Roaring64Map::fastunion(
                    inputs.size(), inputs.data());
            }
            stage.global_virtual_stage = stage.global_virtual_stage || virtual_transition;
            if (open_closure) {
                if (stage.base_body_id >= 0 && stage.base_body_id != update.right_body_id) {
                    throw runtime_error("inconsistent open closure body");
                }
                stage.base_body_id = update.right_body_id;
            }
            // Keep the received hash nodes intact until the existing LHS
            // seal. Repeated exact-size reserve() rehashes all earlier source
            // rows whenever another producer contributes a panel.
            pending->left_panels.push_back(std::move(update.left_rows));
            pending->coverage_panels.push_back(std::move(update.coverage_rows));
        } else {
            if (pending->rhs_replies_pending.at(sender) == 0)
                throw runtime_error("unexpected pipelined RHS reply");
            --pending->rhs_replies_pending.at(sender);
            if (stage.right_rows.empty()) {
                stage.right_rows = std::move(update.right_rows);
            } else {
                const size_t rows = stage.right_rows.size() + update.right_rows.size();
                if (rows > stage.right_rows.bucket_count() * stage.right_rows.max_load_factor())
                    stage.right_rows.reserve(rows);
                stage.right_rows.merge(update.right_rows);
                // Multiple producers may legitimately supply the same RHS key.
                // Only those remaining nodes require the original bitmap OR.
                for (const auto& [mid, targets] : update.right_rows)
                    stage.right_rows.at(mid).bitmap |= targets.bitmap;
            }
        }
        if (left_contribution) seals.add(static_cast<uint32_t>(sender));
        const auto applied = chrono::steady_clock::now();
        trace_apply_us = chrono::duration_cast<chrono::microseconds>(applied-acquired).count();
        if (left_contribution &&
            seals.cardinality() == static_cast<uint64_t>(state.total_partitions)) {
            size_t left_count = 0, coverage_count = 0;
            for (const auto& panel : pending->left_panels) left_count += panel.size();
            for (const auto& panel : pending->coverage_panels) coverage_count += panel.size();
            stage.left_rows.reserve(left_count);
            stage.coverage_rows.reserve(coverage_count);
            for (auto& panel : pending->left_panels) {
                stage.left_rows.merge(panel);
                if (!panel.empty()) throw runtime_error("source assigned more than once");
            }
            for (auto& panel : pending->coverage_panels) stage.coverage_rows.merge(panel);
            pending->left_panels.clear();
            pending->coverage_panels.clear();
            trace_collect_us = chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now()-applied).count();
        }
        if (left_contribution && !open_closure) {
            const auto collected = chrono::steady_clock::now();
            const auto& needed = pending->needed_rhs_keys;
            const auto needed_done = chrono::steady_clock::now();
            trace_need_us = chrono::duration_cast<chrono::microseconds>(needed_done-collected).count();
            trace_needed = needed.cardinality();
            requested_by_producer.resize(state.total_partitions);
            for (int producer = 0; producer < state.total_partitions; ++producer) {
                if (!stage.sealed_senders.contains(static_cast<uint32_t>(producer))) continue;
                requested_by_producer[producer].bitmap = needed.bitmap &
                    pending->rhs_keys_by_producer[producer].bitmap;
                requested_by_producer[producer].bitmap -=
                    pending->requested_rhs_keys_by_producer[producer].bitmap;
                requested_keys += requested_by_producer[producer].cardinality();
                if (requested_by_producer[producer].empty()) continue;
                pending->requested_rhs_keys_by_producer[producer] |= requested_by_producer[producer];
                ++pending->rhs_replies_pending[producer];
                request_rhs = true;
            }
            if (request_rhs && pending->rhs_request_started == chrono::steady_clock::time_point{})
                pending->rhs_request_started = chrono::steady_clock::now();
            trace_directory_us = chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now()-needed_done).count();
        }
        const bool lhs_complete = stage.sealed_senders.cardinality() ==
            static_cast<uint64_t>(state.total_partitions);
        if (lhs_complete && !open_closure) {
            // No further key delta can appear after the complete LHS seal.
            // Replies may arrive in any order; start GCNP only after every
            // request counted before send has returned and merged its rows.
            for (int producer = 0; producer < state.total_partitions; ++producer) {
                if (pending->rhs_replies_pending[producer] == 0)
                    stage.rhs_sealed_senders.add(static_cast<uint32_t>(producer));
                if (left_contribution) {
                    const uint64_t requested = pending->requested_rhs_keys_by_producer[producer].cardinality();
                    avoided_keys += pending->needed_rhs_keys.cardinality() - requested;
                    if (requested == 0) ++empty_requests_avoided;
                }
            }
            if (left_contribution)
                trace_lhs_ready_us = chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - pending->first_lhs_received).count();
        }
        if (stage.sealed_senders.cardinality() == static_cast<uint64_t>(state.total_partitions) &&
            (open_closure || stage.rhs_sealed_senders.cardinality() ==
                static_cast<uint64_t>(state.total_partitions))) {
            combined.query_id = query_id;
            combined.global_exact_stage = true;
            combined.global_stage_id = stage_id;
            combined.middle_partition = state.partition_id;
            combined.middle_preferred_compute_pid = -1;
            combined.defer_factorized_materialization = true;
            combined.global_virtual_stage = stage.global_virtual_stage;
            combined.middle_virtual_shard = open_closure ? 4 : 0;
            combined.right_body_id = stage.base_body_id;
            trace_sources = stage.left_rows.size();
            trace_rhs = stage.right_rows.size();
            combined.left_rows = std::move(stage.left_rows);
            combined.right_rows = std::move(stage.right_rows);
            combined.coverage_rows = std::move(stage.coverage_rows);
            combined.restore_coverage_output = stage.restore_coverage_output;
            execute = true;
            if (pending->rhs_request_started != chrono::steady_clock::time_point{})
                trace_rhs_ready_us = chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - pending->rhs_request_started).count();
        }
    }
    if (IncrementalScalingTraceEnabled()) {
        ostringstream line;
        line << "[scaling-input] worker=" << state.partition_id << " query=" << query_id
             << " stage=" << stage_id << " sender=" << sender
             << " tag=" << update.middle_virtual_shard
             << " wait_us=" << trace_wait_us << " apply_us=" << trace_apply_us
             << " collect_us=" << trace_collect_us
             << " need_us=" << trace_need_us << " directory_us=" << trace_directory_us
             << " needed=" << trace_needed << " sources=" << trace_sources
             << " rhs=" << trace_rhs << " request=" << request_rhs << " execute=" << execute
             << " empty_requests_avoided=" << empty_requests_avoided
             << " lhs_ready_us=" << trace_lhs_ready_us << " rhs_ready_us=" << trace_rhs_ready_us;
        cerr << line.str() << '\n';
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto& query = state.queries.at(query_id);
        if (execute) {
            query.balance_direct_stages.erase(stage_id);
            ++query.balance_local_tasks;
        }
        query.balance_input_merge_us += chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - begin).count();
        query.balance_rhs_requested_keys += requested_keys;
        query.balance_rhs_keys_avoided += avoided_keys;
        query.balance_rhs_empty_requests_avoided += empty_requests_avoided;
    }
    if (request_rhs) {
        for (int owner = 0; owner < state.total_partitions; ++owner) {
            // Empty producer contributions were sealed locally using the
            // exact advertised directory; no request or empty reply is needed.
            if (requested_by_producer[owner].empty()) continue;
            // A key request has no relation encoding. Dispatch it here so it
            // cannot wait behind unrelated RHS replies on the message executor.
            try {
                IncrementalMatrixJoinUpdate request;
                request.query_id = query_id;
                request.global_exact_stage = true;
                request.middle_compute_task = true;
                request.middle_virtual_shard = 2;
                request.global_stage_id = stage_id;
                request.global_sender_pid = state.partition_id;
                request.right_sources = std::move(requested_by_producer[owner]);
                IncrementalSendGlobalExactUpdate(state, owner, std::move(request));
            } catch (const exception& error) {
                IncrementalRecordPeerFailure(state, query_id, owner,
                    MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
            }
        }
    }
    if (execute) {
        if (combined.middle_virtual_shard == 4) {
            IncrementalSubmitCompute(state, query_id,
                [&state, query_id, stage_id, combined = std::move(combined)]() mutable {
                    try {
                        shared_ptr<IncrementalOpenPlan> plan;
                        {
                            lock_guard<mutex> lk(state.mu);
                            plan = state.queries.at(query_id).open_plans.at(
                                combined.right_body_id);
                        }
                        const auto begin = chrono::steady_clock::now();
                        RelationRows closure;
                        closure.reserve(combined.left_rows.size());
                        // Exact same closure, computed once per distinct seed
                        // storage and with one multiway union per source over
                        // its distinct SCC closures instead of one OR per seed.
                        unordered_map<const void*, const RelationDstSet*> by_storage;
                        by_storage.reserve(combined.left_rows.size());
                        vector<uint32_t> comps;
                        vector<const roaring::Roaring64Map*> inputs;
                        for (const auto& [source, seeds] : combined.left_rows) {
                            if (seeds.empty()) continue;
                            const void* identity = seeds.bitmap.identity();
                            auto hit = by_storage.find(identity);
                            if (hit != by_storage.end()) {
                                if (hit->second) closure.emplace(source, *hit->second);
                                continue;
                            }
                            RelationDstSet output;
                            comps.clear(); inputs.clear();
                            for (uint64_t seed : seeds) {
                                uint32_t component = 0;
                                if (!plan->FindComponent(seed, component)) output.push_back(seed);
                                else comps.push_back(component);
                            }
                            sort(comps.begin(), comps.end());
                            comps.erase(unique(comps.begin(), comps.end()), comps.end());
                            for (uint32_t component : comps)
                                inputs.push_back(&IncrementalOpenComponentClosure(*plan, component).bitmap.view());
                            if (!output.empty()) inputs.push_back(&output.bitmap.view());
                            RelationDstSet merged;
                            if (inputs.size() == 1 && comps.size() == 1) {
                                merged = IncrementalOpenComponentClosure(*plan, comps[0]);
                            } else if (!inputs.empty()) {
                                merged.bitmap = roaring::Roaring64Map::fastunion(inputs.size(), inputs.data());
                            }
                            if (merged.empty()) { by_storage.emplace(identity, nullptr); continue; }
                            auto inserted = closure.emplace(source, std::move(merged));
                            by_storage.emplace(identity, &inserted.first->second);
                        }
                        const uint64_t elapsed = chrono::duration_cast<chrono::microseconds>(
                            chrono::steady_clock::now() - begin).count();
                        {
                            lock_guard<mutex> lk(state.mu);
                            auto& query = state.queries.at(query_id);
                            query.balance_open_closure_us += elapsed;
                            query.balance_open_closure_rows += combined.left_rows.size();
                            query.balance_open_closure_seeds += CountRelationPairs(combined.left_rows);
                        }
                        cerr << "[open-closure-balance-execute] worker=" << state.partition_id
                             << " stage=" << stage_id
                             << " rows=" << combined.left_rows.size()
                             << " closure_us=" << elapsed << '\n';
                        IncrementalScheduleCompleteRowsForMessagePreparation(
                            state, query_id, combined.global_stage_id, true,
                            std::move(closure), state.partition_id, -1);
                    } catch (const exception& error) {
                        IncrementalRecordPeerFailure(state, query_id, state.partition_id,
                            MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
                    }
                });
            return;
        }
        IncrementalRunBalancedTask(state, std::move(combined));
    }
}
