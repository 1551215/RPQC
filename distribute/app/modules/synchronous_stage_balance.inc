// Weighted-prefix scheduler: p compute sets, O(p) scheduling metadata per stage.
// Graph partitions and final source ownership remain unchanged.
#include "balance_task_work.inc"

static bool IncrementalDirectBalanceEnabled() { return true; }
static bool IncrementalHelperClosureRestoreEnabled() { return true; }

static size_t IncrementalBalanceBucket(uint64_t source, size_t count) {
    source ^= source >> 30;
    source *= 0xbf58476d1ce4e5b9ULL;
    source ^= source >> 27;
    source *= 0x94d049bb133111ebULL;
    source ^= source >> 31;
    return source % count;
}

static void IncrementalRecordStageWork(
    IncrementalWorkerState&, int, uint64_t, int, uint64_t, uint64_t,
    chrono::steady_clock::time_point);
static void IncrementalDispatchDirectBalancePanel(
    IncrementalWorkerState&, int, uint64_t, IncrementalPrefixAssignment);

static void IncrementalRunBalancedTask(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    const int query_id = update.query_id;
    IncrementalSubmitCompute(state, query_id,
        [&state, query_id, update = std::move(update)]() mutable {
            try {
                const auto trace_begin = chrono::steady_clock::now();
                const int64_t trace_wall_begin = IncrementalTraceWallUs();
                // Ordinary GCNP borrows the original LHS and masks invalid
                // mids in its index, as the single-machine implementation does.
                // Keep the existing coverage/virtual input contract unchanged.
                const bool materialize_filter = update.global_virtual_stage ||
                    !update.coverage_rows.empty();
                if (materialize_filter) {
                    update.left_rows = IncrementalFilterGlobalExactLeftByRhs(
                        state, update.left_rows, update.right_rows);
                    update.scheduled_join_work = CountRelationPairs(update.left_rows);
                }
                const auto trace_filtered = chrono::steady_clock::now();
                if (!update.coverage_rows.empty()) {
                    const auto coverage_begin = chrono::steady_clock::now();
                    IncrementalPreRouteCoverageStats coverage_stats;
                    update.left_rows = IncrementalFilterLeftForCoveredRows(
                        update.left_rows, update.right_rows, update.coverage_rows, &coverage_stats);
                    lock_guard<mutex> lk(state.mu);
                    auto& query = state.queries.at(query_id);
                    query.balance_coverage_us += chrono::duration_cast<chrono::microseconds>(
                        chrono::steady_clock::now() - coverage_begin).count();
                    query.pre_route_coverage_candidate_mids += coverage_stats.candidate_mids;
                    query.pre_route_coverage_signature_rejections += coverage_stats.signature_rejections;
                    query.pre_route_coverage_subset_tests += coverage_stats.subset_tests;
                    query.pre_route_coverage_skipped_mids += coverage_stats.skipped_mids;
                    query.pre_route_coverage_skipped_rhs_pairs += coverage_stats.skipped_rhs_pairs;
                    query.pre_route_coverage_input_left_pairs += coverage_stats.input_left_pairs;
                    query.pre_route_coverage_output_left_pairs += coverage_stats.output_left_pairs;
                }
                const auto begin = chrono::steady_clock::now();
                GcnpExecutionStats stats;
                RelationRows rows = update.global_virtual_stage
                    ? IncrementalMatrixAdvanceVirtualEdge(update.left_rows, update.right_rows)
                    : ComposeRelationRowsGcnp(
                        update.left_rows, update.right_rows, nullptr, nullptr, &stats);
                if (!materialize_filter) update.scheduled_join_work = stats.usable_input_mids;
                const uint64_t elapsed = chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - begin).count();
                {
                    lock_guard<mutex> lk(state.mu);
                    auto& query = state.queries.at(query_id);
                    query.balance_join_us += elapsed;
                    query.middle_scheduler_executed_work += update.scheduled_join_work;
                    query.middle_scheduler_completed_work += update.scheduled_join_work;
                    ++query.matrix_join_updates;
                    query.matrix_join_us += elapsed;
                    query.matrix_join_result_pairs += CountRelationPairs(rows);
                    if (!update.global_virtual_stage) {
                    query.gcnp_wall_us += elapsed;
                    ++query.gcnp_metrics.batches;
                    query.gcnp_metrics.source_rows += stats.usable_source_rows;
                    query.gcnp_input_mids += stats.usable_input_mids;
                    query.gcnp_planned_mids += stats.usable_input_mids;
                    query.gcnp_complete_prepare_us += stats.prepare_us;
                    query.gcnp_complete_group_extract_us += stats.group_extract_us;
                    query.gcnp_complete_pattern_cache_us += stats.pattern_cache_us;
                    query.gcnp_complete_union_us += stats.union_us;
                    query.gcnp_complete_shared_target_us += stats.shared_target_us;
                    query.gcnp_complete_source_materialize_us += stats.source_materialize_us;
                    query.gcnp_complete_groups += stats.groups;
                    query.gcnp_complete_reusable_groups += stats.reusable_groups;
                    query.gcnp_complete_grouped_source_uses += stats.grouped_source_uses;
                    query.gcnp_complete_shared_source_uses += stats.shared_source_uses;
                    }
                }
                if (update.restore_coverage_output) {
                    // Coverage filtering and the GCNP join have already finished.
                    // These source rows belong to this same temporary bucket.
                    const auto restore_begin = chrono::steady_clock::now();
                    IncrementalMergeOwnedRows(rows, update.coverage_rows);
                    const auto restore_us = chrono::duration_cast<chrono::microseconds>(
                        chrono::steady_clock::now() - restore_begin).count();
                    lock_guard<mutex> lk(state.mu);
                    auto& query = state.queries.at(query_id);
                    query.helper_closure_restore_us += restore_us;
                    query.helper_closure_restore_rows += update.coverage_rows.size();
                }
                cerr << "[stage-balance-execute] worker=" << state.partition_id
                     << " origin=" << update.middle_partition
                     << " stage=" << update.global_stage_id
                     << " output_owner=" << update.middle_preferred_compute_pid
                     << " estimated_work=" << update.scheduled_join_work
                     << " join_us=" << elapsed
                     << " unit=rhs_rows gcnp_after_assignment=1" << '\n';
                cerr << "[gcnp-materialize-profile] worker=" << state.partition_id
                     << " stage=" << update.global_stage_id
                     << " shared_rhs_rows=" << stats.shared_rhs_rows
                     << " residual_rhs_rows=" << stats.residual_rhs_rows
                     << " actual_union_work=" << (stats.shared_rhs_rows + stats.residual_rhs_rows)
                     << " shared_copy_ns=" << stats.shared_copy_ns
                     << " residual_lookup_ns=" << stats.residual_lookup_ns
                     << " residual_union_ns=" << stats.residual_union_ns
                     << " shared_attach_ns=" << stats.shared_attach_ns
                     << " output_store_ns=" << stats.output_store_ns
                     << " unit=rhs_rows" << '\n';
                if (IncrementalScalingTraceEnabled()) {
                    ostringstream line;
                    line << "[scaling-compute] worker=" << state.partition_id
                         << " query=" << query_id << " stage=" << update.global_stage_id
                         << " filter_us=" << chrono::duration_cast<chrono::microseconds>(trace_filtered-trace_begin).count()
                         << " coverage_us=" << chrono::duration_cast<chrono::microseconds>(begin-trace_filtered).count()
                         << " join_us=" << elapsed
                         << " rhs_filter=" << (materialize_filter ? "coverage_or_virtual_input" : "fused_gcnp_index")
                         << " usable_source_rows=" << stats.usable_source_rows
                         << " usable_input_mids=" << stats.usable_input_mids
                         << " wall_begin_us=" << trace_wall_begin
                         << " wall_end_us=" << IncrementalTraceWallUs();
                    cerr << line.str() << '\n';
                }
                IncrementalScheduleCompleteRowsForMessagePreparation(
                    state, query_id, update.global_stage_id,
                    update.defer_factorized_materialization, std::move(rows),
                    update.middle_partition, update.middle_preferred_compute_pid);
            } catch (const exception& error) {
                IncrementalRecordPeerFailure(state, query_id, state.partition_id,
                    MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
            }
        });
}

static void IncrementalAcceptBalanceControl(
    IncrementalWorkerState& state, const Message& request) {
    const auto control_begin = chrono::steady_clock::now();
    const auto header = IncrementalReadSourceBalanceHeader(request);
    const int query_id = header.query_id;
    const uint64_t stage_id = header.stage_id;
    const size_t bucket_count = IncrementalBalanceBucketCount(state.total_partitions);
    auto costs = IncrementalReadSourceBalanceValues(request, header);
    if (header.op == 2) {
        if (header.origin != state.partition_id || costs.size() != 3 || costs[2].second != 0)
            throw runtime_error("bad prefix assignment");
        IncrementalPrefixAssignment assignment{costs[0].first, costs[0].second,
            costs[1].first, costs[1].second, costs[2].first};
        const uint64_t expected_target = assignment.total_work / bucket_count +
            (assignment.total_work % bucket_count != 0);
        if (assignment.target_work != expected_target)
            throw runtime_error("bad prefix target");
        {
            lock_guard<mutex> lk(state.mu);
            state.queries.at(query_id).balance_gather_us += chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - control_begin).count();
        }
        IncrementalDispatchDirectBalancePanel(
            state, query_id, stage_id, assignment);
        return;
    }
    if (state.partition_id != 0 || header.origin >= state.total_partitions ||
        costs.size() != 2 || costs[1].second != 0)
        throw runtime_error("bad prefix work report");
    unordered_map<int, vector<pair<uint64_t, uint64_t>>> all_costs;
    {
        lock_guard<mutex> lk(state.mu);
        auto& reports = state.queries.at(query_id).balance_reports;
        auto& pending = reports[stage_id].costs;
        const int origin = header.origin;
        if (origin < 0 || origin >= state.total_partitions ||
            !pending.emplace(origin, std::move(costs)).second) {
            throw runtime_error("bad or duplicate prefix report");
        }
        state.queries.at(query_id).balance_gather_us += chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - control_begin).count();
        if (pending.size() != static_cast<size_t>(state.total_partitions)) return;
        all_costs = std::move(pending);
        reports.erase(stage_id);
    }
    const auto planner_begin = chrono::steady_clock::now();
    vector<uint64_t> original(state.total_partitions);
    uint64_t source_count = 0, largest_source_work = 0;
    for (int origin = 0; origin < state.total_partitions; ++origin) {
        const auto& report = all_costs.at(origin);
        original[origin] = report[0].first;
        if (report[0].second > numeric_limits<uint64_t>::max() - source_count)
            throw overflow_error("prefix source count overflow");
        source_count += report[0].second;
        largest_source_work = max(largest_source_work, report[1].first);
    }
    const auto plan = IncrementalPlanPrefixWork(original);
    const long double mean = static_cast<long double>(plan.total_work) / state.total_partitions;
    const auto planner_us = chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - planner_begin).count();
    {
        lock_guard<mutex> lk(state.mu);
        state.queries.at(query_id).balance_planner_us += planner_us;
    }
    ostringstream diagnostic;
    diagnostic << "[fine-balance-plan] stage=" << stage_id << " buckets=" << bucket_count
         << " max_before=" << *max_element(original.begin(), original.end())
         << " target_work=" << plan.target_work
         << " mean_work=" << mean << " estimated_union_work=" << plan.total_work
         << " sources=" << source_count << " max_source_work=" << largest_source_work
         << " planner_compute_us=" << planner_us
         << " scheduler=weighted_prefix coordinator_threads="
         << IncrementalMatrixMessageThreadCount(state.total_partitions)
         << " assignment_metadata=constant_per_origin unit=rhs_rows gcnp_before_assignment=0" << '\n';
    cerr << diagnostic.str();
    for (int origin = 0; origin < state.total_partitions; ++origin) {
        Message message = IncrementalMakeSourceBalanceMessage(
            2, query_id, stage_id, origin,
            {{plan.offsets[origin], plan.target_work}, {plan.total_work, original[origin]},
             {all_costs.at(origin)[0].second, 0}});
        if (origin == state.partition_id) {
            state.matrix_message_executor.Submit([&state, message = std::move(message), query_id] {
                try { IncrementalAcceptBalanceControl(state, message); }
                catch (const exception& error) {
                    IncrementalRecordPeerFailure(state, query_id, state.partition_id,
                        MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
                }
            });
        } else IncrementalEnqueuePeer(state, origin, std::move(message));
    }
}

static Message HandleIncrementalBalanceControl(
    IncrementalWorkerState& state, const Message& request) {
    const auto header = IncrementalReadSourceBalanceHeader(request);
    const int query_id = header.query_id;
    IncrementalQueryMessageGuard guard(state, query_id);
    if (!guard) return Message(MessageType::RESPONSE_OK);
    if (header.op == 3) {
        // A constant-size work report; record it without queueing behind
        // message preparation so the deterministic plan is not delayed.
        const auto values = IncrementalReadSourceBalanceValues(request, header);
        if (values.size() != 1) throw runtime_error("bad owner-first work report");
        IncrementalRecordStageWork(state, query_id, header.stage_id, header.origin,
            values[0].first, values[0].second, chrono::steady_clock::now());
        return Message(MessageType::RESPONSE_OK);
    }
    // Decode reports and route independent source panels concurrently. Joins
    // still use the separate, single-thread compute executor.
    state.matrix_message_executor.Submit([&state, request, query_id] {
        try { IncrementalAcceptBalanceControl(state, request); }
        catch (const exception& error) {
            IncrementalRecordPeerFailure(state, query_id, state.partition_id,
                MessageType::INCREMENTAL_BALANCE_CONTROL, error.what());
        }
    });
    return Message(MessageType::RESPONSE_OK);
}

#include "synchronous_direct_balance.inc"
