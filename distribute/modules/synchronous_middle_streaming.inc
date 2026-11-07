// Incremental compose for bounded/composite expressions. The complete left
// panel is the GCNP barrier. RHS rows may arrive before or after that barrier;
// once the immutable plan exists, every fresh RHS delta reuses it.

static void IncrementalMaybeFinishSynchronousMiddleCompose(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, int middle_partition) {
    uint64_t final_token = 0;
    bool complete = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end()) return;
        auto& block = bit->second;
        if (block.finalized || !block.synchronous_middle_left_sealed ||
            !block.synchronous_middle_right_sealed ||
            block.incremental_gcnp_plan == nullptr ||
            block.incremental_gcnp_plan_queued ||
            block.synchronous_middle_rhs_apply_queued ||
            !block.synchronous_middle_pending_rhs_rows.empty()) {
            return;
        }
        block.finalized = true;
        final_token = block.finalize_token;
        qit->second.matrix_blocks.erase(bit);
        complete = true;
    }
    if (!complete) return;

    IncrementalMatrixJoinResult result;
    result.query_id = query_id;
    result.token = final_token;
    result.parent_batch_id = key.parent_batch_id;
    result.middle_partial = true;
    result.middle_complete = true;
    result.middle_partition = middle_partition;
    result.destination_pid = middle_partition;
    IncrementalSendDestinationShardResult(state, key.parent_pid, std::move(result));
}

static void IncrementalRunSynchronousMiddleOneShot(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, int middle_partition,
    RelationRows left, RelationRows right, uint64_t final_token) {
    IncrementalSubmitCompute(
        state, query_id,
        [&state, query_id, key, middle_partition, final_token,
         left = std::move(left), right = std::move(right)]() mutable {
            const auto begin = chrono::steady_clock::now();
            GcnpExecutionStats stats;
            RelationRows candidates = ComposeRelationRowsGcnp(
                left, right, nullptr, nullptr, &stats);
            const uint64_t join_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - begin).count());
            const uint64_t input_pairs = CountRelationPairs(left);
            const uint64_t result_pairs = CountRelationPairs(candidates);
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto bit = qit->second.matrix_blocks.find(key);
                if (bit == qit->second.matrix_blocks.end() || bit->second.finalized) return;
                bit->second.finalized = true;
                ++qit->second.matrix_join_updates;
                qit->second.matrix_join_result_pairs += result_pairs;
                qit->second.matrix_join_us += join_us;
                ++qit->second.gcnp_metrics.batches;
                qit->second.gcnp_metrics.source_rows += left.size();
                qit->second.gcnp_input_mids += input_pairs;
                qit->second.gcnp_planned_mids += input_pairs;
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
                qit->second.matrix_blocks.erase(bit);
            }

            IncrementalMatrixJoinResult result;
            result.query_id = query_id;
            result.token = final_token;
            result.parent_batch_id = key.parent_batch_id;
            result.middle_partial = true;
            result.middle_complete = true;
            result.middle_partition = middle_partition;
            result.destination_pid = middle_partition;
            result.rows = std::move(candidates);
            IncrementalSendDestinationShardResult(
                state, key.parent_pid, std::move(result));
        });
}

static void IncrementalPublishSynchronousMiddleDelta(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, int middle_partition,
    RelationRows candidates, uint64_t join_us) {
    RelationRows fresh;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end() || bit->second.finalized) return;
        fresh = IncrementalDifferenceRows(candidates, bit->second.output_rows);
        IncrementalMergeRows(bit->second.output_rows, fresh);
        ++qit->second.matrix_join_updates;
        qit->second.matrix_join_result_pairs += CountRelationPairs(fresh);
        qit->second.matrix_join_us += join_us;
        qit->second.gcnp_wall_us += join_us;
    }
    if (fresh.empty()) return;

    IncrementalMatrixJoinResult result;
    result.query_id = query_id;
    result.parent_batch_id = key.parent_batch_id;
    result.middle_partial = true;
    result.middle_partition = middle_partition;
    result.destination_pid = middle_partition;
    result.rows = std::move(fresh);
    IncrementalSendDestinationShardResult(state, key.parent_pid, std::move(result));
}

static void IncrementalMaybeBuildSynchronousMiddleGcnpPlan(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, int middle_partition) {
    RelationRows left;
    RelationRows right;
    bool queue = false;
    bool one_shot = false;
    uint64_t final_token = 0;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto bit = qit->second.matrix_blocks.find(key);
        if (bit == qit->second.matrix_blocks.end()) return;
        auto& block = bit->second;
        if (!block.synchronous_middle_left_sealed ||
            !block.synchronous_middle_right_sealed || block.finalized ||
            block.incremental_gcnp_plan != nullptr ||
            block.incremental_gcnp_plan_queued ||
            block.synchronous_middle_join_queued) {
            return;
        }
        left = block.left_rows;
        // A synchronous composition accumulates fragmented transport payloads
        // but never exposes them as separate GCNP inputs.  Both complete
        // relation panels are required before the one-shot join.
        block.synchronous_middle_join_queued = true;
        right = block.right_rows;
        final_token = block.finalize_token;
        one_shot = true;
        queue = true;
    }
    if (!queue) return;
    if (one_shot) {
        IncrementalRunSynchronousMiddleOneShot(
            state, query_id, key, middle_partition,
            std::move(left), std::move(right), final_token);
        return;
    }

    IncrementalSubmitCompute(
        state, query_id,
        [&state, query_id, key, middle_partition,
         left = std::move(left)]() mutable {
            const auto begin = chrono::steady_clock::now();
            GcnpExecutionStats stats;
            auto plan = IncrementalBuildGcnpDeltaPlan(left, &stats);
            RelationRows current_right;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto bit = qit->second.matrix_blocks.find(key);
                if (bit == qit->second.matrix_blocks.end() || bit->second.finalized) return;
                auto& block = bit->second;
                if (block.incremental_gcnp_plan != nullptr) return;
                block.incremental_gcnp_plan = plan;
                current_right = block.right_rows;
                ++qit->second.gcnp_metrics.batches;
                qit->second.gcnp_metrics.source_rows += left.size();
                const uint64_t input_pairs = CountRelationPairs(left);
                qit->second.gcnp_input_mids += input_pairs;
                qit->second.gcnp_planned_mids += input_pairs;
                qit->second.gcnp_complete_prepare_us += stats.prepare_us;
                qit->second.gcnp_complete_group_extract_us += stats.group_extract_us;
                qit->second.gcnp_complete_groups += stats.groups;
                qit->second.gcnp_complete_reusable_groups += stats.reusable_groups;
                qit->second.gcnp_complete_grouped_source_uses +=
                    stats.grouped_source_uses;
                qit->second.gcnp_complete_shared_source_uses +=
                    stats.shared_source_uses;
            }
            RelationRows candidates =
                IncrementalPrimeGcnpDeltaPlan(*plan, current_right);
            const uint64_t join_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - begin).count());
            IncrementalPublishSynchronousMiddleDelta(
                state, query_id, key, middle_partition,
                std::move(candidates), join_us);
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit != state.queries.end()) {
                    auto bit = qit->second.matrix_blocks.find(key);
                    if (bit != qit->second.matrix_blocks.end()) {
                        bit->second.incremental_gcnp_plan_queued = false;
                    }
                }
            }
            IncrementalMaybeFinishSynchronousMiddleCompose(
                state, query_id, key, middle_partition);
        });
}

static void IncrementalDrainSynchronousMiddleRhsDeltas(
    IncrementalWorkerState& state, int query_id,
    const IncrementalMatrixBlockKey& key, int middle_partition,
    shared_ptr<IncrementalGcnpDeltaPlan> plan) {
    IncrementalSubmitCompute(
        state, query_id,
        [&state, query_id, key, middle_partition,
         plan = std::move(plan)]() mutable {
          for (;;) {
            RelationRows right_delta;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto bit = qit->second.matrix_blocks.find(key);
                if (bit == qit->second.matrix_blocks.end() || bit->second.finalized) return;
                right_delta.swap(
                    bit->second.synchronous_middle_pending_rhs_rows);
                if (right_delta.empty()) {
                    bit->second.synchronous_middle_rhs_apply_queued = false;
                    break;
                }
            }
            const auto begin = chrono::steady_clock::now();
            RelationRows candidates =
                IncrementalApplyGcnpDeltaPlan(*plan, right_delta);
            const uint64_t join_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - begin).count());
            IncrementalPublishSynchronousMiddleDelta(
                state, query_id, key, middle_partition,
                std::move(candidates), join_us);
          }
            IncrementalMaybeFinishSynchronousMiddleCompose(
                state, query_id, key, middle_partition);
        });
}

static void IncrementalProcessSynchronousMiddlePartitionJoinUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
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
    const bool is_seal = update.middle_left_complete ||
                         update.middle_stream_complete;
    bool acknowledge = false;
    bool schedule_rhs_apply = false;
    shared_ptr<IncrementalGcnpDeltaPlan> plan;
    RelationRows fresh_right;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& block = qit->second.matrix_blocks[key];
        if (!is_seal) {
            // No left update is legal after LEFT_PANEL_END. Before that
            // marker both operands are only accumulated; no join is run.
            IncrementalMergeRows(block.left_rows, update.left_rows);
            fresh_right = IncrementalDifferenceRows(
                update.right_rows, block.right_rows);
            IncrementalMergeRows(block.right_rows, fresh_right);
            block.retain_output_on_destination = false;
            plan = block.incremental_gcnp_plan;
            if (plan != nullptr && !fresh_right.empty()) {
                IncrementalMergeRows(
                    block.synchronous_middle_pending_rhs_rows, fresh_right);
                if (!block.synchronous_middle_rhs_apply_queued) {
                    block.synchronous_middle_rhs_apply_queued = true;
                    schedule_rhs_apply = true;
                }
            }
            acknowledge = !update.middle_rhs_direct;
        } else {
            if (update.middle_left_complete) {
                block.synchronous_middle_left_sealed = true;
                block.input_sealed = true;
                block.left_panel_sealed_at = chrono::steady_clock::now();
            }
            if (update.middle_stream_complete) {
                block.synchronous_middle_right_sealed = true;
                block.finalize_token = update.token;
            }
            // The RHS seal token is completed only by the terminal result.
            acknowledge = !update.middle_stream_complete;
        }
    }

    if (acknowledge) {
        IncrementalMatrixJoinResult result;
        result.query_id = update.query_id;
        result.token = update.token;
        result.parent_batch_id = update.parent_batch_id;
        result.middle_partial = true;
        result.middle_partition = update.middle_partition;
        result.destination_pid = update.middle_partition;
        IncrementalSendDestinationShardResult(
            state, update.parent_pid, std::move(result));
    }
    if (schedule_rhs_apply) {
        IncrementalDrainSynchronousMiddleRhsDeltas(
            state, update.query_id, key, update.middle_partition,
            std::move(plan));
    }
    IncrementalMaybeBuildSynchronousMiddleGcnpPlan(
        state, update.query_id, key, update.middle_partition);
    IncrementalMaybeFinishSynchronousMiddleCompose(
        state, update.query_id, key, update.middle_partition);
}
