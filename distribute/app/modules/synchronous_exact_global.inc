// Stage-global evaluator for a direct exact-count root.  Every worker owns a
// disjoint set of source rows.  For one P[a] o P[b] stage all source owners
// route their complete panels to owner(mid), and owner(mid) runs one GCNP
// after receiving one seal from every source owner.

struct IncrementalGlobalExactPlan {
    int query_id = 0;
    int body_id = 0;
    int target_length = 0;
    int highest_power = 1;
    unordered_map<int, RelationRows> powers;
    vector<int> selected_powers;
    uint64_t stage_namespace = 0;
    function<void(RelationRows)> done;
};

static void IncrementalHandleGlobalExactStageResult(
    IncrementalWorkerState&, IncrementalMatrixJoinResult);

// Keep the same middle-local RHS prefilter as the baseline. Message
// management changes the wire representation, not the GCNP input relation.
static RelationRows IncrementalFilterGlobalExactLeftByRhs(
    const IncrementalWorkerState& state, const RelationRows& left,
    const RelationRows& right) {
    const auto begin = chrono::steady_clock::now();
    RelationDstSet right_keys;
    for (const auto& [mid, targets] : right) {
        if (!targets.empty()) right_keys.push_back(mid);
    }
    RelationRows filtered;
    filtered.reserve(left.size());
    uint64_t shared_rows = 0, intersected_rows = 0;
    for (const auto& [source, mids] : left) {
        if (mids.empty()) continue;
        // The mask is identical to the previous per-mid nonempty-RHS test.
        // Rows that survive unchanged keep their immutable bitmap storage.
        if (mids.bitmap.isSubset(right_keys.bitmap)) {
            filtered.emplace(source, mids);
            ++shared_rows;
        } else {
            RelationDstSet retained;
            retained.bitmap = mids.bitmap & right_keys.bitmap;
            if (!retained.empty()) filtered.emplace(source, std::move(retained));
            ++intersected_rows;
        }
    }
    if (IncrementalScalingTraceEnabled()) {
        cerr << "[scaling-rhs-filter] worker=" << state.partition_id
             << " input_rows=" << left.size() << " output_rows=" << filtered.size()
             << " shared_rows=" << shared_rows << " intersected_rows=" << intersected_rows
             << " filter_us=" << chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - begin).count()
             << " threads=1" << '\n';
    }
    return filtered;
}

static uint64_t IncrementalGlobalExactStageId(int body_id, int output_length) {
    return (static_cast<uint64_t>(static_cast<uint32_t>(body_id)) << 32) |
        static_cast<uint32_t>(output_length);
}

static void IncrementalSendGlobalExactUpdate(
    IncrementalWorkerState& state, int destination_pid,
    IncrementalMatrixJoinUpdate update,
    IncrementalMatrixEncodingTiming* timing = nullptr) {
    if (destination_pid == state.partition_id) {
        auto materialize = [](RelationRows& rows,
            vector<pair<uint64_t, const RelationDstSet*>>& views) {
            if (!views.empty() && !rows.empty())
                throw runtime_error("mixed local owned and borrowed rows");
            for (const auto& row : views) rows.emplace(row.first, *row.second);
            views.clear();
        };
        materialize(update.left_rows, update.left_row_views);
        materialize(update.coverage_rows, update.coverage_row_views);
        update.left_view_owner.reset();
        update.coverage_view_owner.reset();
        IncrementalAcceptMatrixJoinUpdate(state, std::move(update));
        return;
    }
    Message message(MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE);
    message.data = SerializeIncrementalMatrixJoinUpdate(update, timing);
    IncrementalEnqueuePeer(
        state, destination_pid, std::move(message),
        IncrementalWorkerState::PeerWireKind::LHS);
}

static bool IncrementalIsOneHopBody(const RegexQueryPattern& pattern) {
    bool saw_transition = false;
    for (int state_id : pattern.nfa.start_closure) {
        if (state_id == pattern.nfa.accept) continue;
        if (state_id < 0 ||
            state_id >= static_cast<int>(pattern.nfa.states.size())) return false;
        for (const auto& transition :
             pattern.nfa.states[static_cast<size_t>(state_id)].labeled) {
            saw_transition = true;
            bool reaches_accept = false;
            for (int next : pattern.nfa.closure_by_state[
                     static_cast<size_t>(transition.to)]) {
                if (next == pattern.nfa.accept) {
                    reaches_accept = true;
                } else if (next >= 0 &&
                           next < static_cast<int>(pattern.nfa.states.size()) &&
                           !pattern.nfa.states[static_cast<size_t>(next)].labeled.empty()) {
                    return false;
                }
            }
            if (!reaches_accept) return false;
        }
    }
    return saw_transition;
}

static void IncrementalCollectGlobalBaseRelationBulk(
    IncrementalWorkerState& state, int query_id, int body_id,
    const RelationDstSet& requested_sources,
    function<void(RelationRows)> done) {
    RegexQueryPattern pattern;
    int virtual_node = -1;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        const auto& program = qit->second.program;
        if (body_id >= 0 &&
            static_cast<size_t>(body_id) < program.virtual_body_nodes.size()) {
            virtual_node =
                program.virtual_body_nodes[static_cast<size_t>(body_id)];
        }
        if (virtual_node < 0) {
            pattern = program.bodies.at(static_cast<size_t>(body_id)).pattern;
        }
    }
    if (virtual_node >= 0) {
        IncrementalMatrixEvalMacroBatch(
            state, query_id, virtual_node, requested_sources, false,
            std::move(done));
        return;
    }
    if (!IncrementalIsOneHopBody(pattern)) {
        IncrementalMatrixCollectExact(
            state, query_id, body_id, 1, requested_sources, false,
            std::move(done));
        return;
    }

    IncrementalSubmitCompute(
        state, query_id,
        [&state, query_id, body_id, pattern = std::move(pattern),
         requested_sources, done = std::move(done)]() mutable {
            const auto begin = chrono::steady_clock::now();
            RelationRows output;
            RelationDstSet missing;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto& cache = qit->second.global_base_relation_cache[body_id];
                const auto& scanned_sources = qit->second.global_base_sources_scanned[body_id];
                RelationDstSet cached_sources;
                cached_sources.bitmap = requested_sources.bitmap & scanned_sources.bitmap;
                missing.bitmap = requested_sources.bitmap - scanned_sources.bitmap;
                output.reserve(requested_sources.cardinality() * 2 + 1);
                for (uint64_t source : cached_sources) {
                    auto cached = cache.find(source);
                    if (cached != cache.end()) output.emplace(source, cached->second);
                }
            }

            vector<pair<uint64_t, RelationDstSet>> scanned;
            scanned.reserve(missing.cardinality());
            for (uint64_t source : missing) {
                RelationDstSet destinations;
                auto adjacency = state.partition.adjacency_map.find(source);
                if (adjacency != state.partition.adjacency_map.end()) {
                    for (int state_id : pattern.nfa.start_closure) {
                        if (state_id == pattern.nfa.accept) continue;
                        for (const auto& transition : pattern.nfa.states[
                                 static_cast<size_t>(state_id)].labeled) {
                            auto edges = adjacency->second.find(transition.label);
                            if (edges == adjacency->second.end()) continue;
                            for (uint64_t destination : edges->second) {
                                destinations.push_back(destination);
                            }
                        }
                    }
                }
                if (!destinations.empty()) scanned.emplace_back(source, std::move(destinations));
            }
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto& query = qit->second;
                auto& cache = query.global_base_relation_cache[body_id];
                query.global_base_sources_scanned[body_id] |= missing;
                for (auto& [source, destinations] : scanned) {
                    auto inserted = cache.emplace(source, std::move(destinations));
                    if (!inserted.first->second.empty()) {
                        output.emplace(source, inserted.first->second);
                    }
                }
                query.rows_started += missing.cardinality();
                query.base_expand_tasks += missing.cardinality();
                query.base_expand_us += static_cast<uint64_t>(
                    chrono::duration_cast<chrono::microseconds>(
                        chrono::steady_clock::now() - begin).count());
                query.row_cache_hits +=
                    requested_sources.cardinality() - missing.cardinality();
            }
            if (IncrementalScalingTraceEnabled()) {
                cerr << "[scaling-base-cache] worker=" << state.partition_id
                     << " query=" << query_id << " body=" << body_id
                     << " requested=" << requested_sources.cardinality()
                     << " missing=" << missing.cardinality()
                     << " nonempty_scanned=" << scanned.size()
                     << " membership=bitmap rows=nonempty_only threads=1" << '\n';
            }
            done(std::move(output));
        });
}

static bool IncrementalDirectBalanceEnabled();
static bool IncrementalOwnerInputViewsEnabled() {
    return true;
}

static shared_ptr<const RelationRows> IncrementalOwnComposeInput(RelationRows& rows) {
    return IncrementalOwnerInputViewsEnabled()
        ? make_shared<const RelationRows>(std::move(rows)) : nullptr;
}

static void IncrementalStartDirectBalanceCompose(
    IncrementalWorkerState&, int, uint64_t, const RelationRows&,
    const RelationRows&, function<void(RelationRows)>, const RelationRows*,
    int = -1, IncrementalComposeInputOwners = {});

static void IncrementalStartGlobalRelationCompose(
    IncrementalWorkerState& state, int query_id, int body_id,
    uint64_t stage_id, const RelationRows& left, const RelationRows& right,
    function<void(RelationRows)> done,
    const RelationRows* coverage = nullptr,
    IncrementalComposeInputOwners owners = {}) {
    (void)body_id;
    if (IncrementalDirectBalanceEnabled()) {
        IncrementalStartDirectBalanceCompose(state, query_id, stage_id,
            left, right, std::move(done), coverage, -1, std::move(owners));
        return;
    }
    const bool route_coverage = coverage != nullptr && !coverage->empty();
    vector<RelationRows> left_by_middle(
        static_cast<size_t>(state.total_partitions));
    for (const auto& [source, mids] : left) {
        for (uint64_t mid : mids) {
            const int middle = IncrementalOwner(state, mid);
            left_by_middle[static_cast<size_t>(middle)][source].push_back(mid);
        }
    }
    vector<RelationRows> right_by_middle(
        static_cast<size_t>(state.total_partitions));
    for (const auto& [mid, destinations] : right) {
        if (destinations.empty()) continue;
        const int middle = IncrementalOwner(state, mid);
        right_by_middle[static_cast<size_t>(middle)].emplace(
            mid, destinations);
    }

    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto& output = qit->second.global_exact_output_stages[stage_id];
        output = IncrementalGlobalExactOutputStage{};
        output.expected_middles =
            static_cast<uint64_t>(state.total_partitions);
        output.done = std::move(done);

    }

    // Send one complete message per (sender, middle, stage). The middle
    // performs an exact RHS-presence filter after both panels are sealed.
    for (int middle = 0; middle < state.total_partitions; ++middle) {
        auto& middle_left = left_by_middle[static_cast<size_t>(middle)];
        IncrementalMatrixJoinUpdate update;
        update.query_id = query_id;
        update.global_exact_stage = true;
        update.global_exact_seal = true;
        update.global_rhs_seal = true;
        // This entry point is exclusively used by counted-relation stages,
        // including bounded stages and exact prefixes embedded in macros.
        // Keep their matrix results factorized across middle owners until the
        // complete counted compose is materialized at owner(source).
        update.global_virtual_stage = false;
        update.defer_factorized_materialization = true;
        update.global_stage_id = stage_id;
        update.global_sender_pid = state.partition_id;
        update.middle_partition = middle;
        if (route_coverage) {
            update.coverage_rows = IncrementalFilterRowsForLeftSources(
                *coverage, middle_left);
        }
        update.left_rows = std::move(middle_left);
        update.right_rows = std::move(
            right_by_middle[static_cast<size_t>(middle)]);
        IncrementalSendGlobalExactUpdate(
            state, middle, std::move(update));
    }
}

static void IncrementalStartGlobalVirtualTransition(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id,
    const RelationRows& frontier, const RelationRows& macro_rows,
    function<void(RelationRows)> done) {
    if (IncrementalDirectBalanceEnabled()) {
        RelationRows local_right;
        for (const auto& [mid, destinations] : macro_rows) {
            if (!destinations.empty()) {
                local_right.emplace(mid, destinations);
            }
        }
        IncrementalStartDirectBalanceCompose(state, query_id, stage_id,
            frontier, local_right, std::move(done), nullptr, -2);
        return;
    }
    vector<RelationRows> left_by_middle(
        static_cast<size_t>(state.total_partitions));
    for (const auto& [source, mids] : frontier) {
        for (uint64_t mid : mids) {
            left_by_middle[static_cast<size_t>(
                IncrementalOwner(state, mid))][source].push_back(mid);
        }
    }
    RelationRows local_right;
    for (const auto& [mid, destinations] : macro_rows) {
        if (!destinations.empty() &&
            IncrementalOwner(state, mid) == state.partition_id) {
            local_right.emplace(mid, destinations);
        }
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto& output = qit->second.global_exact_output_stages[stage_id];
        output = IncrementalGlobalExactOutputStage{};
        output.expected_middles =
            static_cast<uint64_t>(state.total_partitions);
        output.done = std::move(done);
    }

    for (int middle = 0; middle < state.total_partitions; ++middle) {
        IncrementalMatrixJoinUpdate lhs;
        lhs.query_id = query_id;
        lhs.global_exact_stage = true;
        lhs.global_exact_seal = true;
        lhs.global_virtual_stage = true;
        lhs.global_stage_id = stage_id;
        lhs.global_sender_pid = state.partition_id;
        lhs.middle_partition = middle;
        lhs.left_rows = std::move(
            left_by_middle[static_cast<size_t>(middle)]);
        IncrementalSendGlobalExactUpdate(state, middle, std::move(lhs));
    }

    // Feed resident macro RHS through the normal stage transition. LHS seals
    // may have arrived before this worker finishes the local macro relation;
    // the final synthetic RHS seal must therefore run the same ready check as
    // a network update instead of mutating the middle stage directly.
    for (int sender = 0; sender < state.total_partitions; ++sender) {
        IncrementalMatrixJoinUpdate rhs;
        rhs.query_id = query_id;
        rhs.global_exact_stage = true;
        rhs.global_rhs_seal = true;
        rhs.global_virtual_stage = true;
        rhs.global_stage_id = stage_id;
        rhs.global_sender_pid = sender;
        rhs.middle_partition = state.partition_id;
        if (sender == state.partition_id) {
            rhs.right_rows = std::move(local_right);
        }
        IncrementalAcceptGlobalExactStageUpdate(state, std::move(rhs));
    }
}

static void IncrementalStartGlobalExactCompose(
    IncrementalWorkerState& state, int query_id, int body_id,
    int output_length, const RelationRows& left, const RelationRows& right,
    function<void(RelationRows)> done, IncrementalComposeInputOwners owners = {}) {
    IncrementalStartGlobalRelationCompose(
        state, query_id, body_id,
        IncrementalGlobalExactStageId(body_id, output_length), left, right,
        std::move(done), nullptr, std::move(owners));
}

static string IncrementalCanonicalBitmapKey(const RelationDstSet& values) {
    const size_t bytes = values.bitmap.getSizeInBytes(true);
    string key(bytes, '\0');
    values.bitmap.write(key.data(), true);
    return key;
}

static uint64_t IncrementalBitmapFingerprint(const RelationDstSet& values) {
    const uint64_t cardinality = values.cardinality();
    if (cardinality == 0) return 0;
    auto mix = [](uint64_t hash, uint64_t value) {
        value ^= value >> 30;
        value *= 0xbf58476d1ce4e5b9ULL;
        value ^= value >> 27;
        value *= 0x94d049bb133111ebULL;
        value ^= value >> 31;
        return hash ^ (value + 0x9e3779b97f4a7c15ULL +
                       (hash << 6) + (hash >> 2));
    };
    uint64_t hash = mix(0xcbf29ce484222325ULL, cardinality);
    hash = mix(hash, values.bitmap.getSizeInBytes(true));
    hash = mix(hash, values.bitmap.minimum());
    hash = mix(hash, values.bitmap.maximum());
    return hash;
}

static uint64_t IncrementalSourceSetFingerprint(const RelationDstSet& sources) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    hash ^= sources.cardinality();
    hash *= 0x100000001b3ULL;
    for (uint64_t source : sources) {
        source ^= source >> 30;
        source *= 0xbf58476d1ce4e5b9ULL;
        source ^= source >> 27;
        source *= 0x94d049bb133111ebULL;
        source ^= source >> 31;
        hash ^= source;
        hash *= 0x100000001b3ULL;
    }
    return hash;
}

static uint64_t IncrementalContentBlockId(const string& bytes) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    for (unsigned char byte : bytes) {
        hash ^= byte;
        hash *= 0x100000001b3ULL;
    }
    hash ^= static_cast<uint64_t>(bytes.size()) + 0x9e3779b97f4a7c15ULL;
    return hash;
}

static uint64_t IncrementalRowsEncodingBytes(const RelationRows& rows) {
    uint64_t bytes = 8;
    for (const auto& [_, destinations] : rows) {
        bytes += 16 + destinations.bitmap.getSizeInBytes(true);
    }
    return bytes;
}

static uint64_t IncrementalFactorizedEncodingBytes(
    const RelationRows& residual_rows,
    const vector<IncrementalMatrixJoinResult::SharedBlock>& blocks,
    bool dictionary_coded) {
    uint64_t bytes = IncrementalRowsEncodingBytes(residual_rows) + 8;
    for (const auto& block : blocks) {
        bytes += 8;
        if (dictionary_coded) bytes += 4 + 8 + 8;
        bytes += 8 + block.sources.bitmap.getSizeInBytes(true);
        if (!dictionary_coded || block.destination_codec !=
            IncrementalMatrixJoinResult::SharedBlock::DestinationCodec::REFERENCE) {
            bytes += 8 + block.destinations.bitmap.getSizeInBytes(true);
        }
    }
    return bytes;
}

struct IncrementalCompleteRowGroups {
    vector<RelationRows> unique_rows_by_owner;
    vector<vector<IncrementalMatrixJoinResult::SharedBlock>>
        shared_rows_by_owner;
    vector<uint64_t> ordinary_bytes_by_owner;
    uint64_t grouping_us = 0;
};

static size_t IncrementalBalanceBucket(uint64_t source, size_t count);

static void IncrementalScheduleCompleteRowsForMessagePreparation(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id,
    bool defer_factorized_materialization, RelationRows rows,
    int physical_middle = -1, int output_owner = -1);

// Materialize the join exactly as the baseline does, then encode only exact
// duplicate complete rows as (source-set x destinations) rectangles. Owners
// are independent, so the equality-class construction is parallel without
// adding synchronization to the bitmap join itself.
static IncrementalCompleteRowGroups IncrementalGroupCompleteRowsByOwner(
    const IncrementalWorkerState& state, RelationRows rows,
    const vector<int>* balance_buckets = nullptr, bool home_output = false) {
    const auto begin = chrono::steady_clock::now();
    using OwnedRow = pair<uint64_t, RelationDstSet>;
    vector<vector<OwnedRow>> rows_by_owner(
        static_cast<size_t>(state.total_partitions));
    IncrementalCompleteRowGroups output;
    output.unique_rows_by_owner.resize(
        static_cast<size_t>(state.total_partitions));
    output.shared_rows_by_owner.resize(
        static_cast<size_t>(state.total_partitions));
    output.ordinary_bytes_by_owner.assign(
        static_cast<size_t>(state.total_partitions), 16);

    for (auto& [source, destinations] : rows) {
        const int owner = balance_buckets != nullptr
            ? balance_buckets->at(IncrementalBalanceBucket(source, balance_buckets->size()))
            : IncrementalResultHome(state, source);
        output.ordinary_bytes_by_owner[static_cast<size_t>(owner)] +=
            16 + destinations.bitmap.getSizeInBytes(true);
        rows_by_owner[static_cast<size_t>(owner)].emplace_back(
            source, std::move(destinations));
    }

    const size_t thread_count = min(
        static_cast<size_t>(max(1, state.total_partitions)),
        IncrementalComputeThreadCount(state.total_partitions));
    auto group_owners = [&](size_t thread_index) {
        for (size_t owner = thread_index;
             owner < rows_by_owner.size(); owner += thread_count) {
            auto& owner_rows = rows_by_owner[owner];
            auto& unique_rows = output.unique_rows_by_owner[owner];
            auto& shared_rows = output.shared_rows_by_owner[owner];
            unordered_map<uint64_t, vector<size_t>> groups_by_fingerprint;
            vector<IncrementalMatrixJoinResult::SharedBlock> groups;
            groups_by_fingerprint.reserve(owner_rows.size() * 2 + 1);
            groups.reserve(owner_rows.size());
            for (size_t row_index = 0; row_index < owner_rows.size();
                 ++row_index) {
                auto& [source, destinations] = owner_rows[row_index];
                const uint64_t fingerprint =
                    IncrementalBitmapFingerprint(destinations);
                auto& candidates = groups_by_fingerprint[fingerprint];
                size_t matching_group = groups.size();
                for (size_t group_index : candidates) {
                    if (groups[group_index].destinations.bitmap ==
                        destinations.bitmap) {
                        matching_group = group_index;
                        break;
                    }
                }
                if (matching_group == groups.size()) {
                    IncrementalMatrixJoinResult::SharedBlock block;
                    block.sources.push_back(source);
                    block.destinations = std::move(destinations);
                    groups.push_back(std::move(block));
                    candidates.push_back(groups.size() - 1);
                } else {
                    groups[matching_group].sources.push_back(source);
                }
            }
            unique_rows.reserve(groups.size());
            shared_rows.reserve(groups.size());
            for (auto& group : groups) {
                if (group.sources.cardinality() == 1) {
                    unique_rows.emplace(
                        *group.sources.begin(), std::move(group.destinations));
                } else {
                    shared_rows.push_back(std::move(group));
                }
            }
        }
    };
    if (thread_count == 1) {
        group_owners(0);
    } else {
        vector<thread> workers;
        workers.reserve(thread_count);
        for (size_t thread_index = 0; thread_index < thread_count; ++thread_index) {
            workers.emplace_back(group_owners, thread_index);
        }
        for (auto& worker : workers) worker.join();
    }
    output.grouping_us = static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - begin).count());
    return output;
}

// This is deliberately submitted after the GCNP/join task has completed.
// The grouping itself is unchanged, so correctness and wire format remain
// identical; only its scheduling boundary changes.
static void IncrementalScheduleCompleteRowsForMessagePreparation(
    IncrementalWorkerState& state, int query_id, uint64_t stage_id,
    bool defer_factorized_materialization, RelationRows rows,
    int physical_middle, int output_owner) {
    const auto trace_queued = chrono::steady_clock::now();
    state.matrix_message_executor.Submit(
        [&state, query_id, stage_id, defer_factorized_materialization, trace_queued,
         rows = std::move(rows), physical_middle, output_owner]() mutable {
            const auto trace_begin = chrono::steady_clock::now();
            const size_t trace_rows = rows.size();
            bool home_output = false;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit != state.queries.end())
                    home_output = qit->second.home_output_stages.count(stage_id) != 0;
            }
            IncrementalCompleteRowGroups grouped =
                IncrementalGroupCompleteRowsByOwner(state, std::move(rows), nullptr, home_output);
            if (IncrementalScalingTraceEnabled()) {
                ostringstream line;
                line << "[result-group-timing] worker=" << state.partition_id
                     << " query=" << query_id << " stage=" << stage_id
                     << " rows=" << trace_rows
                     << " queue_us=" << chrono::duration_cast<chrono::microseconds>(trace_begin - trace_queued).count()
                     << " group_us=" << grouped.grouping_us
                     << " wall_end_us=" << IncrementalTraceWallUs();
                cerr << line.str() << '\n';
            }
            uint64_t factorized_messages = 0;
            uint64_t factorized_blocks = 0;
            uint64_t wire_bytes_saved = 0;
            for (int owner = 0; owner < state.total_partitions; ++owner) {
                if (output_owner >= 0 && owner != output_owner) continue;
                IncrementalMatrixJoinResult result;
                result.query_id = query_id;
                result.global_exact_stage = true;
                result.defer_factorized_materialization =
                    defer_factorized_materialization;
                result.global_stage_id = stage_id;
                result.middle_partition = physical_middle >= 0
                    ? physical_middle : state.partition_id;
                result.destination_pid = owner;
                result.rows = std::move(grouped.unique_rows_by_owner[
                    static_cast<size_t>(owner)]);
                result.shared_blocks = std::move(
                    grouped.shared_rows_by_owner[static_cast<size_t>(owner)]);
                result.factorized_rows = !result.shared_blocks.empty();
                if (IncrementalDirectBalanceEnabled()) {
                    if (result.factorized_rows) {
                        ++factorized_messages;
                        factorized_blocks += result.shared_blocks.size();
                        const uint64_t factorized_bytes = IncrementalFactorizedEncodingBytes(
                            result.rows, result.shared_blocks, false);
                        const uint64_t ordinary_bytes = grouped.ordinary_bytes_by_owner[owner];
                        if (ordinary_bytes > factorized_bytes) wire_bytes_saved += ordinary_bytes - factorized_bytes;
                    }
                    // Each complete message is independently decodable; keep
                    // the same owner, rectangles and seal, only pack in parallel.
                    state.matrix_message_executor.Submit(
                        [&state, owner, query_id, result = std::move(result)]() mutable {
                            try { IncrementalSendDestinationShardResult(state, owner, std::move(result)); }
                            catch (const exception& error) {
                                IncrementalRecordPeerFailure(state, query_id, owner,
                                    MessageType::INCREMENTAL_MATRIX_JOIN_RESULT, error.what());
                            }
                        });
                    continue;
                }
                if (!result.factorized_rows) {
                    IncrementalSendDestinationShardResult(
                        state, owner, std::move(result));
                    continue;
                }
                ++factorized_messages;
                factorized_blocks += result.shared_blocks.size();
                const uint64_t factorized_bytes =
                    IncrementalFactorizedEncodingBytes(
                        result.rows, result.shared_blocks, false);
                const uint64_t ordinary_bytes =
                    grouped.ordinary_bytes_by_owner[static_cast<size_t>(owner)];
                if (ordinary_bytes > factorized_bytes) {
                    wire_bytes_saved += ordinary_bytes - factorized_bytes;
                }
                IncrementalSendDestinationShardResult(
                    state, owner, std::move(result));
            }
            if (factorized_messages == 0) return;
            lock_guard<mutex> lk(state.mu);
            auto qit = state.queries.find(query_id);
            if (qit == state.queries.end()) return;
            qit->second.factorized_result_messages += factorized_messages;
            qit->second.factorized_result_shared_blocks += factorized_blocks;
            qit->second.factorized_result_wire_bytes_saved += wire_bytes_saved;
            qit->second.factorized_result_encode_us += grouped.grouping_us;
        });
}

static bool IncrementalTryEncodeResultDictionary(
    IncrementalWorkerState& state, int query_id, int owner,
    const RelationRows& residual_rows,
    vector<IncrementalMatrixJoinResult::SharedBlock>& blocks) {
    lock_guard<mutex> lk(state.mu);
    auto query_it = state.queries.find(query_id);
    if (query_it == state.queries.end()) return false;
    auto& dictionaries = query_it->second.result_send_dictionaries;
    if (dictionaries.size() != static_cast<size_t>(state.total_partitions)) {
        dictionaries.resize(static_cast<size_t>(state.total_partitions));
    }
    auto& dictionary = dictionaries.at(static_cast<size_t>(owner));
    const uint64_t legacy_bytes = IncrementalFactorizedEncodingBytes(
        residual_rows, blocks, false);
    unordered_map<string, uint64_t> pending_ids;
    vector<RelationDstSet> complete_destinations;
    complete_destinations.reserve(blocks.size());
    uint64_t next_dictionary_id = dictionary.next_dictionary_id;
    uint64_t last_dictionary_id = dictionary.last_dictionary_id;
    RelationDstSet last_destinations = dictionary.last_destinations;
    for (auto& block : blocks) {
        complete_destinations.push_back(block.destinations);
        const RelationDstSet& complete = complete_destinations.back();
        const string key = IncrementalCanonicalBitmapKey(complete);
        auto exact = dictionary.exact_ids.find(key);
        auto pending = pending_ids.find(key);
        if (exact != dictionary.exact_ids.end() ||
            pending != pending_ids.end()) {
            const uint64_t dictionary_id = exact != dictionary.exact_ids.end()
                ? exact->second
                : pending->second;
            block.destination_codec =
                IncrementalMatrixJoinResult::SharedBlock::
                    DestinationCodec::REFERENCE;
            block.dictionary_id = dictionary_id;
            block.base_dictionary_id = 0;
            block.destinations = RelationDstSet{};
            last_dictionary_id = dictionary_id;
            last_destinations = complete;
            continue;
        }

        const uint64_t dictionary_id = next_dictionary_id++;
        block.dictionary_id = dictionary_id;
        block.base_dictionary_id = 0;
        block.destination_codec =
            IncrementalMatrixJoinResult::SharedBlock::
                DestinationCodec::DEFINE;
        if (last_dictionary_id != 0) {
            RelationDstSet delta;
            delta.bitmap = complete.bitmap ^
                last_destinations.bitmap;
            if (delta.bitmap.getSizeInBytes(true) <
                complete.bitmap.getSizeInBytes(true)) {
                block.destination_codec =
                    IncrementalMatrixJoinResult::SharedBlock::
                        DestinationCodec::XOR_DELTA;
                block.base_dictionary_id = last_dictionary_id;
                block.destinations = std::move(delta);
            }
        }
        pending_ids.emplace(key, dictionary_id);
        last_dictionary_id = dictionary_id;
        last_destinations = complete;
    }

    const uint64_t dictionary_bytes = IncrementalFactorizedEncodingBytes(
        residual_rows, blocks, true);
    if (dictionary_bytes >= legacy_bytes) {
        for (size_t index = 0; index < blocks.size(); ++index) {
            auto& block = blocks[index];
            block.destination_codec =
                IncrementalMatrixJoinResult::SharedBlock::
                    DestinationCodec::DEFINE;
            block.dictionary_id = 0;
            block.base_dictionary_id = 0;
            block.destinations = std::move(complete_destinations[index]);
        }
        return false;
    }
    for (auto& [key, dictionary_id] : pending_ids) {
        dictionary.exact_ids.emplace(std::move(key), dictionary_id);
    }
    dictionary.next_dictionary_id = next_dictionary_id;
    dictionary.last_dictionary_id = last_dictionary_id;
    dictionary.last_destinations = std::move(last_destinations);
    return true;
}

static bool IncrementalTryEncodeMessageLocalDictionary(
    const RelationRows& residual_rows,
    vector<IncrementalMatrixJoinResult::SharedBlock>& blocks) {
    if (blocks.empty()) return false;
    const uint64_t legacy_bytes = IncrementalFactorizedEncodingBytes(
        residual_rows, blocks, false);
    vector<RelationDstSet> complete_destinations;
    complete_destinations.reserve(blocks.size());
    unordered_map<uint64_t, vector<size_t>> definitions_by_fingerprint;
    definitions_by_fingerprint.reserve(blocks.size() * 2 + 1);
    uint64_t previous_id = 0;
    RelationDstSet previous_destinations;

    for (size_t index = 0; index < blocks.size(); ++index) {
        auto& block = blocks[index];
        complete_destinations.push_back(block.destinations);
        const RelationDstSet& complete = complete_destinations.back();
        const uint64_t fingerprint = IncrementalBitmapFingerprint(complete);
        auto& definitions = definitions_by_fingerprint[fingerprint];
        size_t equal_definition = blocks.size();
        for (size_t candidate : definitions) {
            if (complete_destinations[candidate].bitmap == complete.bitmap) {
                equal_definition = candidate;
                break;
            }
        }
        if (equal_definition != blocks.size()) {
            block.destination_codec =
                IncrementalMatrixJoinResult::SharedBlock::
                    DestinationCodec::REFERENCE;
            block.dictionary_id = equal_definition + 1;
            block.base_dictionary_id = 0;
            block.destinations = RelationDstSet{};
            previous_id = equal_definition + 1;
            previous_destinations = complete;
            continue;
        }

        block.dictionary_id = index + 1;
        block.base_dictionary_id = 0;
        block.destination_codec =
            IncrementalMatrixJoinResult::SharedBlock::
                DestinationCodec::DEFINE;
        if (previous_id != 0) {
            RelationDstSet delta;
            delta.bitmap = complete.bitmap ^ previous_destinations.bitmap;
            if (delta.bitmap.getSizeInBytes(true) <
                complete.bitmap.getSizeInBytes(true)) {
                block.destination_codec =
                    IncrementalMatrixJoinResult::SharedBlock::
                        DestinationCodec::XOR_DELTA;
                block.base_dictionary_id = previous_id;
                block.destinations = std::move(delta);
            }
        }
        definitions.push_back(index);
        previous_id = index + 1;
        previous_destinations = complete;
    }

    const uint64_t dictionary_bytes = IncrementalFactorizedEncodingBytes(
        residual_rows, blocks, true);
    if (dictionary_bytes >= legacy_bytes) {
        for (size_t index = 0; index < blocks.size(); ++index) {
            auto& block = blocks[index];
            block.destination_codec =
                IncrementalMatrixJoinResult::SharedBlock::
                    DestinationCodec::DEFINE;
            block.dictionary_id = 0;
            block.base_dictionary_id = 0;
            block.destinations = std::move(complete_destinations[index]);
        }
        return false;
    }
    return true;
}

#include "synchronous_stage_balance.inc"

static void IncrementalAcceptGlobalExactStageUpdate(
    IncrementalWorkerState& state, IncrementalMatrixJoinUpdate update) {
    bool ready = false;
    bool base_ready = false;
    bool virtual_stage = false;
    bool defer_factorized_materialization = false;
    int base_body_id = -1;
    RelationDstSet requested_sources;
    RelationRows left;
    RelationRows right;
    RelationRows coverage;
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(update.query_id);
        if (qit == state.queries.end()) return;
        auto& stage =
            qit->second.global_exact_middle_stages[update.global_stage_id];
        stage.global_virtual_stage =
            stage.global_virtual_stage || update.global_virtual_stage;
        stage.defer_factorized_materialization =
            stage.defer_factorized_materialization ||
            update.defer_factorized_materialization;
        if (update.global_base_stage) {
            stage.global_base_stage = true;
            stage.base_body_id = update.right_body_id;
            stage.requested_sources.bitmap |= update.right_sources.bitmap;
        } else {
            IncrementalMergeRows(stage.left_rows, update.left_rows);
            IncrementalMergeRows(stage.right_rows, update.right_rows);
            IncrementalMergeRows(stage.coverage_rows, update.coverage_rows);
        }
        if (update.global_rhs_seal && update.global_sender_pid >= 0 &&
            update.global_sender_pid < state.total_partitions) {
            stage.rhs_sealed_senders.add(
                static_cast<uint32_t>(update.global_sender_pid));
        }
        if (update.global_exact_seal && update.global_sender_pid >= 0 &&
            update.global_sender_pid < state.total_partitions) {
            stage.sealed_senders.add(
                static_cast<uint32_t>(update.global_sender_pid));
        }
        const bool rhs_ready_for_join = stage.global_base_stage ||
            stage.rhs_sealed_senders.cardinality() ==
                static_cast<uint64_t>(state.total_partitions);
        if (!stage.join_queued &&
            rhs_ready_for_join &&
            stage.sealed_senders.cardinality() ==
                static_cast<uint64_t>(state.total_partitions)) {
            stage.join_queued = true;
            if (stage.global_base_stage) {
                base_ready = true;
                base_body_id = stage.base_body_id;
                requested_sources = std::move(stage.requested_sources);
            } else {
                left = std::move(stage.left_rows);
                right = std::move(stage.right_rows);
                coverage = std::move(stage.coverage_rows);
                virtual_stage = stage.global_virtual_stage;
                defer_factorized_materialization =
                    stage.defer_factorized_materialization;
            }
            qit->second.global_exact_middle_stages.erase(
                update.global_stage_id);
            ready = !base_ready;
        }
    }
    if (base_ready) {
        const int query_id = update.query_id;
        const uint64_t stage_id = update.global_stage_id;
        // Graph owners read adjacency; every complete source row then moves
        // to the same home used by compose, identity and closure relations.
        auto deliver = [&state, query_id, stage_id](RelationRows rows) {
            vector<RelationRows> by_home(static_cast<size_t>(state.total_partitions));
            for (auto& [source, values] : rows) {
                by_home[static_cast<size_t>(IncrementalResultHome(state, source))]
                    .emplace(source, std::move(values));
            }
            for (int home = 0; home < state.total_partitions; ++home) {
                IncrementalMatrixJoinResult result;
                result.query_id = query_id;
                result.global_exact_stage = true;
                result.global_stage_id = stage_id;
                result.middle_partition = state.partition_id;
                result.destination_pid = home;
                result.rows = std::move(by_home[static_cast<size_t>(home)]);
                IncrementalSendDestinationShardResult(state, home, std::move(result));
            }
        };
        if (base_body_id < 0) {
            deliver(IncrementalMatrixIdentityRows(requested_sources));
            return;
        }
        IncrementalCollectGlobalBaseRelationBulk(
            state, query_id, base_body_id, requested_sources,
            [deliver](RelationRows rows) mutable { deliver(std::move(rows)); });
        return;
    }
    if (!ready) return;

    const int query_id = update.query_id;
    const uint64_t stage_id = update.global_stage_id;
    IncrementalSubmitCompute(
        state, query_id,
        [&state, query_id, stage_id, virtual_stage,
         defer_factorized_materialization,
         left = std::move(left), right = std::move(right),
         coverage = std::move(coverage)]() mutable {
            IncrementalPreRouteCoverageStats coverage_stats;
            const auto join_begin = chrono::steady_clock::now();
            GcnpExecutionStats stats;
            RelationRows candidates = virtual_stage
                ? IncrementalMatrixAdvanceVirtualEdge(left, right)
                : ComposeRelationRowsGcnp(
                    left, right, nullptr, nullptr, &stats);
            const uint64_t join_us = static_cast<uint64_t>(
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - join_begin).count());
            const uint64_t input_pairs = virtual_stage
                ? CountRelationPairs(left) : stats.usable_input_mids;
            const uint64_t result_pairs = CountRelationPairs(candidates);
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit == state.queries.end()) return;
                auto& query = qit->second;
                if (!virtual_stage) {
                    ++query.matrix_join_updates;
                    query.matrix_join_result_pairs += result_pairs;
                    query.matrix_join_us += join_us;
                    ++query.gcnp_metrics.batches;
                    query.gcnp_metrics.source_rows += stats.usable_source_rows;
                    query.gcnp_input_mids += input_pairs;
                    query.gcnp_planned_mids += input_pairs;
                    query.gcnp_wall_us += join_us;
                    query.gcnp_complete_prepare_us += stats.prepare_us;
                    query.gcnp_complete_group_extract_us += stats.group_extract_us;
                    query.gcnp_complete_pattern_cache_us += stats.pattern_cache_us;
                    query.gcnp_complete_union_us += stats.union_us;
                    query.gcnp_complete_shared_target_us += stats.shared_target_us;
                    query.gcnp_complete_source_materialize_us +=
                        stats.source_materialize_us;
                    query.gcnp_complete_groups += stats.groups;
                    query.gcnp_complete_reusable_groups += stats.reusable_groups;
                    query.gcnp_complete_grouped_source_uses +=
                        stats.grouped_source_uses;
                    query.gcnp_complete_shared_source_uses +=
                        stats.shared_source_uses;
                    query.gcnp_complete_external_memo_hits +=
                        stats.external_memo_hits;
                    query.pre_route_coverage_candidate_mids +=
                        coverage_stats.candidate_mids;
                    query.pre_route_coverage_signature_rejections +=
                        coverage_stats.signature_rejections;
                    query.pre_route_coverage_subset_tests +=
                        coverage_stats.subset_tests;
                    query.pre_route_coverage_skipped_mids +=
                        coverage_stats.skipped_mids;
                    query.pre_route_coverage_skipped_rhs_pairs +=
                        coverage_stats.skipped_rhs_pairs;
                    query.pre_route_coverage_input_left_pairs +=
                        coverage_stats.input_left_pairs;
                    query.pre_route_coverage_output_left_pairs +=
                        coverage_stats.output_left_pairs;
                }
            }

            if (virtual_stage) {
                vector<RelationRows> rows_by_owner(
                    static_cast<size_t>(state.total_partitions));
                for (auto& [source, destinations] : candidates) {
                    const int owner = IncrementalOwner(state, source);
                    rows_by_owner[static_cast<size_t>(owner)].emplace(
                        source, std::move(destinations));
                }
                for (int owner = 0; owner < state.total_partitions; ++owner) {
                    IncrementalMatrixJoinResult result;
                    result.query_id = query_id;
                    result.global_exact_stage = true;
                    result.global_stage_id = stage_id;
                    result.middle_partition = state.partition_id;
                    result.destination_pid = owner;
                    result.rows = std::move(
                        rows_by_owner[static_cast<size_t>(owner)]);
                    IncrementalSendDestinationShardResult(
                        state, owner, std::move(result));
                }
                return;
            }

            if (!virtual_stage) {
                IncrementalScheduleCompleteRowsForMessagePreparation(
                    state, query_id, stage_id,
                    defer_factorized_materialization, std::move(candidates));
                return;
            }

        });
}

static void IncrementalHandleMaterializedGlobalExactStageResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result) {
    uint64_t grouped_blocks = 0;
    uint64_t grouped_source_uses = 0;
    function<void(RelationRows)> done;
    RelationRows rows;
    const auto materialize_begin = chrono::steady_clock::now();
    if (result.factorized_rows) {
        grouped_blocks = result.shared_blocks.size();
        for (const auto& block : result.shared_blocks) {
            grouped_source_uses += block.sources.cardinality();
        }

        unordered_map<uint64_t, RelationDstSet>* dictionary = nullptr;
        unordered_map<uint64_t, RelationDstSet> message_dictionary;
        if (result.dictionary_coded_blocks) {
            message_dictionary.reserve(result.shared_blocks.size() * 2 + 1);
            dictionary = &message_dictionary;
        }
        for (auto& block : result.shared_blocks) {
            using Codec = IncrementalMatrixJoinResult::SharedBlock::
                DestinationCodec;
            if (dictionary != nullptr &&
                block.destination_codec == Codec::REFERENCE) {
                auto defined = dictionary->find(block.dictionary_id);
                if (defined == dictionary->end()) {
                    cerr << "[factorized-result] missing dictionary ref="
                         << block.dictionary_id << " sender="
                         << result.middle_partition << '\n';
                    return;
                }
                block.destinations = defined->second;
            } else if (dictionary != nullptr &&
                       block.destination_codec == Codec::XOR_DELTA) {
                auto base = dictionary->find(block.base_dictionary_id);
                if (base == dictionary->end()) {
                    cerr << "[factorized-result] missing delta base="
                         << block.base_dictionary_id << " sender="
                         << result.middle_partition << '\n';
                    return;
                }
                block.destinations.bitmap ^= base->second.bitmap;
                (*dictionary)[block.dictionary_id] = block.destinations;
            } else if (dictionary != nullptr) {
                (*dictionary)[block.dictionary_id] = block.destinations;
            }
        }
    }

    if (result.factorized_rows && !result.shared_blocks.empty()) {
        using BitmapInputs = vector<const SharedBitmap64*>;
        unordered_map<uint64_t, BitmapInputs> inputs_by_source;
        inputs_by_source.reserve(min<uint64_t>(
            grouped_source_uses, numeric_limits<size_t>::max()));
        for (const auto& block : result.shared_blocks) {
            for (uint64_t source : block.sources) {
                inputs_by_source[source].push_back(
                    &block.destinations.bitmap);
            }
        }
        vector<const decltype(inputs_by_source)::value_type*> source_inputs;
        source_inputs.reserve(inputs_by_source.size());
        for (const auto& entry : inputs_by_source) {
            source_inputs.push_back(&entry);
        }
        vector<RelationDstSet> restored(source_inputs.size());
        auto union_scratch = make_unique<GcnpBitmapUnionScratch>();
        for (size_t index = 0; index < source_inputs.size(); ++index) {
            const auto& inputs = source_inputs[index]->second;
            restored[index] = GcnpUnionBitmapInputs(inputs, *union_scratch);
        }
        for (size_t index = 0; index < source_inputs.size(); ++index) {
            const uint64_t source = source_inputs[index]->first;
            auto row = result.rows.find(source);
            if (row == result.rows.end()) {
                result.rows.emplace(source, std::move(restored[index]));
            } else {
                row->second |= restored[index];
            }
        }
        result.shared_blocks.clear();
    }

    const auto commit_begin = chrono::steady_clock::now();
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(result.query_id);
        if (qit == state.queries.end()) return;
        auto stage_it = qit->second.global_exact_output_stages.find(
            result.global_stage_id);
        if (stage_it == qit->second.global_exact_output_stages.end()) return;
        auto& stage = stage_it->second;
        const uint64_t expected = stage.expected_middles == 0
            ? static_cast<uint64_t>(state.total_partitions)
            : stage.expected_middles;
        if (IncrementalDirectBalanceEnabled()) {
            IncrementalMergeOwnedRows(stage.rows, result.rows);
        } else IncrementalMergeRows(stage.rows, result.rows);
        if (result.middle_partition >= 0 &&
            result.middle_partition < state.total_partitions) {
            stage.completed_middles.add(
                static_cast<uint32_t>(result.middle_partition));
        }
        qit->second.factorized_result_blocks_after_coalesce += grouped_blocks;
        qit->second.factorized_result_source_uses += grouped_source_uses;
        if (stage.completed_middles.cardinality() == expected) {
            rows = std::move(stage.rows);
            done = std::move(stage.done);
            qit->second.global_exact_output_stages.erase(stage_it);
        }
    }
    const uint64_t materialize_us = static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - materialize_begin).count());
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(result.query_id);
        if (qit != state.queries.end()) {
            qit->second.factorized_result_expand_us += materialize_us;
            qit->second.result_commit_us +=
                chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - commit_begin).count();
        }
    }
    if (done && IncrementalScalingTraceEnabled()) {
        ostringstream line;
        line << "[stage-timeline] ev=owner_complete worker=" << state.partition_id
             << " query=" << result.query_id << " stage=" << result.global_stage_id
             << " wall_us=" << IncrementalTraceWallUs() << " rows=" << rows.size();
        cerr << line.str() << '\n';
    }
    if (done) done(std::move(rows));
}

static void IncrementalHandleOnlineDeferredGlobalExactStageResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result) {
    const bool balanced_reducer = result.middle_compute_result;
    const uint64_t input_blocks = result.factorized_rows
        ? result.shared_blocks.size() : 0;
    uint64_t grouped_source_uses = 0;
    for (const auto& block : result.shared_blocks) {
        grouped_source_uses += block.sources.cardinality();
    }

    shared_ptr<IncrementalSourceSetAccumulator> accumulator;
    {
        lock_guard<mutex> lk(state.mu);
        auto query_it = state.queries.find(result.query_id);
        if (query_it == state.queries.end()) return;
        auto& stages = balanced_reducer
            ? query_it->second.balance_reduction_stages
            : query_it->second.global_exact_output_stages;
        if (balanced_reducer) stages.try_emplace(result.global_stage_id);
        auto stage_it = stages.find(result.global_stage_id);
        if (stage_it == stages.end()) return;
        auto& stage = stage_it->second;
        if (!stage.source_set_accumulator) {
            const int independent_shards = state.total_partitions == 2
                ? 1 : max(1, state.total_partitions);
            const size_t shard_count = min<size_t>(
                static_cast<size_t>(independent_shards),
                IncrementalComputeThreadCount(state.total_partitions));
            stage.source_set_accumulator =
                make_shared<IncrementalSourceSetAccumulator>(shard_count);
        }
        accumulator = stage.source_set_accumulator;
    }

    const auto group_begin = chrono::steady_clock::now();
    vector<uint64_t> fingerprints(result.shared_blocks.size());
    const size_t lane_count = min(
        accumulator->shards.size(),
        max<size_t>(1, result.shared_blocks.size()));
    auto fingerprint = [&](size_t lane) {
        for (size_t index = lane; index < result.shared_blocks.size();
             index += lane_count) {
            fingerprints[index] = IncrementalSourceSetFingerprint(
                result.shared_blocks[index].sources);
        }
    };
    auto run_parallel = [&](auto task) {
        if (lane_count == 1) {
            task(0);
            return;
        }
        atomic<size_t> pending(lane_count);
        promise<void> completed;
        auto completed_future = completed.get_future();
        vector<exception_ptr> errors(lane_count);
        for (size_t lane = 0; lane < lane_count; ++lane) {
            state.factorization_executor.Submit(
                [&, lane] {
                    try {
                        task(lane);
                    } catch (...) {
                        errors[lane] = current_exception();
                    }
                    if (pending.fetch_sub(1) == 1) completed.set_value();
                });
        }
        completed_future.wait();
        for (const auto& error : errors) {
            if (error) rethrow_exception(error);
        }
    };
    if (!result.shared_blocks.empty()) run_parallel(fingerprint);

    vector<vector<size_t>> block_indices_by_shard(
        accumulator->shards.size());
    for (size_t index = 0; index < result.shared_blocks.size(); ++index) {
        block_indices_by_shard[
            fingerprints[index] % accumulator->shards.size()]
            .push_back(index);
    }
    vector<uint64_t> reused_by_lane(lane_count);
    vector<uint64_t> merged_by_lane(lane_count);
    vector<uint64_t> avoided_by_lane(lane_count);
    auto group_exact = [&](size_t lane) {
        for (size_t shard_index = lane;
             shard_index < accumulator->shards.size();
             shard_index += lane_count) {
            if (block_indices_by_shard[shard_index].empty()) continue;
            auto& shard = *accumulator->shards[shard_index];
            lock_guard<mutex> shard_lock(shard.mu);
            for (size_t block_index : block_indices_by_shard[shard_index]) {
                auto& incoming = result.shared_blocks[block_index];
                auto& candidates = shard.candidates_by_fingerprint[
                    fingerprints[block_index]];
                size_t matching_group = shard.groups.size();
                for (size_t group_index : candidates) {
                    if (shard.groups[group_index].sources.bitmap ==
                        incoming.sources.bitmap) {
                        matching_group = group_index;
                        break;
                    }
                }
                if (matching_group == shard.groups.size()) {
                    IncrementalSourceSetPendingGroup group;
                    group.sources = std::move(incoming.sources);
                    group.destinations.push_back(
                        std::move(incoming.destinations));
                    shard.groups.push_back(std::move(group));
                    candidates.push_back(shard.groups.size() - 1);
                    continue;
                }
                auto& group = shard.groups[matching_group];
                if (group.destinations.size() == 1) {
                    ++reused_by_lane[lane];
                }
                ++merged_by_lane[lane];
                avoided_by_lane[lane] +=
                    group.sources.cardinality() - 1;
                group.destinations.push_back(
                    std::move(incoming.destinations));
            }
        }
    };
    if (!result.shared_blocks.empty()) run_parallel(group_exact);
    const uint64_t group_us = static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - group_begin).count());
    uint64_t reused_groups = 0;
    uint64_t blocks_merged = 0;
    uint64_t union_operations_avoided = 0;
    for (size_t lane = 0; lane < lane_count; ++lane) {
        reused_groups += reused_by_lane[lane];
        blocks_merged += merged_by_lane[lane];
        union_operations_avoided += avoided_by_lane[lane];
    }

    function<void(RelationRows)> done;
    RelationRows rows;
    bool reducer_complete = false;
    {
        lock_guard<mutex> lk(state.mu);
        auto query_it = state.queries.find(result.query_id);
        if (query_it == state.queries.end()) return;
        auto& stages = balanced_reducer
            ? query_it->second.balance_reduction_stages
            : query_it->second.global_exact_output_stages;
        auto stage_it = stages.find(result.global_stage_id);
        if (stage_it == stages.end()) return;
        auto& stage = stage_it->second;
        if (IncrementalDirectBalanceEnabled()) {
            IncrementalMergeOwnedRows(stage.rows, result.rows);
        } else IncrementalMergeRows(stage.rows, result.rows);
        if (result.middle_partition >= 0 &&
            result.middle_partition < state.total_partitions) {
            stage.completed_middles.add(
                static_cast<uint32_t>(result.middle_partition));
        }
        auto& query = query_it->second;
        query.factorized_result_source_uses += grouped_source_uses;
        query.factorized_source_set_blocks_before += input_blocks;
        query.factorized_source_set_reused_groups += reused_groups;
        query.factorized_source_set_blocks_merged += blocks_merged;
        query.factorized_source_union_operations_avoided +=
            union_operations_avoided;
        query.factorized_source_set_group_us += group_us;
        query.factorized_result_coalesce_us += group_us;
        if (balanced_reducer) query.balance_reduce_us += group_us;
        const uint64_t expected = stage.expected_middles == 0
            ? static_cast<uint64_t>(state.total_partitions)
            : stage.expected_middles;
        if (stage.completed_middles.cardinality() == expected) {
            rows = std::move(stage.rows);
            accumulator = std::move(stage.source_set_accumulator);
            done = std::move(stage.done);
            stages.erase(stage_it);
            reducer_complete = balanced_reducer;
        }
    }
    if (!done && !reducer_complete) return;

    uint64_t blocks_after = 0;
    vector<vector<IncrementalMatrixJoinResult::SharedBlock>> balanced_blocks(
        balanced_reducer ? state.total_partitions : 0);
    vector<pair<const RelationDstSet*, RelationDstSet>> restore_groups;
    uint64_t restore_source_uses = 0;
    const auto union_begin = chrono::steady_clock::now();
    // Group target unions stay serial; restoring the completed rectangles
    // below owns separate source rows and does not perform a join.
        for (auto& shard_ptr : accumulator->shards) {
            for (auto& group : shard_ptr->groups) {
                if (group.destinations.empty()) continue;
                ++blocks_after;
                RelationDstSet destinations;
                if (group.destinations.size() >= 8) {
                    vector<const roaring::Roaring64Map*> inputs;
                    inputs.reserve(group.destinations.size());
                    for (const auto& input : group.destinations) {
                        inputs.push_back(&input.bitmap.view());
                    }
                    destinations.bitmap = roaring::Roaring64Map::fastunion(
                        inputs.size(), inputs.data());
                } else {
                    destinations = std::move(group.destinations.front());
                    for (size_t index = 1;
                         index < group.destinations.size(); ++index) {
                        destinations.bitmap |=
                            group.destinations[index].bitmap;
                    }
                }
                if (balanced_reducer) {
                    // Keep shared targets shared across the return hop. Final
                    // owners restore rows with the existing result handler.
                    vector<RelationDstSet> sources_by_owner(state.total_partitions);
                    for (uint64_t source : group.sources) {
                        sources_by_owner[IncrementalOwner(state, source)].push_back(source);
                    }
                    for (int owner = 0; owner < state.total_partitions; ++owner) {
                        if (sources_by_owner[owner].empty()) continue;
                        IncrementalMatrixJoinResult::SharedBlock block;
                        block.sources = std::move(sources_by_owner[owner]);
                        block.destinations = destinations;
                        balanced_blocks[owner].push_back(std::move(block));
                    }
                } else {
                    restore_source_uses += group.sources.cardinality();
                    restore_groups.emplace_back(&group.sources,
                        std::move(destinations));
                }
            }
        }
    const uint64_t union_us = static_cast<uint64_t>(
        chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - union_begin).count());
    uint64_t restore_us = 0;
    size_t restore_lanes = 0;
    if (!restore_groups.empty()) {
        const auto restore_begin = chrono::steady_clock::now();
        restore_lanes = min(IncrementalMatrixMessageThreadCount(
            state.total_partitions), max<size_t>(1,
                (restore_source_uses + 255) / 256));
        if (restore_lanes == 1) {
            for (const auto& entry : restore_groups) {
                for (uint64_t source : *entry.first) {
                    rows[source].bitmap |= entry.second.bitmap;
                }
            }
        } else {
            using RestoreInput = pair<uint64_t, const RelationDstSet*>;
            vector<vector<RestoreInput>> inputs(restore_lanes);
            vector<RelationRows> restored(restore_lanes);
            for (auto& row : rows) {
                restored[row.first % restore_lanes].emplace(row.first,
                    std::move(row.second));
            }
            rows.clear();
            for (const auto& entry : restore_groups) {
                for (uint64_t source : *entry.first) {
                    inputs[source % restore_lanes].emplace_back(
                        source, &entry.second);
                }
            }
            atomic<size_t> pending(restore_lanes);
            promise<void> finished;
            auto ready = finished.get_future();
            vector<exception_ptr> errors(restore_lanes);
            for (size_t lane = 0; lane < restore_lanes; ++lane) {
                state.factorization_executor.Submit([&, lane] {
                    try {
                        auto& output = restored[lane];
                        for (const auto& input : inputs[lane]) {
                            output[input.first].bitmap |= input.second->bitmap;
                        }
                    } catch (...) { errors[lane] = current_exception(); }
                    if (pending.fetch_sub(1) == 1) finished.set_value();
                });
            }
            ready.wait();
            for (const auto& error : errors) {
                if (error) rethrow_exception(error);
            }
            size_t row_count = 0;
            for (const auto& lane : restored) row_count += lane.size();
            rows.reserve(row_count);
            for (auto& lane : restored) rows.merge(lane);
        }
        restore_us = chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - restore_begin).count();
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto query_it = state.queries.find(result.query_id);
        if (query_it != state.queries.end()) {
            auto& query = query_it->second;
            query.factorized_source_set_blocks_after += blocks_after;
            query.factorized_result_blocks_after_coalesce += blocks_after;
            query.factorized_source_set_union_us += union_us;
            query.factorized_result_union_us += union_us;
            query.result_restore_us += restore_us;
            query.result_restore_max_lanes = max<uint64_t>(
                query.result_restore_max_lanes, restore_lanes);
            if (balanced_reducer) query.balance_reduce_us += union_us + restore_us;
        }
    }
    if (reducer_complete) {
        vector<RelationRows> rows_by_owner(state.total_partitions);
        for (auto& [source, targets] : rows) {
            rows_by_owner[IncrementalOwner(state, source)].emplace(source, std::move(targets));
        }
        for (int owner = 0; owner < state.total_partitions; ++owner) {
            IncrementalMatrixJoinResult output;
            output.query_id = result.query_id;
            output.global_exact_stage = true;
            output.global_stage_id = result.global_stage_id;
            output.middle_partition = state.partition_id;
            output.destination_pid = owner;
            output.rows = std::move(rows_by_owner[owner]);
            output.shared_blocks = std::move(balanced_blocks[owner]);
            output.factorized_rows = !output.shared_blocks.empty();
            IncrementalSendDestinationShardResult(state, owner, std::move(output));
        }
        return;
    }
    done(std::move(rows));
}

static void IncrementalHandleGlobalExactStageResult(
    IncrementalWorkerState& state, IncrementalMatrixJoinResult result) {
    if (result.defer_factorized_materialization) {
        IncrementalHandleOnlineDeferredGlobalExactStageResult(
            state, std::move(result));
        return;
    }
    IncrementalHandleMaterializedGlobalExactStageResult(state, std::move(result));
}

static void IncrementalGlobalExactFinish(
    IncrementalWorkerState& state,
    const shared_ptr<IncrementalGlobalExactPlan>& plan,
    RelationRows rows) {
    if (plan->done) {
        auto done = std::move(plan->done);
        done(std::move(rows));
        return;
    }
    lock_guard<mutex> lk(state.mu);
    auto qit = state.queries.find(plan->query_id);
    if (qit == state.queries.end()) return;
    qit->second.final_rows = std::move(rows);
    ++qit->second.roots_done;
}

static uint64_t IncrementalPlannedExactStageId(
    const IncrementalGlobalExactPlan& plan, int length) {
    const uint64_t key = IncrementalGlobalExactStageId(plan.body_id, length);
    if (plan.stage_namespace == 0) return key;
    return plan.stage_namespace ^
        (key + 0x9e3779b97f4a7c15ULL +
         (plan.stage_namespace << 6) + (plan.stage_namespace >> 2));
}

static void IncrementalGlobalExactCombineNext(
    IncrementalWorkerState& state,
    const shared_ptr<IncrementalGlobalExactPlan>& plan,
    size_t next_index, int current_length, RelationRows current) {
    if (next_index >= plan->selected_powers.size()) {
        IncrementalGlobalExactFinish(state, plan, std::move(current));
        return;
    }
    const int right_length = plan->selected_powers[next_index];
    const int output_length = current_length + right_length;
    auto right_it = plan->powers.find(right_length);
    if (right_it == plan->powers.end()) return;
    IncrementalComposeInputOwners owners;
    owners.left = IncrementalOwnComposeInput(current);
    if (IncrementalOwnerInputViewsEnabled())
        owners.right = shared_ptr<const RelationRows>(plan, &right_it->second);
    const auto& left = owners.left ? *owners.left : current;
    // Only the plan's final relation must be owner placed.
    owners.home_output = next_index + 1 < plan->selected_powers.size();
    IncrementalStartGlobalRelationCompose(
        state, plan->query_id, plan->body_id,
        IncrementalPlannedExactStageId(*plan, output_length),
        left, right_it->second,
        [&state, plan, next_index, output_length](RelationRows output) mutable {
            IncrementalGlobalExactCombineNext(
                state, plan, next_index + 1, output_length,
                std::move(output));
        }, nullptr, std::move(owners));
}

static void IncrementalGlobalExactBuildPower(
    IncrementalWorkerState& state,
    const shared_ptr<IncrementalGlobalExactPlan>& plan, int power) {
    if (power > plan->highest_power) {
        if (plan->selected_powers.empty()) {
            IncrementalGlobalExactFinish(state, plan, RelationRows{});
            return;
        }
        const int first = plan->selected_powers.front();
        if (IncrementalOwnerInputViewsEnabled() && plan->selected_powers.size() == 1) {
            // This power is the final result, not an input to a later stage.
            { lock_guard<mutex> lk(state.mu); ++state.queries.at(plan->query_id).owner_view_final_moves; }
            IncrementalGlobalExactFinish(state, plan, std::move(plan->powers.at(first)));
            return;
        }
        RelationRows initial = plan->powers.at(first);
        IncrementalGlobalExactCombineNext(
            state, plan, 1, first, std::move(initial));
        return;
    }
    const int half = power / 2;
    auto input = plan->powers.find(half);
    if (input == plan->powers.end()) return;
    IncrementalComposeInputOwners owners;
    if (IncrementalOwnerInputViewsEnabled()) {
        owners.left = shared_ptr<const RelationRows>(plan, &input->second);
        owners.right = owners.left;
    }
    // A squaring is final only when its power is the sole selected power.
    owners.home_output = !(power == plan->highest_power &&
                           plan->selected_powers.size() == 1);
    IncrementalStartGlobalRelationCompose(
        state, plan->query_id, plan->body_id,
        IncrementalPlannedExactStageId(*plan, power),
        input->second, input->second,
        [&state, plan, power](RelationRows output) mutable {
            plan->powers[power] = std::move(output);
            IncrementalGlobalExactBuildPower(state, plan, power * 2);
        }, nullptr, std::move(owners));
}

static void IncrementalGlobalCollectBaseRelation(
    IncrementalWorkerState&, int, int, RelationDstSet, uint64_t,
    function<void(RelationRows)>);

static void IncrementalCollectGlobalExactBase(
    IncrementalWorkerState& state, int query_id, int body_id,
    RelationDstSet sources, uint64_t stage_id, function<void(RelationRows)> done) {
    IncrementalGlobalCollectBaseRelation(
        state, query_id, body_id, std::move(sources), stage_id, std::move(done));
}

static void IncrementalStartGlobalExactRoot(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources) {
    auto plan = make_shared<IncrementalGlobalExactPlan>();
    plan->query_id = query_id;
    plan->body_id = body_id;
    plan->target_length = length;
    if (length == 0) {
        IncrementalGlobalCollectBaseRelation(
            state, query_id, -1, std::move(sources),
            IncrementalPlannedExactStageId(*plan, 0),
            [&state, plan](RelationRows identity) mutable {
                IncrementalGlobalExactFinish(state, plan, std::move(identity));
            });
        return;
    }
    plan->highest_power = 1;
    while (plan->highest_power <= length / 2) plan->highest_power *= 2;
    for (int power = 1; power <= plan->highest_power; power *= 2) {
        if ((length & power) != 0) plan->selected_powers.push_back(power);
    }
    IncrementalCollectGlobalExactBase(
        state, query_id, body_id, std::move(sources),
        IncrementalPlannedExactStageId(*plan, 1),
        [&state, plan](RelationRows base) mutable {
            plan->powers[1] = std::move(base);
            if (plan->target_length == 1) {
                IncrementalGlobalExactFinish(
                    state, plan, plan->powers.at(1));
                return;
            }
            IncrementalGlobalExactBuildPower(state, plan, 2);
        });
}

// The caller must supply the complete owner-local start domain of this body,
// as an ordinary exact root does. A partial frontier must use the source-aware
// evaluator instead: reusing its powers as complete RHS rows would lose paths.
static void IncrementalEvaluateGlobalExactPowerRelation(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, uint64_t stage_namespace,
    function<void(RelationRows)> done) {
    auto plan = make_shared<IncrementalGlobalExactPlan>();
    plan->query_id = query_id;
    plan->body_id = body_id;
    plan->target_length = length;
    plan->stage_namespace = stage_namespace;
    plan->done = std::move(done);
    if (length == 0) {
        IncrementalGlobalCollectBaseRelation(
            state, query_id, -1, std::move(sources),
            IncrementalPlannedExactStageId(*plan, 0),
            [&state, plan](RelationRows identity) mutable {
                IncrementalGlobalExactFinish(state, plan, std::move(identity));
            });
        return;
    }
    while (plan->highest_power <= length / 2) plan->highest_power *= 2;
    for (int power = 1; power <= plan->highest_power; power *= 2) {
        if ((length & power) != 0) plan->selected_powers.push_back(power);
    }
    IncrementalCollectGlobalExactBase(
        state, query_id, body_id, std::move(sources),
        IncrementalPlannedExactStageId(*plan, 1),
        [&state, plan](RelationRows base) mutable {
            plan->powers[1] = std::move(base);
            if (plan->target_length == 1) {
                IncrementalGlobalExactFinish(state, plan, std::move(plan->powers.at(1)));
            } else IncrementalGlobalExactBuildPower(state, plan, 2);
        });
}
