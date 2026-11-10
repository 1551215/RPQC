// Relation-granular distributed translation of the single-machine bounded
// evaluator.  A stage is a complete distributed relation: every source owner
// contributes one panel and every middle owner waits for all source-owner
// seals before running GCNP once.

struct IncrementalGlobalBoundedStage {
    RelationRows power;
    RelationRows closure;
    bool has_power = false;
};
using IncrementalGlobalBoundedDone =
    function<void(IncrementalGlobalBoundedStage)>;

struct IncrementalCompleteBoundedCache {
    mutex mu;
    RelationDstSet sources;
    unordered_map<int, IncrementalGlobalBoundedStage> stages;
};

static void IncrementalRecordBoundedOwnerTime(
    IncrementalWorkerState& state, int query_id, size_t field,
    chrono::steady_clock::time_point begin) {
    const auto elapsed = chrono::duration_cast<chrono::microseconds>(
        chrono::steady_clock::now() - begin).count();
    lock_guard<mutex> lk(state.mu);
    auto found = state.queries.find(query_id);
    if (found != state.queries.end()) found->second.bounded_owner_us[field] += elapsed;
}

static RelationDstSet IncrementalBoundedCollectDestinations(
    IncrementalWorkerState& state, int query_id, const RelationRows& rows) {
    const auto begin = chrono::steady_clock::now();
    auto destinations = IncrementalMatrixCollectDestinations(rows);
    IncrementalRecordBoundedOwnerTime(state, query_id, 0, begin);
    return destinations;
}

static uint64_t IncrementalGlobalBoundedKey(
    int body_id, int min_rep, int max_rep) {
    uint64_t key = 0xcbf29ce484222325ULL;
    auto add = [&key](uint64_t value) {
        key ^= value + 0x9e3779b97f4a7c15ULL + (key << 6) + (key >> 2);
        key *= 0x100000001b3ULL;
    };
    add(static_cast<uint32_t>(body_id));
    add(static_cast<uint32_t>(min_rep));
    add(static_cast<uint32_t>(max_rep));
    return key;
}

static uint64_t IncrementalGlobalBoundedChildKey(
    uint64_t parent, uint64_t tag) {
    parent ^= tag + 0x9e3779b97f4a7c15ULL + (parent << 6) + (parent >> 2);
    parent *= 0x100000001b3ULL;
    return parent;
}

static void IncrementalGlobalCollectBaseRelation(
    IncrementalWorkerState& state, int query_id, int body_id,
    RelationDstSet sources, uint64_t stage_id,
    function<void(RelationRows)> done) {
    vector<RelationDstSet> sources_by_owner(
        static_cast<size_t>(state.total_partitions));
    for (uint64_t source : sources) {
        sources_by_owner[static_cast<size_t>(IncrementalOwner(state, source))]
            .push_back(source);
    }
    {
        lock_guard<mutex> lk(state.mu);
        auto qit = state.queries.find(query_id);
        if (qit == state.queries.end()) return;
        auto& output = qit->second.global_exact_output_stages[stage_id];
        output = IncrementalGlobalExactOutputStage{};
        output.expected_middles = static_cast<uint64_t>(state.total_partitions);
        output.done = std::move(done);
    }
    for (int owner = 0; owner < state.total_partitions; ++owner) {
        IncrementalMatrixJoinUpdate update;
        update.query_id = query_id;
        update.global_exact_stage = true;
        update.global_exact_seal = true;
        update.global_base_stage = true;
        update.global_stage_id = stage_id;
        update.global_sender_pid = state.partition_id;
        update.middle_partition = owner;
        update.right_body_id = body_id;
        update.right_sources =
            std::move(sources_by_owner[static_cast<size_t>(owner)]);
        IncrementalSendGlobalExactUpdate(state, owner, std::move(update));
    }
}

static void IncrementalGlobalBuildExactRelation(
    IncrementalWorkerState&, int, int, int, RelationDstSet, uint64_t,
    function<void(RelationRows)>);

static void IncrementalGlobalBuildUncachedExactRelation(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, uint64_t stage_key,
    function<void(RelationRows)> done) {
    if (length == 0) {
        IncrementalGlobalCollectBaseRelation(
            state, query_id, -1, std::move(sources),
            IncrementalGlobalBoundedChildKey(stage_key, 0x00), std::move(done));
        return;
    }
    if (length == 1) {
        IncrementalGlobalCollectBaseRelation(
            state, query_id, body_id, std::move(sources),
            IncrementalGlobalBoundedChildKey(stage_key, 0x01),
            std::move(done));
        return;
    }

    const auto split = IncrementalExactBinarySplit(length);
    IncrementalGlobalBuildExactRelation(
        state, query_id, body_id, split.first, std::move(sources),
        IncrementalGlobalBoundedChildKey(stage_key, 0x11),
        [&state, query_id, body_id, split, stage_key,
         done = std::move(done)](RelationRows left) mutable {
            RelationDstSet mids = IncrementalBoundedCollectDestinations(state, query_id, left);
            IncrementalGlobalBuildExactRelation(
                state, query_id, body_id, split.second, std::move(mids),
                IncrementalGlobalBoundedChildKey(stage_key, 0x12),
                [&state, query_id, body_id, stage_key,
                 left = std::move(left),
                 done = std::move(done)](RelationRows right) mutable {
                    IncrementalComposeInputOwners owners;
                    owners.left = IncrementalOwnComposeInput(left);
                    owners.right = IncrementalOwnComposeInput(right);
                    const auto& lhs = owners.left ? *owners.left : left;
                    const auto& rhs = owners.right ? *owners.right : right;
                    IncrementalStartGlobalRelationCompose(
                        state, query_id, body_id,
                        IncrementalGlobalBoundedChildKey(stage_key, 0x13),
                        lhs, rhs, std::move(done), nullptr, std::move(owners));
                });
        });
}

static void IncrementalGlobalBuildExactRelation(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, uint64_t stage_key,
    function<void(RelationRows)> done) {
    if (length <= 1) {
        IncrementalGlobalBuildUncachedExactRelation(
            state, query_id, body_id, length, std::move(sources), stage_key,
            std::move(done));
        return;
    }

    // A demanded mids frontier can name sources on other owners. Reuse the
    // existing identity-source exchange before consulting owner-local caches.
    IncrementalGlobalCollectBaseRelation(
        state, query_id, -1, std::move(sources),
        IncrementalGlobalBoundedChildKey(stage_key, 0x51),
        [&state, query_id, body_id, length, stage_key,
         done = std::move(done)](RelationRows identities) mutable {
            RelationDstSet requested;
            for (const auto& [source, _] : identities) {
                if (IncrementalResultHome(state, source) != state.partition_id)
                    throw runtime_error("exact cache source has wrong home");
                requested.push_back(source);
            }
            identities.clear();
            RelationDstSet missing;
            RelationRows cached;
            {
                lock_guard<mutex> lock(state.mu);
                auto query = state.queries.find(query_id);
                if (query == state.queries.end()) return;
                auto& cache = query->second.global_source_exact_cache[body_id][length];
                missing.bitmap = requested.bitmap - cache.computed_sources.bitmap;
                RelationDstSet hits;
                hits.bitmap = requested.bitmap & cache.computed_sources.bitmap;
                cached.reserve(hits.cardinality());
                for (uint64_t source : hits) {
                    auto row = cache.rows.find(source);
                    if (row != cache.rows.end()) cached.emplace(source, row->second);
                }
            }
            if (IncrementalScalingTraceEnabled()) {
                cerr << "[source-exact-cache] worker=" << state.partition_id
                     << " query=" << query_id << " body=" << body_id
                     << " length=" << length << " requested=" << requested.cardinality()
                     << " hits=" << requested.cardinality() - missing.cardinality()
                     << " missing=" << missing.cardinality()
                     << " scope=query_body_exponent_source threads=1" << '\n';
            }
            // Every owner still participates in the same recursive stages,
            // including an empty local miss set. A local hit is not a global
            // completion barrier and must not strand another owner's seals.
            RelationDstSet computed = missing;
            IncrementalGlobalBuildUncachedExactRelation(
                state, query_id, body_id, length, std::move(missing), stage_key,
                [&state, query_id, body_id, length, computed = std::move(computed),
                 cached = std::move(cached), done = std::move(done)](
                    RelationRows fresh) mutable {
                    {
                        lock_guard<mutex> lock(state.mu);
                        auto query = state.queries.find(query_id);
                        if (query == state.queries.end()) return;
                        auto& cache = query->second.global_source_exact_cache[body_id][length];
                        for (const auto& [source, row] : fresh) {
                            if (!computed.bitmap.contains(source))
                                throw runtime_error("exact cache returned an unrequested source");
                            if (!row.empty()) cache.rows.emplace(source, row);
                        }
                        // Empty completed rows must not become future misses.
                        cache.computed_sources |= computed;
                    }
                    IncrementalMergeOwnedRows(cached, fresh);
                    done(std::move(cached));
                });
        });
}

static void IncrementalEvaluateGlobalExactRelation(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, uint64_t stage_key,
    function<void(RelationRows)> done) {
    IncrementalGlobalBuildExactRelation(
        state, query_id, body_id, length, std::move(sources), stage_key,
        std::move(done));
}

static void IncrementalGlobalBuildBoundedStage(
    IncrementalWorkerState& state, int query_id, int body_id, int depth,
    RelationDstSet sources, bool need_power, uint64_t stage_key,
    IncrementalGlobalBoundedDone done,
    shared_ptr<IncrementalCompleteBoundedCache> complete_cache = nullptr,
    shared_ptr<const unordered_set<int>> required_powers = nullptr) {
    if (!required_powers) {
        auto required = make_shared<unordered_set<int>>();
        unordered_set<int> visited;
        function<void(int)> require_power = [&](int length) {
            if (!required->insert(length).second || length <= 1) return;
            const auto split = IncrementalExactBinarySplit(length);
            require_power(split.first);
            require_power(split.second);
        };
        function<void(int)> visit_suffix = [&](int length) {
            if (length <= 1 || !visited.insert(length).second) return;
            const auto split = IncrementalRangeAdditionSplit(length);
            require_power(split.second);
            visit_suffix(split.first);
            visit_suffix(split.second);
        };
        visit_suffix(depth);
        if (need_power) require_power(depth);
        required_powers = std::move(required);
    }
    need_power = need_power || required_powers->count(depth) != 0;
    if (!complete_cache) {
        complete_cache = make_shared<IncrementalCompleteBoundedCache>();
        complete_cache->sources = sources;
    }
    if (complete_cache) {
        bool hit = false;
        IncrementalGlobalBoundedStage cached;
        {
            lock_guard<mutex> lock(complete_cache->mu);
            auto found = complete_cache->stages.find(depth);
            if (found != complete_cache->stages.end() &&
                (!need_power || found->second.has_power)) {
                cached = found->second;
                hit = true;
            }
        }
        if (hit) {
            done(std::move(cached));
            return;
        }
        // Mirror suffixes always use this evaluation's original source domain.
        // Exact powers expand mids separately, outside this suffix cache.
        sources = complete_cache->sources;
        done = [complete_cache, depth, continuation = std::move(done)](
            IncrementalGlobalBoundedStage result) mutable {
            {
                lock_guard<mutex> lock(complete_cache->mu);
                complete_cache->stages[depth] = result;
            }
            continuation(std::move(result));
        };
    }
    if (depth == 0) {
        IncrementalGlobalCollectBaseRelation(
            state, query_id, -1, std::move(sources),
            IncrementalGlobalBoundedChildKey(stage_key, 0x00),
            [need_power, done = std::move(done)](RelationRows identity) mutable {
                RelationRows power;
                if (need_power) power = identity;
                done(IncrementalGlobalBoundedStage{
                    std::move(power), std::move(identity), need_power});
            });
        return;
    }
    if (depth == 1) {
        // Demanded mids can be duplicated across producers. Canonicalize
        // their identity rows before forming S1, including sources with no edge.
        IncrementalGlobalCollectBaseRelation(
            state, query_id, -1, std::move(sources),
            IncrementalGlobalBoundedChildKey(stage_key, 0x01),
            [&state, query_id, body_id, stage_key, need_power,
             done = std::move(done)](RelationRows identity) mutable {
                RelationDstSet requested;
                for (const auto& [source, _] : identity) requested.push_back(source);
                IncrementalGlobalCollectBaseRelation(
                    state, query_id, body_id, std::move(requested),
                    IncrementalGlobalBoundedChildKey(stage_key, 0x02),
                    [&state, query_id, identity = std::move(identity), need_power,
                     done = std::move(done)](RelationRows power) mutable {
                        auto begin = chrono::steady_clock::now();
                        RelationRows closure = std::move(identity);
                        IncrementalRecordBoundedOwnerTime(state, query_id, 2, begin);
                        begin = chrono::steady_clock::now();
                        IncrementalMergeRows(closure, power);
                        IncrementalRecordBoundedOwnerTime(state, query_id, 1, begin);
                        if (!need_power) power.clear();
                        done(IncrementalGlobalBoundedStage{
                            std::move(power), std::move(closure), need_power});
                    });
            });
        return;
    }

    const auto split = IncrementalRangeAdditionSplit(depth);
    RelationDstSet left_sources = sources;
    IncrementalGlobalBuildBoundedStage(
        state, query_id, body_id, split.second, std::move(sources), false,
        IncrementalGlobalBoundedChildKey(stage_key, 0x61),
        [&state, query_id, body_id, depth, split, need_power, stage_key,
         complete_cache, required_powers, sources = std::move(left_sources),
         done = std::move(done)](IncrementalGlobalBoundedStage right) mutable {
            RelationDstSet original_sources = sources;
            IncrementalGlobalBuildBoundedStage(
                state, query_id, body_id, split.first, std::move(sources), false,
                IncrementalGlobalBoundedChildKey(stage_key, 0x62),
                [&state, query_id, body_id, depth, split, need_power, stage_key,
                 sources = std::move(original_sources), right = std::move(right),
                 done = std::move(done)](IncrementalGlobalBoundedStage left) mutable {
                    struct MirrorContext {
                        RelationRows power, closure, left, left_power;
                        IncrementalGlobalBoundedDone done;
                    };
                    auto context = make_shared<MirrorContext>();
                    context->closure = std::move(right.closure);
                    context->left = std::move(left.closure);
                    if (need_power && !left.has_power)
                        throw runtime_error("mirror schedule omitted a required left power");
                    context->left_power = std::move(left.power);
                    context->done = std::move(done);
                    auto compose_delta = [&state, query_id, body_id, depth, split,
                                          need_power, stage_key, context]() mutable {
                        if (IncrementalScalingTraceEnabled()) {
                            cerr << "[bounded-mirror] worker=" << state.partition_id
                                 << " query=" << query_id << " length=" << depth
                                 << " required_power=" << need_power
                                 << " left_pairs=" << CountRelationPairs(context->left)
                                 << '\n';
                        }
                        RelationDstSet mids = IncrementalBoundedCollectDestinations(
                            state, query_id, context->left);
                        // Even an empty local delta must participate in every
                        // exact/compose stage so that other workers get seals.
                        IncrementalGlobalBuildExactRelation(
                            state, query_id, body_id, split.second, std::move(mids),
                            IncrementalGlobalBoundedChildKey(stage_key, 0x65),
                            [&state, query_id, body_id, split, need_power, stage_key,
                             context](RelationRows rhs) mutable {
                                const bool restore = split.first == split.second &&
                                    IncrementalHelperClosureRestoreEnabled();
                                auto left_owner = IncrementalOwnComposeInput(context->left);
                                auto right_owner = IncrementalOwnComposeInput(rhs);
                                auto coverage_owner = split.first == split.second
                                    ? IncrementalOwnComposeInput(context->closure) : nullptr;
                                const auto& lhs = left_owner ? *left_owner : context->left;
                                const auto& right_rows = right_owner ? *right_owner : rhs;
                                const RelationRows* coverage = split.first == split.second
                                    ? (coverage_owner ? coverage_owner.get() : &context->closure)
                                    : nullptr;
                                IncrementalStartGlobalRelationCompose(
                                    state, query_id, body_id,
                                    IncrementalGlobalBoundedChildKey(stage_key, 0x66),
                                    lhs, right_rows,
                                    [&state, query_id, context, coverage_owner,
                                     need_power, restore](RelationRows shifted) mutable {
                                        const auto begin = chrono::steady_clock::now();
                                        if (restore) {
                                            const auto& prior = coverage_owner
                                                ? *coverage_owner : context->closure;
                                            // Sources absent from D_a (or S_a)
                                            // never reached a helper. Retain their
                                            // S_b and reused P_(a+b) contributions.
                                            for (const auto& [source, values] : prior) {
                                                if (shifted.find(source) == shifted.end())
                                                    shifted.emplace(source, values);
                                            }
                                            context->closure = std::move(shifted);
                                        } else {
                                            if (coverage_owner)
                                                context->closure = *coverage_owner;
                                            IncrementalMergeOwnedRows(context->closure, shifted);
                                        }
                                        IncrementalRecordBoundedOwnerTime(state, query_id, 1, begin);
                                        auto done = std::move(context->done);
                                        done(IncrementalGlobalBoundedStage{
                                            std::move(context->power), std::move(context->closure), need_power});
                                    }, coverage, IncrementalComposeInputOwners{
                                        left_owner, right_owner, coverage_owner, restore});
                            });
                    };
                    if (!need_power) {
                        compose_delta();
                        return;
                    }
                    // S_a P_b = P_(a+b) U (S_a - P_a) P_b. Only build
                    // P_(a+b) here when the mirror schedule already needs it.
                    RelationDstSet power_sources = sources;
                    IncrementalGlobalBuildExactRelation(
                        state, query_id, body_id, depth, std::move(power_sources),
                        IncrementalGlobalBoundedChildKey(stage_key, 0x63),
                        [context, compose_delta = std::move(compose_delta)](
                            RelationRows power) mutable {
                            context->power = std::move(power);
                            IncrementalMergeRows(context->closure, context->power);
                            for (auto row = context->left.begin(); row != context->left.end();) {
                                auto exact = context->left_power.find(row->first);
                                if (exact != context->left_power.end()) row->second.bitmap -= exact->second.bitmap;
                                if (row->second.empty()) row = context->left.erase(row);
                                else ++row;
                            }
                            context->left_power.clear();
                            compose_delta();
                        });
                }, complete_cache, required_powers);
        }, complete_cache, required_powers);
}

static void IncrementalEvaluateGlobalBoundedRelation(
    IncrementalWorkerState& state, int query_id, int body_id,
    int min_rep, int max_rep, RelationDstSet sources, uint64_t stage_key,
    function<void(RelationRows)> done) {
    IncrementalGlobalBuildExactRelation(
        state, query_id, body_id, min_rep, std::move(sources),
        IncrementalGlobalBoundedChildKey(stage_key, 0x41),
        [&state, query_id, body_id, min_rep, max_rep, stage_key,
         done = std::move(done)](RelationRows prefix) mutable {
            RelationDstSet mids = IncrementalBoundedCollectDestinations(state, query_id, prefix);
            IncrementalGlobalBuildBoundedStage(
                state, query_id, body_id, max_rep - min_rep, std::move(mids),
                false, IncrementalGlobalBoundedChildKey(stage_key, 0x42),
                [&state, query_id, body_id, stage_key,
                 prefix = std::move(prefix), done = std::move(done)](
                    IncrementalGlobalBoundedStage suffix) mutable {
                    auto root_prefix = make_shared<RelationRows>(
                        std::move(prefix));
                    auto suffix_owner = IncrementalOwnComposeInput(suffix.closure);
                    const auto& rhs = suffix_owner ? *suffix_owner : suffix.closure;
                    IncrementalStartGlobalRelationCompose(
                        state, query_id, body_id,
                        IncrementalGlobalBoundedChildKey(stage_key, 0x43),
                        *root_prefix, rhs,
                        [&state, query_id, root_prefix,
                         done = std::move(done)](RelationRows shifted) mutable {
                            const auto begin = chrono::steady_clock::now();
                            IncrementalMergeRows(shifted, *root_prefix);
                            IncrementalRecordBoundedOwnerTime(state, query_id, 1, begin);
                            done(std::move(shifted));
                        }, nullptr, IncrementalComposeInputOwners{
                            IncrementalOwnerInputViewsEnabled() ? root_prefix : nullptr,
                            suffix_owner, nullptr});
                });
        });
}

static void IncrementalStartGlobalBoundedRoot(
    IncrementalWorkerState& state, int query_id, int body_id,
    int min_rep, int max_rep, RelationDstSet sources) {
    const uint64_t root_key =
        IncrementalGlobalBoundedKey(body_id, min_rep, max_rep);
    auto complete_cache = make_shared<IncrementalCompleteBoundedCache>();
    complete_cache->sources = sources;
    IncrementalEvaluateGlobalExactPowerRelation(
        state, query_id, body_id, min_rep, std::move(sources),
        IncrementalGlobalBoundedChildKey(root_key, 0x31),
        [&state, query_id, body_id, min_rep, max_rep, root_key, complete_cache](
            RelationRows prefix) mutable {
            RelationDstSet mids = IncrementalBoundedCollectDestinations(state, query_id, prefix);
            IncrementalGlobalBuildBoundedStage(
                state, query_id, body_id, max_rep - min_rep, std::move(mids),
                false, IncrementalGlobalBoundedChildKey(root_key, 0x32),
                [&state, query_id, body_id, root_key,
                 prefix = std::move(prefix)](
                    IncrementalGlobalBoundedStage suffix) mutable {
                    auto root_prefix = make_shared<RelationRows>(
                        std::move(prefix));
                    auto suffix_owner = IncrementalOwnComposeInput(suffix.closure);
                    const auto& rhs = suffix_owner ? *suffix_owner : suffix.closure;
                    IncrementalStartGlobalRelationCompose(
                        state, query_id, body_id,
                        IncrementalGlobalBoundedChildKey(root_key, 0x33),
                        *root_prefix, rhs,
                        [&state, query_id, root_prefix](
                            RelationRows rows) mutable {
                            const auto begin = chrono::steady_clock::now();
                            IncrementalMergeRows(rows, *root_prefix);
                            IncrementalRecordBoundedOwnerTime(state, query_id, 1, begin);
                            lock_guard<mutex> lk(state.mu);
                            auto qit = state.queries.find(query_id);
                            if (qit == state.queries.end()) return;
                            qit->second.final_rows = std::move(rows);
                            ++qit->second.roots_done;
                        }, nullptr, IncrementalComposeInputOwners{
                            IncrementalOwnerInputViewsEnabled() ? root_prefix : nullptr,
                            suffix_owner, nullptr});
                }, complete_cache);
        });
}
