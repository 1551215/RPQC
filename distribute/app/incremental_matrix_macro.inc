// Batched outer-NFA evaluation. Direct exact-count roots use the global
// bottom-up evaluator; demand batches here are reserved for bounded and
// composite frontiers whose source set is discovered incrementally.
using IncrementalMatrixRowsDone = function<void(RelationRows)>;

struct IncrementalMatrixMaterializedRows {
    RelationRows rows;
    int resident_parent_pid = -1;
    uint64_t resident_parent_batch_id = 0;
    int resident_body_id = -1;
    int resident_length = 0;
    bool resident_include_identity = false;
    shared_ptr<IncrementalMatrixResidentDirectory> resident_directory;
    bool resident() const {
        return resident_parent_pid >= 0 || resident_body_id >= 0;
    }
};
using IncrementalMatrixMaterializedDone =
    function<void(IncrementalMatrixMaterializedRows)>;

// Bounded P/S recursion amplifies every temporal source batch into a subtree.
// Accumulate enough unique mids to preserve GCNP quality and keep the control
// tree bounded, while still launching RHS work before the left power seals.
static constexpr uint64_t kIncrementalBoundedDemandSourceBatch = 65536;
static constexpr uint64_t kIncrementalCompositeDemandSourceBatch = 16384;

static RelationDstSet IncrementalMatrixCollectDestinations(const RelationRows& rows) {
    RelationDstSet destinations;
    for (const auto& [_, values] : rows) destinations.bitmap |= values.bitmap;
    return destinations;
}

static RelationRows IncrementalMatrixIdentityRows(const RelationDstSet& sources) {
    RelationRows identity;
    identity.reserve(sources.cardinality());
    for (uint64_t source : sources) identity[source].push_back(source);
    return identity;
}

// Advance a sealed product-automaton frontier through a materialized macro
// edge.  The RHS is virtual adjacency keyed by the current vertex; original
// source ownership stays in the left rows and no outer GCNP plan is built.
static RelationRows IncrementalMatrixAdvanceVirtualEdge(
    const RelationRows& frontier, const RelationRows& macro_rows) {
    RelationRows output;
    output.reserve(frontier.size() * 2 + 1);
    for (const auto& [source, vertices] : frontier) {
        RelationDstSet destinations;
        for (uint64_t vertex : vertices) {
            auto row = macro_rows.find(vertex);
            if (row != macro_rows.end()) destinations.bitmap |= row->second.bitmap;
        }
        if (!destinations.empty()) output.emplace(source, std::move(destinations));
    }
    return output;
}

static uint64_t IncrementalMatrixMacroStageKey(
    int node_id, uint64_t phase) {
    uint64_t key = 0x6a09e667f3bcc909ULL;
    key ^= static_cast<uint32_t>(node_id) +
        0x9e3779b97f4a7c15ULL + (key << 6) + (key >> 2);
    key ^= phase + 0x9e3779b97f4a7c15ULL + (key << 6) + (key >> 2);
    return key;
}

static void IncrementalMatrixCollectExact(
    IncrementalWorkerState& state, int query_id, int body_id, int length,
    RelationDstSet sources, bool retain_output_on_destination,
    IncrementalMatrixRowsDone done,
    IncrementalMatrixRowsDone progress = {}) {
    (void)retain_output_on_destination;
    if (length == 0) {
        done(IncrementalMatrixIdentityRows(sources));
        return;
    }
    struct Context {
        mutex mu;
        RelationRows rows;
        bool completed = false;
        IncrementalMatrixRowsDone done;
    };
    auto context = make_shared<Context>();
    context->done = std::move(done);
    if (sources.empty()) {
        context->done(RelationRows{});
        return;
    }
    // Every exact relation inside bounded/open/sequence macros uses the same
    // synchronous middle data plane as a direct R{k} root. This is the stage
    // barrier: the callback receives the complete source-owned relation only
    // after every middle partition has returned its partial C.
    IncrementalRowsCallback progress_callback;
    if (progress) {
        progress_callback = [progress = std::move(progress)](
            const RelationRows& delta, bool) mutable {
            if (!delta.empty()) progress(delta);
        };
    }
    IncrementalStartDemandBatch(
        state, query_id, body_id, length, std::move(sources),
        [context](const RelationRows& delta, bool complete) mutable {
            IncrementalMatrixRowsDone done;
            RelationRows rows;
            {
                lock_guard<mutex> lk(context->mu);
                if (!delta.empty()) IncrementalMergeRows(context->rows, delta);
                if (complete && !context->completed) {
                    context->completed = true;
                    rows = std::move(context->rows);
                    done = std::move(context->done);
                }
            }
            if (done) done(std::move(rows));
        }, false, std::move(progress_callback));
}

static void IncrementalMatrixCollectCompose(
    IncrementalWorkerState& state, int query_id,
    RelationRows left_rows, RelationRows right_rows,
    bool retain_output_on_destination, IncrementalMatrixRowsDone done) {
    struct Context {
        RelationRows rows;
        bool completed = false;
        IncrementalMatrixRowsDone done;
    };
    auto context = make_shared<Context>();
    context->done = std::move(done);
    IncrementalStartGenericMatrixCompose(
        state, query_id, std::move(left_rows), std::move(right_rows),
        [context](const RelationRows& delta, bool complete) mutable {
            if (!delta.empty()) IncrementalMergeRows(context->rows, delta);
            if (complete && !context->completed) {
                context->completed = true;
                context->done(std::move(context->rows));
            }
        }, retain_output_on_destination);
}


static void IncrementalMatrixRetainRows(
    IncrementalWorkerState& state, int query_id, RelationRows rows,
    IncrementalMatrixRowsDone done) {
    RelationDstSet sources;
    for (const auto& [source, _] : rows) sources.push_back(source);
    IncrementalMatrixCollectCompose(
        state, query_id, IncrementalMatrixIdentityRows(sources), std::move(rows),
        true, std::move(done));
}

// Materialize a control-side operand once into destination-owned blocks, then
// expose only its handle/directory to the following macro continuation.  This
// is the bridge for legacy leaf evaluators while their row cache remains
// source-keyed; the continuation itself never recollects these endpoint rows.
static void IncrementalMatrixRetainRowsMaterialized(
    IncrementalWorkerState& state, int query_id, RelationRows rows,
    bool visible_in_final_fetch, IncrementalMatrixMaterializedDone done) {
    RelationDstSet sources;
    for (const auto& [source, _] : rows) sources.push_back(source);
    auto handle = make_shared<pair<int, uint64_t>>(state.partition_id, 0);
    auto directory = IncrementalBuildResidentDirectory(state, rows);
    IncrementalStartGenericMatrixCompose(
        state, query_id, IncrementalMatrixIdentityRows(sources), std::move(rows),
        [handle, directory, done = std::move(done)](
            const RelationRows&, bool complete) mutable {
            if (!complete) return;
            done(IncrementalMatrixMaterializedRows{
                RelationRows{}, handle->first, handle->second, -1, 0, false,
                std::move(directory)});
        // `return_output_to_parent` is the historical inverse visibility bit:
        // true hides a retained intermediate from final result fetching.
        }, true, RelationRows{}, -1, 0, -1, 0, false, !visible_in_final_fetch,
        shared_ptr<uint64_t>(handle, &handle->second), -1, 0, -1, 0,
        directory);
}


static void IncrementalMatrixEvalOpenBatch(
    IncrementalWorkerState& state, int query_id, int node_id, int body_id,
    int min_rep, RelationDstSet sources, bool retain_output_on_destination,
    IncrementalMatrixRowsDone done) {
    // B{l,} = B^l o B*.  Build the exact B^l prefix with the same complete
    // relation stages as an ordinary counted expression, including GCNP and
    // factorized matrix-result transport.  The callback receives ordinary
    // source rows before the existing SCC closure is evaluated.
    bool complete_start_domain = false;
    {
        lock_guard<mutex> lock(state.mu);
        complete_start_domain = state.queries.at(query_id).program.root == node_id;
    }
    // Only a root has the full body start domain on every owner. Nested macro
    // frontiers remain source-aware, rather than treating partial powers as RHS.
    auto evaluate_prefix = complete_start_domain
        ? IncrementalEvaluateGlobalExactPowerRelation
        : IncrementalEvaluateGlobalExactRelation;
    evaluate_prefix(
        state, query_id, body_id, min_rep, std::move(sources),
        IncrementalMatrixMacroStageKey(node_id, 0x300),
        [&state, query_id, node_id, body_id, min_rep, retain_output_on_destination,
         done = std::move(done)](RelationRows prefix) mutable {
            shared_ptr<IncrementalOpenPlan> plan;
            {
                lock_guard<mutex> lk(state.mu);
                auto qit = state.queries.find(query_id);
                if (qit != state.queries.end()) {
                    auto pit = qit->second.open_plans.find(body_id);
                    if (pit != qit->second.open_plans.end()) plan = pit->second;
                }
            }
            if (plan) {
                if (IncrementalDirectBalanceEnabled()) {
                    IncrementalStartBalancedOpenClosure(
                        state, query_id, body_id,
                        IncrementalMatrixMacroStageKey(node_id, 0x301),
                        std::move(prefix),
                        [&state, query_id, retain_output_on_destination,
                         done = std::move(done)](RelationRows closure) mutable {
                            if (retain_output_on_destination) {
                                IncrementalMatrixRetainRows(
                                    state, query_id, std::move(closure),
                                    std::move(done));
                            } else done(std::move(closure));
                        });
                    return;
                }
                RelationRows closure;
                closure.reserve(prefix.size());
                for (const auto& [source, seeds] : prefix) {
                    auto& output = closure[source];
                    for (uint64_t seed : seeds) {
                        uint32_t component = 0;
                        if (!plan->FindComponent(seed, component)) output.push_back(seed);
                        else output.bitmap |= IncrementalOpenComponentClosure(
                            *plan, component).bitmap;
                    }
                    if (output.empty()) closure.erase(source);
                }
                if (retain_output_on_destination) {
                    IncrementalMatrixRetainRows(
                        state, query_id, std::move(closure), std::move(done));
                } else {
                    done(std::move(closure));
                }
                return;
            }

            struct Fallback {
                RelationRows rows;
                uint64_t pending = 0;
                IncrementalMatrixRowsDone done;
            };
            auto fallback = make_shared<Fallback>();
            fallback->pending = prefix.size();
            fallback->done = std::move(done);
            if (fallback->pending == 0) {
                fallback->done(RelationRows{});
                return;
            }
            for (const auto& [source, _] : prefix) {
                IncrementalEvalOpenLeafScc(
                    state, query_id, body_id, min_rep, source,
                    [&state, query_id, source, retain_output_on_destination,
                     fallback](const RelationDstSet& delta, bool complete) mutable {
                        if (!delta.empty()) fallback->rows[source] |= delta;
                        if (!complete || --fallback->pending != 0) return;
                        if (retain_output_on_destination) {
                            IncrementalMatrixRetainRows(
                                state, query_id, std::move(fallback->rows),
                                std::move(fallback->done));
                        } else {
                            fallback->done(std::move(fallback->rows));
                        }
                    });
            }
        });
}

static void IncrementalMatrixEvalMacroBatch(
    IncrementalWorkerState&, int, int, RelationDstSet, bool,
    IncrementalMatrixRowsDone, IncrementalMatrixRowsDone = {});

static void IncrementalMatrixEvalMacroResident(
    IncrementalWorkerState&, int, int, RelationDstSet,
    bool, IncrementalMatrixMaterializedDone);

// A resident macro result is represented only by its distributed block handle
// and source-row directory.  For a concat, its left block owners execute the
// right child and route directly to the new result owners; endpoint rows never
// return to the macro controller between the two children.
static void IncrementalMatrixEvalMacroResident(
    IncrementalWorkerState& state, int query_id, int node_id,
    RelationDstSet sources, bool visible_in_final_fetch,
    IncrementalMatrixMaterializedDone done) {
    IncrementalMacroNode node;
    {
        lock_guard<mutex> lk(state.mu);
        node = state.queries.at(query_id).program.nodes.at(static_cast<size_t>(node_id));
    }
    if (node.kind == IncrementalMacroKind::SEQUENCE) {
        IncrementalMatrixEvalMacroResident(
            state, query_id, node.left, std::move(sources), false,
            [&state, query_id, node, visible_in_final_fetch,
             done = std::move(done)](IncrementalMatrixMaterializedRows left) mutable {
                auto output_handle = make_shared<pair<int, uint64_t>>(
                    state.partition_id, 0);
                auto output_directory =
                    make_shared<IncrementalMatrixResidentDirectory>();
                IncrementalStartResidentMacroCompose(
                    state, query_id, left.resident_parent_pid,
                    left.resident_parent_batch_id, node.right,
                    [done = std::move(done), output_handle,
                     output_directory](const RelationRows&, bool complete) mutable {
                        if (!complete) return;
                        done(IncrementalMatrixMaterializedRows{
                            RelationRows{}, output_handle->first,
                            output_handle->second, -1, 0, false,
                            std::move(output_directory)});
                    }, output_handle, output_directory, visible_in_final_fetch);
            });
        return;
    }
    // Leaf and choice evaluation still use the mature demand/NFA evaluator for
    // their local state-machine work.  Immediately after that work finishes,
    // the relation is converted into the same destination-resident handle so
    // every following composition takes the distributed continuation path.
    IncrementalMatrixEvalMacroBatch(
        state, query_id, node_id, std::move(sources), false,
        [&state, query_id, visible_in_final_fetch, done = std::move(done)](
            RelationRows rows) mutable {
            IncrementalMatrixRetainRowsMaterialized(
                state, query_id, std::move(rows), visible_in_final_fetch,
                std::move(done));
        });
}

static void IncrementalMatrixEvalMacroBatch(
    IncrementalWorkerState& state, int query_id, int node_id,
    RelationDstSet sources, bool retain_output_on_destination,
    IncrementalMatrixRowsDone done, IncrementalMatrixRowsDone progress) {
    IncrementalMacroNode node;
    {
        lock_guard<mutex> lk(state.mu);
        node = state.queries.at(query_id).program.nodes.at(static_cast<size_t>(node_id));
    }
    // Root materialization uses the resident protocol for every macro kind,
    // including a bounded leaf.  The leaf evaluator still computes the exact
    // dual-state range semantics, but its completed rows are immediately
    // converted into destination-owned blocks instead of being published as
    // the controller's final RelationRows operand.
#if defined(INCREMENTAL_DISABLE_RESIDENT_MATRIX_MACRO)
    const bool resident_root = false;
#else
    const bool resident_root = retain_output_on_destination &&
        (node.kind == IncrementalMacroKind::SEQUENCE ||
         ((node.kind == IncrementalMacroKind::LEAF ||
           node.kind == IncrementalMacroKind::REPETITION) &&
          node.min_rep != node.max_rep));
#endif
    if (resident_root) {
        IncrementalMatrixEvalMacroResident(
            state, query_id, node_id, std::move(sources), true,
            [done = std::move(done)](IncrementalMatrixMaterializedRows) mutable {
                done(RelationRows{});
            });
        return;
    }
    if (node.kind == IncrementalMacroKind::LEAF ||
        node.kind == IncrementalMacroKind::REPETITION) {
        if (node.min_rep == node.max_rep) {
            IncrementalEvaluateGlobalExactRelation(
                state, query_id, node.body_id, node.min_rep,
                std::move(sources),
                IncrementalMatrixMacroStageKey(node_id, 0x100),
                std::move(done));
        } else if (node.max_rep < 0) {
            IncrementalMatrixEvalOpenBatch(
                state, query_id, node_id, node.body_id, node.min_rep,
                std::move(sources), retain_output_on_destination,
                std::move(done));
        } else {
            IncrementalEvaluateGlobalBoundedRelation(
                state, query_id, node.body_id, node.min_rep, node.max_rep,
                std::move(sources),
                IncrementalMatrixMacroStageKey(node_id, 0x200),
                std::move(done));
        }
        return;
    }
    if (node.kind == IncrementalMacroKind::CHOICE) {
        struct Choice {
            mutex mu;
            RelationRows rows;
            int pending = 2;
            IncrementalMatrixRowsDone done;
        };
        auto choice = make_shared<Choice>();
        choice->done = std::move(done);
        auto child = [&state, query_id, retain_output_on_destination,
                      choice](RelationRows rows) mutable {
            RelationRows output;
            IncrementalMatrixRowsDone done;
            {
                lock_guard<mutex> lk(choice->mu);
                IncrementalMergeRows(choice->rows, rows);
                if (--choice->pending != 0) return;
                output = std::move(choice->rows);
                done = std::move(choice->done);
            }
            if (retain_output_on_destination) {
                IncrementalMatrixRetainRows(
                    state, query_id, std::move(output), std::move(done));
            } else {
                done(std::move(output));
            }
        };
        IncrementalMatrixEvalMacroBatch(
            state, query_id, node.left, sources, false, child);
        IncrementalMatrixEvalMacroBatch(
            state, query_id, node.right, std::move(sources), false, child);
        return;
    }

    IncrementalMatrixEvalMacroBatch(
        state, query_id, node.left, std::move(sources), false,
        [&state, query_id, node_id, node, retain_output_on_destination,
         done = std::move(done)](RelationRows frontier) mutable {
            // A macro transition is planned once from the complete upstream
            // frontier.  Small temporal deltas must not create independent
            // GCNP groups or duplicate counted-stage trees.
            RelationDstSet macro_sources =
                IncrementalMatrixCollectDestinations(frontier);
            IncrementalMatrixEvalMacroBatch(
                state, query_id, node.right, std::move(macro_sources), false,
                [&state, query_id, node_id, retain_output_on_destination,
                 frontier = std::move(frontier), done = std::move(done)](
                    RelationRows macro_rows) mutable {
                    IncrementalStartGlobalVirtualTransition(
                        state, query_id,
                        IncrementalMatrixMacroStageKey(node_id, 0x300),
                        frontier, macro_rows,
                        [&state, query_id, retain_output_on_destination,
                         done = std::move(done)](RelationRows output) mutable {
                            if (retain_output_on_destination) {
                                IncrementalMatrixRetainRows(
                                    state, query_id, std::move(output),
                                    std::move(done));
                            } else {
                                done(std::move(output));
                            }
                        });
                });
        });
}
