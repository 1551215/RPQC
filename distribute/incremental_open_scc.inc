// Demand-local SCC closure for an open range.
//
// The prefix B^m is evaluated first.  Only vertices reached by that prefix
// are expanded with B^1, so A/B{m,} never materializes the body graph for all
// graph sources.  Once that demand-local graph is complete, Tarjan's SCC
// decomposition builds its condensation DAG and the reachable components are
// emitted as B^m o B*.  The SCC step is therefore semantic, not a global
// coordinator side shortcut.
struct IncrementalOpenSccContext {
    RelationDstSet values;
    RelationDstSet prefix;
    unordered_map<uint64_t, vector<uint64_t>> adjacency;
    unordered_set<uint64_t> expanded;
    uint64_t pending = 0;
    bool prefix_complete = false;
    bool completed = false;
    IncrementalDeltaCallback done;
};

static void IncrementalOpenSccFinish(
    IncrementalWorkerState& state,
    const shared_ptr<IncrementalOpenSccContext>& context) {
    if (!context->prefix_complete || context->pending != 0 || context->completed) return;

    // Tarjan over the demand-local graph.  The iterative frames avoid a call
    // stack proportional to a long DBpedia path.
    unordered_map<uint64_t, int> index;
    unordered_map<uint64_t, int> lowlink;
    unordered_set<uint64_t> on_stack;
    vector<uint64_t> tarjan_stack;
    struct Frame { uint64_t vertex; size_t next = 0; uint64_t parent = 0; bool has_parent = false; };
    vector<Frame> frames;
    int next_index = 0;
    unordered_map<uint64_t, int> component;
    vector<vector<uint64_t>> components;
    unordered_set<uint64_t> vertices;
    for (uint64_t vertex : context->prefix) vertices.insert(vertex);
    for (const auto& [src, dsts] : context->adjacency) {
        vertices.insert(src);
        vertices.insert(dsts.begin(), dsts.end());
    }
    auto start_vertex = [&](uint64_t root) {
        index[root] = lowlink[root] = next_index++;
        tarjan_stack.push_back(root);
        on_stack.insert(root);
        frames.push_back(Frame{root, 0, 0, false});
    };
    for (uint64_t root : vertices) {
        if (index.count(root)) continue;
        start_vertex(root);
        while (!frames.empty()) {
            Frame& frame = frames.back();
            auto ait = context->adjacency.find(frame.vertex);
            const vector<uint64_t>* edges = ait == context->adjacency.end() ? nullptr : &ait->second;
            if (edges && frame.next < edges->size()) {
                uint64_t next = (*edges)[frame.next++];
                auto nit = index.find(next);
                if (nit == index.end()) {
                    index[next] = lowlink[next] = next_index++;
                    tarjan_stack.push_back(next);
                    on_stack.insert(next);
                    frames.push_back(Frame{next, 0, frame.vertex, true});
                } else if (on_stack.count(next)) {
                    lowlink[frame.vertex] = min(lowlink[frame.vertex], nit->second);
                }
                continue;
            }
            const uint64_t vertex = frame.vertex;
            const bool has_parent = frame.has_parent;
            const uint64_t parent = frame.parent;
            if (has_parent) lowlink[parent] = min(lowlink[parent], lowlink[vertex]);
            if (lowlink[vertex] == index[vertex]) {
                vector<uint64_t> members;
                while (!tarjan_stack.empty()) {
                    uint64_t member = tarjan_stack.back();
                    tarjan_stack.pop_back();
                    on_stack.erase(member);
                    component[member] = static_cast<int>(components.size());
                    members.push_back(member);
                    if (member == vertex) break;
                }
                components.push_back(std::move(members));
            }
            frames.pop_back();
        }
    }

    // The graph was expanded from every prefix vertex, so every discovered
    // component is reachable.  Traverse the condensation anyway; this keeps
    // the closure semantics explicit and makes disconnected demand fragments
    // harmless if a future evaluator supplies them.
    vector<unordered_set<int>> dag(components.size());
    for (const auto& [src, dsts] : context->adjacency) {
        auto sit = component.find(src);
        if (sit == component.end()) continue;
        for (uint64_t dst : dsts) {
            auto dit = component.find(dst);
            if (dit != component.end() && sit->second != dit->second) dag[sit->second].insert(dit->second);
        }
    }
    vector<char> reachable(components.size(), 0);
    vector<int> component_frontier;
    for (uint64_t seed : context->prefix) {
        auto it = component.find(seed);
        if (it != component.end() && !reachable[it->second]) {
            reachable[it->second] = 1;
            component_frontier.push_back(it->second);
        }
    }
    for (size_t pos = 0; pos < component_frontier.size(); ++pos) {
        for (int next : dag[component_frontier[pos]]) {
            if (!reachable[next]) { reachable[next] = 1; component_frontier.push_back(next); }
        }
    }
    RelationDstSet final_values;
    for (size_t cid = 0; cid < components.size(); ++cid) {
        if (!reachable[cid]) continue;
        for (uint64_t vertex : components[cid]) final_values.push_back(vertex);
    }
    RelationDstSet fresh;
    fresh.bitmap = final_values.bitmap - context->values.bitmap;
    context->values.bitmap |= fresh.bitmap;
    if (!fresh.empty()) IncrementalInvoke(state, context->done, std::move(fresh), false);
    context->completed = true;
    IncrementalInvoke(state, context->done, RelationDstSet{}, true);
}

static void IncrementalEvalOpenLeafScc(
    IncrementalWorkerState& state, int query_id, int body_id, int min_rep,
    uint64_t source, IncrementalDeltaCallback done) {
    shared_ptr<IncrementalOpenPlan> plan;
    {
        lock_guard<mutex> lock(state.mu);
        auto query = state.queries.find(query_id);
        if (query != state.queries.end()) {
            auto found = query->second.open_plans.find(body_id);
            if (found != query->second.open_plans.end()) plan = found->second;
        }
    }
    if (plan) {
        struct PlannedContext {
            mutex mu;
            RelationDstSet values;
            IncrementalDeltaCallback done;
        };
        auto planned = make_shared<PlannedContext>();
        planned->done = std::move(done);
        IncrementalSubscribeRow(
            state, IncrementalRowKey{query_id, body_id, min_rep, source},
            [&state, plan, planned](const RelationDstSet& delta, bool complete) {
                RelationDstSet candidates;
                for (uint64_t seed : delta) {
                    uint32_t component = 0;
                    if (!plan->FindComponent(seed, component)) {
                        // B* contains identity even when a seed has no B edge.
                        candidates.push_back(seed);
                        continue;
                    }
                    candidates.bitmap |= IncrementalOpenComponentClosure(
                        *plan, component).bitmap;
                }
                RelationDstSet fresh;
                {
                    lock_guard<mutex> lock(planned->mu);
                    fresh.bitmap = candidates.bitmap - planned->values.bitmap;
                    planned->values.bitmap |= fresh.bitmap;
                }
                if (!fresh.empty()) IncrementalInvoke(state, planned->done, std::move(fresh), false);
                if (complete) IncrementalInvoke(state, planned->done, RelationDstSet{}, true);
            });
        return;
    }
    auto context = make_shared<IncrementalOpenSccContext>();
    context->done = std::move(done);
    auto finish = make_shared<function<void()>>();
    *finish = [&state, context]() { IncrementalOpenSccFinish(state, context); };
    auto expand = make_shared<function<void(const RelationDstSet&)>>();
    auto subscribe = make_shared<function<void(uint64_t)>>();
    *subscribe = [state_ptr = &state, context, expand, finish, subscribe, query_id, body_id](uint64_t mid) {
        if (!context->expanded.insert(mid).second) return;
        ++context->pending;
        IncrementalSubscribeRow(*state_ptr, IncrementalRowKey{query_id, body_id, 1, mid},
            [state_ptr, context, expand, finish, mid](const RelationDstSet& delta, bool complete) {
                auto& edges = context->adjacency[mid];
                if (!delta.empty()) {
                    edges.insert(edges.end(), delta.begin(), delta.end());
                    (*expand)(delta);
                }
                if (complete) { if (context->pending > 0) --context->pending; (*finish)(); }
            });
    };
    *expand = [subscribe](const RelationDstSet& mids) {
        for (uint64_t mid : mids) (*subscribe)(mid);
    };
    IncrementalSubscribeRow(state, IncrementalRowKey{query_id, body_id, min_rep, source},
        [context, expand, finish](const RelationDstSet& delta, bool complete) {
            context->prefix.bitmap |= delta.bitmap;
            if (!delta.empty()) (*expand)(delta);
            if (complete) { context->prefix_complete = true; (*finish)(); }
        });
}
