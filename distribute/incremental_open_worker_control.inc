static void IncrementalPrepareOpenBodyAsync(
    IncrementalWorkerState& state, int query_id, int body_id) {
    vector<uint64_t> sources;
    {
        lock_guard<mutex> lock(state.mu);
        auto query = state.queries.find(query_id);
        if (query == state.queries.end() || body_id < 0 ||
            body_id >= static_cast<int>(query->second.program.bodies.size())) return;
        auto& preparation = query->second.open_body_preparations[body_id];
        if (preparation.started) return;
        preparation.started = true;
        unordered_set<string> labels;
        for (const string& label : CollectRegexStartLabels(
                 query->second.program.bodies[static_cast<size_t>(body_id)].pattern.nfa)) {
            labels.insert(label);
        }
        auto local_sources = state.partition.get_start_point(labels);
        preparation.sources.assign(local_sources.begin(), local_sources.end());
        sources = preparation.sources;
        preparation.bulk = IncrementalDirectBalanceEnabled();
        if (sources.empty() && !preparation.bulk) preparation.complete = true;
    }
    if (IncrementalDirectBalanceEnabled()) {
        RelationDstSet requested;
        for (uint64_t source : sources) requested.push_back(source);
        IncrementalCollectGlobalBaseRelationBulk(
            state, query_id, body_id, requested,
            [&state, query_id, body_id](RelationRows rows) mutable {
                lock_guard<mutex> lock(state.mu);
                auto& preparation = state.queries.at(query_id).open_body_preparations.at(body_id);
                preparation.bulk_rows = std::move(rows);
                preparation.rows_complete = preparation.sources.size();
                preparation.complete = true;
            });
        return;
    }
    for (uint64_t source : sources) {
        IncrementalSubscribeRow(
            state, IncrementalRowKey{query_id, body_id, 1, source},
            [&state, query_id, body_id](const RelationDstSet&, bool complete) {
                if (!complete) return;
                lock_guard<mutex> lock(state.mu);
                auto query = state.queries.find(query_id);
                if (query == state.queries.end()) return;
                auto preparation = query->second.open_body_preparations.find(body_id);
                if (preparation == query->second.open_body_preparations.end() || preparation->second.complete) return;
                ++preparation->second.rows_complete;
                preparation->second.complete =
                    preparation->second.rows_complete == preparation->second.sources.size();
            });
    }
}

static Message HandleIncrementalOpenControl(
    IncrementalWorkerState& state, const Message& request) {
    const string operation = request.GetField("op");
    const int query_id = request.GetFieldInt("query_id");
    const int body_id = request.GetFieldInt("body_id");
    if (operation == "prepare_incremental_open_body") {
        {
            lock_guard<mutex> lock(state.mu);
            if (state.queries.find(query_id) == state.queries.end()) {
                Message out(MessageType::RESPONSE_ERROR);
                out.AddField("error", "open body query is not initialized");
                return out;
            }
        }
        state.executor.Submit([&state, query_id, body_id]() {
            IncrementalPrepareOpenBodyAsync(state, query_id, body_id);
        });
        return Message(MessageType::RESPONSE_OK);
    }
    if (operation == "incremental_open_body_status") {
        Message out(MessageType::RESPONSE_OK);
        lock_guard<mutex> lock(state.mu);
        auto query = state.queries.find(query_id);
        if (query == state.queries.end()) {
            out.AddField("ready", 0);
            out.AddField("rows_total", 0);
            out.AddField("rows_complete", 0);
            return out;
        }
        auto preparation = query->second.open_body_preparations.find(body_id);
        if (preparation == query->second.open_body_preparations.end()) {
            out.AddField("ready", 0);
            out.AddField("rows_total", 0);
            out.AddField("rows_complete", 0);
            return out;
        }
        out.AddField("ready", preparation->second.complete ? 1 : 0);
        out.AddField("rows_total", preparation->second.sources.size());
        out.AddField("rows_complete", preparation->second.rows_complete);
        return out;
    }
    if (operation == "fetch_incremental_open_body") {
        RelationRows rows;
        {
            lock_guard<mutex> lock(state.mu);
            auto query = state.queries.find(query_id);
            if (query == state.queries.end()) {
                Message out(MessageType::RESPONSE_ERROR);
                out.AddField("error", "open body query is not initialized");
                return out;
            }
            auto preparation = query->second.open_body_preparations.find(body_id);
            if (preparation == query->second.open_body_preparations.end() || !preparation->second.complete) {
                Message out(MessageType::RESPONSE_ERROR);
                out.AddField("error", "open body preparation is incomplete");
                return out;
            }
            if (preparation->second.bulk) rows = preparation->second.bulk_rows;
            else {
            rows.reserve(preparation->second.sources.size());
            for (uint64_t source : preparation->second.sources) {
                auto row = query->second.rows.find(IncrementalRowKey{query_id, body_id, 1, source});
                if (row != query->second.rows.end() && !row->second.values.empty()) rows.emplace(source, row->second.values);
            }
            }
        }
        Message out(MessageType::RESPONSE_DATA);
        out.data = SerializeIncrementalFinalRows(rows);
        return out;
    }
    Message out(MessageType::RESPONSE_ERROR);
    out.AddField("error", "unsupported incremental open control operation");
    return out;
}

static Message HandleIncrementalInstallOpenPlan(
    IncrementalWorkerState& state, const Message& request) {
    int query_id = -1;
    int body_id = -1;
    shared_ptr<IncrementalOpenPlan> plan;
    string error;
    if (!ParseIncrementalOpenPlan(request.data, query_id, body_id, plan, error)) {
        Message out(MessageType::RESPONSE_ERROR);
        out.AddField("error", error);
        return out;
    }
    {
        lock_guard<mutex> lock(state.mu);
        auto query = state.queries.find(query_id);
        if (query == state.queries.end()) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", "open body query is not initialized");
            return out;
        }
        query->second.open_plans[body_id] = std::move(plan);
    }
    return Message(MessageType::RESPONSE_OK);
}
