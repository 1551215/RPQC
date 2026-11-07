static vector<int> IncrementalOpenBodyIds(const IncrementalProgram& program) {
    vector<int> body_ids;
    for (const auto& node : program.nodes) {
        if (node.kind == IncrementalMacroKind::LEAF && node.max_rep < 0) body_ids.push_back(node.body_id);
    }
    sort(body_ids.begin(), body_ids.end());
    body_ids.erase(unique(body_ids.begin(), body_ids.end()), body_ids.end());
    return body_ids;
}

static void PrepareIncrementalOpenPlans(
    vector<WorkerInfo>& workers, int query_id, const IncrementalProgram& program) {
    for (int body_id : IncrementalOpenBodyIds(program)) {
        const auto started = chrono::steady_clock::now();
        auto prepare = IncrementalRequestAll(workers, [&, body_id](size_t) {
                Message request(MessageType::STEP_BASIC);
                request.AddField("op", "prepare_incremental_open_body");
                request.AddField("query_id", query_id);
                request.AddField("body_id", body_id);
                return request;
        });
        for (auto& future : prepare) EnsureIncrementalOk(future.get(), "prepare incremental open body");

        while (true) {
            bool ready = true;
            auto statuses = IncrementalRequestAll(workers, [&](size_t) {
                Message request(MessageType::STEP_BASIC);
                request.AddField("op", "incremental_open_body_status");
                request.AddField("query_id", query_id);
                request.AddField("body_id", body_id);
                return request;
            });
            for (auto& status : statuses) {
                Message response = status.get();
                EnsureIncrementalOk(response, "incremental open body status");
                ready = ready && response.GetFieldInt("ready") != 0;
            }
            if (ready) break;
            this_thread::sleep_for(chrono::milliseconds(20));
        }

        const auto body_ready = chrono::steady_clock::now();
        auto responses = IncrementalRequestAll(workers, [&, body_id](size_t) {
            Message request(MessageType::STEP_BASIC);
            request.AddField("op", "fetch_incremental_open_body");
            request.AddField("query_id", query_id);
            request.AddField("body_id", body_id);
            return request;
        });
        vector<future<RelationRows>> fetches;
        for (size_t worker = 0; worker < workers.size(); ++worker) {
            fetches.push_back(async(launch::deferred, [&, worker, body_id]() {
                Message response = responses[worker].get();
                if (response.type != MessageType::RESPONSE_DATA) {
                    throw runtime_error("fetch incremental open body: " + response.GetField("error"));
                }
                RelationRows rows;
                string error;
                if (!ParseIncrementalFinalRows(response.data, rows, error)) {
                    throw runtime_error("decode incremental open body: " + error);
                }
                return rows;
            }));
        }
        RelationRows body_rows;
        for (auto& future : fetches) {
            RelationRows shard = future.get();
            body_rows.merge(shard);
            for (auto& [source, targets] : shard) body_rows.at(source) |= targets;
        }

        const auto fetched = chrono::steady_clock::now();
        auto plan = BuildIncrementalOpenPlan(body_rows);
        const auto built = chrono::steady_clock::now();
        const string payload = SerializeIncrementalOpenPlan(query_id, body_id, *plan);
        const auto encoded = chrono::steady_clock::now();
        auto installs = IncrementalRequestAll(workers, [&](size_t) {
                Message request(MessageType::EXECUTE_OPEN_SCC, payload);
                return request;
        });
        for (auto& future : installs) EnsureIncrementalOk(future.get(), "install incremental open plan");
        const uint64_t elapsed_ms = chrono::duration_cast<chrono::milliseconds>(
            chrono::steady_clock::now() - started).count();
        cout << "[incremental-open-plan] body_id=" << body_id
             << " body_rows=" << body_rows.size()
             << " body_pairs=" << CountRelationPairs(body_rows)
             << " components=" << plan->ComponentCount()
             << " plan_bytes=" << payload.size()
             << " prepare_ms=" << elapsed_ms
             << " body_ready_us=" << chrono::duration_cast<chrono::microseconds>(body_ready - started).count()
             << " fetch_us=" << chrono::duration_cast<chrono::microseconds>(fetched - body_ready).count()
             << " scc_us=" << chrono::duration_cast<chrono::microseconds>(built - fetched).count()
             << " encode_us=" << chrono::duration_cast<chrono::microseconds>(encoded - built).count()
             << " install_us=" << chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now() - encoded).count()
             << endl;
    }
}
