#define INCREMENTAL_MIDDLE_PARTITION_MODE 1
#include "modules/incremental_common.inc"
#include "../incremental_open_plan.inc"
#include "modules/parallel_final_rows.inc"
#include "modules/complete_final_groups.inc"

static void EnsureIncrementalOk(const Message& response, const string& context) {
    if (response.type != MessageType::RESPONSE_OK) throw runtime_error(context + ": " + response.GetField("error"));
}

struct IncrementalStatusFields {
    unordered_map<string, string> fields;
    explicit IncrementalStatusFields(const Message& message) {
        size_t start = 0;
        fields.reserve(192);
        while (start < message.data.size()) {
            const size_t newline = message.data.find('\n', start);
            const size_t end = newline == string::npos
                ? message.data.size() : newline;
            const size_t colon = message.data.find(':', start);
            if (colon != string::npos && colon < end) {
                fields.emplace(message.data.substr(start, colon - start),
                    message.data.substr(colon + 1, end - colon - 1));
            }
            start = end + 1;
        }
    }
    uint64_t GetFieldUInt64(const string& key) const {
        const auto entry = fields.find(key);
        return entry == fields.end() || entry->second.empty()
            ? 0 : stoull(entry->second);
    }
    int GetFieldInt(const string& key) const {
        const auto entry = fields.find(key);
        return entry == fields.end() || entry->second.empty()
            ? 0 : stoi(entry->second);
    }
};

template<class RequestFactory>
static vector<future<Message>> IncrementalRequestAll(
    vector<WorkerInfo>& workers, RequestFactory make_request) {
    for (size_t i = 0; i < workers.size(); ++i) {
        Message request = make_request(i);
        lock_guard<mutex> lock(*workers[i].mu);
        workers[i].client->BeginRequest(request);
    }
    vector<future<Message>> responses;
    responses.reserve(workers.size());
    for (size_t i = 0; i < workers.size(); ++i) {
        responses.push_back(async(launch::deferred, [&workers, i] {
            lock_guard<mutex> lock(*workers[i].mu);
            return workers[i].client->ReceiveResponse();
        }));
    }
    return responses;
}

#include "../incremental_open_coordinator.inc"

int main(int argc, char* argv[]) {
    try { rpq_final::ValidateEnvironment(); }
    catch (const exception& error) {
        cerr << "Configuration error: " << error.what() << endl;
        return 2;
    }
    string workers_text = "127.0.0.1:50051,127.0.0.1:50052";
    string expression = "(1){2}";
    string partition_root;
    int query_id = 1;
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "-w" && i + 1 < argc) workers_text = argv[++i];
        else if (arg == "-q" && i + 1 < argc) expression = argv[++i];
        else if (arg == "--query-id" && i + 1 < argc) query_id = stoi(argv[++i]);
        else if ((arg == "-d" || arg == "--partition-root") && i + 1 < argc) partition_root = argv[++i];
    }
    IncrementalProgram program; string error;
    if (!CompileIncrementalProgram(expression, program, error)) { cerr << "Bad incremental query: " << error << endl; return 1; }
    if (partition_root.empty()) { cerr << "partition root is required via -d" << endl; return 1; }
    auto workers = ParseWorkers(workers_text);
    if (!HealthCheckWorkers(workers)) { cerr << "Worker health check failed" << endl; return 1; }
    if (query_id <= 0) { cerr << "query id must be positive" << endl; return 1; }
    try {
        vector<future<void>> init;
        for (size_t i = 0; i < workers.size(); ++i) init.push_back(async(launch::deferred, [&workers, i, query_id, &expression, &workers_text]() {
            Message request(MessageType::INCREMENTAL_INIT_QUERY); request.AddField("query_id", query_id); request.AddField("expression", expression); request.AddField("workers", workers_text);
            lock_guard<mutex> lock(*workers[i].mu); EnsureIncrementalOk(workers[i].client->SendRequest(request), "incremental init");
        }));
        for (auto& future : init) future.get();
        vector<future<void>> activate;
        for (size_t i = 0; i < workers.size(); ++i) activate.push_back(async(launch::deferred, [&workers, i, query_id]() {
            Message request(MessageType::INCREMENTAL_INIT_QUERY);
            request.AddField("query_id", query_id);
            request.AddField("activate_only", 1);
            lock_guard<mutex> lock(*workers[i].mu);
            EnsureIncrementalOk(workers[i].client->SendRequest(request), "incremental activate");
        }));
        for (auto& future : activate) future.get();
        const auto begin = chrono::steady_clock::now();
        PrepareIncrementalOpenPlans(workers, query_id, program);
        auto starts = IncrementalRequestAll(workers, [query_id](size_t) {
            Message request(MessageType::INCREMENTAL_START_ROOT); request.AddField("query_id", query_id);
            return request;
        });
        for (auto& future : starts) EnsureIncrementalOk(future.get(), "incremental root start");

        uint64_t roots_total = 0, rows_started = 0, cache_hits = 0, remote_requests = 0, remote_request_rpcs = 0, remote_complete_rpcs = 0, remote_stream_rpcs = 0, delta_count = 0;
        uint64_t gcnp_batches = 0, gcnp_source_rows = 0, gcnp_groups = 0, gcnp_reusable_groups = 0, gcnp_cache_hits = 0;
        uint64_t base_expand_us = 0, gcnp_wall_us = 0, gcnp_input_mids = 0, gcnp_planned_mids = 0, gcnp_bypassed_mids = 0, join_batch_count = 0, join_batch_children = 0, join_batch_child_events = 0, join_batch_publish_pairs = 0, join_batch_wall_us = 0, emit_merge_us = 0;
        uint64_t gcnp_complete_prepare_us = 0, gcnp_complete_group_extract_us = 0, gcnp_complete_pattern_cache_us = 0, gcnp_complete_union_us = 0, gcnp_complete_shared_target_us = 0, gcnp_complete_source_materialize_us = 0, gcnp_complete_groups = 0, gcnp_complete_reusable_groups = 0, gcnp_complete_grouped_source_uses = 0, gcnp_complete_shared_source_uses = 0, gcnp_complete_external_memo_hits = 0;
        uint64_t speculative_gcnp_tasks = 0, speculative_gcnp_completed = 0, speculative_gcnp_cancelled = 0, speculative_gcnp_us = 0, speculative_pattern_memo_stores = 0, speculative_pattern_memo_hits = 0;
        uint64_t panel_wait_blocks = 0, panel_left_input_batches = 0;
        const array<string, 4> owner_view_fields = {"owner_view_left_rows",
            "owner_view_coverage_rows", "owner_view_rhs_inputs", "owner_view_final_moves"};
        array<uint64_t, 4> owner_view_counts{};
        const array<string, 8> bounded_owner_fields = {"bounded_destinations_us",
            "bounded_closure_merge_us", "bounded_closure_copy_us",
            "helper_closure_restore_us", "helper_closure_restore_rows",
            "async_accept_queue_us", "async_accept_run_us", "async_accept_messages"};
        vector<array<uint64_t, 8>> bounded_owner_times(workers.size());
        uint64_t all_left_panels_sealed_elapsed_us = 0;
        uint64_t left_panel_wait_us = 0, left_panel_wait_us_max = 0;
        uint64_t rhs_ready_before_left_blocks = 0, rhs_ready_wait_for_left_us = 0, rhs_ready_wait_for_left_us_max = 0;
        uint64_t left_sealed_before_rhs_blocks = 0, left_wait_for_rhs_us = 0, left_wait_for_rhs_us_max = 0;
        uint64_t coverage_filter_candidate_mids = 0, coverage_filter_signature_rejections = 0, coverage_filter_subset_tests = 0;
        uint64_t coverage_filter_skipped_mids = 0, coverage_filter_skipped_rhs_pairs = 0;
        uint64_t demand_batches_started = 0, demand_batch_emits = 0, demand_batch_pairs = 0, demand_compose_calls = 0, demand_compose_us = 0;
        uint64_t matrix_join_updates = 0, matrix_join_result_pairs = 0, matrix_join_us = 0, range_coverage_pairs = 0;
        uint64_t factorized_result_messages = 0;
        uint64_t factorized_result_shared_blocks = 0;
        uint64_t factorized_result_dictionary_merges = 0;
        uint64_t factorized_result_wire_bytes_saved = 0;
        uint64_t factorized_result_coalesce_us = 0;
        uint64_t factorized_result_expand_us = 0;
        uint64_t factorized_result_union_us = 0;
        uint64_t factorized_result_encode_us = 0;
        uint64_t factorized_result_decode_us = 0;
        uint64_t factorized_result_blocks_after_coalesce = 0;
        uint64_t factorized_result_source_uses = 0;
        uint64_t factorized_source_set_blocks_before = 0;
        uint64_t factorized_source_set_blocks_after = 0;
        uint64_t factorized_source_set_reused_groups = 0;
        uint64_t factorized_source_set_blocks_merged = 0;
        uint64_t factorized_source_union_operations_avoided = 0;
        uint64_t factorized_source_set_group_us = 0;
        uint64_t factorized_source_set_union_us = 0;
        uint64_t global_rhs_signature_messages = 0, global_rhs_signature_mids = 0;
        uint64_t global_signature_left_pairs_before = 0;
        uint64_t global_signature_left_pairs_after = 0;
        uint64_t global_signature_left_pairs_filtered = 0;
        uint64_t middle_scheduler_assigned_shards = 0, middle_scheduler_submitted_work = 0;
        uint64_t middle_scheduler_completed_work = 0, middle_scheduler_executed_work = 0;
        uint64_t pre_route_coverage_candidate_mids = 0, pre_route_coverage_signature_rejections = 0;
        uint64_t pre_route_coverage_subset_tests = 0, pre_route_coverage_skipped_mids = 0;
        uint64_t pre_route_coverage_skipped_rhs_pairs = 0;
        uint64_t pre_route_coverage_input_left_pairs = 0, pre_route_coverage_output_left_pairs = 0;
        uint64_t rsfm_selected_left_pairs = 0, rsfm_new_left_pairs = 0;
        uint64_t rsfm_replay_left_pairs = 0, rsfm_right_pairs = 0;
        uint64_t rsfm_full_broadcast_pairs = 0, rsfm_routed_messages = 0;
        uint64_t rsfm_signature_activations = 0, rsfm_empty_left_dispatches = 0;
        uint64_t rsfm_resident_rhs_pairs_avoided = 0;
        uint64_t resident_rhs_expected_rows = 0, resident_rhs_actual_rows = 0;
        uint64_t resident_rhs_expected_pairs = 0, resident_rhs_actual_pairs = 0;
        uint64_t resident_rhs_mismatched_shards = 0;
        uint64_t peer_wire_bytes = 0;
        uint64_t peer_base_wire_bytes = 0, peer_row_wire_bytes = 0;
        uint64_t peer_demand_wire_bytes = 0, peer_signature_wire_bytes = 0;
        uint64_t peer_rhs_wire_bytes = 0, peer_lhs_wire_bytes = 0;
        uint64_t peer_matrix_result_wire_bytes = 0, peer_other_wire_bytes = 0;
        struct WorkerTiming {
            uint64_t wall_us = 0, compute_wall_us = 0;
            uint64_t communication_transport_us = 0, communication_e2e_us = 0;
            uint64_t bytes = 0, messages = 0, scheduler_shards = 0;
            uint64_t scheduler_submitted_work = 0, scheduler_completed_work = 0;
            uint64_t scheduler_executed_work = 0;
            uint64_t balance_planner_us = 0, balance_join_us = 0;
            uint64_t balance_remote_tasks = 0, balance_local_tasks = 0;
            uint64_t balance_reduce_us = 0, balance_dispatch_us = 0;
            uint64_t balance_coverage_us = 0, balance_gather_us = 0;
            uint64_t balance_input_route_us = 0, balance_input_merge_us = 0;
            uint64_t balance_open_closure_us = 0, balance_open_closure_rows = 0;
            uint64_t balance_open_closure_seeds = 0;
            uint64_t balance_rhs_select_us = 0, balance_rhs_pack_us = 0;
            uint64_t balance_rhs_examined_rows = 0, balance_rhs_requests = 0;
            uint64_t balance_rhs_requested_keys = 0, balance_rhs_keys_avoided = 0;
            uint64_t result_commit_us = 0, result_queue_wait_us = 0;
            uint64_t result_restore_us = 0, result_restore_max_lanes = 0;
            uint64_t balance_rhs_queue_us = 0, balance_rhs_encode_us = 0;
            uint64_t balance_rhs_wrap_us = 0, balance_rhs_raw_bytes = 0;
            uint64_t balance_rhs_wire_bytes = 0;
        };
        vector<WorkerTiming> worker_timings(workers.size());
        const array<string, 12> biclique_fields = {"biclique_attempts", "biclique_selected",
            "biclique_blocks", "biclique_shared_pairs", "biclique_original_bytes",
            "biclique_encoded_bytes", "biclique_encode_us", "biclique_decode_us",
            "biclique_hash_us", "biclique_plan_us", "biclique_pack_us", "biclique_parallel_batches"};
        vector<array<uint64_t, 12>> biclique_profiles(workers.size());
        cout << "[biclique-config] mode=" << BicliqueWireMode()
             << " compression_threads=" << BicliqueCompressionThreads() << endl;
        const bool diagnostic_status = getenv("INCREMENTAL_DIAGNOSTIC_STATUS") != nullptr;
        const bool balance_controller = true;
        cout << "[coordinator-mode] stage_balance=" << rpq_final::kBalanceMode
             << " concurrent_controller=" << balance_controller << endl;
        uint64_t status_collection_us = 0;
        uint64_t status_parse_us = 0, status_response_bytes = 0;
        uint64_t status_poll_rounds = 0;
        uint64_t status_round = 0;
        bool collect_status_metrics = false;
        uint64_t fetch_bytes = 0, decode_sum_us = 0, decode_max_us = 0, final_merge_us = 0;
        size_t fetched_workers = 0, early_fetched_workers = 0;
        vector<unique_ptr<TcpClient>> fetch_clients(workers.size());
        vector<TcpClient*> pending_fetches(workers.size(), nullptr);
        vector<chrono::steady_clock::time_point> fetch_started(workers.size());
        vector<vector<RelationRows>> incoming;
        incoming.reserve(workers.size());
        CompleteFinalGroups complete_groups;
        auto start_fetch = [&](size_t index) {
            if (fetch_clients[index]) return;
            // Keep status RPCs on their existing connection. Result receive
            // and decoding stay on this same coordinator thread.
            fetch_clients[index] = make_unique<TcpClient>(workers[index].host, workers[index].port);
            Message request(MessageType::INCREMENTAL_FETCH_RESULTS);
            request.AddField("query_id", query_id);
            fetch_started[index] = chrono::steady_clock::now();
            fetch_clients[index]->BeginRequest(request);
            pending_fetches[index] = fetch_clients[index].get();
        };
        auto finish_fetch = [&](size_t index, Message response, bool early) {
            pending_fetches[index] = nullptr;
            if (response.type != MessageType::RESPONSE_DATA)
                throw runtime_error("incremental fetch: " + response.GetField("error"));
            const auto received = chrono::steady_clock::now();
            const uint64_t bytes = response.data.size();
            uint64_t decode_us = 0;
            if (balance_controller) {
                const auto decode_begin = chrono::steady_clock::now();
                ParseCompleteFinalGroups(response.data, complete_groups);
                decode_us = chrono::duration_cast<chrono::microseconds>(
                    chrono::steady_clock::now() - decode_begin).count();
            } else {
                RelationRows rows;
                if (!ParseIncrementalFinalRows(response.data, rows, error))
                    throw runtime_error("incremental final decode: " + error);
                incoming.push_back(PartitionFinalRows(std::move(rows)));
            }
            decode_sum_us += decode_us;
            decode_max_us = max(decode_max_us, decode_us);
            fetch_bytes += bytes;
            ++fetched_workers;
            if (early) ++early_fetched_workers;
            if (IncrementalScalingTraceEnabled()) {
                ostringstream line;
                line << "[scaling-final-fetch] worker=" << index << " bytes=" << bytes
                     << " rpc_us=" << chrono::duration_cast<chrono::microseconds>(received-fetch_started[index]).count()
                     << " decode_us=" << decode_us
                     << " total_us=" << chrono::duration_cast<chrono::microseconds>(chrono::steady_clock::now()-fetch_started[index]).count()
                     << " early=" << early;
                cerr << line.str() << '\n';
            }
        };
        while (true) {
            bool all_done = true; roots_total = rows_started = cache_hits = remote_requests = remote_request_rpcs = remote_complete_rpcs = remote_stream_rpcs = delta_count = 0;
            vector<string> worker_status;
            if (diagnostic_status) worker_status.reserve(workers.size());
            gcnp_batches = gcnp_source_rows = gcnp_groups = gcnp_reusable_groups = gcnp_cache_hits = 0;
            base_expand_us = gcnp_wall_us = gcnp_input_mids = gcnp_planned_mids = gcnp_bypassed_mids = join_batch_count = join_batch_children = join_batch_child_events = join_batch_publish_pairs = join_batch_wall_us = emit_merge_us = 0;
            gcnp_complete_prepare_us = gcnp_complete_group_extract_us = gcnp_complete_pattern_cache_us = gcnp_complete_union_us = gcnp_complete_shared_target_us = gcnp_complete_source_materialize_us = 0;
            gcnp_complete_groups = gcnp_complete_reusable_groups = gcnp_complete_grouped_source_uses = gcnp_complete_shared_source_uses = 0;
            gcnp_complete_external_memo_hits = 0;
            speculative_gcnp_tasks = speculative_gcnp_completed = speculative_gcnp_cancelled = speculative_gcnp_us = 0;
            speculative_pattern_memo_stores = speculative_pattern_memo_hits = 0;
            panel_wait_blocks = panel_left_input_batches = 0;
            owner_view_counts.fill(0);
            all_left_panels_sealed_elapsed_us = 0;
            left_panel_wait_us = left_panel_wait_us_max = 0;
            rhs_ready_before_left_blocks = rhs_ready_wait_for_left_us = rhs_ready_wait_for_left_us_max = 0;
            left_sealed_before_rhs_blocks = left_wait_for_rhs_us = left_wait_for_rhs_us_max = 0;
            coverage_filter_candidate_mids = coverage_filter_signature_rejections = coverage_filter_subset_tests = 0;
            coverage_filter_skipped_mids = coverage_filter_skipped_rhs_pairs = 0;
            demand_batches_started = demand_batch_emits = demand_batch_pairs = demand_compose_calls = demand_compose_us = 0;
            matrix_join_updates = matrix_join_result_pairs = matrix_join_us = range_coverage_pairs = 0;
            factorized_result_messages = factorized_result_shared_blocks = 0;
            factorized_result_dictionary_merges = 0;
            factorized_result_wire_bytes_saved = 0;
            factorized_result_coalesce_us = 0;
            factorized_result_expand_us = 0;
            factorized_result_union_us = 0;
            factorized_result_encode_us = 0;
            factorized_result_decode_us = 0;
            factorized_result_blocks_after_coalesce = 0;
            factorized_result_source_uses = 0;
            factorized_source_set_blocks_before = 0;
            factorized_source_set_blocks_after = 0;
            factorized_source_set_reused_groups = 0;
            factorized_source_set_blocks_merged = 0;
            factorized_source_union_operations_avoided = 0;
            factorized_source_set_group_us = 0;
            factorized_source_set_union_us = 0;
            global_rhs_signature_messages = global_rhs_signature_mids = 0;
            global_signature_left_pairs_before = 0;
            global_signature_left_pairs_after = 0;
            global_signature_left_pairs_filtered = 0;
            middle_scheduler_assigned_shards = middle_scheduler_submitted_work = 0;
            middle_scheduler_completed_work = middle_scheduler_executed_work = 0;
            pre_route_coverage_candidate_mids = pre_route_coverage_signature_rejections = 0;
            pre_route_coverage_subset_tests = pre_route_coverage_skipped_mids = 0;
            pre_route_coverage_skipped_rhs_pairs = 0;
            pre_route_coverage_input_left_pairs = pre_route_coverage_output_left_pairs = 0;
            rsfm_selected_left_pairs = rsfm_new_left_pairs = rsfm_replay_left_pairs = rsfm_right_pairs = 0;
            rsfm_full_broadcast_pairs = rsfm_routed_messages = 0;
            rsfm_signature_activations = rsfm_empty_left_dispatches = 0;
            rsfm_resident_rhs_pairs_avoided = 0;
            resident_rhs_expected_rows = resident_rhs_actual_rows = 0;
            resident_rhs_expected_pairs = resident_rhs_actual_pairs = 0;
            resident_rhs_mismatched_shards = 0;
            peer_wire_bytes = 0;
            peer_base_wire_bytes = peer_row_wire_bytes = peer_demand_wire_bytes = 0;
            peer_signature_wire_bytes = peer_rhs_wire_bytes = peer_lhs_wire_bytes = 0;
            peer_matrix_result_wire_bytes = peer_other_wire_bytes = 0;
            const auto status_begin = chrono::steady_clock::now();
            vector<future<Message>> statuses;
            if (balance_controller) {
                statuses = IncrementalRequestAll(workers, [&](size_t) {
                        Message request(MessageType::INCREMENTAL_QUERY_STATUS);
                        request.AddField("query_id", query_id);
                        request.AddField("fast_poll", collect_status_metrics ? 0 : 1);
                        return request;
                });
            }
            for (size_t worker_index = 0; worker_index < workers.size(); ++worker_index) {
                auto& worker = workers[worker_index];
                Message request(MessageType::INCREMENTAL_QUERY_STATUS); request.AddField("query_id", query_id);
                Message raw_response;
                if (balance_controller) raw_response = statuses[worker_index].get();
                else { lock_guard<mutex> lock(*worker.mu); raw_response = worker.client->SendRequest(request); }
                EnsureIncrementalOk(raw_response, "incremental status");
                const auto parse_begin = chrono::steady_clock::now();
                const IncrementalStatusFields response(raw_response);
                status_response_bytes += raw_response.data.size();
                if (diagnostic_status) {
                    worker_status.push_back(
                        to_string(response.GetFieldInt("roots_initialized")) + "/" +
                        to_string(response.GetFieldUInt64("roots_done")) + "/" +
                        to_string(response.GetFieldUInt64("roots_total")) +
                        ":rows=" + to_string(response.GetFieldUInt64("rows_started")) +
                        ":join=" + to_string(response.GetFieldUInt64("matrix_join_updates")));
                }
                all_done = all_done && response.GetFieldInt("done") != 0;
                if (response.GetFieldInt("done") != 0) start_fetch(worker_index);
                if (balance_controller && !collect_status_metrics) {
                    status_parse_us += chrono::duration_cast<chrono::microseconds>(
                        chrono::steady_clock::now() - parse_begin).count();
                    continue;
                }
                roots_total += response.GetFieldUInt64("roots_total"); rows_started += response.GetFieldUInt64("rows_started"); cache_hits += response.GetFieldUInt64("row_cache_hits"); remote_requests += response.GetFieldUInt64("remote_requests"); remote_request_rpcs += response.GetFieldUInt64("remote_request_rpcs"); remote_complete_rpcs += response.GetFieldUInt64("remote_complete_rpcs"); remote_stream_rpcs += response.GetFieldUInt64("remote_stream_rpcs");
                delta_count += response.GetFieldUInt64("delta_count");
                gcnp_batches += response.GetFieldUInt64("gcnp_batches");
                gcnp_source_rows += response.GetFieldUInt64("gcnp_source_rows");
                gcnp_groups += response.GetFieldUInt64("gcnp_groups");
                gcnp_reusable_groups += response.GetFieldUInt64("gcnp_reusable_groups");
                gcnp_cache_hits += response.GetFieldUInt64("gcnp_cache_hits");
                base_expand_us += response.GetFieldUInt64("base_expand_us");
                for (size_t i = 0; i < owner_view_fields.size(); ++i)
                    owner_view_counts[i] += response.GetFieldUInt64(owner_view_fields[i]);
                for (size_t i = 0; i < bounded_owner_fields.size(); ++i)
                    bounded_owner_times[worker_index][i] = response.GetFieldUInt64(bounded_owner_fields[i]);
                for (size_t i = 0; i < biclique_fields.size(); ++i)
                    biclique_profiles[worker_index][i] = response.GetFieldUInt64(biclique_fields[i]);
                gcnp_wall_us += response.GetFieldUInt64("gcnp_wall_us");
                gcnp_input_mids += response.GetFieldUInt64("gcnp_input_mids");
                gcnp_planned_mids += response.GetFieldUInt64("gcnp_planned_mids");
                gcnp_bypassed_mids += response.GetFieldUInt64("gcnp_bypassed_mids");
                gcnp_complete_prepare_us += response.GetFieldUInt64("gcnp_complete_prepare_us");
                gcnp_complete_group_extract_us += response.GetFieldUInt64("gcnp_complete_group_extract_us");
                gcnp_complete_pattern_cache_us += response.GetFieldUInt64("gcnp_complete_pattern_cache_us");
                gcnp_complete_union_us += response.GetFieldUInt64("gcnp_complete_union_us");
                gcnp_complete_shared_target_us += response.GetFieldUInt64("gcnp_complete_shared_target_us");
                gcnp_complete_source_materialize_us += response.GetFieldUInt64("gcnp_complete_source_materialize_us");
                gcnp_complete_groups += response.GetFieldUInt64("gcnp_complete_groups");
                gcnp_complete_reusable_groups += response.GetFieldUInt64("gcnp_complete_reusable_groups");
                gcnp_complete_grouped_source_uses += response.GetFieldUInt64("gcnp_complete_grouped_source_uses");
                gcnp_complete_shared_source_uses += response.GetFieldUInt64("gcnp_complete_shared_source_uses");
                gcnp_complete_external_memo_hits += response.GetFieldUInt64("gcnp_complete_external_memo_hits");
                speculative_gcnp_tasks += response.GetFieldUInt64("speculative_gcnp_tasks");
                speculative_gcnp_completed += response.GetFieldUInt64("speculative_gcnp_completed");
                speculative_gcnp_cancelled += response.GetFieldUInt64("speculative_gcnp_cancelled");
                speculative_gcnp_us += response.GetFieldUInt64("speculative_gcnp_us");
                speculative_pattern_memo_stores += response.GetFieldUInt64("speculative_pattern_memo_stores");
                speculative_pattern_memo_hits += response.GetFieldUInt64("speculative_pattern_memo_hits");
                panel_wait_blocks += response.GetFieldUInt64("panel_wait_blocks");
                panel_left_input_batches += response.GetFieldUInt64("panel_left_input_batches");
                all_left_panels_sealed_elapsed_us = max(
                    all_left_panels_sealed_elapsed_us,
                    response.GetFieldUInt64("all_left_panels_sealed_elapsed_us"));
                left_panel_wait_us += response.GetFieldUInt64("left_panel_wait_us");
                left_panel_wait_us_max = max(left_panel_wait_us_max, response.GetFieldUInt64("left_panel_wait_us_max"));
                rhs_ready_before_left_blocks += response.GetFieldUInt64("rhs_ready_before_left_blocks");
                rhs_ready_wait_for_left_us += response.GetFieldUInt64("rhs_ready_wait_for_left_us");
                rhs_ready_wait_for_left_us_max = max(rhs_ready_wait_for_left_us_max, response.GetFieldUInt64("rhs_ready_wait_for_left_us_max"));
                left_sealed_before_rhs_blocks += response.GetFieldUInt64("left_sealed_before_rhs_blocks");
                left_wait_for_rhs_us += response.GetFieldUInt64("left_wait_for_rhs_us");
                left_wait_for_rhs_us_max = max(left_wait_for_rhs_us_max, response.GetFieldUInt64("left_wait_for_rhs_us_max"));
                coverage_filter_candidate_mids += response.GetFieldUInt64("coverage_filter_candidate_mids");
                coverage_filter_signature_rejections += response.GetFieldUInt64("coverage_filter_signature_rejections");
                coverage_filter_subset_tests += response.GetFieldUInt64("coverage_filter_subset_tests");
                coverage_filter_skipped_mids += response.GetFieldUInt64("coverage_filter_skipped_mids");
                coverage_filter_skipped_rhs_pairs += response.GetFieldUInt64("coverage_filter_skipped_rhs_pairs");
                join_batch_count += response.GetFieldUInt64("join_batch_count");
                join_batch_children += response.GetFieldUInt64("join_batch_children");
                join_batch_child_events += response.GetFieldUInt64("join_batch_child_events");
                join_batch_publish_pairs += response.GetFieldUInt64("join_batch_publish_pairs");
                join_batch_wall_us += response.GetFieldUInt64("join_batch_wall_us");
                emit_merge_us += response.GetFieldUInt64("emit_merge_us");
                demand_batches_started += response.GetFieldUInt64("demand_batches_started");
                demand_batch_emits += response.GetFieldUInt64("demand_batch_emits");
                demand_batch_pairs += response.GetFieldUInt64("demand_batch_pairs");
                demand_compose_calls += response.GetFieldUInt64("demand_compose_calls");
                demand_compose_us += response.GetFieldUInt64("demand_compose_us");
            matrix_join_updates += response.GetFieldUInt64("matrix_join_updates");
            matrix_join_result_pairs += response.GetFieldUInt64("matrix_join_result_pairs");
            matrix_join_us += response.GetFieldUInt64("matrix_join_us");
            factorized_result_messages +=
                response.GetFieldUInt64("factorized_result_messages");
            factorized_result_shared_blocks +=
                response.GetFieldUInt64("factorized_result_shared_blocks");
            factorized_result_dictionary_merges +=
                response.GetFieldUInt64("factorized_result_dictionary_merges");
            factorized_result_wire_bytes_saved +=
                response.GetFieldUInt64("factorized_result_wire_bytes_saved");
            factorized_result_coalesce_us +=
                response.GetFieldUInt64("factorized_result_coalesce_us");
            factorized_result_expand_us +=
                response.GetFieldUInt64("factorized_result_expand_us");
            factorized_result_union_us +=
                response.GetFieldUInt64("factorized_result_union_us");
            factorized_result_encode_us +=
                response.GetFieldUInt64("factorized_result_encode_us");
            factorized_result_decode_us +=
                response.GetFieldUInt64("factorized_result_decode_us");
            factorized_result_blocks_after_coalesce += response.GetFieldUInt64(
                "factorized_result_blocks_after_coalesce");
            factorized_result_source_uses += response.GetFieldUInt64(
                "factorized_result_source_uses");
            factorized_source_set_blocks_before += response.GetFieldUInt64(
                "factorized_source_set_blocks_before");
            factorized_source_set_blocks_after += response.GetFieldUInt64(
                "factorized_source_set_blocks_after");
            factorized_source_set_reused_groups += response.GetFieldUInt64(
                "factorized_source_set_reused_groups");
            factorized_source_set_blocks_merged += response.GetFieldUInt64(
                "factorized_source_set_blocks_merged");
            factorized_source_union_operations_avoided += response.GetFieldUInt64(
                "factorized_source_union_operations_avoided");
            factorized_source_set_group_us += response.GetFieldUInt64(
                "factorized_source_set_group_us");
            factorized_source_set_union_us += response.GetFieldUInt64(
                "factorized_source_set_union_us");
            global_rhs_signature_messages +=
                response.GetFieldUInt64("global_rhs_signature_messages");
            global_rhs_signature_mids +=
                response.GetFieldUInt64("global_rhs_signature_mids");
            global_signature_left_pairs_before +=
                response.GetFieldUInt64("global_signature_left_pairs_before");
            global_signature_left_pairs_after +=
                response.GetFieldUInt64("global_signature_left_pairs_after");
            global_signature_left_pairs_filtered +=
                response.GetFieldUInt64("global_signature_left_pairs_filtered");
            middle_scheduler_assigned_shards += response.GetFieldUInt64("middle_scheduler_assigned_shards");
            middle_scheduler_submitted_work += response.GetFieldUInt64("middle_scheduler_submitted_work");
            middle_scheduler_completed_work += response.GetFieldUInt64("middle_scheduler_completed_work");
            middle_scheduler_executed_work += response.GetFieldUInt64("middle_scheduler_executed_work");
            range_coverage_pairs += response.GetFieldUInt64("range_coverage_pairs");
            pre_route_coverage_candidate_mids += response.GetFieldUInt64("pre_route_coverage_candidate_mids");
            pre_route_coverage_signature_rejections += response.GetFieldUInt64("pre_route_coverage_signature_rejections");
            pre_route_coverage_subset_tests += response.GetFieldUInt64("pre_route_coverage_subset_tests");
            pre_route_coverage_skipped_mids += response.GetFieldUInt64("pre_route_coverage_skipped_mids");
            pre_route_coverage_skipped_rhs_pairs += response.GetFieldUInt64("pre_route_coverage_skipped_rhs_pairs");
            pre_route_coverage_input_left_pairs += response.GetFieldUInt64("pre_route_coverage_input_left_pairs");
            pre_route_coverage_output_left_pairs += response.GetFieldUInt64("pre_route_coverage_output_left_pairs");
            rsfm_selected_left_pairs += response.GetFieldUInt64("rsfm_selected_left_pairs");
            rsfm_new_left_pairs += response.GetFieldUInt64("rsfm_new_left_pairs");
            rsfm_replay_left_pairs += response.GetFieldUInt64("rsfm_replay_left_pairs");
            rsfm_right_pairs += response.GetFieldUInt64("rsfm_right_pairs");
            rsfm_full_broadcast_pairs += response.GetFieldUInt64("rsfm_full_broadcast_pairs");
            rsfm_routed_messages += response.GetFieldUInt64("rsfm_routed_messages");
            rsfm_signature_activations += response.GetFieldUInt64("rsfm_signature_activations");
            rsfm_empty_left_dispatches += response.GetFieldUInt64("rsfm_empty_left_dispatches");
            rsfm_resident_rhs_pairs_avoided += response.GetFieldUInt64("rsfm_resident_rhs_pairs_avoided");
            resident_rhs_expected_rows += response.GetFieldUInt64("resident_rhs_expected_rows");
            resident_rhs_actual_rows += response.GetFieldUInt64("resident_rhs_actual_rows");
            resident_rhs_expected_pairs += response.GetFieldUInt64("resident_rhs_expected_pairs");
            resident_rhs_actual_pairs += response.GetFieldUInt64("resident_rhs_actual_pairs");
            resident_rhs_mismatched_shards += response.GetFieldUInt64("resident_rhs_mismatched_shards");
            peer_wire_bytes += response.GetFieldUInt64("peer_wire_bytes");
            peer_base_wire_bytes += response.GetFieldUInt64("peer_base_wire_bytes");
            peer_row_wire_bytes += response.GetFieldUInt64("peer_row_wire_bytes");
            peer_demand_wire_bytes += response.GetFieldUInt64("peer_demand_wire_bytes");
            peer_signature_wire_bytes += response.GetFieldUInt64("peer_signature_wire_bytes");
            peer_rhs_wire_bytes += response.GetFieldUInt64("peer_rhs_wire_bytes");
            peer_lhs_wire_bytes += response.GetFieldUInt64("peer_lhs_wire_bytes");
            peer_matrix_result_wire_bytes += response.GetFieldUInt64("peer_matrix_result_wire_bytes");
            peer_other_wire_bytes += response.GetFieldUInt64("peer_other_wire_bytes");
                worker_timings[worker_index] = WorkerTiming{
                response.GetFieldUInt64("worker_query_wall_us"),
                response.GetFieldUInt64("worker_compute_wall_us"),
                response.GetFieldUInt64("worker_communication_wall_us"),
                response.GetFieldUInt64("worker_communication_e2e_union_us"),
                response.GetFieldUInt64("peer_wire_bytes"),
                response.GetFieldUInt64("peer_wire_messages"),
                response.GetFieldUInt64("middle_scheduler_assigned_shards"),
                response.GetFieldUInt64("middle_scheduler_submitted_work"),
                response.GetFieldUInt64("middle_scheduler_completed_work"),
                response.GetFieldUInt64("middle_scheduler_executed_work"),
                response.GetFieldUInt64("balance_planner_us"),
                response.GetFieldUInt64("balance_join_us"),
                response.GetFieldUInt64("balance_remote_tasks"),
                response.GetFieldUInt64("balance_local_tasks"),
                response.GetFieldUInt64("balance_reduce_us"),
                response.GetFieldUInt64("balance_dispatch_us"),
                response.GetFieldUInt64("balance_coverage_us"),
                response.GetFieldUInt64("balance_gather_us"),
                response.GetFieldUInt64("balance_input_route_us"),
                response.GetFieldUInt64("balance_input_merge_us"),
                response.GetFieldUInt64("balance_open_closure_us"),
                response.GetFieldUInt64("balance_open_closure_rows"),
                response.GetFieldUInt64("balance_open_closure_seeds"),
                response.GetFieldUInt64("balance_rhs_select_us"),
                response.GetFieldUInt64("balance_rhs_pack_us"),
                response.GetFieldUInt64("balance_rhs_examined_rows"),
                response.GetFieldUInt64("balance_rhs_requests"),
                response.GetFieldUInt64("balance_rhs_requested_keys"),
                response.GetFieldUInt64("balance_rhs_keys_avoided")};
            worker_timings[worker_index].result_commit_us = response.GetFieldUInt64("result_commit_us");
            worker_timings[worker_index].result_queue_wait_us = response.GetFieldUInt64("result_queue_wait_us");
            worker_timings[worker_index].result_restore_us = response.GetFieldUInt64("result_restore_us");
            worker_timings[worker_index].result_restore_max_lanes = response.GetFieldUInt64("result_restore_max_lanes");
            worker_timings[worker_index].balance_rhs_queue_us = response.GetFieldUInt64("balance_rhs_queue_us");
            worker_timings[worker_index].balance_rhs_encode_us = response.GetFieldUInt64("balance_rhs_encode_us");
            worker_timings[worker_index].balance_rhs_wrap_us = response.GetFieldUInt64("balance_rhs_wrap_us");
            worker_timings[worker_index].balance_rhs_raw_bytes = response.GetFieldUInt64("balance_rhs_raw_bytes");
            worker_timings[worker_index].balance_rhs_wire_bytes = response.GetFieldUInt64("balance_rhs_wire_bytes");
            status_parse_us += chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - parse_begin).count();
            }
            ++status_poll_rounds;
            status_collection_us += chrono::duration_cast<chrono::microseconds>(
                chrono::steady_clock::now() - status_begin).count();
            for (;;) {
                Message response;
                size_t index = 0;
                if (!TcpClient::TryReceiveAnyResponse(pending_fetches, response, index)) break;
                finish_fetch(index, std::move(response), !all_done);
            }
            if (all_done) {
                if (!balance_controller || collect_status_metrics) break;
                collect_status_metrics = true;
                continue;
            }
            if (diagnostic_status && (++status_round % 20) == 0) {
                cerr << "[incremental-status] query_id=" << query_id;
                for (size_t i = 0; i < worker_status.size(); ++i) {
                    cerr << " w" << i << "=" << worker_status[i];
                }
                cerr << endl;
            }
            this_thread::sleep_for(chrono::milliseconds(50));
        }
        const uint64_t execution_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - begin).count();
        for (size_t i = 0; i < workers.size(); ++i) {
            start_fetch(i);
        }
        while (fetched_workers < workers.size()) {
            Message response;
            const size_t i = TcpClient::ReceiveAnyResponse(pending_fetches, response);
            finish_fetch(i, std::move(response), false);
        }
        const auto merge_begin = chrono::steady_clock::now();
        ParallelFinalRows final_rows;
        if (balance_controller) {
            complete_groups.ValidateSources();
            final_rows.rows = complete_groups.rows;
            final_rows.pairs = complete_groups.pairs;
        } else {
            final_rows = ReduceFinalRows(std::move(incoming));
        }
        final_merge_us = chrono::duration_cast<chrono::microseconds>(
            chrono::steady_clock::now() - merge_begin).count();
        const uint64_t total_ms = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - begin).count();
        for (size_t worker = 0; worker < workers.size(); ++worker) {
            cout << "[biclique-wire] worker=" << worker << " mode=" << BicliqueWireMode();
            for (size_t i = 0; i < biclique_fields.size(); ++i)
                cout << ' ' << biclique_fields[i] << '=' << biclique_profiles[worker][i];
            cout << endl;
        }
        cout << "[incremental-demand] query=" << expression << " result_pairs=" << final_rows.pairs << endl;
        cout << "[coordinator-balance] status_collection_us=" << status_collection_us
             << " early_final_fetch_workers=" << early_fetched_workers
             << " status_parse_us=" << status_parse_us
             << " status_response_bytes=" << status_response_bytes
             << " status_poll_rounds=" << status_poll_rounds
             << " final_decode_sum_us=" << decode_sum_us
             << " final_decode_max_us=" << decode_max_us
             << " final_merge_us=" << final_merge_us
             << " final_reduce_lanes=" << kFinalSourceLanes
             << " complete_result_groups=" << complete_groups.groups.size()
             << " complete_result_rows=" << final_rows.rows << endl;
        cout << "[incremental-demand-metrics] total_ms=" << total_ms << " execution_ms=" << execution_ms << " final_fetch_ms=" << (total_ms - execution_ms) << " roots_total=" << roots_total << " unique_rows_started=" << rows_started << " row_cache_hits=" << cache_hits << " remote_row_requests=" << remote_requests << " remote_request_rpcs=" << remote_request_rpcs << " remote_complete_rpcs=" << remote_complete_rpcs << " remote_stream_rpcs=" << remote_stream_rpcs << " row_delta_batches=" << delta_count << " base_expand_us_sum=" << base_expand_us << " gcnp_batches=" << gcnp_batches << " gcnp_source_rows=" << gcnp_source_rows << " gcnp_groups=" << gcnp_groups << " gcnp_reusable_groups=" << gcnp_reusable_groups << " gcnp_cache_hits=" << gcnp_cache_hits << " gcnp_input_mids=" << gcnp_input_mids << " gcnp_planned_mids=" << gcnp_planned_mids << " gcnp_bypassed_mids=" << gcnp_bypassed_mids << " gcnp_wall_us_sum=" << gcnp_wall_us << " gcnp_complete_prepare_us_sum=" << gcnp_complete_prepare_us << " gcnp_complete_group_extract_us_sum=" << gcnp_complete_group_extract_us << " gcnp_complete_pattern_cache_us_sum=" << gcnp_complete_pattern_cache_us << " gcnp_complete_union_us_sum=" << gcnp_complete_union_us << " gcnp_complete_shared_target_us_sum=" << gcnp_complete_shared_target_us << " gcnp_complete_source_materialize_us_sum=" << gcnp_complete_source_materialize_us << " gcnp_complete_groups=" << gcnp_complete_groups << " gcnp_complete_reusable_groups=" << gcnp_complete_reusable_groups << " gcnp_complete_grouped_source_uses=" << gcnp_complete_grouped_source_uses << " gcnp_complete_shared_source_uses=" << gcnp_complete_shared_source_uses << " gcnp_complete_external_memo_hits=" << gcnp_complete_external_memo_hits << " speculative_gcnp_tasks=" << speculative_gcnp_tasks << " speculative_gcnp_completed=" << speculative_gcnp_completed << " speculative_gcnp_cancelled=" << speculative_gcnp_cancelled << " speculative_gcnp_us_sum=" << speculative_gcnp_us << " speculative_pattern_memo_stores=" << speculative_pattern_memo_stores << " speculative_pattern_memo_hits=" << speculative_pattern_memo_hits << " coverage_filter_candidate_mids=" << coverage_filter_candidate_mids << " coverage_filter_signature_rejections=" << coverage_filter_signature_rejections << " coverage_filter_subset_tests=" << coverage_filter_subset_tests << " coverage_filter_skipped_mids=" << coverage_filter_skipped_mids << " coverage_filter_skipped_rhs_pairs=" << coverage_filter_skipped_rhs_pairs << " pre_route_coverage_candidate_mids=" << pre_route_coverage_candidate_mids << " pre_route_coverage_signature_rejections=" << pre_route_coverage_signature_rejections << " pre_route_coverage_subset_tests=" << pre_route_coverage_subset_tests << " pre_route_coverage_skipped_mids=" << pre_route_coverage_skipped_mids << " pre_route_coverage_skipped_rhs_pairs=" << pre_route_coverage_skipped_rhs_pairs << " pre_route_coverage_input_left_pairs=" << pre_route_coverage_input_left_pairs << " pre_route_coverage_output_left_pairs=" << pre_route_coverage_output_left_pairs << " join_batch_count=" << join_batch_count << " join_batch_children=" << join_batch_children << " join_batch_child_events=" << join_batch_child_events << " join_batch_publish_pairs=" << join_batch_publish_pairs << " join_batch_wall_us_sum=" << join_batch_wall_us << " emit_merge_us_sum=" << emit_merge_us << " demand_batches_started=" << demand_batches_started << " demand_batch_emits=" << demand_batch_emits << " demand_compose_calls=" << demand_compose_calls << " demand_compose_us_sum=" << demand_compose_us << " matrix_join_updates=" << matrix_join_updates << " matrix_join_result_pairs=" << matrix_join_result_pairs << " matrix_join_us_sum=" << matrix_join_us << " global_rhs_signature_messages=" << global_rhs_signature_messages << " global_rhs_signature_mids=" << global_rhs_signature_mids << " global_signature_left_pairs_before=" << global_signature_left_pairs_before << " global_signature_left_pairs_after=" << global_signature_left_pairs_after << " global_signature_left_pairs_filtered=" << global_signature_left_pairs_filtered << " range_coverage_pairs=" << range_coverage_pairs << " rsfm_selected_left_pairs=" << rsfm_selected_left_pairs << " rsfm_new_left_pairs=" << rsfm_new_left_pairs << " rsfm_replay_left_pairs=" << rsfm_replay_left_pairs << " rsfm_right_pairs=" << rsfm_right_pairs << " rsfm_full_broadcast_pairs=" << rsfm_full_broadcast_pairs << " rsfm_routed_messages=" << rsfm_routed_messages << " rsfm_signature_activations=" << rsfm_signature_activations << " rsfm_empty_left_dispatches=" << rsfm_empty_left_dispatches << " rsfm_resident_rhs_pairs_avoided=" << rsfm_resident_rhs_pairs_avoided << " resident_rhs_expected_rows=" << resident_rhs_expected_rows << " resident_rhs_actual_rows=" << resident_rhs_actual_rows << " resident_rhs_expected_pairs=" << resident_rhs_expected_pairs << " resident_rhs_actual_pairs=" << resident_rhs_actual_pairs << " resident_rhs_mismatched_shards=" << resident_rhs_mismatched_shards << " peer_wire_bytes=" << peer_wire_bytes << " peer_base_wire_bytes=" << peer_base_wire_bytes << " peer_row_wire_bytes=" << peer_row_wire_bytes << " peer_demand_wire_bytes=" << peer_demand_wire_bytes << " peer_signature_wire_bytes=" << peer_signature_wire_bytes << " peer_rhs_wire_bytes=" << peer_rhs_wire_bytes << " peer_lhs_wire_bytes=" << peer_lhs_wire_bytes << " peer_matrix_result_wire_bytes=" << peer_matrix_result_wire_bytes << " peer_other_wire_bytes=" << peer_other_wire_bytes << " final_fetch_wire_bytes=" << fetch_bytes << " query_communication_bytes_total=" << (peer_wire_bytes + fetch_bytes) << endl;
        cout << "[factorized-result] messages=" << factorized_result_messages
             << " shared_blocks=" << factorized_result_shared_blocks
             << " blocks_after_coalesce="
             << factorized_result_blocks_after_coalesce
             << " source_uses=" << factorized_result_source_uses
             << " coalesce_us=" << factorized_result_coalesce_us
             << " expand_us=" << factorized_result_expand_us
             << " union_us=" << factorized_result_union_us
             << " encode_us=" << factorized_result_encode_us
             << " decode_us=" << factorized_result_decode_us
             << " dictionary_merges=" << factorized_result_dictionary_merges
             << " wire_bytes_saved=" << factorized_result_wire_bytes_saved
             << endl;
        cout << "[factorized-source-set] blocks_before="
             << factorized_source_set_blocks_before
             << " blocks_after=" << factorized_source_set_blocks_after
             << " reused_groups=" << factorized_source_set_reused_groups
             << " blocks_merged=" << factorized_source_set_blocks_merged
             << " source_union_ops_avoided="
             << factorized_source_union_operations_avoided
             << " group_us=" << factorized_source_set_group_us
             << " destination_union_us=" << factorized_source_set_union_us
             << endl;
        cout << "[incremental-panel-wait] blocks=" << panel_wait_blocks
             << " left_input_batches=" << panel_left_input_batches
             << " all_left_panels_sealed_elapsed_us="
             << all_left_panels_sealed_elapsed_us
             << " left_panel_wait_us_sum=" << left_panel_wait_us
             << " left_panel_wait_us_max=" << left_panel_wait_us_max
             << " rhs_ready_before_left_blocks=" << rhs_ready_before_left_blocks
             << " rhs_ready_wait_for_left_us_sum=" << rhs_ready_wait_for_left_us
             << " rhs_ready_wait_for_left_us_max=" << rhs_ready_wait_for_left_us_max
             << " left_sealed_before_rhs_blocks=" << left_sealed_before_rhs_blocks
             << " left_wait_for_rhs_us_sum=" << left_wait_for_rhs_us
             << " left_wait_for_rhs_us_max=" << left_wait_for_rhs_us_max << endl;
        cout << "[owner-input-views]";
        // The flag is recorded separately from query arithmetic counters.
        for (size_t i = 0; i < owner_view_fields.size(); ++i)
            cout << ' ' << owner_view_fields[i] << '=' << owner_view_counts[i];
        cout << endl;
        uint64_t max_worker_wall_us = 0, sum_worker_compute_wall_us = 0;
        cout << "[transport-owned-buffers] enabled=" << Message::OwnedBuffersEnabled() << endl;
        for (size_t worker = 0; worker < bounded_owner_times.size(); ++worker) {
            cout << "[bounded-owner-profile] worker=" << worker;
            for (size_t i = 0; i < bounded_owner_fields.size(); ++i)
                cout << ' ' << bounded_owner_fields[i] << '=' << bounded_owner_times[worker][i];
            cout << endl;
        }
        uint64_t sum_worker_communication_e2e_us = 0;
        uint64_t max_worker_communication_e2e_us = 0, sum_worker_messages = 0;
        for (size_t worker_index = 0; worker_index < worker_timings.size(); ++worker_index) {
            const auto& timing = worker_timings[worker_index];
            max_worker_wall_us = max(max_worker_wall_us, timing.wall_us);
            sum_worker_compute_wall_us += timing.compute_wall_us;
            sum_worker_communication_e2e_us += timing.communication_e2e_us;
            max_worker_communication_e2e_us = max(
                max_worker_communication_e2e_us, timing.communication_e2e_us);
            sum_worker_messages += timing.messages;
            cout << "[incremental-worker-metrics] worker=" << worker_index
                 << " balance_join_us=" << timing.balance_join_us
                 << " balance_planner_us=" << timing.balance_planner_us
                 << " balance_reduce_us=" << timing.balance_reduce_us
                 << " balance_dispatch_us=" << timing.balance_dispatch_us
                 << " balance_coverage_us=" << timing.balance_coverage_us
                 << " balance_gather_us=" << timing.balance_gather_us
                 << " balance_input_route_us=" << timing.balance_input_route_us
                 << " balance_open_closure_us=" << timing.balance_open_closure_us
                 << " balance_open_closure_rows=" << timing.balance_open_closure_rows
                 << " balance_open_closure_seeds=" << timing.balance_open_closure_seeds
                 << " balance_input_merge_us=" << timing.balance_input_merge_us
                 << " balance_rhs_select_us=" << timing.balance_rhs_select_us
                 << " balance_rhs_pack_us=" << timing.balance_rhs_pack_us
                 << " balance_rhs_queue_us=" << timing.balance_rhs_queue_us
                 << " balance_rhs_encode_us=" << timing.balance_rhs_encode_us
                 << " balance_rhs_wrap_us=" << timing.balance_rhs_wrap_us
                 << " balance_rhs_raw_bytes=" << timing.balance_rhs_raw_bytes
                 << " balance_rhs_wire_bytes=" << timing.balance_rhs_wire_bytes
                 << " balance_rhs_examined_rows=" << timing.balance_rhs_examined_rows
                 << " balance_rhs_requests=" << timing.balance_rhs_requests
                 << " balance_rhs_requested_keys=" << timing.balance_rhs_requested_keys
                 << " balance_rhs_keys_avoided=" << timing.balance_rhs_keys_avoided
                 << " balance_remote_tasks=" << timing.balance_remote_tasks
                 << " balance_local_tasks=" << timing.balance_local_tasks
                 << " result_commit_us=" << timing.result_commit_us
                 << " result_queue_wait_us=" << timing.result_queue_wait_us
                 << " result_restore_us=" << timing.result_restore_us
                 << " result_restore_max_lanes=" << timing.result_restore_max_lanes
                 << " query_wall_us=" << timing.wall_us
                 << " compute_wall_us=" << timing.compute_wall_us
                 << " communication_wall_us=" << timing.communication_e2e_us
                 << " communication_e2e_union_us=" << timing.communication_e2e_us
                 << " communication_transport_union_us=" << timing.communication_transport_us
                 << " peer_send_bytes=" << timing.bytes
                 << " peer_send_messages=" << timing.messages
                 << " scheduler_shards=" << timing.scheduler_shards
                 << " scheduler_work_submitted=" << timing.scheduler_submitted_work
                 << " scheduler_work_completed=" << timing.scheduler_completed_work
                 << " scheduler_work_executed=" << timing.scheduler_executed_work << endl;
        }
        cout << "[incremental-query-summary] total_ms=" << total_ms
             << " result_pairs=" << final_rows.pairs
             << " peer_wire_bytes_total=" << peer_wire_bytes
             << " peer_wire_messages_total=" << sum_worker_messages
             << " worker_wall_max_us=" << max_worker_wall_us
             << " worker_compute_wall_us_sum=" << sum_worker_compute_wall_us
             << " worker_communication_e2e_union_us_sum=" << sum_worker_communication_e2e_us
             << " worker_communication_e2e_union_us_max=" << max_worker_communication_e2e_us
             << " final_fetch_wire_bytes=" << fetch_bytes
             << " query_communication_bytes_total=" << (peer_wire_bytes + fetch_bytes) << endl;
    } catch (const exception& ex) { cerr << "incremental coordinator failed: " << ex.what() << endl; return 1; }
    return 0;
}
