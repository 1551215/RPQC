#include "modules/incremental_common.inc"

int main(int argc, char* argv[]) {
    string workers_text;
    int query_id = 1;
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "-w" && i + 1 < argc) workers_text = argv[++i];
        else if (arg == "-q" && i + 1 < argc) query_id = stoi(argv[++i]);
    }
    if (workers_text.empty()) { cerr << "workers are required via -w" << endl; return 1; }
    auto workers = ParseWorkers(workers_text);
    for (size_t i = 0; i < workers.size(); ++i) {
        Message request(MessageType::INCREMENTAL_QUERY_STATUS);
        request.AddField("query_id", query_id);
        Message response = workers[i].client->SendRequest(request);
        if (response.type != MessageType::RESPONSE_OK) { cerr << "worker=" << i << " error=" << response.GetField("error") << endl; continue; }
        cout << "worker=" << i
             << " roots_initialized=" << response.GetFieldInt("roots_initialized")
             << " roots_done=" << response.GetFieldUInt64("roots_done")
             << " roots_total=" << response.GetFieldUInt64("roots_total")
             << " rows_started=" << response.GetFieldUInt64("rows_started")
             << " cache_hits=" << response.GetFieldUInt64("row_cache_hits")
             << " remote_requests=" << response.GetFieldUInt64("remote_requests")
             << " request_rpcs=" << response.GetFieldUInt64("remote_request_rpcs")
             << " complete_rpcs=" << response.GetFieldUInt64("remote_complete_rpcs")
             << " stream_rpcs=" << response.GetFieldUInt64("remote_stream_rpcs")
             << " delta_batches=" << response.GetFieldUInt64("delta_count")
             << " base_us=" << response.GetFieldUInt64("base_expand_us")
             << " gcnp_batches=" << response.GetFieldUInt64("gcnp_batches")
             << " gcnp_us=" << response.GetFieldUInt64("gcnp_wall_us")
             << " gcnp_mids=" << response.GetFieldUInt64("gcnp_input_mids")
             << " gcnp_planned_mids=" << response.GetFieldUInt64("gcnp_planned_mids")
             << " gcnp_bypassed_mids=" << response.GetFieldUInt64("gcnp_bypassed_mids")
             << " join_batches=" << response.GetFieldUInt64("join_batch_count")
             << " join_children=" << response.GetFieldUInt64("join_batch_children")
             << " join_events=" << response.GetFieldUInt64("join_batch_child_events")
             << " join_us=" << response.GetFieldUInt64("join_batch_wall_us")
             << " emit_us=" << response.GetFieldUInt64("emit_merge_us")
             << " emit_l16=" << response.GetFieldUInt64("emit_pairs_l16")
             << " emit_l32=" << response.GetFieldUInt64("emit_pairs_l32")
             << " gcnp_sources=" << response.GetFieldUInt64("gcnp_source_rows")
             << " gcnp_groups=" << response.GetFieldUInt64("gcnp_groups")
             << " gcnp_reusable_groups=" << response.GetFieldUInt64("gcnp_reusable_groups")
             << " gcnp_cache_hits=" << response.GetFieldUInt64("gcnp_cache_hits")
             << " demand_started=" << response.GetFieldUInt64("demand_batches_started")
             << " demand_open=" << response.GetFieldUInt64("demand_batches_open")
             << " demand_pending_right=" << response.GetFieldUInt64("demand_pending_right")
             << " demand_pending_matrix=" << response.GetFieldUInt64("demand_pending_matrix")
             << " demand_left_done=" << response.GetFieldUInt64("demand_left_done")
             << " demand_emits=" << response.GetFieldUInt64("demand_batch_emits")
             << " demand_pairs=" << response.GetFieldUInt64("demand_batch_pairs")
             << " demand_compose_calls=" << response.GetFieldUInt64("demand_compose_calls")
             << " demand_compose_us=" << response.GetFieldUInt64("demand_compose_us")
             << " matrix_updates=" << response.GetFieldUInt64("matrix_join_updates")
             << " matrix_pairs=" << response.GetFieldUInt64("matrix_join_result_pairs")
             << " matrix_us=" << response.GetFieldUInt64("matrix_join_us")
             << " peer_wire_bytes=" << response.GetFieldUInt64("peer_wire_bytes")
             << " peer_lhs_bytes=" << response.GetFieldUInt64("peer_lhs_wire_bytes")
             << " peer_signature_bytes=" << response.GetFieldUInt64("peer_signature_wire_bytes")
             << " scheduler_shards=" << response.GetFieldUInt64("middle_scheduler_assigned_shards")
             << " scheduler_work_submitted=" << response.GetFieldUInt64("middle_scheduler_submitted_work")
             << " scheduler_work_completed=" << response.GetFieldUInt64("middle_scheduler_completed_work")
             << " scheduler_work_executed=" << response.GetFieldUInt64("middle_scheduler_executed_work")
             << " scheduler_work_outstanding_max=" << response.GetFieldUInt64("middle_scheduler_outstanding_work_max")
             << " done=" << response.GetFieldInt("done")
             << endl;
    }
}
