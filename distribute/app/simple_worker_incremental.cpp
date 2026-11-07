#define INCREMENTAL_MIDDLE_PARTITION_MODE 1
#define INCREMENTAL_SYNCHRONOUS_MATRIX_MODE 1
#define INCREMENTAL_DISABLE_RESIDENT_MATRIX_MACRO 1
#include "modules/incremental_worker_runtime.inc"
#include "modules/distributed_line_log.h"

static Message HandleIncrementalWorkerRequest(IncrementalWorkerState& state, const Message& request) {
    // Transport health is not query progress. Surface lost asynchronous work
    // instead of allowing the coordinator to poll an incomplete query forever.
    if (request.type != MessageType::HEALTH_CHECK &&
        request.type != MessageType::INCREMENTAL_INIT_QUERY &&
        state.peer_failed.load(memory_order_acquire)) {
        lock_guard<mutex> lk(state.peer_failure_mu);
        if (state.peer_failed.load(memory_order_relaxed)) {
            Message out(MessageType::RESPONSE_ERROR);
            out.AddField("error", state.peer_failure);
            return out;
        }
    }
    switch (request.type) {
        case MessageType::HEALTH_CHECK: { Message out(MessageType::RESPONSE_OK); out.AddField("status", "healthy"); return out; }
        case MessageType::INCREMENTAL_INIT_QUERY: return HandleIncrementalInitQuery(state, request);
        case MessageType::INCREMENTAL_START_ROOT: return HandleIncrementalStartRoot(state, request);
        case MessageType::INCREMENTAL_REQUEST_ROW: return HandleIncrementalRequestRow(state, request);
        case MessageType::INCREMENTAL_REQUEST_ROW_BATCH: return HandleIncrementalRequestRowBatch(state, request);
        case MessageType::INCREMENTAL_ROW_DELTA: return HandleIncrementalRowDelta(state, request);
        case MessageType::INCREMENTAL_ROW_DELTA_BATCH: return HandleIncrementalRowDeltaBatch(state, request);
        case MessageType::INCREMENTAL_ROW_COMPLETE: return HandleIncrementalRowComplete(state, request);
        case MessageType::INCREMENTAL_ROW_COMPLETE_BATCH: return HandleIncrementalRowCompleteBatch(state, request);
        case MessageType::INCREMENTAL_ROW_STREAM_BATCH: return HandleIncrementalRowStreamBatch(state, request);
        case MessageType::INCREMENTAL_DEMAND_BATCH_REQUEST: return HandleIncrementalDemandBatchRequest(state, request);
        case MessageType::INCREMENTAL_DEMAND_BATCH_STREAM: return HandleIncrementalDemandBatchStream(state, request);
        case MessageType::INCREMENTAL_MATRIX_JOIN_UPDATE: return HandleIncrementalMatrixJoinUpdate(state, request);
        case MessageType::INCREMENTAL_MATRIX_JOIN_RESULT: return HandleIncrementalMatrixJoinResult(state, request);
        case MessageType::INCREMENTAL_MATRIX_RESIDENT_CONTINUE: return HandleIncrementalMatrixResidentContinueRequest(state, request);
        case MessageType::INCREMENTAL_BASE_EXPAND: return HandleIncrementalBaseExpand(state, request);
        case MessageType::INCREMENTAL_BASE_EXPAND_RESULT: return HandleIncrementalBaseResult(state, request);
        case MessageType::INCREMENTAL_BASE_EXPAND_BATCH: return HandleIncrementalBaseExpandBatch(state, request);
        case MessageType::INCREMENTAL_BASE_EXPAND_RESULT_BATCH: return HandleIncrementalBaseResultBatch(state, request);
        case MessageType::INCREMENTAL_QUERY_STATUS: return HandleIncrementalStatus(state, request);
        case MessageType::INCREMENTAL_BALANCE_CONTROL: return HandleIncrementalBalanceControl(state, request);
        case MessageType::INCREMENTAL_FETCH_RESULTS: return HandleIncrementalFetchResults(state, request);
        case MessageType::STEP_BASIC: return HandleIncrementalOpenControl(state, request);
        case MessageType::EXECUTE_OPEN_SCC: return HandleIncrementalInstallOpenPlan(state, request);
        default: { Message out(MessageType::RESPONSE_ERROR); out.AddField("error", "unsupported incremental rpc"); return out; }
    }
}

int main(int argc, char* argv[]) {
    ScopedDistributedLineLogging log_buffer;
    try { rpq_final::ValidateEnvironment(); }
    catch (const exception& error) {
        cerr << "Configuration error: " << error.what() << endl;
        return 2;
    }
    int partition_id = 0, partition_count = 4, port = 50051; string partition_dir = "partitions/partition_0";
    for (int i = 1; i < argc; ++i) { string arg = argv[i]; if (arg == "-p" && i + 1 < argc) partition_id = stoi(argv[++i]); else if (arg == "-n" && i + 1 < argc) partition_count = stoi(argv[++i]); else if (arg == "--port" && i + 1 < argc) port = stoi(argv[++i]); else if (arg == "-d" && i + 1 < argc) partition_dir = argv[++i]; }
    IncrementalWorkerState state(partition_id, partition_count); state.partition_root = PartitionRootFromPartitionDir(partition_dir);
    if (!state.partition.LoadFromDirectory(partition_dir)) return 1;
    string router_error; if (TryLoadRouterFromRoot(state.partition_root, state.router, router_error)) state.router_configured = true;
    TcpServer server(port);
    server.SetHandler([&state, &log_buffer](const Message& request) {
        if (request.type == MessageType::INCREMENTAL_INIT_QUERY) log_buffer.BeginQuery();
        Message response = HandleIncrementalWorkerRequest(state, request);
        if (request.type == MessageType::INCREMENTAL_INIT_QUERY ||
            request.type == MessageType::INCREMENTAL_FETCH_RESULTS ||
            (request.type == MessageType::INCREMENTAL_QUERY_STATUS &&
                request.GetFieldInt("fast_poll") == 0) ||
            response.type == MessageType::RESPONSE_ERROR)
            log_buffer.Checkpoint();
        return response;
    });
    if (!server.Start()) { cerr << "Failed to start incremental worker" << endl; return 1; }
    cout << "[incremental-demand] worker ready pid=" << partition_id << " port=" << port << endl;
    cout << "[compute-config] join_threads=" << rpq_final::kJoinThreads
         << " coverage_threads=" << rpq_final::kCoverageThreads << endl;
    while (true) sleep(1);
}
