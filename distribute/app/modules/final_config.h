#pragma once
#include <cstdlib>
#include <stdexcept>
#include <string>

namespace rpq_final {
inline constexpr int kBalanceMode = 5;
inline constexpr unsigned kJoinThreads = 1;
inline constexpr unsigned kCoverageThreads = 1;
inline constexpr unsigned kMessageThreads = 1;
inline constexpr unsigned kCompressionThreads = 1;
inline constexpr unsigned kFinalThreads = 1;
inline constexpr int kBicliqueWireMode = 2;

// Old launchers may still export matching settings. A conflicting legacy
// setting must fail explicitly, not silently select a different experiment.
inline void ValidateEnvironment() {
    struct Setting { const char* name; const char* expected; };
    const Setting settings[] = {
        {"INCREMENTAL_STAGE_BALANCE", "5"},
        {"INCREMENTAL_BALANCE_BUCKETS", "p"},
        {"INCREMENTAL_COMPUTE_THREADS", "1"},
        {"GCNP_THREADS", "1"},
        {"GCNP_COVERAGE_THREADS", "1"},
        {"MESSAGE_MATRIX_THREADS", "1"},
        {"INCREMENTAL_FINAL_THREADS", "1"},
        {"INCREMENTAL_BICLIQUE_WIRE", "2"},
        {"INCREMENTAL_BICLIQUE_THREADS", "1"},
        {"INCREMENTAL_OWNER_INPUT_VIEWS", "1"},
        {"INCREMENTAL_HELPER_CLOSURE_RESTORE", "1"},
        {"INCREMENTAL_NFA_LIVE_STATES", "1"},
        {"INCREMENTAL_RHS_BORROW_ROWS", "1"},
        {"SIMPLE_RPC_OWNED_BUFFERS", "1"},
        {"SIMPLE_RPC_COMPRESS", "0"},
        {"INCREMENTAL_RHS_PACKET_ZLIB", "0"},
        {"INCREMENTAL_DEFER_RHS_MERGE", "0"},
        {"INCREMENTAL_ASYNC_DIRECT_ACCEPT", "0"},
        {"INCREMENTAL_ENABLE_SPECULATIVE_GCNP", "0"},
        {"INCREMENTAL_GROUP_TASKS", "0"},
        {"INCREMENTAL_RHS_REPLY_PRIORITY", "0"},
        {"INCREMENTAL_RHS_TRANSPORT_PRIORITY", "0"},
        {"INCREMENTAL_P_BUCKET_FAST_ROUTE", "0"},
        {"INCREMENTAL_RECEIVE_VIEW", "0"},
        {"INCREMENTAL_RHS_ROW_CACHE", "0"}
    };
    for (const auto& setting : settings) {
        const char* value = std::getenv(setting.name);
        if (value != nullptr && *value != '\0' && std::string(value) != setting.expected)
            throw std::invalid_argument(std::string("retired runtime override: ") +
                setting.name + "=" + value + "; final implementation requires " + setting.expected);
    }
}
}
