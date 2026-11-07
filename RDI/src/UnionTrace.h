#pragma once

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>
#include "AlgorithmTiming.h"

// Actual set unions in nested-count and macro-automaton execution paths.
// Kept separate from BLPC_STATS, which measures a different Count_state path.
namespace UnionTrace {
inline bool enabled() {
    static const bool value = [] {
        const char* flag = std::getenv("RDI_TRACE_UNIONS");
        return flag != nullptr && std::string(flag) == "1";
    }();
    return value;
}
inline std::string csv(const std::string& value) {
    std::string result = "\"";
    for (char c : value) {
        if (c == '"') result += "\"\"";
        else result += c;
    }
    return result + '"';
}
inline void record(const std::string& operation, const std::string& context,
                   uint64_t left_pairs, uint64_t right_pairs,
                   uint64_t added_pairs, double union_ms) {
    if (!enabled()) return;
    AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
    static std::ofstream out("union_stage_detail.csv", std::ios::app);
    static uint64_t stage = 0;
    if (!out) {
        std::cerr << "cannot open union_stage_detail.csv" << std::endl;
        std::abort();
    }
    if (out.tellp() == 0) {
        out << "stage_id,operation,context,left_pairs,right_pairs,output_pairs,"
               "added_pairs,overlap_pairs,union_time_ms\n";
    }
    const uint64_t output_pairs = left_pairs + added_pairs;
    const uint64_t overlap_pairs = right_pairs - added_pairs;
    out << ++stage << ',' << csv(operation) << ',' << csv(context) << ','
        << left_pairs << ',' << right_pairs << ',' << output_pairs << ','
        << added_pairs << ',' << overlap_pairs << ',' << union_ms << '\n';
    out.flush();
    std::cout << "UNION_STAGE stage_id=" << stage << " operation=" << operation
              << " context=" << context << " left_pairs=" << left_pairs
              << " right_pairs=" << right_pairs << " output_pairs=" << output_pairs
              << " added_pairs=" << added_pairs << " overlap_pairs=" << overlap_pairs
              << " union_time_ms=" << union_ms << std::endl;
}
}
