#pragma once

#include <chrono>
#include <cstddef>
#include <string>
#include "AlgorithmTiming.h"

void RecordCompositionMeasurement(const std::string& operation,
	double elapsed_ms, double planning_ms, double merge_ms,
	size_t naive_rows, size_t actual_rows);
void RecordRelationUnionMeasurement(const std::string& operation, double merge_ms);

enum class QueryTimingPhase {
	BasicNfa, IncrementalCache, FormatConversion, ResultMaterialization,
	RelationStats, Other
};

// Snapshot includes already-recorded compose/union work. Nested scopes charge
// only their exclusive wall time, including recursive counted transitions.
double QueryAccountedTimeMs();
void RecordQueryPhaseTime(QueryTimingPhase phase, double elapsed_ms);

class QueryPhaseTimer {
public:
	explicit QueryPhaseTimer(QueryTimingPhase phase)
		: phase_(phase), accounted_(QueryAccountedTimeMs()),
		  algorithm_phase_(phase == QueryTimingPhase::BasicNfa ? AlgorithmPhase::AT :
			  phase == QueryTimingPhase::RelationStats ? AlgorithmPhase::Diagnostics :
			  AlgorithmPhase::Inherit) {}
	~QueryPhaseTimer() { stop(); }
	QueryPhaseTimer(const QueryPhaseTimer&) = delete;
	QueryPhaseTimer& operator=(const QueryPhaseTimer&) = delete;
	void stop() {
		if (stopped_) return;
		const double elapsed = std::chrono::duration<double, std::milli>(
			std::chrono::steady_clock::now() - started_).count();
		RecordQueryPhaseTime(phase_, elapsed - (QueryAccountedTimeMs() - accounted_));
		algorithm_phase_.stop();
		stopped_ = true;
	}
private:
	QueryTimingPhase phase_;
	double accounted_;
	std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
	bool stopped_ = false;
	AlgorithmPhaseTimer algorithm_phase_;
};

// One timer per execution phase, never one timer per bitmap or endpoint.
class OperationStopwatch {
public:
	void start() { started_ = std::chrono::steady_clock::now(); }
	double elapsed_ms() const {
		return std::chrono::duration<double, std::milli>(
			std::chrono::steady_clock::now() - started_).count();
	}
private:
	std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
};

class RelationUnionTimer {
public:
	explicit RelationUnionTimer(const char* operation) : operation_(operation) {}
	~RelationUnionTimer() { stop(); }
	void stop() {
		if (!stopped_) {
			elapsed_ms_ = timer_.elapsed_ms();
			stopped_ = true;
			RecordRelationUnionMeasurement(operation_, elapsed_ms_);
		}
	}
	double elapsed_ms() const { return elapsed_ms_; }
private:
	const char* operation_;
	OperationStopwatch timer_;
	bool stopped_ = false;
	double elapsed_ms_ = 0.0;
};
