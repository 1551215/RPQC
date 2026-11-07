#pragma once

#include <array>
#include <chrono>
#include <cstddef>

enum class AlgorithmPhase { AC, AT, GC, MP, Diagnostics, Unaccounted, Inherit };

struct AlgorithmTiming {
	using Clock = std::chrono::steady_clock;
	std::array<double, 6> milliseconds{};
	std::array<size_t, 6> entries{};
	AlgorithmPhase current = AlgorithmPhase::Unaccounted;
	Clock::time_point changed_at;
	bool running = false;
};

// Only the query thread owns phase scopes. Parallel join workers are measured
// by the enclosing wall-clock scope, rather than by adding their CPU times.
inline thread_local AlgorithmTiming g_algorithm_timing;

inline void ResetAlgorithmTiming(AlgorithmTiming::Clock::time_point start)
{
	g_algorithm_timing = AlgorithmTiming{};
	g_algorithm_timing.changed_at = start;
	g_algorithm_timing.running = true;
}

inline void ChargeAlgorithmPhase(AlgorithmTiming::Clock::time_point now)
{
	auto& timing = g_algorithm_timing;
	timing.milliseconds[static_cast<size_t>(timing.current)] +=
		std::chrono::duration<double, std::milli>(now - timing.changed_at).count();
	timing.changed_at = now;
}

inline void FinishAlgorithmTiming(AlgorithmTiming::Clock::time_point end)
{
	if (!g_algorithm_timing.running) return;
	ChargeAlgorithmPhase(end);
	g_algorithm_timing.running = false;
}

// Switching phases pauses the parent; returning resumes it. Recursive calls
// therefore contribute exactly once, including AT invoked from a counted MP.
class AlgorithmPhaseTimer {
public:
	explicit AlgorithmPhaseTimer(AlgorithmPhase phase) : phase_(phase) { start(); }
	~AlgorithmPhaseTimer() { stop(); }
	AlgorithmPhaseTimer(const AlgorithmPhaseTimer&) = delete;
	AlgorithmPhaseTimer& operator=(const AlgorithmPhaseTimer&) = delete;
	void start() {
		if (active_ || phase_ == AlgorithmPhase::Inherit || !g_algorithm_timing.running) return;
		ChargeAlgorithmPhase(AlgorithmTiming::Clock::now());
		previous_ = g_algorithm_timing.current;
		g_algorithm_timing.current = phase_;
		++g_algorithm_timing.entries[static_cast<size_t>(phase_)];
		active_ = true;
	}
	void stop() {
		if (!active_) return;
		if (g_algorithm_timing.running) {
			ChargeAlgorithmPhase(AlgorithmTiming::Clock::now());
			g_algorithm_timing.current = previous_;
		}
		active_ = false;
	}
private:
	AlgorithmPhase phase_;
	AlgorithmPhase previous_ = AlgorithmPhase::Unaccounted;
	bool active_ = false;
};
