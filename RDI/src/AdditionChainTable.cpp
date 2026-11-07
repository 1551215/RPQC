#include "AdditionChainTable.h"

#include <iostream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>

namespace {

constexpr int kPrecomputedAdditionPlanLimit = 100;
std::vector<std::vector<AdditionPlanStep>> g_addition_plans(kPrecomputedAdditionPlanLimit + 1);
bool g_addition_plans_loaded = false;

std::pair<int, int> BinaryBasisSplit(int target)
{
	if (target <= 1) {
		return {0, 0};
	}
	int right = 1;
	while ((static_cast<long long>(right) << 1) < target) {
		right <<= 1;
	}
	return {target - right, right};
}

std::vector<AdditionPlanStep> BuildBinaryBasisPlan(int target)
{
	std::vector<AdditionPlanStep> plan;
	if (target <= 1) {
		return plan;
	}

	// First materialize every binary basis relation up to the highest set bit.
	// The second phase combines set bits from small to large, so every compose
	// has the smaller exponent on the left.
	int highest_power = 1;
	while (highest_power <= target / 2) {
		highest_power *= 2;
	}
	for (int power = 2; power <= highest_power; power *= 2) {
		plan.push_back({power, power / 2, power / 2});
	}

	int accumulated = 0;
	for (int basis = 1; basis <= highest_power; basis *= 2) {
		if ((target & basis) == 0) {
			continue;
		}
		if (accumulated == 0) {
			accumulated = basis;
			continue;
		}
		const int smaller = accumulated;
		const int larger = basis;
		plan.push_back({accumulated + basis, smaller, larger});
		accumulated += basis;
	}
	return plan;
}

} // namespace

void LoadAdditionChainTable(const std::string& path)
{
	(void)path;
	for (int target = 2; target <= kPrecomputedAdditionPlanLimit; ++target) {
		g_addition_plans[target] = BuildBinaryBasisPlan(target);
	}
	g_addition_plans_loaded = true;
	std::cerr << "Using in-code binary-basis plans; addition-chain files are disabled"
		<< std::endl;
}

const std::vector<AdditionPlanStep>& GetAdditionPlan(int target)
{
	static std::unordered_map<int, std::vector<AdditionPlanStep>> fallback_cache;
	if (target <= kPrecomputedAdditionPlanLimit) {
		if (!g_addition_plans_loaded) {
			LoadAdditionChainTable("");
		}
		return g_addition_plans[target];
	}

	auto it = fallback_cache.find(target);
	if (it != fallback_cache.end()) {
		return it->second;
	}
	auto inserted = fallback_cache.emplace(target, BuildBinaryBasisPlan(target));
	return inserted.first->second;
}

std::pair<int, int> GetAdditionSplit(int target)
{
	const auto& plan = GetAdditionPlan(target);
	if (plan.empty()) {
		return {0, 0};
	}
	const auto& last = plan.back();
	return {last.left, last.right};
}

std::unordered_map<int, int> BuildAdditionPlanUseCounts(int target)
{
	std::unordered_map<int, int> use_counts;
	for (const auto& step : GetAdditionPlan(target)) {
		use_counts[step.left]++;
		use_counts[step.right]++;
	}
	return use_counts;
}

const std::vector<AdditionPlanStep>& GetBinaryBasisPlan(int target)
{
	static std::unordered_map<int, std::vector<AdditionPlanStep>> cache;
	auto it = cache.find(target);
	if (it != cache.end()) {
		return it->second;
	}
	auto inserted = cache.emplace(target, BuildBinaryBasisPlan(target));
	return inserted.first->second;
}

std::pair<int, int> GetBinaryBasisSplit(int target)
{
	return BinaryBasisSplit(target);
}

std::unordered_map<int, int> BuildBinaryBasisPlanUseCounts(int target)
{
	std::unordered_map<int, int> use_counts;
	for (const auto& step : GetBinaryBasisPlan(target)) {
		use_counts[step.left]++;
		use_counts[step.right]++;
	}
	return use_counts;
}
