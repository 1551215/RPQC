#pragma once

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

struct AdditionPlanStep {
	int value;
	int left;
	int right;
};

void LoadAdditionChainTable(const std::string& path);
const std::vector<AdditionPlanStep>& GetAdditionPlan(int target);
std::pair<int, int> GetAdditionSplit(int target);
std::unordered_map<int, int> BuildAdditionPlanUseCounts(int target);

// Binary-basis evaluation first materializes R1, R2, R4, ... and then
// combines the set bits with the smaller accumulated exponent on the left.
const std::vector<AdditionPlanStep>& GetBinaryBasisPlan(int target);
std::pair<int, int> GetBinaryBasisSplit(int target);
std::unordered_map<int, int> BuildBinaryBasisPlanUseCounts(int target);
