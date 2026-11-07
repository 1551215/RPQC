#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include "NFA_state.h"

class Regex;

class NestedCountEvaluator {
public:
	using VertexSet = std::unordered_set<size_t>;
	using Relation = RoaringRelation;

	void evaluate(const std::shared_ptr<Regex>& regex,
		const VertexSet& sources, Relation& result);

private:
	struct RowCache {
		Relation rows;
		VertexSet computed_sources;
	};
	struct StageKey {
		const Regex* body = nullptr;
		int length = 0;
		bool operator==(const StageKey& other) const {
			return body == other.body && length == other.length;
		}
	};
	struct StageKeyHash {
		size_t operator()(const StageKey& key) const;
	};
	struct BoundedStageCache {
		RowCache power;
		RowCache closure;
	};

	std::unordered_map<const Regex*, RowCache> node_cache_;
	std::unordered_map<StageKey, RowCache, StageKeyHash> exact_cache_;
	std::unordered_map<StageKey, BoundedStageCache, StageKeyHash> bounded_cache_;

	void compute_node(const std::shared_ptr<Regex>& regex,
		const VertexSet& sources, Relation& result);
	void evaluate_exact(const std::shared_ptr<Regex>& body, int length,
		const VertexSet& sources, Relation& result);
	void evaluate_bounded_stage(const std::shared_ptr<Regex>& body, int length,
		const VertexSet& sources, Relation& power, Relation& closure);
	void evaluate_range(const std::shared_ptr<Regex>& body, int min_rep,
		int max_rep, const VertexSet& sources, Relation& result);
	void evaluate_open_range(const std::shared_ptr<Regex>& body, int min_rep,
		const VertexSet& sources, Relation& result);

	static void copy_cached_rows(const RowCache& cache,
		const VertexSet& sources, Relation& result);
	static VertexSet collect_destinations(const Relation& relation);
	static Relation identity(const VertexSet& sources);
	static Relation compose(const Relation& left, const Relation& right);
	static void merge(const Relation& source, Relation& destination,
		const std::string& context);
	static std::pair<int, int> binary_split(int length);
};
