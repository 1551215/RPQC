#include "NestedCountEvaluator.h"

#include "NFA_state.h"
#include "Regex.h"
#include "graph.h"
#include "UnionTrace.h"
#include "RelationOperationMetrics.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <stdexcept>

using std::dynamic_pointer_cast;
using std::shared_ptr;

namespace {

bool use_naive_bounded_suffix_union() {
	const char* mode = std::getenv("BOUNDED_SUFFIX_MODE");
	return mode != nullptr && std::strcmp(mode, "naive_union") == 0;
}

}  // namespace

size_t NestedCountEvaluator::StageKeyHash::operator()(const StageKey& key) const {
	size_t hash = std::hash<const Regex*>{}(key.body);
	hash ^= std::hash<int>{}(key.length) + 0x9e3779b9U + (hash << 6) + (hash >> 2);
	return hash;
}

void NestedCountEvaluator::copy_cached_rows(const RowCache& cache,
	const VertexSet& sources, Relation& result) {
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	result.clear();
	for (size_t source : sources) {
		auto row = cache.rows.find(source);
		if (row != cache.rows.end() && !row->second.isEmpty()) {
			result.emplace(source, row->second);
		}
	}
}

NestedCountEvaluator::VertexSet NestedCountEvaluator::collect_destinations(
	const Relation& relation) {
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	VertexSet destinations;
	for (const auto& entry : relation) {
		for (uint32_t value : entry.second) destinations.insert(value);
	}
	return destinations;
}

NestedCountEvaluator::Relation NestedCountEvaluator::identity(
	const VertexSet& sources) {
	QueryPhaseTimer measurement(QueryTimingPhase::ResultMaterialization);
	Relation result;
	for (size_t source : sources) result[source].add(static_cast<uint32_t>(source));
	return result;
}

void NestedCountEvaluator::merge(const Relation& source, Relation& destination,
	const std::string& context) {
	const bool trace = UnionTrace::enabled();
	uint64_t left_pairs = 0, right_pairs = 0, added_pairs = 0;
	if (trace) {
		for (const auto& entry : destination) left_pairs += entry.second.cardinality();
	}
	RelationUnionTimer merge_timer("nested_relation_union");
	for (const auto& entry : source) {
		auto& output = destination[entry.first];
		const size_t before = trace ? output.cardinality() : 0;
		output |= entry.second;
		if (trace) {
			right_pairs += entry.second.cardinality();
			added_pairs += output.cardinality() - before;
		}
	}
	merge_timer.stop();
	if (trace) {
		const double elapsed = merge_timer.elapsed_ms();
		UnionTrace::record("nested_relation_union", context,
			left_pairs, right_pairs, added_pairs, elapsed);
	}
}

NestedCountEvaluator::Relation NestedCountEvaluator::compose(
	const Relation& left, const Relation& right) {
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	if (left.empty() || right.empty()) return {};
	return GcnpComposeRoaringRelation(left, right);
}

std::pair<int, int> NestedCountEvaluator::binary_split(int length) {
	int power = 1;
	while (power <= length / 2) power *= 2;
	if (power == length) return {length / 2, length / 2};
	return {length - power, power};
}

void NestedCountEvaluator::evaluate(const shared_ptr<Regex>& regex,
	const VertexSet& sources, Relation& result) {
	AlgorithmPhaseTimer phase(regex && regex->type == Regex::RegexType::RangeRepetition
		? AlgorithmPhase::MP : AlgorithmPhase::AT);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	result.clear();
	if (!regex || sources.empty()) return;
	RowCache& cache = node_cache_[regex.get()];
	VertexSet missing;
	for (size_t source : sources) {
		if (!cache.computed_sources.count(source)) missing.insert(source);
	}
	if (!missing.empty()) {
		Relation fresh;
		compute_node(regex, missing, fresh);
		for (size_t source : missing) {
			cache.computed_sources.insert(source);
			auto row = fresh.find(source);
			if (row != fresh.end() && !row->second.isEmpty()) {
				cache.rows[source] = std::move(row->second);
			}
		}
	}
	copy_cached_rows(cache, sources, result);
}

void NestedCountEvaluator::compute_node(const shared_ptr<Regex>& regex,
	const VertexSet& sources, Relation& result) {
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	result.clear();
	switch (regex->type) {
	case Regex::RegexType::Blank:
		result = identity(sources);
		return;
	case Regex::RegexType::Primitive: {
		QueryPhaseTimer basic_nfa(QueryTimingPhase::BasicNfa);
		auto primitive = dynamic_pointer_cast<Primitives>(regex);
		for (size_t source : sources) {
			auto vertex = Graph::adjacency_map.find(source);
			if (vertex == Graph::adjacency_map.end()) continue;
			auto edges = vertex->second.find(primitive->label);
			if (edges == vertex->second.end()) continue;
			for (size_t dst : edges->second) result[source].add(static_cast<uint32_t>(dst));
		}
		return;
	}
	case Regex::RegexType::Sequence: {
		auto sequence = dynamic_pointer_cast<Sequence>(regex);
		Relation left, right;
		evaluate(sequence->first, sources, left);
		evaluate(sequence->second, collect_destinations(left), right);
		result = compose(left, right);
		return;
	}
	case Regex::RegexType::Choice: {
		auto choice = dynamic_pointer_cast<Choice>(regex);
		Relation left, right;
		evaluate(choice->thisOne, sources, left);
		evaluate(choice->thatOne, sources, right);
		result = std::move(left);
		merge(right, result, "choice:" + choice->toString());
		return;
	}
	case Regex::RegexType::RangeRepetition: {
		auto range = dynamic_pointer_cast<RangeRepetition>(regex);
		evaluate_range(range->internal, range->min, range->max, sources, result);
		return;
	}
	case Regex::RegexType::QuoraRepetition: {
		auto optional = dynamic_pointer_cast<QuoraRepetition>(regex);
		evaluate_range(optional->internal, 0, 1, sources, result);
		return;
	}
	case Regex::RegexType::StarRepetition: {
		auto star = dynamic_pointer_cast<StarRepetition>(regex);
		evaluate_open_range(star->internal, 0, sources, result);
		return;
	}
	case Regex::RegexType::PlusRepetition: {
		auto plus = dynamic_pointer_cast<PlusRepetition>(regex);
		evaluate_open_range(plus->internal, 1, sources, result);
		return;
	}
	default:
		throw std::runtime_error("unsupported regex node in nested count body");
	}
}

void NestedCountEvaluator::evaluate_exact(const shared_ptr<Regex>& body,
	int length, const VertexSet& sources, Relation& result) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	result.clear();
	if (length < 0 || sources.empty()) return;
	RowCache& cache = exact_cache_[StageKey{body.get(), length}];
	VertexSet missing;
	for (size_t source : sources) {
		if (!cache.computed_sources.count(source)) missing.insert(source);
	}
	if (!missing.empty()) {
		Relation fresh;
		if (length == 0) {
			fresh = identity(missing);
		} else if (length == 1) {
			evaluate(body, missing, fresh);
		} else {
			const auto split = binary_split(length);
			Relation left, right;
			evaluate_exact(body, split.first, missing, left);
			evaluate_exact(body, split.second, collect_destinations(left), right);
			fresh = compose(left, right);
		}
		for (size_t source : missing) {
			cache.computed_sources.insert(source);
			auto row = fresh.find(source);
			if (row != fresh.end() && !row->second.isEmpty()) {
				cache.rows[source] = std::move(row->second);
			}
		}
	}
	copy_cached_rows(cache, sources, result);
}

void NestedCountEvaluator::evaluate_bounded_stage(const shared_ptr<Regex>& body,
	int length, const VertexSet& sources, Relation& power, Relation& closure) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	power.clear();
	closure.clear();
	if (length < 0 || sources.empty()) return;
	BoundedStageCache& cache = bounded_cache_[StageKey{body.get(), length}];
	VertexSet missing;
	for (size_t source : sources) {
		if (!cache.power.computed_sources.count(source)) missing.insert(source);
	}
	if (!missing.empty()) {
		Relation fresh_power, fresh_closure;
		if (length == 0) {
			fresh_power = identity(missing);
			fresh_closure = fresh_power;
		} else if (length == 1) {
			evaluate(body, missing, fresh_power);
			fresh_closure = identity(missing);
			merge(fresh_power, fresh_closure, "bounded_length_1");
		} else {
			const auto split = binary_split(length);
			Relation left_power, left_closure, right_power, right_closure;
			evaluate_bounded_stage(body, split.first, missing,
				left_power, left_closure);
			evaluate_bounded_stage(body, split.second,
				collect_destinations(left_power), right_power, right_closure);
			fresh_power = compose(left_power, right_power);
			fresh_closure = std::move(left_closure);
			merge(compose(left_power, right_closure), fresh_closure,
				"bounded_length_" + std::to_string(length));
		}
		for (size_t source : missing) {
			cache.power.computed_sources.insert(source);
			cache.closure.computed_sources.insert(source);
			auto p = fresh_power.find(source);
			if (p != fresh_power.end() && !p->second.isEmpty()) {
				cache.power.rows[source] = std::move(p->second);
			}
			auto s = fresh_closure.find(source);
			if (s != fresh_closure.end() && !s->second.isEmpty()) {
				cache.closure.rows[source] = std::move(s->second);
			}
		}
	}
	copy_cached_rows(cache.power, sources, power);
	copy_cached_rows(cache.closure, sources, closure);
}

void NestedCountEvaluator::evaluate_range(const shared_ptr<Regex>& body,
	int min_rep, int max_rep, const VertexSet& sources, Relation& result) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	result.clear();
	if (min_rep < 0 || (max_rep >= 0 && max_rep < min_rep)) return;
	if (max_rep < 0) {
		evaluate_open_range(body, min_rep, sources, result);
		return;
	}
	if (min_rep == max_rep) {
		evaluate_exact(body, min_rep, sources, result);
		return;
	}
	Relation prefix;
	evaluate_exact(body, min_rep, sources, prefix);
	const VertexSet suffix_sources = collect_destinations(prefix);
	if (use_naive_bounded_suffix_union()) {
		const int max_extra_len = max_rep - min_rep;
		{
		AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
		std::cout << "[NestedCount][naive_union] max_extra_len="
		          << max_extra_len << " requested_sources="
		          << suffix_sources.size() << std::endl;
		}
		Relation suffix = identity(suffix_sources);
		for (int exact_len = 1; exact_len <= max_extra_len; ++exact_len) {
			Relation exact;
			evaluate_exact(body, exact_len, suffix_sources, exact);
			merge(exact, suffix, "naive_suffix_length_" + std::to_string(exact_len));
		}
		result = compose(prefix, suffix);
		return;
	}
	Relation suffix_power, suffix_closure;
	evaluate_bounded_stage(body, max_rep - min_rep,
		suffix_sources, suffix_power, suffix_closure);
	result = compose(prefix, suffix_closure);
}

void NestedCountEvaluator::evaluate_open_range(const shared_ptr<Regex>& body,
	int min_rep, const VertexSet& sources, Relation& result) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	Relation prefix;
	evaluate_exact(body, min_rep, sources, prefix);
	if (prefix.empty()) {
		result.clear();
		return;
	}
	const VertexSet mids = collect_destinations(prefix);
	Relation closure = identity(mids);
	Relation frontier = closure;
	while (!frontier.empty()) {
		Relation body_rows;
		evaluate(body, collect_destinations(frontier), body_rows);
		Relation next = compose(frontier, body_rows);
		Relation delta;
		for (const auto& entry : next) {
			auto known = closure.find(entry.first);
			auto fresh = known == closure.end() ? entry.second : entry.second - known->second;
			if (!fresh.isEmpty()) delta.emplace(entry.first, std::move(fresh));
		}
		if (delta.empty()) break;
		merge(delta, closure, "open_range_closure");
		frontier = std::move(delta);
	}
	result = compose(prefix, closure);
}
