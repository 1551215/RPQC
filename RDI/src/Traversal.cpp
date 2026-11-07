#pragma once
#include "Traversal.h"
#include "AdditionChainTable.h"
#include "NFA_state.h"
#include "RelationOperationMetrics.h"
#include <ctime>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <queue>
#include <sstream>
#include <iomanip>
#include <set>
#ifdef __linux__
#include <sys/resource.h>
#endif

QueryTimingStats g_query_timing_stats;
static QueryExperimentSummary g_query_experiment_summary;
static double g_explicit_other_time_ms = 0.0;

double QueryAccountedTimeMs()
{
	return g_query_timing_stats.basic_nfa_time_ms +
		g_query_experiment_summary.gcnp_planning_time_ms +
		g_query_experiment_summary.relation_merge_time_ms +
		g_query_experiment_summary.incremental_cache_time_ms +
		g_query_experiment_summary.relation_format_conversion_time_ms +
		g_query_experiment_summary.result_materialization_time_ms +
		g_query_experiment_summary.relation_stats_time_ms + g_explicit_other_time_ms;
}

void RecordQueryPhaseTime(QueryTimingPhase phase, double elapsed_ms)
{
	switch (phase) {
	case QueryTimingPhase::BasicNfa:
		g_query_timing_stats.basic_nfa_time_ms += elapsed_ms; break;
	case QueryTimingPhase::IncrementalCache:
		g_query_experiment_summary.incremental_cache_time_ms += elapsed_ms; break;
	case QueryTimingPhase::FormatConversion:
		g_query_experiment_summary.relation_format_conversion_time_ms += elapsed_ms; break;
	case QueryTimingPhase::ResultMaterialization:
		g_query_experiment_summary.result_materialization_time_ms += elapsed_ms; break;
	case QueryTimingPhase::RelationStats:
		g_query_experiment_summary.relation_stats_time_ms += elapsed_ms; break;
	case QueryTimingPhase::Other:
		g_explicit_other_time_ms += elapsed_ms; break;
	}
}

namespace {

struct CsvWriters {
	bool initialized = false;
	std::ofstream query_summary;
	std::ofstream stage_detail;
	std::ofstream operation_detail;
};

CsvWriters g_csv_writers;
size_t g_stage_counter = 0;
size_t g_operation_counter = 0;
std::set<string> g_materialized_relations_seen;
size_t g_blpc_weighted_naive_total = 0;
size_t g_blpc_weighted_actual_total = 0;
size_t g_bounded_max_p_pairs = 0;
size_t g_bounded_max_s_pairs = 0;
size_t g_bounded_total_p_materialized_pairs = 0;
size_t g_bounded_total_s_materialized_pairs = 0;

string csv_escape(const string& value)
{
	if (value.find_first_of(",\"\n") == string::npos) {
		return value;
	}
	string out = "\"";
	for (char ch : value) {
		if (ch == '"') {
			out += "\"\"";
		} else {
			out.push_back(ch);
		}
	}
	out.push_back('"');
	return out;
}

void ensure_csv_writers()
{
	if (g_csv_writers.initialized) {
		return;
	}
	g_csv_writers.query_summary.open("query_summary.csv", std::ios::app);
	g_csv_writers.stage_detail.open("stage_detail.csv", std::ios::app);
	g_csv_writers.operation_detail.open("operation_detail.csv", std::ios::app);
	if (g_csv_writers.query_summary.tellp() == 0) {
		g_csv_writers.query_summary
			<< "dataset,query_id,query_class,total_time_ms,"
			<< "body_pairs,body_nonempty_rows,body_avg_row_size,body_max_row_size,"
			<< "num_compositions,max_relation_name,max_relation_pairs,"
			<< "total_materialized_relation_pairs,total_composition_input_pairs,total_candidate_pairs,"
			<< "blpc_L,blpc_U,blpc_repetition_rate,blpc_weighted_saving_rate,"
			<< "total_naive_union_work,total_actual_union_work,union_work_unit,"
			<< "exact_prefix_time_ms,bounded_or_closure_time_ms,final_compose_time_ms,peak_memory_mb,"
			<< "gcnp_planning_time_ms,relation_merge_time_ms,other_time_ms,"
			<< "composition_merge_time_ms,relation_union_time_ms,basic_nfa_time_ms,"
			<< "incremental_cache_time_ms,relation_format_conversion_time_ms,"
			<< "result_materialization_time_ms,relation_stats_time_ms,"
			<< "nfa_total_time_ms,relation_merge_total_time_ms,remaining_other_time_ms,"
			<< "timing_schema,ac_time_ms,at_time_ms,gc_time_ms,mp_time_ms,"
			<< "algorithm_total_time_ms,diagnostic_time_ms,unaccounted_time_ms,"
			<< "ac_scopes,at_scopes,gc_scopes,mp_scopes,algorithm_timing_schema\n";
	}
	if (g_csv_writers.stage_detail.tellp() == 0) {
		g_csv_writers.stage_detail
			<< "dataset,query_id,stage_id,stage_name,left_relation,right_relation,output_relation,"
			<< "left_pairs,right_pairs,output_pairs,left_nonempty_rows,right_nonempty_rows,output_nonempty_rows,"
			<< "candidate_pairs,candidate_to_output_ratio,blpc_L,blpc_U,blpc_repetition_rate,"
			<< "naive_union_work,actual_union_work,union_work_saving_rate,union_work_unit,compose_time_ms,"
			<< "gcnp_planning_time_ms,relation_merge_time_ms,other_time_ms\n";
	}
	if (g_csv_writers.operation_detail.tellp() == 0) {
		g_csv_writers.operation_detail
			<< "dataset,query_id,operation_id,operation_type,operation_name,elapsed_ms,"
			<< "gcnp_planning_time_ms,relation_merge_time_ms,other_time_ms,"
			<< "naive_union_work,actual_union_work,union_work_unit\n";
	}
	g_csv_writers.query_summary << std::setprecision(12);
	g_csv_writers.stage_detail << std::setprecision(12);
	g_csv_writers.operation_detail << std::setprecision(12);
	g_csv_writers.initialized = true;
}

RelationStats make_relation_stats(const string& relation_name, size_t pair_count,
	size_t nonempty_rows, size_t max_row_size)
{
	RelationStats stats;
	stats.relation_name = relation_name;
	stats.pair_count = pair_count;
	stats.nonempty_rows = nonempty_rows;
	stats.max_row_size = max_row_size;
	stats.avg_row_size = nonempty_rows == 0 ? 0.0 :
		static_cast<double>(pair_count) / static_cast<double>(nonempty_rows);
	return stats;
}

void update_relation_peak(const string& relation_name, size_t pair_count)
{
	if (pair_count > g_query_experiment_summary.max_relation_pairs) {
		g_query_experiment_summary.max_relation_pairs = pair_count;
		g_query_experiment_summary.max_relation_name = relation_name;
	}
}

using Relation = unordered_map<size_t, unordered_set<size_t>>;
using DegreeMap = unordered_map<size_t, size_t>;
using Bitmap = vector<uint64_t>;

#ifndef RANGE_SINGLE_DEBUG_OPEN_RANGE
#define RANGE_SINGLE_DEBUG_OPEN_RANGE 0
#endif

#ifndef RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC
#define RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC 0
#endif

struct RelationMeta {
	Relation rel;
	size_t edge_count = 0;
	Bitmap src_bitmap;
	Bitmap dst_bitmap;
	DegreeMap out_degree;
	DegreeMap in_degree;
};

size_t relation_bitmap_words()
{
	return (Graph::max_vertex_id / 64) + 1;
}

Bitmap make_empty_bitmap()
{
	return Bitmap(relation_bitmap_words(), 0);
}

void bitmap_set(Bitmap& bitmap, size_t vertex)
{
	const size_t word = vertex / 64;
	const size_t bit = vertex % 64;
	if (word >= bitmap.size()) {
		bitmap.resize(word + 1, 0);
	}
	bitmap[word] |= (1ULL << bit);
}

bool bitmap_test(const Bitmap& bitmap, size_t vertex)
{
	const size_t word = vertex / 64;
	const size_t bit = vertex % 64;
	return word < bitmap.size() && ((bitmap[word] >> bit) & 1ULL);
}

bool bitmap_any(const Bitmap& bitmap)
{
	for (auto word : bitmap) {
		if (word != 0) {
			return true;
		}
	}
	return false;
}

Bitmap bitmap_intersection(const Bitmap& left, const Bitmap& right)
{
	Bitmap out(std::min(left.size(), right.size()), 0);
	for (size_t i = 0; i < out.size(); ++i) {
		out[i] = left[i] & right[i];
	}
	return out;
}

long long read_proc_status_kb(const char* key)
{
#ifdef __linux__
	std::ifstream in("/proc/self/status");
	std::string line;
	while (std::getline(in, line)) {
		if (line.rfind(key, 0) == 0) {
			std::istringstream iss(line.substr(std::strlen(key)));
			long long kb = -1;
			iss >> kb;
			return kb;
		}
	}
#endif
	return -1;
}

long long current_rss_kb()
{
	return read_proc_status_kb("VmRSS:");
}

size_t estimate_degree_map_bytes(const DegreeMap& degree_map)
{
	return degree_map.bucket_count() * sizeof(void*) +
		degree_map.size() * sizeof(std::pair<const size_t, size_t>);
}

size_t estimate_relation_meta_extra_bytes(const RelationMeta& meta)
{
	size_t bytes = 0;
	bytes += meta.src_bitmap.capacity() * sizeof(uint64_t);
	bytes += meta.dst_bitmap.capacity() * sizeof(uint64_t);
	bytes += estimate_degree_map_bytes(meta.out_degree);
	bytes += estimate_degree_map_bytes(meta.in_degree);
	bytes += sizeof(meta.edge_count);
	return bytes;
}

size_t estimate_cache_extra_bytes(const unordered_map<int, RelationMeta>& cache)
{
	size_t bytes = cache.bucket_count() * sizeof(void*);
	for (const auto& kv : cache) {
		bytes += sizeof(kv);
		bytes += estimate_relation_meta_extra_bytes(kv.second);
	}
	return bytes;
}

size_t relation_pair_count(const Relation& rel)
{
	size_t total = 0;
	for (const auto& kv : rel) {
		total += kv.second.size();
	}
	return total;
}

void update_exact_cache_observation(const unordered_map<int, RelationMeta>& cache)
{
	long long rss_kb = current_rss_kb();
	if (rss_kb > g_query_timing_stats.exact_cache_peak_rss_kb) {
		g_query_timing_stats.exact_cache_peak_rss_kb = rss_kb;
	}
	if (static_cast<long long>(cache.size()) > g_query_timing_stats.exact_cache_peak_entries) {
		g_query_timing_stats.exact_cache_peak_entries = static_cast<long long>(cache.size());
	}
	long long extra_bytes = static_cast<long long>(estimate_cache_extra_bytes(cache));
	if (extra_bytes > g_query_timing_stats.exact_cache_peak_extra_bytes) {
		g_query_timing_stats.exact_cache_peak_extra_bytes = extra_bytes;
	}
	g_query_timing_stats.exact_cache_final_extra_bytes = extra_bytes;
}

bool merge_targets(const unordered_set<size_t>& src, unordered_set<size_t>& dst)
{
	size_t before = dst.size();
	dst.insert(src.begin(), src.end());
	return dst.size() != before;
}

bool merge_relation_into(const Relation& src, Relation& dst)
{
	RelationUnionTimer timer("dense_relation_union");
	bool changed = false;
	for (const auto& kv : src) {
		changed = merge_targets(kv.second, dst[kv.first]) || changed;
	}
	return changed;
}

bool relations_equal(const Relation& a, const Relation& b)
{
	if (a.size() != b.size()) {
		return false;
	}
	for (const auto& kv : a) {
		auto it = b.find(kv.first);
		if (it == b.end() || it->second != kv.second) {
			return false;
		}
	}
	return true;
}

Relation build_identity_relation(const unordered_set<size_t>& vertices)
{
	Relation identity;
	for (auto v : vertices) {
		identity[v].insert(v);
	}
	return identity;
}

unordered_set<size_t> collect_relation_dsts(const Relation& rel)
{
	unordered_set<size_t> dsts;
	for (const auto& kv : rel) {
		dsts.insert(kv.second.begin(), kv.second.end());
	}
	return dsts;
}

vector<size_t> topo_sort_scc_dag(const unordered_map<size_t, unordered_set<size_t>>& dag,
	size_t node_count)
{
	vector<size_t> indegree(node_count, 0);
	for (const auto& kv : dag) {
		for (auto dst : kv.second) {
			if (dst < node_count) {
				indegree[dst]++;
			}
		}
	}

	queue<size_t> ready;
	for (size_t node = 0; node < node_count; ++node) {
		if (indegree[node] == 0) {
			ready.push(node);
		}
	}

	vector<size_t> order;
	order.reserve(node_count);
	while (!ready.empty()) {
		const size_t node = ready.front();
		ready.pop();
		order.push_back(node);

		auto it = dag.find(node);
		if (it == dag.end()) {
			continue;
		}
		for (auto dst : it->second) {
			if (--indegree[dst] == 0) {
				ready.push(dst);
			}
		}
	}

	if (order.size() != node_count) {
		for (size_t node = 0; node < node_count; ++node) {
			order.push_back(node);
		}
	}
	return order;
}

void get_open_range_re_scc_rewrite(
	int min_len,
	Relation& g,
	Relation& result,
	unordered_set<size_t>& start_vertexes,
	unordered_set<size_t>& out)
{
	Relation prefix_exact;
	if (min_len == 0) {
		prefix_exact = build_identity_relation(start_vertexes);
	} else {
		range_join(min_len, g, prefix_exact);
	}

	TarjanSCC tarjan(g);
	unordered_map<size_t, unordered_set<size_t>> scc_adjacency_map;
	create_scc_graph(tarjan, g, scc_adjacency_map);
	const size_t scc_count = tarjan.sccs.size();
	const auto topo_order = topo_sort_scc_dag(scc_adjacency_map, scc_count);

	vector<unordered_set<size_t>> sources_by_scc(scc_count);
	size_t prefix_pairs = 0;
	size_t seeded_pairs = 0;
	for (const auto& kv : prefix_exact) {
		unordered_set<size_t> row_sccs;
		prefix_pairs += kv.second.size();
		for (auto mid : kv.second) {
			auto comp_it = tarjan.scc_ids.find(mid);
			if (comp_it == tarjan.scc_ids.end()) {
				continue;
			}
			row_sccs.insert(comp_it->second);
		}
		for (auto comp_id : row_sccs) {
			if (sources_by_scc[comp_id].insert(kv.first).second) {
				seeded_pairs++;
			}
		}
	}

	for (auto comp_id : topo_order) {
		auto succ_it = scc_adjacency_map.find(comp_id);
		if (succ_it == scc_adjacency_map.end() || sources_by_scc[comp_id].empty()) {
			continue;
		}
		const auto& srcs = sources_by_scc[comp_id];
		for (auto succ : succ_it->second) {
			sources_by_scc[succ].insert(srcs.begin(), srcs.end());
		}
	}

	size_t final_pairs = 0;
	for (size_t comp_id = 0; comp_id < scc_count; ++comp_id) {
		const auto& srcs = sources_by_scc[comp_id];
		if (srcs.empty()) {
			continue;
		}
		const auto& component_vertices = tarjan.sccs[comp_id];
		final_pairs += srcs.size() * component_vertices.size();
		for (auto src : srcs) {
			auto& row = result[src];
			row.insert(component_vertices.begin(), component_vertices.end());
		}
		out.insert(component_vertices.begin(), component_vertices.end());
	}

#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	size_t active_sccs = 0;
	size_t propagated_pairs = 0;
	for (size_t comp_id = 0; comp_id < scc_count; ++comp_id) {
		if (!sources_by_scc[comp_id].empty()) {
			active_sccs++;
			propagated_pairs += sources_by_scc[comp_id].size();
		}
	}
	std::cerr << "[range-single][rewrite-scc] B{" << min_len << ",inf}"
	          << " => B^" << min_len << " o B*" << std::endl;
	std::cerr << "[range-single][rewrite-scc] prefix_pairs=" << prefix_pairs
	          << " seeded_source_scc_pairs=" << seeded_pairs
	          << " propagated_source_scc_pairs=" << propagated_pairs
	          << " active_sccs=" << active_sccs
	          << " scc_count=" << scc_count
	          << std::endl;
	std::cerr << "[range-single][rewrite-scc] final_pairs=" << final_pairs
	          << " final_rows=" << result.size()
	          << " out_vertices=" << out.size()
	          << std::endl;
#endif
}

bool join_impl(const Relation& g1, const Relation& g2, Relation& result)
{
	OperationStopwatch timer;
	size_t row_work = 0;
	size_t candidate_pairs = 0;
	bool changed = false;
	for (const auto& kv : g1) {
		const auto key = kv.first;
		unordered_set<size_t> pending;
		for (auto elem : kv.second) {
			auto it = g2.find(elem);
			if (it != g2.end()) {
				if (!it->second.empty()) ++row_work;
				candidate_pairs += it->second.size();
				pending.insert(it->second.begin(), it->second.end());
			}
		}
		if (!pending.empty()) {
			changed = merge_targets(pending, result[key]) || changed;
		}
	}
	const double merge_ms = timer.elapsed_ms();
	RecordCompositionMeasurement("dense_join", merge_ms, 0.0, merge_ms, row_work, row_work);
	RecordComposeStage("dense_join", "dense_left", "dense_right", "dense_output",
		relation_pair_count(g1), relation_pair_count(g2), relation_pair_count(result),
		g1.size(), g2.size(), result.size(), candidate_pairs, merge_ms,
		0, 0, row_work, row_work, 0.0, merge_ms);
	return changed;
}

bool self_join_impl(const Relation& g, Relation& result)
{
	return join_impl(g, g, result);
}

RelationMeta build_relation_meta(const Relation& rel)
{
	RelationMeta meta;
	meta.rel = rel;
	meta.src_bitmap = make_empty_bitmap();
	meta.dst_bitmap = make_empty_bitmap();
	for (const auto& kv : rel) {
		bitmap_set(meta.src_bitmap, kv.first);
		meta.out_degree[kv.first] = kv.second.size();
		meta.edge_count += kv.second.size();
		for (auto dst : kv.second) {
			bitmap_set(meta.dst_bitmap, dst);
			meta.in_degree[dst]++;
		}
	}
	return meta;
}

RelationMeta build_joined_meta(const RelationMeta& left, const RelationMeta& right,
	const Bitmap& active_domain)
{
	RelationMeta meta;
	if (!bitmap_any(active_domain)) {
		return meta;
	}
	OperationStopwatch timer;
	size_t row_work = 0;
	size_t candidate_pairs = 0;
	meta.src_bitmap = make_empty_bitmap();
	meta.dst_bitmap = make_empty_bitmap();
	for (const auto& kv : left.rel) {
		const auto src = kv.first;
		auto& out_row = meta.rel[src];
		for (auto mid : kv.second) {
			if (!bitmap_test(active_domain, mid)) {
				continue;
			}
			auto it = right.rel.find(mid);
			if (it == right.rel.end()) {
				continue;
			}
			if (!it->second.empty()) ++row_work;
			candidate_pairs += it->second.size();
			for (auto dst : it->second) {
				auto inserted = out_row.insert(dst).second;
				if (!inserted) {
					continue;
				}
				bitmap_set(meta.src_bitmap, src);
				bitmap_set(meta.dst_bitmap, dst);
				meta.out_degree[src]++;
				meta.in_degree[dst]++;
				meta.edge_count++;
			}
		}
		if (out_row.empty()) {
			meta.rel.erase(src);
		} else {
			meta.out_degree[src] = out_row.size();
		}
	}
	const double merge_ms = timer.elapsed_ms();
	RecordCompositionMeasurement("dense_meta_join", merge_ms, 0.0, merge_ms, row_work, row_work);
	RecordComposeStage("dense_meta_join", "dense_left", "dense_right", "dense_output",
		left.edge_count, right.edge_count, meta.edge_count,
		left.rel.size(), right.rel.size(), meta.rel.size(), candidate_pairs, merge_ms,
		0, 0, row_work, row_work, 0.0, merge_ms);
	return meta;
}

Bitmap compute_active_domain(const RelationMeta& left, const RelationMeta& right)
{
	return bitmap_intersection(left.dst_bitmap, right.src_bitmap);
}

uint64_t estimate_join_cost(const RelationMeta& left, const RelationMeta& right,
	const Bitmap& active_domain)
{
	uint64_t cost = static_cast<uint64_t>(left.edge_count);
	const auto& smaller = (left.in_degree.size() <= right.out_degree.size())
		? left.in_degree
		: right.out_degree;
	for (const auto& kv : smaller) {
		const auto mid = kv.first;
		if (!bitmap_test(active_domain, mid)) {
			continue;
		}
		auto in_it = left.in_degree.find(mid);
		auto out_it = right.out_degree.find(mid);
		if (in_it == left.in_degree.end() || out_it == right.out_degree.end()) {
			continue;
		}
		cost += static_cast<uint64_t>(in_it->second) *
			static_cast<uint64_t>(out_it->second);
	}
	return cost;
}

bool prefer_reversed_join(const RelationMeta& left, const RelationMeta& right,
	const Bitmap& forward_active,
	const Bitmap& reverse_active)
{
	const uint64_t forward_cost = estimate_join_cost(left, right, forward_active);
	const uint64_t reverse_cost = estimate_join_cost(right, left, reverse_active);
	return reverse_cost < forward_cost;
}

void release_relation_if_dead(int len, int target, std::unordered_map<int, int>& use_counts,
	unordered_map<int, RelationMeta>& cache)
{
	auto it = use_counts.find(len);
	if (it == use_counts.end()) {
		return;
	}
	if (--it->second != 0) {
		return;
	}
	use_counts.erase(it);
	if (len != 1 && len != target) {
		cache.erase(len);
	}
}

const RelationMeta& exact_power_cached(int num, const RelationMeta& /* base */,
	unordered_map<int, RelationMeta>& cache)
{
	update_exact_cache_observation(cache);
	auto it = cache.find(num);
	if (it != cache.end()) {
		return it->second;
	}

	auto use_counts = BuildAdditionPlanUseCounts(num);
	for (const auto& step : GetAdditionPlan(num)) {
		if (cache.find(step.value) != cache.end()) {
			continue;
		}
		const int left_len = std::min(step.left, step.right);
		const int right_len = std::max(step.left, step.right);
		const RelationMeta& left = cache.at(left_len);
		const RelationMeta& right = cache.at(right_len);
		const auto forward_active = compute_active_domain(left, right);
		if (!bitmap_any(forward_active)) {
			cache.emplace(step.value, RelationMeta{});
			release_relation_if_dead(left_len, num, use_counts, cache);
			release_relation_if_dead(right_len, num, use_counts, cache);
			continue;
		}
		const auto reverse_active = compute_active_domain(right, left);
		RelationMeta value = prefer_reversed_join(left, right, forward_active, reverse_active)
			? build_joined_meta(right, left, reverse_active)
			: build_joined_meta(left, right, forward_active);
		cache.emplace(step.value, std::move(value));
		release_relation_if_dead(left_len, num, use_counts, cache);
		release_relation_if_dead(right_len, num, use_counts, cache);
		update_exact_cache_observation(cache);
	}
	return cache.at(num);
}

const RelationMeta& prefix_union_cached(
	int num,
	const RelationMeta& base,
	const unordered_set<size_t>& identity_sources,
	unordered_map<int, RelationMeta>& exact_cache,
	unordered_map<int, RelationMeta>& prefix_cache)
{
	auto it = prefix_cache.find(num);
	if (it != prefix_cache.end()) {
		return it->second;
	}
	if (num <= 1) {
		return prefix_cache.at(1);
	}

	auto split = GetAdditionSplit(num);
	if (split.first <= 0 || split.second <= 0) {
		throw std::runtime_error("No addition split for prefix union target: " + std::to_string(num));
	}
	const int left_len = split.first;
	const int right_len = split.second;
	const RelationMeta& left_prefix = prefix_union_cached(
		left_len, base, identity_sources, exact_cache, prefix_cache);
	const RelationMeta& right_prefix = prefix_union_cached(
		right_len, base, identity_sources, exact_cache, prefix_cache);
	const RelationMeta& left_exact = exact_power_cached(left_len, base, exact_cache);
	Relation shifted;
	join_impl(left_exact.rel, right_prefix.rel, shifted);
	Relation prefix = left_prefix.rel;
	merge_relation_into(shifted, prefix);
	auto inserted = prefix_cache.emplace(num, build_relation_meta(prefix));
	return inserted.first->second;
}

Relation build_bounded_suffix_relation(
	int max_extra_len,
	const Relation& base_relation,
	const unordered_set<size_t>& identity_sources)
{
	if (max_extra_len < 0 || identity_sources.empty()) {
		return {};
	}
	if (max_extra_len == 0) {
		return build_identity_relation(identity_sources);
	}
	unordered_map<int, RelationMeta> exact_cache;
	exact_cache.emplace(1, build_relation_meta(base_relation));
	unordered_map<int, RelationMeta> prefix_cache;
	prefix_cache.emplace(1, build_relation_meta(build_identity_relation(identity_sources)));
	const RelationMeta& prefix = prefix_union_cached(
		max_extra_len + 1,
		exact_cache.at(1),
		identity_sources,
		exact_cache,
		prefix_cache);
	return prefix.rel;
}

} // namespace

void BeginQueryExperiment(const string& dataset, const string& query_id,
	const string& query_class, const string& expression)
{
	g_query_experiment_summary = QueryExperimentSummary{};
	g_explicit_other_time_ms = 0.0;
	cout << std::setprecision(12);
	g_query_experiment_summary.dataset = dataset;
	g_query_experiment_summary.query_id = query_id;
	g_query_experiment_summary.query_class = query_class;
	g_query_experiment_summary.expression = expression;
	g_stage_counter = 0;
	g_operation_counter = 0;
	g_materialized_relations_seen.clear();
	g_blpc_weighted_naive_total = 0;
	g_blpc_weighted_actual_total = 0;
	g_bounded_max_p_pairs = 0;
	g_bounded_max_s_pairs = 0;
	g_bounded_total_p_materialized_pairs = 0;
	g_bounded_total_s_materialized_pairs = 0;
}

void RecordCompositionMeasurement(const std::string& operation,
	double elapsed_ms, double planning_ms, double merge_ms,
	size_t naive_rows, size_t actual_rows)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	ensure_csv_writers();
	g_query_experiment_summary.gcnp_planning_time_ms += planning_ms;
	g_query_experiment_summary.relation_merge_time_ms += merge_ms;
	g_query_experiment_summary.composition_merge_time_ms += merge_ms;
	g_blpc_weighted_naive_total += naive_rows;
	g_blpc_weighted_actual_total += actual_rows;
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	g_csv_writers.operation_detail
		<< csv_escape(g_query_experiment_summary.dataset) << ","
		<< csv_escape(g_query_experiment_summary.query_id) << ","
		<< ++g_operation_counter << ",COMPOSE," << csv_escape(operation) << ","
		<< elapsed_ms << "," << planning_ms << "," << merge_ms << ","
		<< elapsed_ms - planning_ms - merge_ms << ","
		<< naive_rows << "," << actual_rows << ",rhs_rows\n";
	g_csv_writers.operation_detail.flush();
}

void RecordRelationUnionMeasurement(const std::string& operation, double merge_ms)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	ensure_csv_writers();
	g_query_experiment_summary.relation_merge_time_ms += merge_ms;
	g_query_experiment_summary.relation_union_time_ms += merge_ms;
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	g_csv_writers.operation_detail
		<< csv_escape(g_query_experiment_summary.dataset) << ","
		<< csv_escape(g_query_experiment_summary.query_id) << ","
		<< ++g_operation_counter << ",UNION," << csv_escape(operation) << ","
		<< merge_ms << ",0," << merge_ms << ",0,,,rhs_rows\n";
	g_csv_writers.operation_detail.flush();
}

void RecordQueryPeakMemoryMb(double peak_memory_mb)
{
	if (peak_memory_mb > g_query_experiment_summary.query_peak_memory_mb) {
		g_query_experiment_summary.query_peak_memory_mb = peak_memory_mb;
	}
}

void RecordBodyRelationStats(const string& relation_name,
	size_t pair_count, size_t nonempty_rows, size_t max_row_size)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	g_query_experiment_summary.body_stats =
		make_relation_stats(relation_name, pair_count, nonempty_rows, max_row_size);
	update_relation_peak(relation_name, pair_count);
}

void RecordMaterializedRelationStats(const string& relation_name,
	size_t pair_count, size_t nonempty_rows, size_t max_row_size)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	update_relation_peak(relation_name, pair_count);
	if (g_materialized_relations_seen.insert(relation_name).second) {
		g_query_experiment_summary.total_materialized_relation_pairs += pair_count;
	}
	if (!relation_name.empty() && relation_name[0] == 'P') {
		g_bounded_max_p_pairs = std::max(g_bounded_max_p_pairs, pair_count);
		g_bounded_total_p_materialized_pairs += pair_count;
	} else if (!relation_name.empty() && relation_name[0] == 'S') {
		g_bounded_max_s_pairs = std::max(g_bounded_max_s_pairs, pair_count);
		g_bounded_total_s_materialized_pairs += pair_count;
	}
	(void)nonempty_rows;
	(void)max_row_size;
}

void RecordComposeStage(
	const string& stage_name,
	const string& left_relation,
	const string& right_relation,
	const string& output_relation,
	size_t left_pairs,
	size_t right_pairs,
	size_t output_pairs,
	size_t left_nonempty_rows,
	size_t right_nonempty_rows,
	size_t output_nonempty_rows,
	size_t candidate_pairs,
	double compose_time_ms,
	size_t blpc_L,
	size_t blpc_U,
	size_t naive_union_work,
	size_t actual_union_work,
	double gcnp_planning_time_ms,
	double relation_merge_time_ms)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	ensure_csv_writers();
	++g_stage_counter;
	g_query_experiment_summary.num_compositions++;
	g_query_experiment_summary.total_composition_input_pairs += left_pairs + right_pairs;
	g_query_experiment_summary.total_candidate_pairs += candidate_pairs;
	g_query_experiment_summary.blpc_L += blpc_L;
	g_query_experiment_summary.blpc_U += blpc_U;
	update_relation_peak(output_relation, output_pairs);

	const double candidate_to_output_ratio = output_pairs == 0 ? 0.0 :
		static_cast<double>(candidate_pairs) / static_cast<double>(output_pairs);
	const double blpc_repetition_rate = blpc_L == 0 ? 0.0 :
		1.0 - static_cast<double>(blpc_U) / static_cast<double>(blpc_L);
	const double union_work_saving_rate = naive_union_work == 0 ? 0.0 :
		1.0 - static_cast<double>(actual_union_work) /
			static_cast<double>(naive_union_work);

	cout << "COMPOSE"
	     << " stage_id=" << g_stage_counter
	     << " stage_name=" << stage_name
	     << " left_relation=" << left_relation
	     << " right_relation=" << right_relation
	     << " output_relation=" << output_relation
	     << " left_pairs=" << left_pairs
	     << " right_pairs=" << right_pairs
	     << " output_pairs=" << output_pairs
	     << " left_nonempty_rows=" << left_nonempty_rows
	     << " right_nonempty_rows=" << right_nonempty_rows
	     << " output_nonempty_rows=" << output_nonempty_rows
	     << " candidate_pairs=" << candidate_pairs
	     << " candidate_to_output_ratio=" << candidate_to_output_ratio
	     << " naive_union_work=" << naive_union_work
	     << " actual_union_work=" << actual_union_work
	     << " union_work_saving_rate=" << union_work_saving_rate
	     << " union_work_unit=rhs_rows"
	     << " compose_time_ms=" << compose_time_ms
	     << " gcnp_planning_time_ms=" << gcnp_planning_time_ms
	     << " relation_merge_time_ms=" << relation_merge_time_ms
	     << " other_time_ms=" << compose_time_ms - gcnp_planning_time_ms - relation_merge_time_ms
	     << endl;
	cout << "BLPC_STATS"
	     << " stage_id=" << g_stage_counter
	     << " local_pattern_instances_L=" << blpc_L
	     << " distinct_local_patterns_U=" << blpc_U
	     << " repeated_pattern_instances=" << (blpc_L >= blpc_U ? (blpc_L - blpc_U) : 0)
	     << " pattern_repetition_rate=" << blpc_repetition_rate
	     << " naive_union_work=" << naive_union_work
	     << " actual_union_work=" << actual_union_work
	     << " union_work_saving_rate=" << union_work_saving_rate
	     << " union_work_unit=rhs_rows"
	     << endl;

	g_csv_writers.stage_detail
		<< csv_escape(g_query_experiment_summary.dataset) << ","
		<< csv_escape(g_query_experiment_summary.query_id) << ","
		<< g_stage_counter << ","
		<< csv_escape(stage_name) << ","
		<< csv_escape(left_relation) << ","
		<< csv_escape(right_relation) << ","
		<< csv_escape(output_relation) << ","
		<< left_pairs << "," << right_pairs << "," << output_pairs << ","
		<< left_nonempty_rows << "," << right_nonempty_rows << "," << output_nonempty_rows << ","
		<< candidate_pairs << "," << candidate_to_output_ratio << ","
		<< blpc_L << "," << blpc_U << "," << blpc_repetition_rate << ","
		<< naive_union_work << "," << actual_union_work << ","
		<< union_work_saving_rate << ",rhs_rows," << compose_time_ms << ","
		<< gcnp_planning_time_ms << "," << relation_merge_time_ms << ","
		<< compose_time_ms - gcnp_planning_time_ms - relation_merge_time_ms << "\n";
	// A query can run in a forked worker so its logs survive _exit().
	g_csv_writers.stage_detail.flush();
}

void RecordBoundedSummary(
	int min_len,
	int max_len,
	size_t pm_pairs,
	size_t sd_pairs,
	size_t max_p_pairs,
	size_t max_s_pairs,
	size_t total_p_materialized_pairs,
	size_t total_s_materialized_pairs,
	double exact_prefix_time_ms,
	double dual_state_time_ms,
	double final_compose_time_ms,
	size_t final_output_pairs)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	g_query_experiment_summary.exact_prefix_time_ms = exact_prefix_time_ms;
	g_query_experiment_summary.bounded_or_closure_time_ms = dual_state_time_ms;
	g_query_experiment_summary.final_compose_time_ms = final_compose_time_ms;
	cout << "BOUNDED_SUMMARY"
	     << " m=" << min_len
	     << " n=" << max_len
	     << " d=" << (max_len - min_len)
	     << " Pm_pairs=" << pm_pairs
	     << " Sd_pairs=" << sd_pairs
	     << " max_P_pairs=" << max_p_pairs
	     << " max_S_pairs=" << max_s_pairs
	     << " total_P_materialized_pairs=" << total_p_materialized_pairs
	     << " total_S_materialized_pairs=" << total_s_materialized_pairs
	     << " exact_prefix_time_ms=" << exact_prefix_time_ms
	     << " dual_state_time_ms=" << dual_state_time_ms
	     << " final_compose_time_ms=" << final_compose_time_ms
	     << " final_output_pairs=" << final_output_pairs
	     << endl;
}

void RecordDualStateStep(
	int a,
	int b,
	int c,
	size_t pa_pairs,
	size_t pb_pairs,
	size_t pc_pairs,
	size_t sa_pairs,
	size_t sb_pairs,
	size_t t_pairs,
	size_t sc_pairs,
	size_t p_comp_candidate_pairs,
	size_t t_comp_candidate_pairs,
	double p_comp_time_ms,
	double t_comp_time_ms,
	double s_union_time_ms)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	cout << "DUAL_STATE_STEP"
	     << " a=" << a
	     << " b=" << b
	     << " c=" << c
	     << " Pa_pairs=" << pa_pairs
	     << " Pb_pairs=" << pb_pairs
	     << " Pc_pairs=" << pc_pairs
	     << " Sa_pairs=" << sa_pairs
	     << " Sb_pairs=" << sb_pairs
	     << " T_pairs=" << t_pairs
	     << " Sc_pairs=" << sc_pairs
	     << " P_comp_candidate_pairs=" << p_comp_candidate_pairs
	     << " T_comp_candidate_pairs=" << t_comp_candidate_pairs
	     << " P_comp_time_ms=" << p_comp_time_ms
	     << " T_comp_time_ms=" << t_comp_time_ms
	     << " S_union_time_ms=" << s_union_time_ms
	     << endl;
}

void RecordUnboundedSummary(
	int min_len,
	size_t body_pairs,
	size_t pm_pairs,
	size_t closure_pairs,
	size_t final_output_pairs,
	double exact_prefix_time_ms,
	double closure_time_ms,
	double final_compose_time_ms,
	size_t final_compose_candidate_pairs,
	size_t closure_iterations)
{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	g_query_experiment_summary.exact_prefix_time_ms = exact_prefix_time_ms;
	g_query_experiment_summary.bounded_or_closure_time_ms = closure_time_ms;
	g_query_experiment_summary.final_compose_time_ms = final_compose_time_ms;
	cout << "UNBOUNDED_SUMMARY"
	     << " m=" << min_len
	     << " body_pairs=" << body_pairs
	     << " Pm_pairs=" << pm_pairs
	     << " closure_pairs=" << closure_pairs
	     << " final_output_pairs=" << final_output_pairs
	     << " exact_prefix_time_ms=" << exact_prefix_time_ms
	     << " closure_time_ms=" << closure_time_ms
	     << " final_compose_time_ms=" << final_compose_time_ms
	     << " final_compose_candidate_pairs=" << final_compose_candidate_pairs
	     << " closure_iterations=" << closure_iterations
	     << endl;
}

void FinalizeQueryExperiment(double total_time_ms)
{
	ensure_csv_writers();
	const auto& algorithm = g_algorithm_timing;
	const auto& phase_ms = algorithm.milliseconds;
	const double algorithm_total_ms = phase_ms[0] + phase_ms[1] + phase_ms[2] + phase_ms[3];
	const double nfa_total_ms = g_query_timing_stats.basic_nfa_time_ms +
		g_query_experiment_summary.incremental_cache_time_ms;
	const double merge_total_ms = g_query_experiment_summary.relation_merge_time_ms +
		g_query_experiment_summary.relation_format_conversion_time_ms +
		g_query_experiment_summary.result_materialization_time_ms;
	g_query_experiment_summary.total_time_ms = total_time_ms;
	g_query_experiment_summary.other_time_ms = total_time_ms -
		g_query_experiment_summary.gcnp_planning_time_ms -
		merge_total_ms - nfa_total_ms;
	const double remaining_other_ms = g_query_experiment_summary.other_time_ms -
		g_query_experiment_summary.relation_stats_time_ms;
	g_query_experiment_summary.blpc_repetition_rate =
		g_query_experiment_summary.blpc_L == 0 ? 0.0 :
		1.0 - static_cast<double>(g_query_experiment_summary.blpc_U) /
			static_cast<double>(g_query_experiment_summary.blpc_L);
	g_query_experiment_summary.blpc_weighted_saving_rate =
		g_blpc_weighted_naive_total == 0 ? 0.0 :
		1.0 - static_cast<double>(g_blpc_weighted_actual_total) /
			static_cast<double>(g_blpc_weighted_naive_total);

	cout << "QUERY_SUMMARY"
	     << " query_id=" << g_query_experiment_summary.query_id
	     << " dataset=" << g_query_experiment_summary.dataset
	     << " query_class=" << g_query_experiment_summary.query_class
	     << " total_time_ms=" << total_time_ms
	     << " body_pair_count=" << g_query_experiment_summary.body_stats.pair_count
	     << " body_nonempty_rows=" << g_query_experiment_summary.body_stats.nonempty_rows
	     << " body_avg_row_size=" << g_query_experiment_summary.body_stats.avg_row_size
	     << " body_max_row_size=" << g_query_experiment_summary.body_stats.max_row_size
	     << " num_compositions=" << g_query_experiment_summary.num_compositions
	     << " max_relation_name=" << g_query_experiment_summary.max_relation_name
	     << " max_relation_pairs=" << g_query_experiment_summary.max_relation_pairs
	     << " total_materialized_relation_pairs=" << g_query_experiment_summary.total_materialized_relation_pairs
	     << " total_composition_input_pairs=" << g_query_experiment_summary.total_composition_input_pairs
	     << " total_candidate_pairs=" << g_query_experiment_summary.total_candidate_pairs
	     << " total_naive_union_work=" << g_blpc_weighted_naive_total
	     << " total_actual_union_work=" << g_blpc_weighted_actual_total
	     << " union_work_unit=rhs_rows"
	     << " query_peak_memory_mb=" << g_query_experiment_summary.query_peak_memory_mb
	     << " gcnp_planning_time_ms=" << g_query_experiment_summary.gcnp_planning_time_ms
	     << " relation_merge_time_ms=" << g_query_experiment_summary.relation_merge_time_ms
	     << " other_time_ms=" << g_query_experiment_summary.other_time_ms
	     << " composition_merge_time_ms=" << g_query_experiment_summary.composition_merge_time_ms
	     << " relation_union_time_ms=" << g_query_experiment_summary.relation_union_time_ms
	     << " basic_nfa_time_ms=" << g_query_timing_stats.basic_nfa_time_ms
	     << " incremental_cache_time_ms=" << g_query_experiment_summary.incremental_cache_time_ms
	     << " relation_format_conversion_time_ms=" << g_query_experiment_summary.relation_format_conversion_time_ms
	     << " result_materialization_time_ms=" << g_query_experiment_summary.result_materialization_time_ms
	     << " relation_stats_time_ms=" << g_query_experiment_summary.relation_stats_time_ms
	     << " nfa_total_time_ms=" << nfa_total_ms
	     << " relation_merge_total_time_ms=" << merge_total_ms
	     << " remaining_other_time_ms=" << remaining_other_ms
	     << " timing_schema=exclusive_v3"
	     << " ac_time_ms=" << phase_ms[0]
	     << " at_time_ms=" << phase_ms[1]
	     << " gc_time_ms=" << phase_ms[2]
	     << " mp_time_ms=" << phase_ms[3]
	     << " algorithm_total_time_ms=" << algorithm_total_ms
	     << " diagnostic_time_ms=" << phase_ms[4]
	     << " unaccounted_time_ms=" << phase_ms[5]
	     << " ac_scopes=" << algorithm.entries[0]
	     << " at_scopes=" << algorithm.entries[1]
	     << " gc_scopes=" << algorithm.entries[2]
	     << " mp_scopes=" << algorithm.entries[3]
	     << " algorithm_timing_schema=ac_at_gc_mp_v1"
	     << endl;

	g_csv_writers.query_summary
		<< csv_escape(g_query_experiment_summary.dataset) << ","
		<< csv_escape(g_query_experiment_summary.query_id) << ","
		<< csv_escape(g_query_experiment_summary.query_class) << ","
		<< total_time_ms << ","
		<< g_query_experiment_summary.body_stats.pair_count << ","
		<< g_query_experiment_summary.body_stats.nonempty_rows << ","
		<< g_query_experiment_summary.body_stats.avg_row_size << ","
		<< g_query_experiment_summary.body_stats.max_row_size << ","
		<< g_query_experiment_summary.num_compositions << ","
		<< csv_escape(g_query_experiment_summary.max_relation_name) << ","
		<< g_query_experiment_summary.max_relation_pairs << ","
		<< g_query_experiment_summary.total_materialized_relation_pairs << ","
		<< g_query_experiment_summary.total_composition_input_pairs << ","
		<< g_query_experiment_summary.total_candidate_pairs << ","
		<< g_query_experiment_summary.blpc_L << ","
		<< g_query_experiment_summary.blpc_U << ","
		<< g_query_experiment_summary.blpc_repetition_rate << ","
		<< g_query_experiment_summary.blpc_weighted_saving_rate << ","
		<< g_blpc_weighted_naive_total << "," << g_blpc_weighted_actual_total << ",rhs_rows,"
		<< g_query_experiment_summary.exact_prefix_time_ms << ","
		<< g_query_experiment_summary.bounded_or_closure_time_ms << ","
		<< g_query_experiment_summary.final_compose_time_ms << ","
		<< g_query_experiment_summary.query_peak_memory_mb << ","
		<< g_query_experiment_summary.gcnp_planning_time_ms << ","
		<< g_query_experiment_summary.relation_merge_time_ms << ","
		<< g_query_experiment_summary.other_time_ms << ","
		<< g_query_experiment_summary.composition_merge_time_ms << ","
		<< g_query_experiment_summary.relation_union_time_ms << ","
		<< g_query_timing_stats.basic_nfa_time_ms << ","
		<< g_query_experiment_summary.incremental_cache_time_ms << ","
		<< g_query_experiment_summary.relation_format_conversion_time_ms << ","
		<< g_query_experiment_summary.result_materialization_time_ms << ","
		<< g_query_experiment_summary.relation_stats_time_ms << ","
		<< nfa_total_ms << "," << merge_total_ms << "," << remaining_other_ms
		<< ",exclusive_v3," << phase_ms[0] << "," << phase_ms[1]
		<< "," << phase_ms[2] << "," << phase_ms[3]
		<< "," << algorithm_total_ms << "," << phase_ms[4] << "," << phase_ms[5];
	for (size_t i = 0; i < 4; ++i) g_csv_writers.query_summary << "," << algorithm.entries[i];
	g_csv_writers.query_summary << ",ac_at_gc_mp_v1\n";
	g_csv_writers.query_summary.flush();
}

const QueryExperimentSummary& GetQueryExperimentSummary()
{
	return g_query_experiment_summary;
}

void ResetQueryTimingStats()
{
	g_query_timing_stats = QueryTimingStats{};
}

const QueryTimingStats& GetQueryTimingStats()
{
	return g_query_timing_stats;
}

void PrintQueryTimingStats()
{
	std::cout
		<< "[Timing] count_path_summary:"
		<< " getStartPoints_ms=" << g_query_timing_stats.get_start_points_ms
		<< " getStartPoints_calls=" << g_query_timing_stats.get_start_points_calls
		<< " getStartPoints_vertices=" << g_query_timing_stats.get_start_points_total_vertices
		<< " get_basic_re_batch_ms=" << g_query_timing_stats.get_basic_re_batch_ms
		<< " get_basic_re_batch_calls=" << g_query_timing_stats.get_basic_re_batch_calls
		<< " get_basic_re_batch_total_starts=" << g_query_timing_stats.get_basic_re_batch_total_starts
		<< " get_basic_re_single_ms=" << g_query_timing_stats.get_basic_re_single_ms
		<< " get_basic_re_single_calls=" << g_query_timing_stats.get_basic_re_single_calls
		<< " get_range_re_ms=" << g_query_timing_stats.get_range_re_ms
		<< " get_range_re_calls=" << g_query_timing_stats.get_range_re_calls
		<< " range_join_ms=" << g_query_timing_stats.range_join_ms
		<< " range_join_calls=" << g_query_timing_stats.range_join_calls
		<< " join_ms=" << g_query_timing_stats.join_ms
		<< " join_calls=" << g_query_timing_stats.join_calls
		<< " self_join_ms=" << g_query_timing_stats.self_join_ms
		<< " self_join_calls=" << g_query_timing_stats.self_join_calls
		<< " get_quora_re_ms=" << g_query_timing_stats.get_quora_re_ms
		<< " get_quora_re_calls=" << g_query_timing_stats.get_quora_re_calls
		<< " get_star_re_ms=" << g_query_timing_stats.get_star_re_ms
		<< " get_star_re_calls=" << g_query_timing_stats.get_star_re_calls
		<< " count_init_map_ms=" << g_query_timing_stats.count_init_map_ms
		<< " count_init_map_calls=" << g_query_timing_stats.count_init_map_calls
		<< " count_init_map_getStartPoints_ms=" << g_query_timing_stats.count_init_map_get_start_points_ms
		<< " count_init_map_basic_re_ms=" << g_query_timing_stats.count_init_map_basic_re_ms
		<< " count_init_map_range_re_ms=" << g_query_timing_stats.count_init_map_range_re_ms
			<< " count_range_dfs_ms=" << g_query_timing_stats.count_range_dfs_ms
			<< " count_range_dfs_calls=" << g_query_timing_stats.count_range_dfs_calls
			<< " range_complex_hits=" << g_query_timing_stats.range_complex_hits
			<< " range_simple_hits=" << g_query_timing_stats.range_simple_hits
			<< " exact_cache_begin_rss_kb=" << g_query_timing_stats.exact_cache_begin_rss_kb
			<< " exact_cache_end_rss_kb=" << g_query_timing_stats.exact_cache_end_rss_kb
			<< " exact_cache_peak_rss_kb=" << g_query_timing_stats.exact_cache_peak_rss_kb
			<< " exact_cache_peak_entries=" << g_query_timing_stats.exact_cache_peak_entries
			<< " exact_cache_peak_extra_bytes=" << g_query_timing_stats.exact_cache_peak_extra_bytes
			<< " exact_cache_final_extra_bytes=" << g_query_timing_stats.exact_cache_final_extra_bytes
			<< std::endl;
}
void dfs(size_t vertex, shared_ptr<NFA_state> state,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2>& M,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, int, Hashfunc2, Equalfunc2>& visited,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, pair<size_t,shared_ptr<NFA_state> >, Hashfunc2, Equalfunc2>& master,
	unordered_set<size_t>& out) {
        //cout<<"dfs:"<<state->type<<" "<<state->label<<" "<<vertex<<endl;
	M[{vertex, state}] = {};
	visited[{vertex,state}]=1;
	master[{vertex,state}]={vertex,state};
	if (state->type == Type::MatchState)
	{
		M[{vertex, state}].insert(vertex);
		if (out.find(vertex) == out.end())
		{
			out.insert(vertex);
		}
		return;
	}
	unordered_map<string,vector<shared_ptr<NFA_state>>> labels;
	vector<shared_ptr<NFA_state>> next_states;
	state->next_states(next_states);
	for (auto& next_state : next_states) {
		if (next_state->type == Type::MatchState){
			M[{vertex,state}].insert(vertex);
		}else if (next_state->type == Type::RangeRepetition){
			shared_ptr<Count_state> count_state=dynamic_pointer_cast<Count_state>(next_state);
			if (count_state->complex){
				g_query_timing_stats.range_complex_hits++;
				const auto& cached_range = count_state->use_lazy_exact_cache()
					? count_state->get_or_build_lazy_range_row(vertex, count_state->internal_start)
					: count_state->range_repetition_map[vertex];
				for(auto r : cached_range){
					dfs(r,next_state,M,visited,master,out);
						for (auto& p : M[{r, next_state}])
							M[{vertex, state}].insert(p);
				}
			}else{
				g_query_timing_stats.range_simple_hits++;
				unordered_set<size_t> mid_result;
				
				count_state->range_dfs(vertex,count_state->internal_start,mid_result);
				for (auto r:mid_result){
					dfs(r,next_state,M,visited,master,out);
						for (auto& p : M[{r, next_state}])
							M[{vertex, state}].insert(p);
				}
			}
		}else {
			for(auto& next_vertex:Graph::adjacency_map[vertex][next_state->label]){
				if (visited[{ next_vertex,next_state }] ==0)
				{
					dfs(next_vertex, next_state, M,visited,master,out);
						for (auto& p : M[{next_vertex, next_state}])
							M[{vertex, state}].insert(p);
					if (master[{ next_vertex, next_state }].first !=next_vertex||master[{ next_vertex, next_state }].second !=next_state){
						master[{vertex,state}]=master[{ next_vertex, next_state }];
					}
				}else if (visited[{ next_vertex, next_state }]==1){
					master[{vertex,state}]=master[{ next_vertex, next_state }];
				}else if (visited[{ next_vertex, next_state }]==2){
						for (auto& p : M[{next_vertex, next_state}])
							M[{vertex, state}].insert(p);
				}
			}
		}
	}
	// RDFGraphType::out_edge_iterator ei, ei_end;
	// for (tie(ei, ei_end) = out_edges(vertex, Graph::G); ei != ei_end; ++ei) {
	// 	size_t next_vertex = target(*ei, Graph::G);
	// 	std::string label = get(boost::edge_weight, Graph::G, *ei);
	// 	if(labels.find(label)!=labels.end()){
	// 		for (auto next_state:labels[label]){
	// 			if (visited[{ next_vertex,next_state }] ==0)
	// 			{
	// 				dfs(next_vertex, next_state, M,visited,master,out);
	// 				for (auto& p : M[{next_vertex, next_state}])
	// 					M[{vertex, state}].insert(p);
	// 				if (master[{ next_vertex, next_state }].first !=next_vertex||master[{ next_vertex, next_state }].second !=next_state){
	// 					master[{vertex,state}]=master[{ next_vertex, next_state }];
	// 				}
	// 			}else if (visited[{ next_vertex, next_state }]==1){
	// 				master[{vertex,state}]=master[{ next_vertex, next_state }];
	// 			}else if (visited[{ next_vertex, next_state }]==2){
	// 				for (auto& p : M[{next_vertex, next_state}])
	// 					M[{vertex, state}].insert(p);
	// 			}
	// 		}
	// 	}
	// }
	visited[{vertex,state}]=2;
}

void getStartPoints(unordered_set<string> &firstSet,unordered_set<string> &lastSet ,unordered_set<size_t>& startPoints) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::AT);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	auto t0 = std::chrono::steady_clock::now();
	unordered_set<size_t> firstP;
	for (auto& label : firstSet)
	{
		//cout<<"first:"<<label<<endl;
		startPoints.insert(Graph::startPoints[label].begin(), Graph::startPoints[label].end());
	}
	// for (auto&label : lastSet)
	// {
	// 	for (auto v : Graph::endPoints[label])
	// 	{
	// 		if (firstP.find(v) != firstP.end())
	// 			startPoints.insert(v);
	// 	}
	// }
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_start_points_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.get_start_points_calls++;
	g_query_timing_stats.get_start_points_total_vertices += static_cast<long long>(startPoints.size());
}

void get_basic_re(shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t> >& g,
	unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::AT);
	auto t0 = std::chrono::steady_clock::now();
	QueryPhaseTimer basic_nfa(QueryTimingPhase::BasicNfa);
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2> M;
    unordered_map<pair<size_t, shared_ptr<NFA_state> >, int, Hashfunc2, Equalfunc2> visited;
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, pair<size_t,shared_ptr<NFA_state> >, Hashfunc2, Equalfunc2> master;
    for (auto start_vertex : start_vertexes)
	{
		dfs(start_vertex, start_state, M, visited,master,out);
		g[start_vertex] = move(M[{start_vertex, start_state}]);
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_basic_re_batch_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	basic_nfa.stop();
	g_query_timing_stats.get_basic_re_batch_calls++;
	g_query_timing_stats.get_basic_re_batch_total_starts += static_cast<long long>(start_vertexes.size());
}

void get_basic_re(shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t> >& g,
	size_t start_vertex, unordered_set<size_t>& out)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::AT);
	auto t0 = std::chrono::steady_clock::now();
	QueryPhaseTimer basic_nfa(QueryTimingPhase::BasicNfa);
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2> M;
    unordered_map<pair<size_t, shared_ptr<NFA_state> >, int, Hashfunc2, Equalfunc2> visited;
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, pair<size_t,shared_ptr<NFA_state> >, Hashfunc2, Equalfunc2> master;
	dfs(start_vertex, start_state, M, visited,master,out);
	//cout<<start_state->toString()<<start_state->out1->toString()<<endl;
	g[start_vertex] = move(M[{start_vertex, start_state}]);
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_basic_re_single_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	basic_nfa.stop();
	g_query_timing_stats.get_basic_re_single_calls++;
}

void get_range_re(int min, int max, unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out, shared_ptr<NFA_state> base_start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	auto t0 = std::chrono::steady_clock::now();
	unordered_map<size_t, unordered_set<size_t> > temp_res1;
	unordered_map<size_t, unordered_set<size_t> > temp_res2;
	unordered_map<size_t, unordered_set<size_t> > temp_res3;
	//get_basic_re(rangeR->internal, g, Graph::vertices, basic_out);
	//std::cout<<"debug get_range_re:"<<min<<" "<<max<<endl;
	if (max >= 0)
	{
		g_query_timing_stats.exact_cache_begin_rss_kb = current_rss_kb();
		g_query_timing_stats.exact_cache_peak_rss_kb = g_query_timing_stats.exact_cache_begin_rss_kb;
		g_query_timing_stats.exact_cache_peak_entries = 0;
		g_query_timing_stats.exact_cache_peak_extra_bytes = 0;
		g_query_timing_stats.exact_cache_final_extra_bytes = 0;
		if (max == 0) {
			result = build_identity_relation(start_vertexes);
		} else if (min == max) {
			unordered_map<int, RelationMeta> power_cache;
			power_cache.emplace(1, build_relation_meta(g));
			update_exact_cache_observation(power_cache);
			const RelationMeta& exact = exact_power_cached(max, power_cache.at(1), power_cache);
			merge_relation_into(exact.rel, result);
			g_query_timing_stats.exact_cache_end_rss_kb = current_rss_kb();
			g_query_timing_stats.exact_cache_final_extra_bytes =
				static_cast<long long>(estimate_cache_extra_bytes(power_cache));
		} else if (min == 0) {
			result = build_bounded_suffix_relation(max, g, start_vertexes);
		} else {
			unordered_map<int, RelationMeta> power_cache;
			power_cache.emplace(1, build_relation_meta(g));
			update_exact_cache_observation(power_cache);
			const RelationMeta& prefix_exact = exact_power_cached(min, power_cache.at(1), power_cache);
			unordered_set<size_t> mids = collect_relation_dsts(prefix_exact.rel);
			if (!mids.empty()) {
				Relation suffix_base;
				unordered_set<size_t> suffix_out;
				get_basic_re(base_start_state, suffix_base, mids, suffix_out);
				Relation suffix = build_bounded_suffix_relation(max - min, suffix_base, mids);
				join_impl(prefix_exact.rel, suffix, result);
			}
			g_query_timing_stats.exact_cache_end_rss_kb = current_rss_kb();
			g_query_timing_stats.exact_cache_final_extra_bytes =
				static_cast<long long>(estimate_cache_extra_bytes(power_cache));
			if (g_query_timing_stats.exact_cache_peak_rss_kb < g_query_timing_stats.exact_cache_end_rss_kb) {
				g_query_timing_stats.exact_cache_peak_rss_kb = g_query_timing_stats.exact_cache_end_rss_kb;
			}
		}
		auto t1 = std::chrono::steady_clock::now();
		g_query_timing_stats.get_range_re_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
		g_query_timing_stats.get_range_re_calls++;
		return;
	}
	if (min == 0 && max == -1)
	{
		get_star_re(g, result,start_vertexes,out);
		return;
	}
	if (min == 0 && max == 1)
	{
		get_quora_re(g,result,start_vertexes,out);
		return;
	}
	if (min == 0 && max == 0)
	{
		for (auto& kv : g)
			result[kv.first].insert(kv.first);
		return;
	}
	if (min == 1 && max == 1)
	{
		result = move(g);
		return;
	}
	//{0,x}
	if (min == 0)
	{
		get_quora_re(g);
		range_join(max, g, result);
	}
		//{x,}
		else if (max == -1)
		{
#if RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC
			get_open_range_re_scc_rewrite(min, g, result, start_vertexes, out);
#else
			Relation prefix_exact;
			Relation closure_star;
			if (min > 1)
			{
				range_join(min, g, temp_res1);
			prefix_exact = temp_res1;
			get_star_re(g, temp_res3,start_vertexes,out);
			closure_star = temp_res3;
			join(temp_res1, temp_res3, result);
		}
		else {
			range_join(1, g, temp_res1);
			prefix_exact = temp_res1;
			get_star_re(g, temp_res3,start_vertexes,out);
			closure_star = temp_res3;
			join(g, temp_res3, result);
		}
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
		std::cerr << "[range-single][rewrite] B{" << min << ",inf}"
		          << " => B^" << min << " o B*" << std::endl;
		std::cerr << "[range-single][rewrite] prefix_exact_pairs="
		          << relation_pair_count(prefix_exact)
		          << " star_pairs=" << relation_pair_count(closure_star)
		          << " final_pairs=" << relation_pair_count(result)
		          << std::endl;
			std::cerr << "[range-single][rewrite] prefix_source_rows="
			          << prefix_exact.size()
			          << " star_source_rows=" << closure_star.size()
			          << " final_source_rows=" << result.size()
			          << std::endl;
#endif
#endif
		}
	//{x,x}
	else if (min == max)
	{
		range_join(min, g, result);
	}
	//{x,y}
	else{
		unordered_map<size_t, unordered_set<size_t> > exact_one = g;
		if (min > 1) {
            // cout<<"debug:11111"<<endl;
			range_join(min, g, temp_res1);
            // for (auto kv:temp_res1) {
            //     cout<<"debug_get_range_re:"<<kv.first<<endl;
            //     for(auto v:kv.second){
            //         cout<<"\t "<<"debug_get_range_re:"<<v<<endl;
            //     }
            // }
			get_quora_re(g);
			if (max - min > 1)
			{
				range_join(max - min, g, temp_res3);
				join(temp_res1, temp_res3, result);
			}
			else {
				join(temp_res1, g, result);
			}
		}
		else {
			get_quora_re(g);
			if (max - min > 1)
			{
				range_join(max - min, g, temp_res3);
				join(exact_one, temp_res3, result);
			}
			else {
				join(exact_one, g, result);
			}
		}
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_range_re_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.get_range_re_calls++;
}

void range_join(int num, unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result)
{
	auto t0 = std::chrono::steady_clock::now();
	if (num == 0)
	{
		for (auto& kv : g)
			result[kv.first].insert(kv.first);
	}
	else if (num == 1)
	{
		result = g;
	}
	else {
		unordered_map<int, RelationMeta> power_cache;
		power_cache.emplace(1, build_relation_meta(g));
		result = exact_power_cached(num, power_cache.at(1), power_cache).rel;
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.range_join_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.range_join_calls++;
}

void self_join(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result)
{
	auto t0 = std::chrono::steady_clock::now();
	self_join_impl(g, result);
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.self_join_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.self_join_calls++;
}

void self_join(unordered_map<size_t, unordered_set<size_t> >& g)
{
	auto t0 = std::chrono::steady_clock::now();
	unordered_map<size_t, unordered_set<size_t> > result;
	self_join_impl(g, result);
	g.clear();
	for (auto kv : result) {
		auto key = kv.first;
		auto set = kv.second;
		g[key] = move(set);
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.self_join_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.self_join_calls++;
}

void join(unordered_map<size_t, unordered_set<size_t> >& g1, unordered_map<size_t, unordered_set<size_t> >& g2, unordered_map<size_t, unordered_set<size_t> >& result)
{
	auto t0 = std::chrono::steady_clock::now();
	join_impl(g1, g2, result);
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.join_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.join_calls++;
}

// void get_basic_related_re(shared_ptr<Regex> r, unordered_map<size_t, unordered_set<size_t> >& g,
// 	unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out)
// {
// 	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2> M;
// 	shared_ptr<NFA_state> start_state = make_shared<NFA_state>(Type::Blank);
// 	start_state->out1 = r->start;
// 	queue<size_t> Q;
// 	for (auto start_vertex : start_vertexes)
// 	{
// 		Q.push(start_vertex);
// 	}
// 	while (!Q.empty())
// 	{
// 		size_t v = Q.front();
// 		Q.pop();
// 		dfs(v, start_state, M, out);
// 		g[v] = move(M[{v, start_state}]);
// 	}
// }

void get_quora_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out)
{
	auto t0 = std::chrono::steady_clock::now();
	for (const auto& kv : g) {
		auto& key = kv.first;
		g[key].insert(key);
		out.insert(key);
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_quora_re_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.get_quora_re_calls++;
}

void create_scc_graph(TarjanSCC &tarjan, std::unordered_map<size_t, std::unordered_set<size_t> >& adjacency_map,std::unordered_map<size_t, std::unordered_set<size_t> >& scc_adjacency_map) {
	for (int i=0;i<tarjan.sccs.size();i++)
	{
		scc_adjacency_map[i] = {};
		for (auto u : tarjan.sccs[i])
		{
			for (auto v : adjacency_map[u])
			{
				auto ni = tarjan.scc_ids[v];
				if (ni != i)
					scc_adjacency_map[i].insert(ni);
			}
		}
	}
}

void dfs(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, size_t v)
{
	result[v] = {};
	for (auto& nv : g[v])
	{
		if(result.find(nv)==result.end())
		{
			dfs(g, result, nv);
		}
		result[v].insert(nv);
		result[v].insert(result[nv].begin(),result[nv].end());
	}
}

void get_star_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out)
{
	auto t0 = std::chrono::steady_clock::now();
	unordered_map<size_t, std::unordered_set<size_t> > scc_adjacency_map;
	unordered_map<size_t, std::unordered_set<size_t> > scc_result;

	TarjanSCC tarjan(g);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	std::cerr << "[range-single][star] computing SCC closure:"
	          << " graph_rows=" << g.size()
	          << " start_vertices=" << start_vertexes.size()
	          << " scc_count=" << tarjan.sccs.size()
	          << std::endl;
#endif
	create_scc_graph(tarjan, g, scc_adjacency_map);
	for (auto& pair : scc_adjacency_map)
	{
		auto& v = pair.first;
		if (scc_result.find(v) == scc_result.end())
			dfs(scc_adjacency_map, scc_result, v);
	}
	for (auto& v : start_vertexes)
	{
		auto &key = tarjan.scc_ids[v];
		for (auto& value : scc_result[key])
		{
			result[v].insert(tarjan.sccs[value].begin(), tarjan.sccs[value].end());
		}
		result[v].insert(tarjan.sccs[key].begin(), tarjan.sccs[key].end());
		result[v].insert(v);
	}
	for (auto& kv : result)
	{
		out.insert(kv.second.begin(), kv.second.end());
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_star_re_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.get_star_re_calls++;
}

// void get_plus_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out)
// {
// 	unordered_map<size_t, std::unordered_set<size_t> > scc_adjacency_map;
// 	unordered_map<size_t, std::unordered_set<size_t> > scc_result;

// 	TarjanSCC tarjan(g);
// 	create_scc_graph(tarjan, g, scc_adjacency_map);
// 	for (auto& pair : scc_adjacency_map)
// 	{
// 		auto& v = pair.first;
// 		if (scc_result.find(v) == scc_result.end())
// 			dfs(scc_adjacency_map, scc_result, v);
// 	}
// 	for (auto& v : start_vertexes)
// 	{
// 		auto& key = tarjan.scc_ids[v];
// 		for (auto& value : scc_result[key])
// 		{
// 			result[v].insert(tarjan.sccs[value].begin(), tarjan.sccs[value].end());
// 		}
// 		if(tarjan.sccs[key].size()>1)
// 			result[v].insert(tarjan.sccs[key].begin(), tarjan.sccs[key].end());
// 	}
// 	for (auto& kv : result)
// 	{
// 		out.insert(kv.second.begin(), kv.second.end());
// 	}
// }

void get_quora_re(unordered_map<size_t, unordered_set<size_t> >& g)
{
	auto t0 = std::chrono::steady_clock::now();
	unordered_set<size_t> identity_vertices;
	for (const auto& kv : g) {
		identity_vertices.insert(kv.first);
		identity_vertices.insert(kv.second.begin(), kv.second.end());
	}
	for (const auto& v : identity_vertices) {
		g[v].insert(v);
	}
	auto t1 = std::chrono::steady_clock::now();
	g_query_timing_stats.get_quora_re_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.get_quora_re_calls++;
}
