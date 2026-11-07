#include "NFA_state.h"
#include "Regex.h"
#include "graph.h"

struct QueryTimingStats {
	double basic_nfa_time_ms = 0.0;
	long long get_start_points_ms = 0;
	long long get_start_points_calls = 0;
	long long get_start_points_total_vertices = 0;

	long long get_basic_re_batch_ms = 0;
	long long get_basic_re_batch_calls = 0;
	long long get_basic_re_batch_total_starts = 0;

	long long get_basic_re_single_ms = 0;
	long long get_basic_re_single_calls = 0;

	long long get_range_re_ms = 0;
	long long get_range_re_calls = 0;

	long long range_join_ms = 0;
	long long range_join_calls = 0;

	long long join_ms = 0;
	long long join_calls = 0;

	long long self_join_ms = 0;
	long long self_join_calls = 0;

	long long get_quora_re_ms = 0;
	long long get_quora_re_calls = 0;

	long long get_star_re_ms = 0;
	long long get_star_re_calls = 0;

	long long count_init_map_ms = 0;
	long long count_init_map_calls = 0;
	long long count_init_map_get_start_points_ms = 0;
	long long count_init_map_basic_re_ms = 0;
	long long count_init_map_range_re_ms = 0;

	long long count_range_dfs_ms = 0;
	long long count_range_dfs_calls = 0;

	long long range_complex_hits = 0;
	long long range_simple_hits = 0;

	long long exact_cache_begin_rss_kb = -1;
	long long exact_cache_end_rss_kb = -1;
	long long exact_cache_peak_rss_kb = -1;
	long long exact_cache_peak_entries = 0;
	long long exact_cache_peak_extra_bytes = 0;
	long long exact_cache_final_extra_bytes = 0;
};

struct RelationStats {
	string relation_name;
	size_t pair_count = 0;
	size_t nonempty_rows = 0;
	double avg_row_size = 0.0;
	size_t max_row_size = 0;
};

struct QueryExperimentSummary {
	string dataset;
	string query_id;
	string query_class;
	string expression;

	double total_time_ms = 0.0;
	double query_peak_memory_mb = -1.0;
	double gcnp_planning_time_ms = 0.0;
	double relation_merge_time_ms = 0.0;
	double other_time_ms = 0.0;
	double incremental_cache_time_ms = 0.0;
	double relation_format_conversion_time_ms = 0.0;
	double result_materialization_time_ms = 0.0;
	double relation_stats_time_ms = 0.0;
	double composition_merge_time_ms = 0.0;
	double relation_union_time_ms = 0.0;

	RelationStats body_stats;

	size_t num_compositions = 0;
	string max_relation_name;
	size_t max_relation_pairs = 0;
	size_t total_materialized_relation_pairs = 0;
	size_t total_composition_input_pairs = 0;
	size_t total_candidate_pairs = 0;

	size_t blpc_L = 0;
	size_t blpc_U = 0;
	double blpc_repetition_rate = 0.0;
	double blpc_weighted_saving_rate = 0.0;

	double exact_prefix_time_ms = 0.0;
	double bounded_or_closure_time_ms = 0.0;
	double final_compose_time_ms = 0.0;
};

extern QueryTimingStats g_query_timing_stats;

void ResetQueryTimingStats();
void PrintQueryTimingStats();
const QueryTimingStats& GetQueryTimingStats();
void BeginQueryExperiment(const string& dataset, const string& query_id,
	const string& query_class, const string& expression);
void RecordQueryPeakMemoryMb(double peak_memory_mb);
void RecordBodyRelationStats(const string& relation_name,
	size_t pair_count, size_t nonempty_rows, size_t max_row_size);
void RecordMaterializedRelationStats(const string& relation_name,
	size_t pair_count, size_t nonempty_rows, size_t max_row_size);
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
	double gcnp_planning_time_ms = 0.0,
	double relation_merge_time_ms = 0.0);
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
	size_t final_output_pairs);
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
	double s_union_time_ms);
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
	size_t closure_iterations = 0);
void FinalizeQueryExperiment(double total_time_ms);
const QueryExperimentSummary& GetQueryExperimentSummary();

void dfs(size_t vertex, shared_ptr<NFA_state> state,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2>& M,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, int, Hashfunc2, Equalfunc2>& visited,
	unordered_map<pair<size_t, shared_ptr<NFA_state> >, pair<size_t,shared_ptr<NFA_state> >, Hashfunc2, Equalfunc2>& master,
	unordered_set<size_t>& out);
void getStartPoints(unordered_set<string> &firstSet,unordered_set<string> &lastSet ,unordered_set<size_t>& startPoints);
void get_basic_re(shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t> >& g,
	unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
void get_basic_re(shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t> >& g,
	size_t start_vertex, unordered_set<size_t>& out);
void get_star_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
void get_range_re(int min,int max,unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out, shared_ptr<NFA_state> base_start_state);
void range_join(int num, unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result);
void self_join(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result);
void self_join(unordered_map<size_t, unordered_set<size_t> >& g);
void join(unordered_map<size_t, unordered_set<size_t> >& g1, unordered_map<size_t, unordered_set<size_t> >& g2, unordered_map<size_t, unordered_set<size_t> >& result);

//void get_basic_related_re(shared_ptr<Regex> r, unordered_map<size_t, unordered_set<size_t> >& g, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
void get_quora_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
void create_scc_graph(TarjanSCC& tarjan, std::unordered_map<size_t, std::unordered_set<size_t> >& adjacency_map, std::unordered_map<size_t, std::unordered_set<size_t> >& scc_adjacency_map);
void dfs(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, size_t v);
//void get_star_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
//void get_plus_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& start_vertexes, unordered_set<size_t>& out);
void get_quora_re(unordered_map<size_t, unordered_set<size_t> >& g);
//void get_plus_re(unordered_map<size_t, unordered_set<size_t> >& g, unordered_map<size_t, unordered_set<size_t> >& result);
//void regular_path_query(shared_ptr<Regex> r, unordered_map<size_t, unordered_set<size_t> >& g);
