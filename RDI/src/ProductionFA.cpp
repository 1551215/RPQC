#include <cmath>
#include <ctime>
#include <cstdlib>
#include <memory>
#include <queue>
#include <unordered_set>
#include"ProductionFA.h"
#include "NestedCountEvaluator.h"
#include "NFA_state.h"
#include "Parser.h"
#include "graph.h"
#include "Traversal.h"
#include "UnionTrace.h"
#include "RelationOperationMetrics.h"

namespace {

using DenseRelation = unordered_map<size_t, unordered_set<size_t>>;
using Relation = RoaringRelation;
// Product-automaton frontier: original query source -> current graph vertices.
// Counted subexpressions are installed as virtual adjacency rows and advance
// this frontier exactly like ordinary labeled edges.
using ActiveFrontier = RoaringRelation;

struct TransitionRowCache {
	Relation rows;
	unordered_set<size_t> computed_sources;
};

struct AutomatonState;

struct TransitionOp {
	shared_ptr<Regex> regex;
	string name;
	shared_ptr<TransitionRowCache> cache = make_shared<TransitionRowCache>();
};

struct AutomatonTransition {
	shared_ptr<TransitionOp> op;
	AutomatonState* target = nullptr;
};

struct AutomatonState {
	int id = -1;
	bool accepting = false;
	vector<AutomatonState*> epsilon;
	vector<AutomatonTransition> transitions;
};

struct AutomatonFragment {
	AutomatonState* start = nullptr;
	AutomatonState* accept = nullptr;
};

struct AutomatonNFA {
	vector<unique_ptr<AutomatonState>> states;
	AutomatonState* start = nullptr;
	AutomatonState* accept = nullptr;
	int next_id = 0;

	AutomatonState* make_state(bool accepting = false)
	{
		auto state = std::make_unique<AutomatonState>();
		state->id = next_id++;
		state->accepting = accepting;
		AutomatonState* raw = state.get();
		states.push_back(std::move(state));
		return raw;
	}
};

void union_relations(
	const DenseRelation& src,
	DenseRelation& dst)
{
	RelationUnionTimer timer("materialized_relation_union");
	for (const auto& kv : src) {
		auto& row = dst[kv.first];
		row.insert(kv.second.begin(), kv.second.end());
	}
}

unordered_set<size_t> collect_targets(
	const Relation& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	unordered_set<size_t> targets;
	for (const auto& kv : rel) {
		for (uint32_t target : kv.second) {
			targets.insert(static_cast<size_t>(target));
		}
	}
	return targets;
}

unordered_set<size_t> collect_all_graph_vertices()
{
	unordered_set<size_t> vertices;
	for (const auto& kv : Graph::adjacency_map) {
		vertices.insert(kv.first);
		for (const auto& label_targets : kv.second) {
			vertices.insert(label_targets.second.begin(), label_targets.second.end());
		}
	}
	return vertices;
}

shared_ptr<Regex> clone_as_standalone_regex(const shared_ptr<Regex>& r)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::AC);
	shared_ptr<Regex> cloned = r->copy();
	Parser::patchMatch(cloned);
	return cloned;
}

DenseRelation build_identity_relation(const unordered_set<size_t>& sources)
{
	DenseRelation identity;
	for (size_t src : sources) {
		identity[src].insert(src);
	}
	return identity;
}

Relation build_roaring_identity_relation(const unordered_set<size_t>& sources)
{
	Relation identity;
	for (size_t src : sources) {
		identity.emplace(src, roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(src)));
	}
	return identity;
}

ActiveFrontier build_active_identity_frontier(
	const unordered_set<size_t>& sources)
{
	ActiveFrontier frontier;
	for (size_t source : sources) {
		frontier.emplace(
			source,
			roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(source)));
	}
	return frontier;
}

unordered_set<size_t> collect_active_vertices(const ActiveFrontier& frontier)
{
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	unordered_set<size_t> vertices;
	vertices.reserve(frontier.size() * 2 + 1);
	for (const auto& entry : frontier) {
		for (uint32_t vertex : entry.second) vertices.insert(vertex);
	}
	return vertices;
}

ActiveFrontier advance_virtual_transition(
	const ActiveFrontier& frontier,
	const Relation& transition_rows,
	const string& transition_name)
{
	ActiveFrontier next;
	const bool trace = UnionTrace::enabled();
	uint64_t right_pairs = 0, added_pairs = 0;
	RelationUnionTimer merge_timer("macro_transition_union");
	for (const auto& active : frontier) {
		roaring::Roaring target;
		for (uint32_t vertex : active.second) {
			auto row = transition_rows.find(vertex);
			if (row == transition_rows.end()) continue;
			const uint64_t before = trace ? target.cardinality() : 0;
			target |= row->second;
			if (trace) {
				right_pairs += row->second.cardinality();
				added_pairs += target.cardinality() - before;
			}
		}
		if (!target.isEmpty()) next.emplace(active.first, std::move(target));
	}
	merge_timer.stop();
	if (trace) {
		const double elapsed = merge_timer.elapsed_ms();
		UnionTrace::record("macro_transition_union", transition_name,
			0, right_pairs, added_pairs, elapsed);
	}
	return next;
}

bool merge_state_rows(Relation& dst, const Relation& src,
	const string& context)
{
	bool changed = false;
	const bool trace = UnionTrace::enabled();
	uint64_t left_pairs = 0, right_pairs = 0, added_pairs = 0;
	if (trace) {
		for (const auto& kv : dst) left_pairs += kv.second.cardinality();
	}
	RelationUnionTimer merge_timer("macro_state_union");
	for (const auto& kv : src) {
		auto& row = dst[kv.first];
		const uint64_t before = row.cardinality();
		row |= kv.second;
		const uint64_t added = row.cardinality() - before;
		if (added != 0) {
			changed = true;
		}
		if (trace) {
			right_pairs += kv.second.cardinality();
			added_pairs += added;
		}
	}
	merge_timer.stop();
	if (trace) {
		const double elapsed = merge_timer.elapsed_ms();
		UnionTrace::record("macro_state_union", context,
			left_pairs, right_pairs, added_pairs, elapsed);
	}
	return changed;
}

bool merge_all_state_rows(
	unordered_map<AutomatonState*, Relation>& state_rows,
	AutomatonState* state,
	const Relation& src,
	const string& context)
{
	return merge_state_rows(state_rows[state], src,
		context + "_state_" + std::to_string(state->id));
}

bool is_macro_atomic_regex(const shared_ptr<Regex>& r)
{
	return r
		&& r->type != Regex::RegexType::Sequence
		&& r->type != Regex::RegexType::Choice
		&& r->type != Regex::RegexType::Blank;
}

bool contains_nested_count(const shared_ptr<Regex>& r, bool inside_count = false)
{
	if (!r) return false;
	switch (r->type) {
	case Regex::RegexType::RangeRepetition: {
		auto range = dynamic_pointer_cast<RangeRepetition>(r);
		return inside_count || contains_nested_count(range->internal, true);
	}
	case Regex::RegexType::Sequence: {
		auto sequence = dynamic_pointer_cast<Sequence>(r);
		return contains_nested_count(sequence->first, inside_count)
			|| contains_nested_count(sequence->second, inside_count);
	}
	case Regex::RegexType::Choice: {
		auto choice = dynamic_pointer_cast<Choice>(r);
		return contains_nested_count(choice->thisOne, inside_count)
			|| contains_nested_count(choice->thatOne, inside_count);
	}
	case Regex::RegexType::PlusRepetition:
		return contains_nested_count(
			dynamic_pointer_cast<PlusRepetition>(r)->internal, inside_count);
	case Regex::RegexType::StarRepetition:
		return contains_nested_count(
			dynamic_pointer_cast<StarRepetition>(r)->internal, inside_count);
	case Regex::RegexType::QuoraRepetition:
		return contains_nested_count(
			dynamic_pointer_cast<QuoraRepetition>(r)->internal, inside_count);
	default:
		return false;
	}
}

const char* get_hybrid_eval_mode()
{
	const char* mode = std::getenv("BLPC_HYBRID_EVAL_MODE");
	return (mode && mode[0] != '\0') ? mode : "macro";
}

bool use_materialize_scheduler_mode()
{
	return std::string(get_hybrid_eval_mode()) == "materialize";
}

shared_ptr<TransitionOp> build_transition_op(const shared_ptr<Regex>& r)
{
	auto op = make_shared<TransitionOp>();
	op->regex = r;
	op->name = r ? r->toString() : "blank";
	return op;
}

AutomatonFragment build_macro_fragment(AutomatonNFA& nfa, const shared_ptr<Regex>& r)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::AC);
	if (!r) {
		AutomatonState* s = nfa.make_state();
		AutomatonState* t = nfa.make_state();
		s->epsilon.push_back(t);
		return {s, t};
	}

	if (r->type == Regex::RegexType::Blank) {
		AutomatonState* s = nfa.make_state();
		AutomatonState* t = nfa.make_state();
		s->epsilon.push_back(t);
		return {s, t};
	}

	if (r->type == Regex::RegexType::Choice) {
		shared_ptr<Choice> choice = dynamic_pointer_cast<Choice>(r);
		AutomatonFragment left = build_macro_fragment(nfa, choice->thisOne);
		AutomatonFragment right = build_macro_fragment(nfa, choice->thatOne);
		AutomatonState* s = nfa.make_state();
		AutomatonState* t = nfa.make_state();
		s->epsilon.push_back(left.start);
		s->epsilon.push_back(right.start);
		left.accept->epsilon.push_back(t);
		right.accept->epsilon.push_back(t);
		return {s, t};
	}

	if (r->type == Regex::RegexType::Sequence) {
		shared_ptr<Sequence> seq = dynamic_pointer_cast<Sequence>(r);
		AutomatonFragment first = build_macro_fragment(nfa, seq->first);
		AutomatonFragment second = build_macro_fragment(nfa, seq->second);
		first.accept->epsilon.push_back(second.start);
		return {first.start, second.accept};
	}

	AutomatonState* s = nfa.make_state();
	AutomatonState* t = nfa.make_state();
	s->transitions.push_back(AutomatonTransition{build_transition_op(r), t});
	return {s, t};
}

void eval_atomic_transition_rows(
	const shared_ptr<Regex>& atom,
	const unordered_set<size_t>& sources,
	DenseRelation& result,
	NestedCountEvaluator& nested_evaluator)
{
	result.clear();
	if (!atom || sources.empty()) {
		return;
	}

	if (atom->type == Regex::RegexType::RangeRepetition) {
		Relation rows;
		nested_evaluator.evaluate(atom, sources, rows);
		for (const auto& entry : rows) {
			for (uint32_t dst : entry.second) result[entry.first].insert(dst);
		}
		return;
	}

	shared_ptr<Regex> standalone = clone_as_standalone_regex(atom);
	unordered_set<size_t> out;
	unordered_set<size_t> source_copy = sources;
	// get_basic_re expects the parser's epsilon entry before the first atom.
	// A standalone clone starts directly at its Basic state; without this
	// wrapper the Basic state is mistaken for an already-matched state.
	shared_ptr<NFA_state> entry;
	{
		AlgorithmPhaseTimer construction(AlgorithmPhase::AC);
		entry = make_shared<NFA_state>(Type::Blank, standalone->start, nullptr);
	}
	get_basic_re(entry, result, source_copy, out);
}

void eval_atomic_transition_rows(
	const shared_ptr<Regex>& atom,
	const unordered_set<size_t>& sources,
	DenseRelation& result)
{
	NestedCountEvaluator nested_evaluator;
	eval_atomic_transition_rows(atom, sources, result, nested_evaluator);
}

void ensure_transition_rows_cached(
	const shared_ptr<TransitionOp>& op,
	const unordered_set<size_t>& sources,
	NestedCountEvaluator& nested_evaluator)
{
	AlgorithmPhaseTimer phase(op->regex && op->regex->type == Regex::RegexType::RangeRepetition
		? AlgorithmPhase::MP : AlgorithmPhase::AT);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	unordered_set<size_t> missing_sources;
	for (size_t src : sources) {
		if (!op->cache->computed_sources.count(src)) {
			missing_sources.insert(src);
		}
	}
	if (missing_sources.empty()) {
		return;
	}
	{
	QueryPhaseTimer logging(QueryTimingPhase::Other);
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	cout << "[HybridEval][frontier] transition=" << op->name
		<< " requested_sources=" << sources.size()
		<< " missing_sources=" << missing_sources.size()
		<< " cached_sources=" << (sources.size() - missing_sources.size())
		<< endl;
	}

	if (op->regex->type == Regex::RegexType::RangeRepetition) {
		Relation rows;
		nested_evaluator.evaluate(op->regex, missing_sources, rows);
		for (size_t src : missing_sources) op->cache->computed_sources.insert(src);
		MoveDisjointRelationRows(rows, op->cache->rows);
		return;
	}
	DenseRelation newly_computed_rows;
	eval_atomic_transition_rows(
		op->regex, missing_sources, newly_computed_rows, nested_evaluator);
	QueryPhaseTimer conversion(QueryTimingPhase::FormatConversion);
	for (size_t src : missing_sources) {
		op->cache->computed_sources.insert(src);
		auto found = newly_computed_rows.find(src);
		if (found != newly_computed_rows.end() && !found->second.empty()) {
			roaring::Roaring row;
			for (size_t dst : found->second) {
				row.add(static_cast<uint32_t>(dst));
			}
			op->cache->rows.emplace(src, std::move(row));
		}
	}
}

}

Production_FA::Production_FA(string E) {
	AlgorithmPhaseTimer phase(AlgorithmPhase::AC);
	shared_ptr<Parser>p = make_shared<Parser>(E);
	shared_ptr<Regex> re = p->re;
	this->re=re;
	this->p=p;
	this->E = E;
	this->startState = re->start;
}

void Production_FA::init_range_repetitions(){
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	for(auto t:p->rangeRepetitions){
		//cout<<"debug init_range_repetitions:"<<t->toString()<<endl;
		shared_ptr<Count_state> count_state=dynamic_pointer_cast<Count_state>(t->start);
		//cout<<t->internal->start->toString()<<endl;
		if (count_state->use_lazy_exact_cache()
			|| count_state->use_lazy_open_range_cache()
			|| count_state->use_lazy_bounded_range_cache()) {
			continue;
		}
		if (count_state->complex){
			count_state->init_map(t->min, t->max, count_state->internal_start, t->firstSet->labels, t->lastSet->labels);
		}
	}
}

bool Production_FA::contains_range_repetition(const shared_ptr<Regex>& r) const
{
	if (!r) {
		return false;
	}
	switch (r->type) {
	case Regex::RegexType::RangeRepetition:
		return true;
	case Regex::RegexType::Sequence: {
		shared_ptr<Sequence> seq = dynamic_pointer_cast<Sequence>(r);
		return seq && (contains_range_repetition(seq->first) || contains_range_repetition(seq->second));
	}
	case Regex::RegexType::Choice: {
		shared_ptr<Choice> choice = dynamic_pointer_cast<Choice>(r);
		return choice && (contains_range_repetition(choice->thisOne) || contains_range_repetition(choice->thatOne));
	}
	default:
		return false;
	}
}

bool Production_FA::should_use_macro_scheduler(const shared_ptr<Regex>& r) const
{
	switch (r->type) {
	case Regex::RegexType::Sequence: {
		shared_ptr<Sequence> seq = dynamic_pointer_cast<Sequence>(r);
		return seq && (contains_range_repetition(seq->first) || contains_range_repetition(seq->second));
	}
	case Regex::RegexType::Choice: {
		shared_ptr<Choice> choice = dynamic_pointer_cast<Choice>(r);
		return choice && (contains_range_repetition(choice->thisOne) || contains_range_repetition(choice->thatOne));
	}
	default:
		return false;
	}
}

void Production_FA::compute_via_macro_scheduler(
	const unordered_set<size_t>& sources,
	RoaringRelation& result)
{
	result.clear();
	if (sources.empty()) {
		return;
	}

	AutomatonNFA nfa;
	NestedCountEvaluator nested_evaluator;
	AutomatonFragment frag = build_macro_fragment(nfa, this->re);
	nfa.start = frag.start;
	nfa.accept = frag.accept;
	nfa.accept->accepting = true;

	unordered_map<AutomatonState*, ActiveFrontier> state_rows;
	queue<AutomatonState*> worklist;
	unordered_set<AutomatonState*> in_queue;

	state_rows[nfa.start] = build_active_identity_frontier(sources);
	worklist.push(nfa.start);
	in_queue.insert(nfa.start);

	while (!worklist.empty()) {
		AutomatonState* state = worklist.front();
		worklist.pop();
		in_queue.erase(state);

			ActiveFrontier current_rows = std::move(state_rows[state]);
		state_rows.erase(state);
		for (AutomatonState* eps : state->epsilon) {
			if (merge_all_state_rows(state_rows, eps, current_rows, "epsilon") && !in_queue.count(eps)) {
				worklist.push(eps);
				in_queue.insert(eps);
			}
		}

		for (const auto& tr : state->transitions) {
				const unordered_set<size_t> frontier_sources =
					collect_active_vertices(current_rows);
			if (frontier_sources.empty()) {
				continue;
			}
			ensure_transition_rows_cached(
				tr.op, frontier_sources, nested_evaluator);
			const Relation& transition_rows = tr.op->cache->rows;
			if (transition_rows.empty()) {
				continue;
			}
				ActiveFrontier composed_rows =
					advance_virtual_transition(current_rows, transition_rows, tr.op->name);
			if (merge_all_state_rows(state_rows, tr.target, composed_rows, "transition") && !in_queue.count(tr.target)) {
				worklist.push(tr.target);
				in_queue.insert(tr.target);
			}
		}
		if (state == nfa.accept) {
				QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
				for (const auto& active : current_rows) {
					result[active.first] |= active.second;
				}
		}
	}
}

void Production_FA::compute_materialized_relation_for_regex(
	const shared_ptr<Regex>& r,
	unordered_map<size_t, unordered_set<size_t>>& result)
{
	result.clear();
	if (!r) {
		return;
	}

	if (r->type == Regex::RegexType::Blank) {
		const unordered_set<size_t> vertices = collect_all_graph_vertices();
		result = build_identity_relation(vertices);
		return;
	}

	if (r->type == Regex::RegexType::Sequence) {
		shared_ptr<Sequence> seq = dynamic_pointer_cast<Sequence>(r);
		DenseRelation left_rows;
		DenseRelation right_rows;
		compute_materialized_relation_for_regex(seq->first, left_rows);
		compute_materialized_relation_for_regex(seq->second, right_rows);
		if (left_rows.empty() || right_rows.empty()) {
			result.clear();
			return;
		}
		join(left_rows, right_rows, result);
		return;
	}

	if (r->type == Regex::RegexType::Choice) {
		shared_ptr<Choice> choice = dynamic_pointer_cast<Choice>(r);
		DenseRelation left_rows;
		DenseRelation right_rows;
		compute_materialized_relation_for_regex(choice->thisOne, left_rows);
		compute_materialized_relation_for_regex(choice->thatOne, right_rows);
		result = std::move(left_rows);
		union_relations(right_rows, result);
		return;
	}

	unordered_set<size_t> start_vertexes;
	if (r->firstSet->empty) {
		start_vertexes = collect_all_graph_vertices();
	} else {
		getStartPoints(r->firstSet->labels, r->lastSet->labels, start_vertexes);
	}

	eval_atomic_transition_rows(r, start_vertexes, result);
}

void Production_FA::compute_via_materialize_scheduler(
	const unordered_set<size_t>& sources,
	unordered_map<size_t, unordered_set<size_t>>& result)
{
	compute_materialized_relation_for_regex(this->re, result);

	// Materializing subexpressions evaluates their rows over the whole graph.
	// The outer query still has the same start-vertex restriction as the normal
	// scheduler, so discard rows whose source is not reachable by the root's
	// first label set before returning the result.
	for (auto it = result.begin(); it != result.end();) {
		if (sources.find(it->first) == sources.end()) {
			it = result.erase(it);
		} else {
			++it;
		}
	}
}

void Production_FA::compute_RPQ(unordered_map<size_t, unordered_set<size_t>>& result){
	AlgorithmPhaseTimer phase(AlgorithmPhase::AT);
	bitmap_result.clear();
	has_bitmap_result = false;
	auto publish_bitmap = [&]() {
		has_bitmap_result = true;
		const char* export_pairs = std::getenv("RDI_EXPORT_PAIRS");
		if (export_pairs && std::string(export_pairs) == "1") {
			for (const auto& row : bitmap_result)
				for (uint32_t dst : row.second) result[row.first].insert(dst);
		}
	};
	unordered_set<size_t> out;
	unordered_set<size_t> start_vertexes;
	//cout<<"come in compute_RPQ"<<endl;
	//clock_t st=clock();
	if (this->re->firstSet->empty) {
		for (const auto& kv : Graph::adjacency_map) {
			start_vertexes.insert(kv.first);
		}
	} else {
		getStartPoints(this->re->firstSet->labels, this->re->lastSet->labels, start_vertexes);
	}
	//clock_t getStartPointsTime=clock();
	//cout<<"get startPoints time"<<getStartPointsTime-st<<endl;
	//cout<<start_vertexes.size()<<endl;
	{
		AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
		cout<<"come in compute_RPQ"<<endl;
	}
	if (contains_nested_count(this->re)) {
		{
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			cout << "[NestedCount] recursive macro-state execution" << endl;
		}
		compute_via_macro_scheduler(start_vertexes, bitmap_result);
		publish_bitmap();
		return;
	}
	shared_ptr<RangeRepetition> top_range_candidate;
	if (!this->p->rangeRepetitions.empty()) {
		shared_ptr<RangeRepetition> top_range = this->p->rangeRepetitions.back();
		if (top_range.get() == this->re.get()) {
			top_range_candidate = top_range;
		} else {
			shared_ptr<Sequence> seq_root = dynamic_pointer_cast<Sequence>(this->re);
			if (seq_root
				&& seq_root->first
				&& seq_root->first->type == Regex::RegexType::Blank
				&& seq_root->second
				&& seq_root->second.get() == top_range.get()) {
				top_range_candidate = top_range;
			}
		}
		if (top_range_candidate) {
			shared_ptr<Count_state> count_state = dynamic_pointer_cast<Count_state>(top_range_candidate->start);
			if (count_state && count_state->use_lazy_exact_cache()) {
				count_state->build_exact_rows_for_sources(start_vertexes, top_range_candidate->min, count_state->internal_start, result, &bitmap_result);
				publish_bitmap();
				return;
			}
			if (count_state && count_state->use_lazy_open_range_cache()) {
				count_state->build_open_range_rows_for_sources(start_vertexes, top_range_candidate->min, count_state->internal_start, result, &bitmap_result);
				publish_bitmap();
				return;
			}
			if (count_state && count_state->use_lazy_bounded_range_cache()) {
				count_state->build_bounded_range_rows_for_sources(
					start_vertexes,
					top_range_candidate->min,
					top_range_candidate->max,
					count_state->internal_start,
					result, &bitmap_result);
				publish_bitmap();
				return;
			}
		}
	}
	if (should_use_macro_scheduler(this->re)) {
		{
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			cout << "[HybridEval] mode=" << get_hybrid_eval_mode() << endl;
		}
		if (use_materialize_scheduler_mode()) {
			compute_via_materialize_scheduler(start_vertexes, result);
			return;
		}
		compute_via_macro_scheduler(start_vertexes, bitmap_result);
		publish_bitmap();
		return;
	}
	get_basic_re(this->startState, result,start_vertexes, out);
	//clock_t getBasicReTime=clock();
	//cout<<"get basic re time"<<getBasicReTime-getStartPointsTime<<endl;
};


struct cmp {
	bool operator()(const pair<size_t, shared_ptr<NFA_state> > &p1, const pair<size_t, shared_ptr<NFA_state> >& p2) {
		return Graph::topo_order[p1.first] > Graph::topo_order[p2.first];
	}
}; 

// void Production_FA::parse(shared_ptr<Regex> r,unordered_set<size_t> &in, unordered_map<size_t, unordered_set<size_t> >& result,unordered_set<size_t>& out) {
// 	switch (r->type) {
// 	case Regex::RegexType::Sequence:{
// 		if (r->lazy)
// 		{
// 			get_basic_re(r, result, in, out);
// 		}
// 		else {
// 			shared_ptr<Sequence> sequenceR = dynamic_pointer_cast<Sequence>(r);
// 			unordered_map<size_t, unordered_set<size_t> > mid_res1;
// 			unordered_map<size_t, unordered_set<size_t> > mid_res2;
// 			unordered_set<size_t> mid_out1;
// 			unordered_set<size_t> mid_out2;
// 			parse(sequenceR->first, in, mid_res1, mid_out1);
// 			parse(sequenceR->second, mid_out1, mid_res2, mid_out2);
// 			join(mid_res1, mid_res2, result);
// 		}
// 		return;
// 	}
// 	case Regex::RegexType::Choice:{
// 		if (r->lazy)
// 		{
// 			get_basic_re(r, result, in, out);
// 		}
// 		else {
// 			unordered_map<size_t, unordered_set<size_t> > mid_res1;
// 			unordered_map<size_t, unordered_set<size_t> > mid_res2;
// 			shared_ptr<Choice> choiceR = dynamic_pointer_cast<Choice>(r);
// 			unordered_set<size_t> mid_out1;
// 			unordered_set<size_t> mid_out2;
// 			parse(choiceR->thisOne, in, mid_res1, mid_out1);
// 			parse(choiceR->thatOne, in, mid_res2, mid_out2);
// 			for (auto v : in) {
// 				result[v] = {};
// 				if (mid_res1.find(v) != mid_res1.end())
// 				{
// 					result[v].insert(mid_res1[v].begin(), mid_res1[v].end());
// 				}
// 				if (mid_res2.find(v) != mid_res2.end())
// 				{
// 					result[v].insert(mid_res2[v].begin(), mid_res2[v].end());
// 				}
// 			}
// 			out.insert(mid_out1.begin(), mid_out1.end());
// 			out.insert(mid_out2.begin(), mid_out2.end());
// 		}
// 		return;
// 	}
// 	case Regex::RegexType::PlusRepetition: {
// 		unordered_map<size_t, unordered_set<size_t> > g;
// 		shared_ptr<PlusRepetition> plusRepetitionR = dynamic_pointer_cast<PlusRepetition>(r);
// 		unordered_set<size_t> mid_out;
// 		unordered_set<size_t> startPoints;
// 		getStartPoints(r, in, startPoints);
// 		parse(plusRepetitionR->internal, startPoints, g, mid_out);
// 		get_plus_re(g, result,in,out);
// 		return;
// 	}
// 	case Regex::RegexType::StarRepetition: {
// 		unordered_map<size_t, unordered_set<size_t> > g;
// 		shared_ptr<StarRepetition> starRepetitionR = dynamic_pointer_cast<StarRepetition>(r);
// 		unordered_set<size_t> mid_out;
// 		unordered_set<size_t> startPoints;
// 		getStartPoints(r, in, startPoints);
// 		parse(starRepetitionR->internal,startPoints, g, mid_out);
// 		get_star_re(g, result,in,out);
// 		return;
// 	}
// 	case Regex::RegexType::QuoraRepetition: {
// 		unordered_map<size_t, unordered_set<size_t> > g;
// 		shared_ptr<QuoraRepetition> quoraRepetitionR = dynamic_pointer_cast<QuoraRepetition>(r);
// 		unordered_set<size_t> mid_out;
// 		parse(quoraRepetitionR->internal, in, g, mid_out);
// 		get_quora_re(g, result,in,out);
// 		return;
// 	}
// 	case Regex::RegexType::RangeRepetition: {
// 		unordered_map<size_t, unordered_set<size_t> > g;
// 		shared_ptr<RangeRepetition> rangeRepetitionR = dynamic_pointer_cast<RangeRepetition>(r);
// 		unordered_set<size_t> mid_out;
// 		unordered_set<size_t> startPoints;
// 		getStartPoints(r, in, startPoints);
// 		parse(rangeRepetitionR->internal, startPoints, g, mid_out);
// 		get_range_re(rangeRepetitionR->min,rangeRepetitionR->max,g, result,in,out);
// 		return;
// 	}
// 	default: {
// 		get_basic_re(r, result, in, out);
// 		cout << r->type << endl;
// 		return;
// 	}
// 	}
// }

// void Production_FA::CheckAndSetCounter(set<int>& pre_counter, set<int>& nxt_counter, int op)
// {
// 	switch (op) {
// 	case Operation::Copy:{
// 		for (auto t : pre_counter)
// 			nxt_counter.insert(t);
// 		return;
// 	}
// 	case Operation::SetPos0:{
// 		nxt_counter.insert(1);
// 		return;
// 	}
// 	case Operation::Shift: {
// 		for (auto t : pre_counter)
// 			nxt_counter.insert(t+1);
// 		return;
// 	}
// 	case Operation::Check:
// 	case Operation::CheckZeroCount: {
// 		return;
// 	}
// 	}
// }
