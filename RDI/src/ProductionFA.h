#pragma once
#include <memory>
#include<set>
#include"NFA_state.h"
#include "Parser.h"
#include"Regex.h"
#include "graph.h"
class Production_FA {
public:
	shared_ptr<NFA_state> startState;
	shared_ptr<NFA_state> matchState;
	shared_ptr<Parser> p;
	shared_ptr<Regex> re;
	Production_FA(string E);
	vector<std::tuple<std::string, std::string, std::string>> graph_RDF;
	void init_range_repetitions();
	void compute_RPQ(unordered_map<size_t, unordered_set<size_t>>& result);
	RoaringRelation bitmap_result;
	bool has_bitmap_result = false;

	// void dfs(size_t vertex, shared_ptr<NFA_state> state, 
	// 	unordered_map<pair<size_t, shared_ptr<NFA_state> >, unordered_set<size_t>, Hashfunc2, Equalfunc2>& M, unordered_set<size_t>& out);
	//void getStartPoints(shared_ptr<Regex> r, unordered_set<size_t>& in, unordered_set<size_t>& startPoints);
	//void parse(shared_ptr<Regex> r, unordered_set<size_t>& in, unordered_map<size_t, unordered_set<size_t> >& result, unordered_set<size_t>& out);
	int  referCount = 0;
private:
	bool contains_range_repetition(const shared_ptr<Regex>& r) const;
	bool should_use_macro_scheduler(const shared_ptr<Regex>& r) const;
	void compute_via_macro_scheduler(
		const unordered_set<size_t>& sources,
		RoaringRelation& result);
	void compute_via_materialize_scheduler(
		const unordered_set<size_t>& sources,
		unordered_map<size_t, unordered_set<size_t>>& result);
	void compute_materialized_relation_for_regex(
		const shared_ptr<Regex>& r,
		unordered_map<size_t, unordered_set<size_t>>& result);
	//void CheckAndSetCounter(set<int>& pre_counter, set<int>& nxt_counter, int op);

	string E;
	//shared_ptr<unordered_map<int, shared_ptr<Production_State> >> cmap;
	//shared_ptr<unordered_map<int, shared_ptr<Production_State>>> nmap;
	shared_ptr<unordered_set<shared_ptr<NFA_state> > > NFA_statesets;
	unordered_map<shared_ptr<VP>, unordered_set<shared_ptr<VP> > > shortcut;
	vector<shared_ptr<VP> > res;
};
