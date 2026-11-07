#pragma once
#include<unordered_map>
#include<unordered_set>
#include<set>
#include<vector>
#include<string>
#include<algorithm>
#include<memory>
#include<bitset>
#include<stdexcept>
#include<utility>
#include "roaring/roaring.hh"
#include "SharedBitmapRow.h"
#include "vendor/unordered_dense.h"
//#include<boost/dynamic_bitset.hpp>
extern int wlen;
extern int mode;
using namespace std;

using RoaringRelation = ankerl::unordered_dense::map<size_t, SharedBitmapRow>;

inline void MoveDisjointRelationRows(RoaringRelation& source, RoaringRelation& destination)
{
	if (destination.empty()) {
		destination = std::move(source);
		return;
	}
	destination.reserve(destination.size() + source.size());
	for (auto& row : source) {
		if (!destination.try_emplace(row.first, std::move(row.second)).second) {
			throw std::logic_error("Cached and newly computed source rows overlap");
		}
	}
	source.clear();
}

RoaringRelation GcnpComposeRoaringRelation(const RoaringRelation& left,
	const RoaringRelation& right);

struct pair_comp {
	bool operator() (const std::pair<int, int>& A, const std::pair<int, int>& B)const {
		if (A.first < B.first) {
			return true;
		}
		else if (A.first == B.first) {
			return A.second < B.second;
		}
		else {
			return false;
		}
	}
};
struct Hashfunc {
	size_t operator() (const pair<int, int>& key) const {
		auto seed = hash<int>()(key.first);
		return seed ^= std::hash<int>()(key.second) + 0x9e3779b9 + (seed << 6) + (seed >> 2);;//��ϣ���������Ż�
	}
};


struct Equalfunc {
	bool operator() (const pair<int, int>& a, const pair<int, int>& b) const {
		return a.first == b.first && a.second == b.second;
	}
};


//��ʾNFA��״̬
class Pattern;
class NFA_state {
public:
	int id;
	//int lastlist1;
	string label;
	int type;
	NFA_state(int type,string label);
	NFA_state(int type);
	NFA_state(int type,string label, shared_ptr<NFA_state> out1, shared_ptr<NFA_state> out2);
	NFA_state(int type, shared_ptr<NFA_state> out1, shared_ptr<NFA_state> out2);
	virtual string toString();
	bool operator<(const NFA_state& rhs)const { return this->id < rhs.id; }
	bool operator==(const NFA_state& rhs)const { return this->id == rhs.id; }
	shared_ptr<unordered_set<std::pair<int, int>, Hashfunc> > cur_set;
	shared_ptr < unordered_set < std::pair<int, int>, Hashfunc > > next_set;
	unordered_map<int, pair<int,int> > count_range;
	//int groupType;
	//int groupNo;
	vector<shared_ptr<NFA_state> > reverse_out;
	shared_ptr<NFA_state> out1;
	shared_ptr<NFA_state> out2;
	int op1;
	int op2;
	void next_states(string label, int op, vector<pair<shared_ptr<NFA_state>, int> >& res,set<int>& counter);
	virtual void next_states(vector<shared_ptr<NFA_state> >& res);
	bool lock = false;
	static int count;
private:
	const static int Split = 256;
};

class Count_state :public NFA_state {
public:
	unordered_map<size_t,unordered_set<size_t>> range_repetition_map; 
	unordered_map<size_t, unordered_set<size_t>> g;
	unordered_map<int, unordered_map<size_t, unordered_set<size_t>>> exact_repetition_row_cache;
	unordered_map<size_t, vector<size_t>> g_sparse_rows;
	unordered_map<int, unordered_map<size_t, vector<size_t>>> exact_repetition_sparse_cache;
	unordered_map<int, RoaringRelation> exact_repetition_bitmap_cache;
	// S_k(source) = union_{i=0..k} P_i(source). Bounded ranges build these
	// rows on demand from the complete P_a frontier instead of pre-expanding a
	// k-hop closure domain.
	unordered_map<int, RoaringRelation> bounded_suffix_bitmap_cache;
	unordered_set<string> first_labels;
	unordered_set<string> last_labels;
	unordered_set<size_t> nonempty_start_vertices;
	bool nonempty_start_vertices_ready = false;
	shared_ptr<NFA_state> internal_start;
	int min, max;
	bool complex;
	bool body_can_be_empty = false;
	void checkComplex(){
		if (mode==0)
			complex=false;
		else if (mode==1)
			complex=true;
		else {
			if (max>=10)
			complex=true;
		}
	}
	Count_state(int type,int min,int max) :NFA_state(type),min(min),max(max) {checkComplex();};
	Count_state(int type, string label, int min, int max) :NFA_state(type, label), min(min), max(max) {checkComplex();};
	Count_state(int type, string label, shared_ptr<NFA_state> out1, shared_ptr<NFA_state> out2, int min, int max) :NFA_state(type, label, out1, out2), min(min), max(max) {checkComplex();};
	Count_state(int type, shared_ptr<NFA_state> out1, shared_ptr<NFA_state> out2, int min, int max) :NFA_state(type, out1, out2), min(min), max(max) {checkComplex();};
	//virtual void next_states(string label, int op, vector<pair<shared_ptr<NFA_state>, int> >& res, set<int>& counter);
	bool check(set<int>& counter);
	bool use_lazy_exact_cache() const;
	bool use_lazy_open_range_cache() const;
	bool use_lazy_bounded_range_cache() const;
	void init_map(int min,int max,shared_ptr<NFA_state> start_state,unordered_set<string> &firstSet,unordered_set<string> &lastSet);
	const unordered_set<size_t>& get_or_build_lazy_range_row(size_t vertex,shared_ptr<NFA_state> start_state);
	void build_exact_rows_for_sources(const unordered_set<size_t>& sources, int len, shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t>>& out_rows, RoaringRelation* bitmap_out = nullptr);
	void build_open_range_rows_for_sources(const unordered_set<size_t>& sources, int len, shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t>>& out_rows, RoaringRelation* bitmap_out = nullptr);
	void build_bounded_range_rows_for_sources(const unordered_set<size_t>& sources, int min_len, int max_len, shared_ptr<NFA_state> start_state, unordered_map<size_t, unordered_set<size_t>>& out_rows, RoaringRelation* bitmap_out = nullptr);
	void range_dfs(size_t vertex,shared_ptr<NFA_state> start_state,unordered_set<size_t> &result);
private:
	void record_body_stats();
	void ensure_nonempty_start_vertices();
	void batch_ensure_body_rows(const unordered_set<size_t>& sources, shared_ptr<NFA_state> start_state);
	void ensure_body_row(size_t vertex, shared_ptr<NFA_state> start_state);
	const vector<size_t>& get_or_build_exact_sparse_row(size_t vertex, int len, shared_ptr<NFA_state> start_state);
	void build_exact_sparse_rows_for_sources(const unordered_set<size_t>& sources, int len, shared_ptr<NFA_state> start_state, unordered_map<size_t, vector<size_t>>& out_rows);
	void build_exact_bitmap_rows_for_sources(const unordered_set<size_t>& sources, int len, shared_ptr<NFA_state> start_state, RoaringRelation& out_rows);
	void build_bounded_suffix_bitmap_rows_for_sources(const unordered_set<size_t>& sources, int len, shared_ptr<NFA_state> start_state, RoaringRelation& out_rows);
	const unordered_set<size_t>& get_or_build_exact_row(size_t vertex, int len, shared_ptr<NFA_state> start_state);
};
struct stateptrCmp
{
	bool operator()(const shared_ptr<NFA_state>& s1, const shared_ptr<NFA_state>& s2)const;
};
//class ScaleState :public NFA_state {
//public:
//	bitset<256> scale;
//	//ScaleState(bitset<256> s);
//	string toString();
//};

class NfaList
{
public:
	vector<shared_ptr<NFA_state> >ss;
	NfaList(vector<shared_ptr<NFA_state> > states);
	bool operator<(const NfaList& o)const;

};
struct NfalistCmp
{
	bool operator()(const shared_ptr<NfaList>& n1, const shared_ptr<NfaList>& n2)const;
};
class DState
{
public:
	shared_ptr<NfaList> nfalist;
	shared_ptr<DState> next[256];
	shared_ptr<DState> left;
	shared_ptr<DState> right;
	DState(shared_ptr<NfaList> list);
	DState() {}
};

class Frag
{
public:
	shared_ptr<NFA_state> start;
	vector<shared_ptr<NFA_state> > out1;
	vector<shared_ptr<NFA_state> > out2;
	Frag(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state> > o);
	Frag(shared_ptr<NFA_state> s);
	Frag(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state> > o1, vector<shared_ptr<NFA_state> >o2);
	string toString();
};

class Hasher
{
public:
	size_t operator()(const shared_ptr<NFA_state>& rhs)const;
	size_t operator()(const shared_ptr<NfaList>& rhs)const;
};


struct Equalfunc2 {
	bool operator() (const pair<int, shared_ptr<NFA_state> >& a, const pair<int, shared_ptr<NFA_state> >& b) const {
		return a.first == b.first && a.second->id == b.second->id;
	}
};
struct Hashfunc2 {
	size_t operator() (const pair<int, shared_ptr<NFA_state> >& key) const {
		auto seed = hash<int>()(key.first);
		return seed ^= std::hash<int>()(key.second->id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
	}
};
