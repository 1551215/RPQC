#pragma once
#include "AdditionChainTable.h"
#include "NFA_state.h"
#include "FixedBitmapUnion.h"
#include "Type.h"
#include <array>
#include <atomic>
#include <cstddef>
#include <memory>
#include <queue>
#include <thread>
#include <iostream>
#include <unordered_set>
#include "Traversal.h"
#include "UnionTrace.h"
#include "RelationOperationMetrics.h"
#include "graph.h"
#include "roaring/roaring.hh"
//#include "boost/smart_ptr/make_shared_object.hpp"
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <stdexcept>

#ifndef RANGE_SINGLE_DEBUG_OPEN_RANGE
#define RANGE_SINGLE_DEBUG_OPEN_RANGE 0
#endif

#ifndef RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC
#define RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC 1
#endif

#ifndef RANGE_SINGLE_USE_ROARING_OPEN_RANGE
#define RANGE_SINGLE_USE_ROARING_OPEN_RANGE 1
#endif

namespace {

vector<size_t> topo_sort_comp_dag(
	const unordered_map<size_t, unordered_set<size_t>>& dag,
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
		order.clear();
		for (size_t node = 0; node < node_count; ++node) {
			order.push_back(node);
		}
	}
	return order;
}

}

int NFA_state::count = 1;
int wlen;
int mode;
bool stateptrCmp::operator()(const shared_ptr<NFA_state>& s1, const shared_ptr<NFA_state>& s2)const
{
	return s1->id < s2->id;
}
NFA_state::NFA_state(int type,string label) :type(type), label(label),id(count++), out1(nullptr)
{
}
NFA_state::NFA_state(int type) :type(type), label(""), id(count++), out1(nullptr)
{
}

NFA_state::NFA_state(int type,string label, shared_ptr<NFA_state> out1, shared_ptr <NFA_state> out2) :type(type),label(label), id(count++), out1(out1), out2(out2)
{
}

NFA_state::NFA_state(int type, shared_ptr<NFA_state> out1, shared_ptr <NFA_state> out2) :type(type), label(""), id(count++), out1(out1), out2(out2)
{
}


string NFA_state::toString()
{
	if (this == NULL)
		return "";
	shared_ptr<NFA_state>out2 = this->out2;
	return "[type:" + std::to_string(type) + "label:"+label +",out1:" +
		(out1 == NULL ? "NULL" : out1->toString()) +
		",out2:" + (out2 == NULL ? "NULL" : out2->toString()) + "]";
}

void state_op(shared_ptr<NFA_state> state,vector<shared_ptr<NFA_state> >& res)
{
	switch (state->type) {
	case Type::MatchState:
	{
		res.emplace_back(state);
		return;
	}

	case Type::Split:
	{
		state->next_states(res);
		state->next_states(res);
		return;
	}
	case Type::Blank:
	{
		state->next_states(res);
		return;
	}
	case Type::RangeRepetition:
	{
		res.emplace_back(state);
		return;
	}

	default:
	{
		//cout<<"debug111:"<<state->type<<endl;
			res.emplace_back(state);
			return;
	}
	}
}

void NFA_state::next_states(vector<shared_ptr<NFA_state> >& res)
{
	if(out1)
	    state_op(out1, res);
	if (out2)
		state_op(out2,res);
}

void NFA_state::next_states(string label, int op, vector<pair<shared_ptr<NFA_state>, int> >& res, set<int>& counter)
{
	switch (type){
		case Type::MatchState:
		{
			res.emplace_back(make_pair(this, 0));
			return;
		}

		case Type::Split:
		{
			out1->next_states(label, op|op1, res, counter);
			out2->next_states(label, op|op2, res, counter);
			return;
		}
		case Type::Blank:
		{
			out1->next_states(label, op|op1,res, counter);
			return;
		}
		case Type::AnyChar:
		{
			res.emplace_back(make_pair(this,op));
			return;
		}
		default:
		{
			if (label == this->label)
			{
				res.emplace_back(make_pair(this, op));
				return;
			}
		}
	}
}

bool Count_state::check(set<int>& counter) {
	auto it = counter.lower_bound(min);
	// �������������Ƿ��� end���������ֵС�ڵ��� max
	return it != counter.end() && *it <= max;
}

bool Count_state::use_lazy_exact_cache() const
{
	return complex && min == max && max >= 0;
}

bool Count_state::use_lazy_open_range_cache() const
{
	return complex && max == -1 && min >= 1;
}

bool Count_state::use_lazy_bounded_range_cache() const
{
	return complex && max >= 0 && min >= 0 && min < max;
}

namespace {

using SparseRelation = unordered_map<size_t, vector<size_t>>;

static vector<size_t> SortedUniqueVectorFromSet(const unordered_set<size_t>& input)
{
	QueryPhaseTimer measurement(QueryTimingPhase::FormatConversion);
	vector<size_t> values(input.begin(), input.end());
	sort(values.begin(), values.end());
	values.erase(unique(values.begin(), values.end()), values.end());
	return values;
}

static vector<size_t> SortedUniqueVectorFromBuffer(vector<size_t>& buffer)
{
	sort(buffer.begin(), buffer.end());
	buffer.erase(unique(buffer.begin(), buffer.end()), buffer.end());
	return buffer;
}

static void SparseJoinRelation(const SparseRelation& left, const SparseRelation& right, SparseRelation& out)
{
	for (const auto& kv : left) {
		vector<size_t> buffer;
		for (size_t mid : kv.second) {
			auto it = right.find(mid);
			if (it != right.end()) {
				buffer.insert(buffer.end(), it->second.begin(), it->second.end());
			}
		}
		out[kv.first] = SortedUniqueVectorFromBuffer(buffer);
	}
}

static size_t SparseRelationPairCount(const SparseRelation& rel)
{
	size_t total = 0;
	for (const auto& kv : rel) {
		total += kv.second.size();
	}
	return total;
}

static roaring::Roaring RoaringFromSortedVector(const vector<size_t>& input)
{
	roaring::Roaring bitmap;
	for (size_t value : input) {
		bitmap.add(static_cast<uint32_t>(value));
	}
	return bitmap;
}

static roaring::Roaring GcnpUnionBitmaps(
	const vector<const roaring::Roaring*>& inputs);

// A compose owns no right rows and never mutates them. Page only occupied ID
// ranges so sparse right relations also avoid repeated hashing/cardinality scans.
class ReadOnlyRowLookup {
public:
	explicit ReadOnlyRowLookup(const RoaringRelation& relation) {
		unordered_map<const void*, const SharedBitmapRow*> known_storage;
		unordered_map<size_t, vector<const SharedBitmapRow*>> values_by_hash;
		known_storage.reserve(relation.size());
		values_by_hash.reserve(relation.size());
		string encoded;
		size_t distinct_values = 0, equal_value_aliases = 0;
		size_t max_key = 0;
		for (const auto& row : relation) {
			max_key = std::max(max_key, row.first);
		}
		const size_t page_count = (max_key >> page_bits) + 1;
		pages_.resize(page_count);
		bytes_ = page_count * sizeof(std::unique_ptr<Entry[]>);
		for (const auto& row : relation) {
			const SharedBitmapRow* canonical = nullptr;
			auto known = known_storage.find(row.second.storage_identity());
			if (known != known_storage.end()) {
				canonical = known->second;
			} else {
				encoded.resize(row.second.bitmap().getSizeInBytes(true));
				row.second.bitmap().write(encoded.data(), true);
				auto& candidates = values_by_hash[hash<string>{}(encoded)];
				for (const auto* candidate : candidates) {
					if (row.second.bitmap() == candidate->bitmap()) {
						canonical = candidate;
						++equal_value_aliases;
						break;
					}
				}
				if (canonical == nullptr) {
					canonical = &row.second;
					candidates.push_back(canonical);
					++distinct_values;
				}
				known_storage.emplace(row.second.storage_identity(), canonical);
			}
			auto& page = pages_[row.first >> page_bits];
			if (!page) {
				page = std::make_unique<Entry[]>(page_size);
				bytes_ += page_size * sizeof(Entry);
			}
			page[row.first & page_mask] = {canonical, canonical->cardinality()};
		}
		std::cout << "[GCNP_RHS_STORAGE] rows=" << relation.size()
			<< " distinct_values=" << distinct_values
			<< " equal_value_aliases=" << equal_value_aliases
			<< " equality=complete_bitmap scope=compose threads=1" << std::endl;
	}

	const SharedBitmapRow* find(size_t key) const {
		const auto* entry = find_entry(key);
		return entry == nullptr ? nullptr : entry->row;
	}
	const SharedBitmapRow& at(size_t key) const {
		const auto* row = find(key);
		if (row == nullptr) throw std::out_of_range("Missing immutable right row");
		return *row;
	}
	uint64_t cardinality(size_t key) const {
		const auto* entry = find_entry(key);
		return entry == nullptr ? 0 : entry->cardinality;
	}
	size_t bytes() const { return bytes_; }

private:
	static constexpr size_t page_bits = 8;
	static constexpr size_t page_size = 1ULL << page_bits;
	static constexpr size_t page_mask = page_size - 1;
	struct Entry {
		const SharedBitmapRow* row = nullptr;
		uint64_t cardinality = 0;
	};
	const Entry* find_entry(size_t key) const {
		const size_t page_index = key >> page_bits;
		if (page_index >= pages_.size() || !pages_[page_index]) return nullptr;
		return &pages_[page_index][key & page_mask];
	}
	vector<std::unique_ptr<Entry[]>> pages_;
	size_t bytes_ = 0;
};

struct ComposeExecutionProfile {
	using Clock = chrono::steady_clock;
	bool enabled = std::getenv("GCNP_PROFILE_EXECUTION") != nullptr;
	size_t samples = 0;
	size_t sampled_inputs = 0, sampled_distinct_inputs = 0;
	double lookup_ms = 0, union_ms = 0, store_ms = 0;
	void observe_inputs(const vector<const roaring::Roaring*>& inputs) {
		AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
		auto identities = inputs;
		sort(identities.begin(), identities.end(), std::less<const roaring::Roaring*>{});
		sampled_inputs += inputs.size();
		sampled_distinct_inputs += std::unique(identities.begin(), identities.end()) - identities.begin();
	}
	void record(Clock::time_point begin, Clock::time_point merge, Clock::time_point store) {
		++samples;
		lookup_ms += chrono::duration<double, std::milli>(merge - begin).count();
		union_ms += chrono::duration<double, std::milli>(store - merge).count();
		store_ms += chrono::duration<double, std::milli>(Clock::now() - store).count();
	}
	void log(const char* mode, size_t lookup_bytes, double lookup_build_ms) const {
		if (!enabled) return;
		std::cout << "[COMPOSE_PROFILE] mode=" << mode << " sampled_rows=" << samples
			<< " sampled_lookup_ms=" << lookup_ms << " sampled_union_ms=" << union_ms
			<< " sampled_store_ms=" << store_ms << " right_lookup_bytes=" << lookup_bytes
			<< " sampled_inputs=" << sampled_inputs << " sampled_distinct_inputs=" << sampled_distinct_inputs
			<< " right_lookup_build_ms=" << lookup_build_ms << std::endl;
	}
};

struct OpenRangeExecutionProfile {
	bool enabled = std::getenv("GCNP_PROFILE_OPEN_RANGE") != nullptr;
	OperationStopwatch timer;
	double previous_ms = 0;
	void checkpoint(const char* stage) {
		if (!enabled) return;
		const double now_ms = timer.elapsed_ms();
		std::cout << "[OPEN_RANGE_PROFILE] stage=" << stage
			<< " elapsed_ms=" << now_ms << " stage_ms=" << now_ms - previous_ms
			<< std::endl;
		previous_ms = timer.elapsed_ms();
	}
	~OpenRangeExecutionProfile() { checkpoint("release_temporaries"); }
};

// SCC output membership is already known. Buffer singleton destinations in
// their ascending emission order, then construct each native bitmap in bulk.
class OpenRangeRowBuilder {
	struct Row {
		size_t source;
		roaring::Roaring targets;
		vector<uint32_t> singleton_targets;
	};
	static constexpr size_t page_bits = 8;
	static constexpr size_t page_size = 1ULL << page_bits;
	static constexpr size_t page_mask = page_size - 1;
	vector<Row> rows_;
	vector<std::unique_ptr<Row*[]>> pages_;

	Row& at(uint32_t source) {
		return *pages_[source >> page_bits][source & page_mask];
	}

public:
	explicit OpenRangeRowBuilder(const RoaringRelation& prefix) {
		rows_.reserve(prefix.size());
		for (const auto& entry : prefix) {
			rows_.push_back({entry.first, {}, {}});
			const size_t page_index = entry.first >> page_bits;
			if (pages_.size() <= page_index) pages_.resize(page_index + 1);
			auto& page = pages_[page_index];
			if (!page) page = std::make_unique<Row*[]>(page_size);
			page[entry.first & page_mask] = &rows_.back();
		}
	}
	void add_singleton(const roaring::Roaring& sources, uint32_t target) {
		for (uint32_t source : sources) at(source).singleton_targets.push_back(target);
	}
	void add_component(const roaring::Roaring& sources, const roaring::Roaring& targets) {
		for (uint32_t source : sources) at(source).targets |= targets;
	}
	RoaringRelation finish() {
		RoaringRelation output;
		output.reserve(rows_.size());
		for (auto& row : rows_) {
			if (!row.singleton_targets.empty()) {
				roaring::Roaring singletons(row.singleton_targets.size(), row.singleton_targets.data());
				if (row.targets.isEmpty()) row.targets = std::move(singletons);
				else row.targets |= singletons;
			}
			if (!row.targets.isEmpty()) output.emplace(row.source, std::move(row.targets));
			vector<uint32_t>().swap(row.singleton_targets);
		}
		return output;
	}
};

static SharedBitmapRow UnionImmutableBitmapRows(
	const vector<const roaring::Roaring*>& inputs,
	const SharedBitmapRow* first, const SharedBitmapRow* last)
{
	if (inputs.empty()) return {};
	if (inputs.size() == 1) return *first;
	if (inputs.size() == 2) {
		const bool first_is_smaller = first->cardinality() <= last->cardinality();
		const auto* smaller = first_is_smaller ? first : last;
		const auto* larger = first_is_smaller ? last : first;
		// No new result cache: this binary union can return an existing immutable
		// operand when it already is the complete union. Mutations still detach.
		if (smaller->bitmap().isSubset(larger->bitmap())) return *larger;
		return first->bitmap() | last->bitmap();
	}
	// Match the distributed executor: logical RHS rows keep their work count,
	// while one immutable bitmap is only passed to the OR kernel once.
	static unordered_map<const roaring::Roaring*, uint64_t> input_epochs;
	static uint64_t input_epoch = 0;
	static vector<const roaring::Roaring*> unique_inputs;
	if (++input_epoch == 0) {
		input_epochs.clear();
		input_epoch = 1;
	}
	unique_inputs.clear();
	unique_inputs.reserve(inputs.size());
	for (const auto* input : inputs) {
		auto& epoch = input_epochs[input];
		if (epoch == input_epoch) continue;
		epoch = input_epoch;
		unique_inputs.push_back(input);
	}
	if (unique_inputs.size() == 1) return *first;
	return GcnpUnionBitmaps(unique_inputs);
}

static unordered_set<size_t> CollectDistinctRelationMids(const RoaringRelation& relation)
{
	unordered_set<size_t> mids;
	// Graph IDs bound every bitmap endpoint. A membership bit avoids repeated
	// hash probes while retaining the original first-insertion order in mids.
	vector<uint64_t> seen((Graph::max_vertex_id >> 6) + 1, 0);
	for (const auto& row : relation) {
		for (uint32_t mid : row.second) {
			auto& word = seen[mid >> 6];
			const uint64_t mask = uint64_t{1} << (mid & 63);
			if ((word & mask) == 0) {
				word |= mask;
				mids.insert(mid);
			}
		}
	}
	return mids;
}

static void RoaringJoinRelation(const RoaringRelation& left, const ReadOnlyRowLookup& right,
	RoaringRelation& out, ComposeExecutionProfile& profile)
{
	vector<const roaring::Roaring*> inputs;
	out.reserve(left.size());
	size_t ordinal = 0;
	for (const auto& kv : left) {
		const bool sample = profile.enabled && (ordinal++ % 1024 == 0);
		const auto begin = sample ? ComposeExecutionProfile::Clock::now() : ComposeExecutionProfile::Clock::time_point{};
		inputs.clear();
		inputs.reserve(kv.second.cardinality());
		const SharedBitmapRow* first_input = nullptr;
		const SharedBitmapRow* only_input = nullptr;
		for (uint32_t mid : kv.second) {
			const auto* row = right.find(static_cast<size_t>(mid));
			if (row != nullptr && !row->isEmpty()) {
				if (inputs.empty()) first_input = row;
				only_input = row;
				inputs.push_back(&row->bitmap());
			}
		}
		const auto merge = sample ? ComposeExecutionProfile::Clock::now() : begin;
		if (sample) profile.observe_inputs(inputs);
		// Share only the union kernel, never GCNP grouping or pattern reuse.
		SharedBitmapRow row = UnionImmutableBitmapRows(inputs, first_input, only_input);
		const auto store = sample ? ComposeExecutionProfile::Clock::now() : begin;
		if (!row.isEmpty()) {
			out.emplace(kv.first, std::move(row));
		}
		if (sample) profile.record(begin, merge, store);
	}
}

static void RoaringUnionRelationInPlace(const RoaringRelation& src, RoaringRelation& dst)
{
	RelationUnionTimer timer("roaring_relation_union");
	for (const auto& kv : src) {
		auto it = dst.find(kv.first);
		if (it == dst.end()) {
			dst.emplace(kv.first, kv.second);
		} else {
			it->second |= kv.second;
		}
	}
}

static RoaringRelation BuildIdentityRoaringRelation(const unordered_set<size_t>& vertices)
{
	RoaringRelation identity;
	for (size_t v : vertices) {
		identity.emplace(v, roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(v)));
	}
	return identity;
}

static size_t RoaringRelationPairCount(const RoaringRelation& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t total = 0;
	for (const auto& kv : rel) {
		total += kv.second.cardinality();
	}
	return total;
}

static size_t RoaringRelationNonEmptyRows(const RoaringRelation& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t rows = 0;
	for (const auto& kv : rel) {
		if (!kv.second.isEmpty()) {
			++rows;
		}
	}
	return rows;
}

static size_t RoaringRelationMaxRowSize(const RoaringRelation& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t max_row = 0;
	for (const auto& kv : rel) {
		max_row = std::max(max_row, static_cast<size_t>(kv.second.cardinality()));
	}
	return max_row;
}

struct BitmapRelationStats {
	size_t pairs = 0;
	size_t rows = 0;
	size_t max_row = 0;
};

static BitmapRelationStats MeasureRoaringRelation(const RoaringRelation& relation)
{
	QueryPhaseTimer statistics(QueryTimingPhase::RelationStats);
	BitmapRelationStats stats;
	for (const auto& row : relation) {
		const size_t size = row.second.cardinality();
		stats.pairs += size;
		stats.rows += size != 0;
		stats.max_row = std::max(stats.max_row, size);
	}
	return stats;
}

static size_t DenseRelationPairCount(const unordered_map<size_t, unordered_set<size_t>>& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t total = 0;
	for (const auto& kv : rel) {
		total += kv.second.size();
	}
	return total;
}

static size_t DenseRelationNonEmptyRows(const unordered_map<size_t, unordered_set<size_t>>& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t rows = 0;
	for (const auto& kv : rel) {
		if (!kv.second.empty()) {
			++rows;
		}
	}
	return rows;
}

static size_t DenseRelationMaxRowSize(const unordered_map<size_t, unordered_set<size_t>>& rel)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t max_row = 0;
	for (const auto& kv : rel) {
		max_row = std::max(max_row, kv.second.size());
	}
	return max_row;
}

static RoaringRelation RoaringRelationFromSparseRows(
	const unordered_map<size_t, vector<size_t>>& rows)
{
	QueryPhaseTimer measurement(QueryTimingPhase::FormatConversion);
	RoaringRelation relation;
	relation.reserve(rows.size() * 2 + 1);
	for (const auto& kv : rows) {
		if (!kv.second.empty()) {
			relation.emplace(kv.first, RoaringFromSortedVector(kv.second));
		}
	}
	return relation;
}

static void StoreSparseRowsFromRoaringRelation(
	const RoaringRelation& relation,
	const vector<size_t>& sources,
	unordered_map<size_t, vector<size_t>>& cache_rows)
{
	QueryPhaseTimer measurement(QueryTimingPhase::ResultMaterialization);
	for (size_t src : sources) {
		auto it = relation.find(src);
		if (it == relation.end()) {
			cache_rows[src] = {};
			continue;
		}
		vector<size_t> row;
		row.reserve(it->second.cardinality());
		for (uint32_t dst : it->second) {
			row.push_back(static_cast<size_t>(dst));
		}
		cache_rows[src] = std::move(row);
	}
}

struct BlpcStats {
	size_t total_local_patterns = 0;
	size_t distinct_local_patterns = 0;
	size_t cacheable_patterns = 0;
	size_t cached_result_bytes = 0;
	size_t cache_hits = 0;
	size_t cache_misses = 0;
	// Original nonempty right rows merged; excludes shared-result consumers.
	size_t actual_union_work = 0;
	size_t output_rows = 0;
	size_t output_pairs = 0;
};

struct BlpcComposeResult {
	RoaringRelation relation;
	BlpcStats stats;
	size_t candidate_pairs = 0;
	double compose_time_ms = 0.0;
	size_t naive_union_work = 0;
	double gcnp_planning_time_ms = 0.0;
	double relation_merge_time_ms = 0.0;
};

static size_t ComputeCandidatePairs(const RoaringRelation& left, const RoaringRelation& right)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t total = 0;
	for (const auto& kv : left) {
		for (uint32_t mid : kv.second) {
			auto it = right.find(static_cast<size_t>(mid));
			if (it != right.end()) {
				total += it->second.cardinality();
			}
		}
	}
	return total;
}

static size_t ComputeCandidatePairs(const RoaringRelation& left, const ReadOnlyRowLookup& right)
{
	QueryPhaseTimer measurement(QueryTimingPhase::RelationStats);
	size_t total = 0;
	for (const auto& kv : left) {
		for (uint32_t mid : kv.second) total += right.cardinality(mid);
	}
	return total;
}

static uint64_t HashRoaringBitmap(const roaring::Roaring& bitmap)
{
	uint64_t h = 1469598103934665603ULL;
	for (uint32_t value : bitmap) {
		h ^= static_cast<uint64_t>(value) + 0x9e3779b97f4a7c15ULL;
		h *= 1099511628211ULL;
	}
	return h;
}

struct GcnpGroup {
	roaring::Roaring sources;
	roaring::Roaring common_mids;
};

// Fixed radix addressing, not a density-dependent choice of index algorithm.
class GcnpColumnIndex {
public:
	struct Column {
		roaring::Roaring sources;
		vector<uint32_t> source_ordinals;
		uint64_t right_cardinality = 0;
		bool present = false;
	};

	Column& insert(uint32_t mid) {
		const size_t page_index = mid >> page_bits;
		if (pages_.size() <= page_index) pages_.resize(page_index + 1);
		auto& page = pages_[page_index];
		if (!page) page = std::make_unique<Column[]>(page_size);
		auto& column = page[mid & page_mask];
		column.present = true;
		return column;
	}

	Column* find(uint32_t mid) {
		const size_t page_index = mid >> page_bits;
		if (page_index >= pages_.size() || !pages_[page_index]) return nullptr;
		auto* column = &pages_[page_index][mid & page_mask];
		return column->present ? column : nullptr;
	}

	Column& at(uint32_t mid) {
		auto* column = find(mid);
		if (column == nullptr) throw std::out_of_range("Missing GCNP index column");
		return *column;
	}

	void finalize() {
		for (auto& page : pages_) {
			if (!page) continue;
			for (size_t i = 0; i < page_size; ++i) {
				auto& column = page[i];
				if (!column.present) continue;
				// The native constructor accepts arbitrary insertion order; alias
				// batches contain disjoint source ordinals, not new memberships.
				column.sources = roaring::Roaring(column.source_ordinals.size(),
					column.source_ordinals.data());
				vector<uint32_t>().swap(column.source_ordinals);
			}
		}
	}

	void release() { pages_.clear(); }

private:
	static constexpr size_t page_bits = 8;
	static constexpr size_t page_size = 1ULL << page_bits;
	static constexpr size_t page_mask = page_size - 1;
	vector<std::unique_ptr<Column[]>> pages_;
};

static void SortGcnpSourceRows(vector<pair<size_t, const SharedBitmapRow*>>& sources)
{
	// Stable radix sorting preserves the previous ascending source-ID order.
	// Every ID uses all digits; there is no size- or distribution-based dispatch.
	constexpr size_t digit_bits = 16;
	constexpr size_t digit_count = 1ULL << digit_bits;
	vector<pair<size_t, const SharedBitmapRow*>> scratch(sources.size());
	for (size_t shift = 0; shift < numeric_limits<size_t>::digits; shift += digit_bits) {
		std::array<size_t, digit_count> positions{};
		for (const auto& source : sources) ++positions[(source.first >> shift) & (digit_count - 1)];
		size_t offset = 0;
		for (auto& position : positions) {
			const size_t count = position;
			position = offset;
			offset += count;
		}
		for (const auto& source : sources) {
			scratch[positions[(source.first >> shift) & (digit_count - 1)]++] = source;
		}
		sources.swap(scratch);
	}
}

struct GcnpPatternCacheEntry {
	roaring::Roaring pattern;
	vector<size_t> group_ids;
	size_t source_uses = 0;
	size_t rhs_bytes = 0;
	bool cached = false;
	roaring::Roaring cached_targets;
};

static size_t GcnpPatternRhsBytes(const roaring::Roaring& pattern,
	const RoaringRelation& right)
{
	size_t bytes = 0;
	for (uint32_t mid : pattern) {
		auto targets = right.find(static_cast<size_t>(mid));
		if (targets != right.end()) bytes += targets->second.getSizeInBytes();
	}
	return bytes;
}

static size_t PatternUnionRowWork(const roaring::Roaring& pattern,
	const RoaringRelation& right)
{
	size_t rows = 0;
	for (uint32_t mid : pattern) {
		auto targets = right.find(static_cast<size_t>(mid));
		if (targets != right.end() && !targets->second.isEmpty()) ++rows;
	}
	return rows;
}

// One fixed directory OR implementation for both modes. Empty and unary
// identities are handled by the immutable-row helper, not a tuning cutoff.
static roaring::Roaring GcnpUnionBitmaps(
	const vector<const roaring::Roaring*>& inputs)
{
	static FixedBitmapUnionScratch scratch;
	return scratch.merge(inputs);
}

static roaring::Roaring GcnpBuildPatternTargets(const roaring::Roaring& pattern,
	const RoaringRelation& right)
{
	vector<const roaring::Roaring*> inputs;
	inputs.reserve(pattern.cardinality());
	for (uint32_t mid : pattern) {
		auto targets = right.find(static_cast<size_t>(mid));
		if (targets != right.end() && !targets->second.isEmpty()) {
			inputs.push_back(&targets->second.bitmap());
		}
	}
	return GcnpUnionBitmaps(inputs);
}

static bool GcnpCacheWorthwhile(size_t uses, size_t rhs_bytes, size_t cached_bytes)
{
	if (uses < 2) return false;
	return static_cast<long double>(uses - 1) * rhs_bytes >
		static_cast<long double>(uses) * cached_bytes;
}

static void AddBlpcStats(BlpcStats& total, const BlpcStats& part)
{
	total.total_local_patterns += part.total_local_patterns;
	total.distinct_local_patterns += part.distinct_local_patterns;
	total.cacheable_patterns += part.cacheable_patterns;
	total.cached_result_bytes += part.cached_result_bytes;
	total.cache_hits += part.cache_hits;
	total.cache_misses += part.cache_misses;
	total.actual_union_work += part.actual_union_work;
	total.output_rows += part.output_rows;
	total.output_pairs += part.output_pairs;
}

static bool UseGcnpCompose()
{
	const char* mode = std::getenv("GCNP_MODE");
	return mode == nullptr || std::strcmp(mode, "plain") != 0;
}

// GCNP ablation: use a sequential row-wise join with the shared union kernel. It
// has no source partitioning, common-pattern union, or GCNP worker parallelism.
static BlpcComposeResult PlainSequentialComposeRoaringDetailed(
	const RoaringRelation& left, const RoaringRelation& right)
{
	auto join_begin = chrono::steady_clock::now();
	RoaringRelation out;
	// Keep source rows independent; no grouping or cross-source reuse.
	OperationStopwatch merge_timer;
	ReadOnlyRowLookup right_lookup(right);
	const double lookup_build_ms = merge_timer.elapsed_ms();
	ComposeExecutionProfile profile;
	RoaringJoinRelation(left, right_lookup, out, profile);
	const double merge_time_ms = merge_timer.elapsed_ms();
	BlpcStats stats;
	size_t candidate_pairs = 0;
	QueryPhaseTimer statistics(QueryTimingPhase::RelationStats);
	for (const auto& source_row : left) {
		for (uint32_t mid : source_row.second) {
			const auto* targets = right_lookup.find(static_cast<size_t>(mid));
			if (targets == nullptr || targets->isEmpty()) continue;
			candidate_pairs += targets->cardinality();
			++stats.actual_union_work;
		}
	}
	for (const auto& output_row : out) {
		stats.output_rows++;
		stats.output_pairs += output_row.second.cardinality();
	}
	BlpcComposeResult result;
	result.candidate_pairs = candidate_pairs;
	statistics.stop();
	result.compose_time_ms = chrono::duration<double, std::milli>(
		chrono::steady_clock::now() - join_begin).count();
	result.naive_union_work = stats.actual_union_work;
	result.relation_merge_time_ms = merge_time_ms;
	result.stats = stats;
	result.relation = std::move(out);
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	profile.log("plain", right_lookup.bytes(), lookup_build_ms);
	std::cout << "[GCNP] mode=plain_sequential threads=1 union_kernel=fixed_container_directory_or binary_or=native" << std::endl;
	return result;
}

// GCNP remains source-row driven: the largest unassigned row is the seed.
// The original exact mode uses the whole seed row. The default mode instead
// chooses a reusable core, so groups can share a proper subset without
// source-to-source comparisons.
static BlpcComposeResult GcnpComposeRoaringDetailed(
	const RoaringRelation& left, const RoaringRelation& right)
{
	AlgorithmPhaseTimer group_construction(AlgorithmPhase::GC);
	auto join_begin = chrono::steady_clock::now();
	OperationStopwatch planning_timer;
	size_t naive_union_work = 0;
	size_t candidate_pairs = 0;
	const char* pattern_mode = std::getenv("GCNP_PATTERN_MODE");
	const bool use_exact_seed = pattern_mode != nullptr &&
		std::strcmp(pattern_mode, "exact_seed") == 0;
	const bool use_seed_core_prefix = !use_exact_seed;
	const auto index_begin = chrono::steady_clock::now();
	vector<pair<size_t, const SharedBitmapRow*>> sources;
	sources.reserve(left.size());
	for (const auto& entry : left) sources.emplace_back(entry.first, &entry.second);
	SortGcnpSourceRows(sources);

	// The left relation outlives planning/execution. Keep the same filtered
	// sets as read-only row/mask views rather than allocating per source.
	vector<uint64_t> usable_cardinalities(sources.size());
	roaring::Roaring right_keys;
	GcnpColumnIndex columns;
	for (const auto& entry : right) {
		if (entry.first > numeric_limits<uint32_t>::max()) continue;
		const auto mid = static_cast<uint32_t>(entry.first);
		right_keys.add(mid);
		columns.insert(mid).right_cardinality = entry.second.cardinality();
	}
	constexpr size_t planning_threads = 1;
	for (size_t source_index = 0; source_index < sources.size(); ++source_index) {
		const auto& original = *sources[source_index].second;
		uint64_t usable_count = 0;
		for (uint32_t mid : original) {
			const auto found = columns.find(mid);
			if (found == nullptr) continue;
			auto& column = *found;
			++usable_count;
			naive_union_work += column.right_cardinality != 0;
			candidate_pairs += column.right_cardinality;
			// Every inverted column receives ascending source ordinals.
			column.source_ordinals.push_back(static_cast<uint32_t>(source_index));
		}
		usable_cardinalities[source_index] = usable_count;
	}
	const auto index_build_end = chrono::steady_clock::now();
	columns.finalize();
	const auto index_finalize_end = chrono::steady_clock::now();

	roaring::Roaring unassigned;
	roaring::api::roaring_bulk_context_t unassigned_bulk{};
	for (uint32_t source_index = 0; source_index < sources.size(); ++source_index) {
		if (usable_cardinalities[source_index] != 0) {
			roaring::api::roaring_bitmap_add_bulk(&unassigned.roaring, &unassigned_bulk, source_index);
		}
	}
	const auto index_end = chrono::steady_clock::now();

	vector<GcnpGroup> groups;
	size_t partial_core_groups = 0;
	auto close_group = [&](const roaring::Roaring& group_sources, size_t guaranteed_common_count) {
		roaring::Roaring common_mids;
		bool initialized = false;
		for (uint32_t source_index : group_sources) {
			if (!initialized) {
				common_mids = sources[source_index].second->bitmap() & right_keys;
				initialized = true;
			} else {
				common_mids &= sources[source_index].second->bitmap();
			}
			// Every selected row contains the chosen prefix. Once the
			// intersection reaches that lower bound it cannot shrink further.
			if (common_mids.cardinality() == guaranteed_common_count) break;
		}
		return common_mids;
	};
	vector<pair<size_t, uint32_t>> seed_candidates;
	seed_candidates.reserve(sources.size());
		for (uint32_t source_index = 0; source_index < sources.size(); ++source_index) {
			if (usable_cardinalities[source_index] != 0) {
				seed_candidates.emplace_back(usable_cardinalities[source_index], source_index);
			}
		}
	priority_queue<pair<size_t, uint32_t>> pattern_heap(
		std::less<pair<size_t, uint32_t>>{}, std::move(seed_candidates));
	const auto heap_end = chrono::steady_clock::now();
		while (!unassigned.isEmpty()) {
			while (!pattern_heap.empty() &&
				!unassigned.contains(pattern_heap.top().second)) {
				pattern_heap.pop();
			}
			if (pattern_heap.empty()) break;
			const uint32_t seed_source = pattern_heap.top().second;
			pattern_heap.pop();
			const roaring::Roaring seed_pattern = sources[seed_source].second->bitmap() & right_keys;

			roaring::Roaring group_sources;
			size_t guaranteed_common_count = 0;
			if (use_seed_core_prefix) {
				// A prefix is sufficient to expose partial common patterns without
				// enumerating source pairs or arbitrary subsets of the seed row.
				vector<pair<size_t, uint32_t>> ordered_mids;
				ordered_mids.reserve(seed_pattern.cardinality());
				for (uint32_t mid : seed_pattern) {
					ordered_mids.emplace_back(columns.at(mid).sources.and_cardinality(unassigned), mid);
				}
				sort(ordered_mids.begin(), ordered_mids.end(),
					[](const auto& lhs, const auto& rhs) {
						return lhs.first != rhs.first ? lhs.first > rhs.first : lhs.second < rhs.second;
					});

				roaring::Roaring prefix_sources;
				size_t best_score = 0;
				size_t best_group_size = 0;
				size_t prefix_size = 0;
				for (const auto& [_, mid] : ordered_mids) {
					if (prefix_size == 0) prefix_sources = columns.at(mid).sources & unassigned;
					else prefix_sources &= columns.at(mid).sources;
					if (prefix_sources.isEmpty()) break;
					++prefix_size;
					const size_t group_size = prefix_sources.cardinality();
					const size_t score = group_size > 1 ? (group_size - 1) * prefix_size : 0;
					// Equal sharing value keeps the larger group, then the longer core.
					if (group_sources.isEmpty() || score > best_score ||
						(score == best_score && group_size > best_group_size)) {
						group_sources = prefix_sources;
						best_score = score;
						best_group_size = group_size;
						guaranteed_common_count = prefix_size;
					}
				}
			} else {
				group_sources = unassigned;
				for (uint32_t mid : seed_pattern) {
					group_sources &= columns.at(mid).sources;
				}
				guaranteed_common_count = seed_pattern.cardinality();
			}
			if (group_sources.isEmpty()) group_sources.add(seed_source);

			roaring::Roaring common_mids = close_group(group_sources, guaranteed_common_count);
			groups.push_back({group_sources, std::move(common_mids)});
			if (use_seed_core_prefix &&
				groups.back().common_mids.cardinality() < seed_pattern.cardinality()) {
				++partial_core_groups;
			}
			unassigned -= group_sources;
		}
	const auto grouping_end = chrono::steady_clock::now();
	const size_t candidate_pattern_count = sources.size();
	columns.release();
	decltype(pattern_heap){}.swap(pattern_heap);
	right_keys = roaring::Roaring{};
	unassigned = roaring::Roaring{};
	const auto release_end = chrono::steady_clock::now();
	double planning_time_ms = planning_timer.elapsed_ms();
	group_construction.stop();

	size_t reusable_group_count = 0;
	uint64_t all_common_mids = 0;
	uint64_t reusable_common_mids = 0;
	size_t max_common_mids = 0;
	size_t max_reusable_common_mids = 0;
	size_t all_mid_bins[6] = {};
	size_t reusable_mid_bins[6] = {};
	auto mid_bin = [](size_t count) {
		return count <= 1 ? 0 : count == 2 ? 1 : count <= 4 ? 2 :
			count <= 8 ? 3 : count <= 16 ? 4 : 5;
	};
	QueryPhaseTimer group_statistics(QueryTimingPhase::RelationStats);
	for (const GcnpGroup& group : groups) {
		const size_t mid_count = group.common_mids.cardinality();
		all_common_mids += mid_count;
		max_common_mids = std::max(max_common_mids, mid_count);
		++all_mid_bins[mid_bin(mid_count)];
		if (group.sources.cardinality() > 1 && !group.common_mids.isEmpty()) {
			++reusable_group_count;
			reusable_common_mids += mid_count;
			max_reusable_common_mids = std::max(max_reusable_common_mids, mid_count);
			++reusable_mid_bins[mid_bin(mid_count)];
		}
	}
	std::cout << "[GCNP_GROUP_MIDS] groups=" << groups.size()
		<< " total_common_mids=" << all_common_mids
		<< " avg_common_mids=" << (groups.empty() ? 0.0 :
			static_cast<double>(all_common_mids) / groups.size())
		<< " max_common_mids=" << max_common_mids
		<< " bins_1_2_3to4_5to8_9to16_17plus=";
	for (size_t i = 0; i < 6; ++i) std::cout << (i ? "," : "") << all_mid_bins[i];
	std::cout << " reusable_groups=" << reusable_group_count
		<< " reusable_total_common_mids=" << reusable_common_mids
		<< " reusable_avg_common_mids=" << (reusable_group_count == 0 ? 0.0 :
			static_cast<double>(reusable_common_mids) / reusable_group_count)
		<< " reusable_max_common_mids=" << max_reusable_common_mids
		<< " reusable_bins_1_2_3to4_5to8_9to16_17plus=";
	for (size_t i = 0; i < 6; ++i) std::cout << (i ? "," : "") << reusable_mid_bins[i];
	std::cout << std::endl;
	group_statistics.stop();

	// Each group exhausts all unassigned sources supporting its prefix.
	// Equal common patterns in later groups would contradict that property;
	// only within-group pattern reuse is needed for this partitioner.
	OperationStopwatch merge_timer;
	ReadOnlyRowLookup right_lookup(right);
	const double lookup_build_ms = merge_timer.elapsed_ms();
	const size_t global_cached_pattern_count = 0;
	constexpr size_t thread_count = 1;
	vector<RoaringRelation> worker_outputs(thread_count);
	vector<BlpcStats> worker_stats(thread_count);
	vector<size_t> fully_shared_rows(thread_count, 0);
	vector<size_t> residual_bitmaps(thread_count, 0);
	vector<ComposeExecutionProfile> worker_profiles(thread_count);
	atomic<size_t> next_group(0);
	auto execute_groups = [&](size_t worker_id) {
		RoaringRelation& local_out = worker_outputs[worker_id];
		BlpcStats& local_stats = worker_stats[worker_id];
		ComposeExecutionProfile& profile = worker_profiles[worker_id];
		local_out.reserve(left.size() / thread_count + 1);
		vector<const roaring::Roaring*> shared_inputs;
		vector<const roaring::Roaring*> output_inputs;
		for (;;) {
			const size_t group_index = next_group.fetch_add(1, memory_order_relaxed);
			if (group_index >= groups.size()) break;
			const GcnpGroup& group = groups[group_index];
			const bool has_common_pattern = group.sources.cardinality() > 1 &&
				!group.common_mids.isEmpty();
			const bool reusable = has_common_pattern;
			SharedBitmapRow shared_targets;
			const SharedBitmapRow* shared_targets_ptr = nullptr;
			size_t shared_union_work = 0;
			if (reusable) {
				shared_inputs.clear();
				shared_inputs.reserve(group.common_mids.cardinality());
				const SharedBitmapRow* first_input = nullptr;
				const SharedBitmapRow* only_input = nullptr;
				for (uint32_t mid : group.common_mids) {
					const SharedBitmapRow& row = right_lookup.at(static_cast<size_t>(mid));
					const roaring::Roaring& targets = row;
					if (!targets.isEmpty()) {
						++shared_union_work;
						if (shared_inputs.empty()) first_input = &row;
						only_input = &row;
						shared_inputs.push_back(&targets);
					}
				}
				shared_targets = UnionImmutableBitmapRows(shared_inputs, first_input, only_input);
				shared_targets_ptr = &shared_targets;
				local_stats.total_local_patterns += group.sources.cardinality();
				local_stats.distinct_local_patterns++;
				local_stats.cacheable_patterns++;
				local_stats.cache_hits += group.sources.cardinality() - 1;
				local_stats.cached_result_bytes += shared_targets.getSizeInBytes();
			}
			const uint64_t common_count = reusable ? group.common_mids.cardinality() : 0;
			vector<uint32_t> common_values;
			common_values.reserve(common_count);
			if (reusable) {
				for (uint32_t mid : group.common_mids) common_values.push_back(mid);
			}
			for (uint32_t source_index : group.sources) {
				const bool sample = profile.enabled && (source_index % 1024 == 0);
				const auto begin = sample ? ComposeExecutionProfile::Clock::now() : ComposeExecutionProfile::Clock::time_point{};
				auto merge = begin;
				const roaring::Roaring& usable = sources[source_index].second->bitmap();
				SharedBitmapRow output;
				// common_mids is a subset of every row in this group. Equal
				// cardinalities therefore imply an empty residual, without a diff.
				if (reusable && usable_cardinalities[source_index] == common_count) {
					output = *shared_targets_ptr;
					++fully_shared_rows[worker_id];
				} else {
					output_inputs.clear();
					const SharedBitmapRow* first_input = nullptr;
					const SharedBitmapRow* only_input = nullptr;
					if (reusable && !shared_targets_ptr->isEmpty()) {
						first_input = shared_targets_ptr;
						only_input = shared_targets_ptr;
						output_inputs.push_back(&shared_targets_ptr->bitmap());
					}
					size_t common_index = 0;
					for (uint32_t mid : usable) {
						// The same ascending difference for every pattern length;
						// no temporary bitmap or small/large-pattern policy.
						while (common_index < common_count && common_values[common_index] < mid) ++common_index;
						if (common_index < common_count && common_values[common_index] == mid) continue;
						const auto* row = right_lookup.find(static_cast<size_t>(mid));
						if (row != nullptr && !row->isEmpty()) {
							++local_stats.actual_union_work;
							if (output_inputs.empty()) first_input = row;
							only_input = row;
							output_inputs.push_back(&row->bitmap());
						}
					}
					if (sample) merge = ComposeExecutionProfile::Clock::now();
					if (sample) profile.observe_inputs(output_inputs);
					output = UnionImmutableBitmapRows(output_inputs, first_input, only_input);
				}
				const auto store = sample ? ComposeExecutionProfile::Clock::now() : begin;
				if (!output.isEmpty()) {
					local_stats.output_rows++;
					local_stats.output_pairs += output.cardinality();
					local_out.emplace(sources[source_index].first, std::move(output));
				}
				if (sample) profile.record(begin, merge, store);
			}
			if (reusable) local_stats.actual_union_work += shared_union_work;
		}
	};
	execute_groups(0);

	RoaringRelation out;
	BlpcStats stats;
	for (size_t worker_id = 0; worker_id < thread_count; ++worker_id) {
		AddBlpcStats(stats, worker_stats[worker_id]);
	}
	if (thread_count == 1) {
		out = std::move(worker_outputs.front());
	} else {
		out.reserve(left.size() * 2 + 1);
		for (size_t worker_id = 0; worker_id < thread_count; ++worker_id) {
			for (auto& row : worker_outputs[worker_id]) {
				out.emplace(row.first, std::move(row.second));
			}
		}
	}
	const double merge_time_ms = merge_timer.elapsed_ms();

	BlpcComposeResult result;
	result.candidate_pairs = candidate_pairs;
	result.compose_time_ms = chrono::duration<double, std::milli>(
		chrono::steady_clock::now() - join_begin).count();
	result.naive_union_work = naive_union_work;
	result.gcnp_planning_time_ms = planning_time_ms;
	result.relation_merge_time_ms = merge_time_ms;
	result.stats = stats;
	result.relation = std::move(out);
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	for (const auto& profile : worker_profiles) profile.log("gcnp", right_lookup.bytes(), lookup_build_ms);
	std::cout << "[GCNP] mode="
		<< (use_seed_core_prefix ? "seed_core_prefix_greedy" : "exact_seed_greedy")
		<< " sources=" << sources.size()
		<< " candidate_patterns=" << candidate_pattern_count
		<< " partial_core_groups=" << partial_core_groups
		<< " reusable_groups=" << reusable_group_count
		<< " global_cached_patterns=" << global_cached_pattern_count
		<< " total_groups=" << groups.size()
		<< " union_kernel=fixed_container_directory_or binary_or=native"
		<< " threads=" << thread_count << std::endl;
	size_t fully_shared_count = 0, residual_count = 0;
	for (size_t worker_id = 0; worker_id < thread_count; ++worker_id) {
		fully_shared_count += fully_shared_rows[worker_id];
		residual_count += residual_bitmaps[worker_id];
	}
	std::cout << "[GCNP_EXECUTION] fully_shared_rows=" << fully_shared_count
		<< " residual_bitmaps=" << residual_count
		<< " output_handoff=" << (thread_count == 1 ? "move" : "merge") << std::endl;
	std::cout << "[GCNP_PLANNER] index_threads=" << planning_threads
		<< " index_ms=" << chrono::duration<double, std::milli>(index_end - index_begin).count()
		<< " index_build_ms=" << chrono::duration<double, std::milli>(index_build_end - index_begin).count()
		<< " index_merge_ms=0"
		<< " index_finalize_ms=" << chrono::duration<double, std::milli>(index_finalize_end - index_build_end).count()
		<< " index_batch_rows=0 index_storage=paged_vector_bulk"
		<< " usable_rows=immutable_mask_views"
		<< " index_scan=single_pass source_rows=radix_borrowed candidate_stats=fused"
		<< " source_rows_visited=" << sources.size() << " index_order=ascending_source"
		<< " heap_ms=" << chrono::duration<double, std::milli>(heap_end - index_end).count()
		<< " grouping_ms=" << chrono::duration<double, std::milli>(grouping_end - heap_end).count()
		<< " release_ms=" << chrono::duration<double, std::milli>(release_end - grouping_end).count()
		<< std::endl;
	return result;
}

#if 0
// Legacy outer-product, fixed-block, adaptive, clustering, ordered-DP and
// sort-merge implementations are intentionally excluded from this binary.
constexpr uint64_t kAdaptiveLeafWidth = 64;
constexpr size_t kAdaptiveFullPlannerThreshold = 512;
constexpr size_t kAdaptivePlannerSampleFloor = 512;
constexpr size_t kAdaptivePlannerSampleCap = 4096;
constexpr size_t kAdaptivePlannerByteBudget = 64ULL << 20;
// Ordered-DP planning is deliberately bounded. The planner only changes the
// execution layout; composition is always evaluated from the full relations.
constexpr size_t kOrderedPlannerMaxBoundaries = 128;
constexpr size_t kOrderedPlannerMaxPredecessors = 16;
constexpr size_t kOrderedPlannerJaccardWindow = 32;

struct AdaptiveColumn {
	uint32_t mid = 0;
	roaring::Roaring sources;
};

struct AdaptiveGroup {
	roaring::Roaring sources;
	vector<uint32_t> mids;
	size_t rhs_bytes = 0;
	size_t max_rhs_bytes = 0;
	size_t target_union_bytes = 0;
	bool use_reuse = false;
	roaring::Roaring target_union;
};

struct AdaptivePlanNode {
	uint64_t begin = 0;
	uint64_t end = 0;
	bool keep_interval = true;
	unique_ptr<AdaptivePlanNode> left;
	unique_ptr<AdaptivePlanNode> right;
};

struct AdaptivePlanResult {
	size_t cost = 0;
	unique_ptr<AdaptivePlanNode> node;
};

struct AdaptiveSplit {
	vector<AdaptiveColumn> left;
	vector<AdaptiveColumn> right;
	size_t repartition_work = 0;
};

struct OrderedPartitionPlan {
	vector<uint32_t> source_order;
	vector<size_t> boundaries;
	size_t estimated_cost = 0;
};

static size_t SaturatingAdd(size_t lhs, size_t rhs)
{
	if (lhs > numeric_limits<size_t>::max() - rhs) {
		return numeric_limits<size_t>::max();
	}
	return lhs + rhs;
}

static size_t SaturatingMultiply(size_t lhs, size_t rhs)
{
	if (lhs == 0 || rhs == 0) {
		return 0;
	}
	if (lhs > numeric_limits<size_t>::max() / rhs) {
		return numeric_limits<size_t>::max();
	}
	return lhs * rhs;
}

static uint64_t AdaptiveHash(uint32_t value)
{
	uint64_t x = value + 0x9e3779b97f4a7c15ULL;
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
	x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
	return x ^ (x >> 31);
}

static bool UseOrderedAdaptivePlanner()
{
	const char* mode = std::getenv("OUTER_ADAPTIVE_PLANNER");
	return mode != nullptr &&
		(std::strcmp(mode, "ordered_dp") == 0 || std::strcmp(mode, "ordered_dp_full") == 0);
}

static bool UseFullOrderedAdaptivePlanner()
{
	const char* mode = std::getenv("OUTER_ADAPTIVE_PLANNER");
	return mode != nullptr && std::strcmp(mode, "ordered_dp_full") == 0;
}

static bool UseClusterBinaryAdaptivePlanner()
{
	const char* mode = std::getenv("OUTER_ADAPTIVE_PLANNER");
	return mode != nullptr && std::strcmp(mode, "cluster_binary") == 0;
}

static size_t OrderedFullDpSourceLimit()
{
	const char* configured = std::getenv("OUTER_ORDERED_DP_FULL_MAX_SOURCES");
	if (configured == nullptr || *configured == '\0') {
		return numeric_limits<size_t>::max();
	}
	const unsigned long long value = std::strtoull(configured, nullptr, 10);
	return value == 0 ? numeric_limits<size_t>::max() :
		static_cast<size_t>(value);
}

static vector<AdaptiveColumn> MakeAdaptiveColumns(
	const unordered_map<size_t, roaring::Roaring>& left_columns)
{
	vector<AdaptiveColumn> columns;
	columns.reserve(left_columns.size());
	for (const auto& entry : left_columns) {
		if (!entry.second.isEmpty()) {
			columns.push_back({static_cast<uint32_t>(entry.first), entry.second});
		}
	}
	return columns;
}

static vector<AdaptiveColumn> SelectAdaptivePlannerSample(
	vector<AdaptiveColumn> columns, const RoaringRelation& right,
	size_t* total_mids, size_t* selected_left_bytes, size_t* selected_right_bytes)
{
	*total_mids = columns.size();
	*selected_left_bytes = 0;
	*selected_right_bytes = 0;
	if (columns.size() <= kAdaptiveFullPlannerThreshold) {
		for (const auto& column : columns) {
			*selected_left_bytes = SaturatingAdd(*selected_left_bytes, column.sources.getSizeInBytes());
			auto right_it = right.find(column.mid);
			if (right_it != right.end()) {
				*selected_right_bytes = SaturatingAdd(*selected_right_bytes, right_it->second.getSizeInBytes());
			}
		}
		return columns;
	}

	const size_t percentage_target = (columns.size() + 19) / 20; // ceil(5%)
	const size_t target = min(kAdaptivePlannerSampleCap,
		max(kAdaptivePlannerSampleFloor, percentage_target));
	sort(columns.begin(), columns.end(), [](const AdaptiveColumn& lhs, const AdaptiveColumn& rhs) {
		const uint64_t lhs_hash = AdaptiveHash(lhs.mid);
		const uint64_t rhs_hash = AdaptiveHash(rhs.mid);
		return lhs_hash == rhs_hash ? lhs.mid < rhs.mid : lhs_hash < rhs_hash;
	});

	vector<AdaptiveColumn> sample;
	sample.reserve(min(target, columns.size()));
	for (auto& column : columns) {
		if (sample.size() >= target) {
			break;
		}
		const size_t left_bytes = column.sources.getSizeInBytes();
		size_t right_bytes = 0;
		auto right_it = right.find(column.mid);
		if (right_it != right.end()) {
			right_bytes = right_it->second.getSizeInBytes();
		}
		if (!sample.empty() &&
			(SaturatingAdd(*selected_left_bytes, left_bytes) > kAdaptivePlannerByteBudget ||
			 SaturatingAdd(*selected_right_bytes, right_bytes) > kAdaptivePlannerByteBudget)) {
			break;
		}
		*selected_left_bytes = SaturatingAdd(*selected_left_bytes, left_bytes);
		*selected_right_bytes = SaturatingAdd(*selected_right_bytes, right_bytes);
		sample.push_back(std::move(column));
	}
	return sample;
}

static vector<AdaptiveGroup> BuildAdaptiveGroups(
	const vector<AdaptiveColumn>& columns, const RoaringRelation& right,
	bool materialize_target_unions, size_t* plan_cost)
{
	vector<AdaptiveGroup> groups;
	groups.reserve(columns.size());
	unordered_map<uint64_t, vector<size_t>> hash_to_group_ids;
	for (const auto& column : columns) {
		if (column.sources.isEmpty()) {
			continue;
		}
		if (plan_cost != nullptr) {
			*plan_cost = SaturatingAdd(*plan_cost, column.sources.getSizeInBytes());
		}
		const uint64_t hash = HashRoaringBitmap(column.sources);
		auto& candidates = hash_to_group_ids[hash];
		size_t group_id = numeric_limits<size_t>::max();
		for (size_t candidate : candidates) {
			if (groups[candidate].sources == column.sources) {
				group_id = candidate;
				break;
			}
		}
		if (group_id == numeric_limits<size_t>::max()) {
			AdaptiveGroup group;
			group.sources = column.sources;
			groups.push_back(std::move(group));
			group_id = groups.size() - 1;
			candidates.push_back(group_id);
		}
		AdaptiveGroup& group = groups[group_id];
		group.mids.push_back(column.mid);
		auto right_it = right.find(column.mid);
		if (right_it != right.end()) {
			const size_t rhs_bytes = right_it->second.getSizeInBytes();
			group.rhs_bytes = SaturatingAdd(group.rhs_bytes, rhs_bytes);
			group.max_rhs_bytes = max(group.max_rhs_bytes, rhs_bytes);
		}
	}

	for (auto& group : groups) {
		const size_t source_count = group.sources.cardinality();
		const size_t direct_cost = SaturatingMultiply(source_count, group.rhs_bytes);
		if (source_count <= 1 || group.mids.size() <= 1) {
			continue;
		}
		if (!materialize_target_unions) {
			// During planning, avoid materializing unions at every DP node. The
			// largest input row is a lower bound for the union size, so this is
			// an optimistic reuse estimate used only to choose source intervals.
			const size_t reuse_lower_bound = SaturatingAdd(group.rhs_bytes,
				SaturatingMultiply(source_count, group.max_rhs_bytes));
			if (reuse_lower_bound < direct_cost) {
				group.use_reuse = true;
				group.target_union_bytes = group.max_rhs_bytes;
			}
			continue;
		}
		roaring::Roaring target_union;
		for (uint32_t mid : group.mids) {
			auto right_it = right.find(mid);
			if (right_it != right.end()) {
				target_union |= right_it->second;
			}
		}
		const size_t reuse_cost = SaturatingAdd(group.rhs_bytes,
			SaturatingMultiply(source_count, target_union.getSizeInBytes()));
		if (reuse_cost < direct_cost) {
			group.use_reuse = true;
			group.target_union_bytes = target_union.getSizeInBytes();
			group.target_union = std::move(target_union);
		}
	}
	return groups;
}

static size_t AdaptiveGroupsCost(const vector<AdaptiveGroup>& groups, size_t plan_cost)
{
	for (const auto& group : groups) {
		const size_t source_count = group.sources.cardinality();
		const size_t direct_cost = SaturatingMultiply(source_count, group.rhs_bytes);
		if (group.use_reuse) {
			const size_t reuse_cost = SaturatingAdd(group.rhs_bytes,
				SaturatingMultiply(source_count, group.target_union_bytes));
			plan_cost = SaturatingAdd(plan_cost, reuse_cost);
		} else {
			plan_cost = SaturatingAdd(plan_cost, direct_cost);
		}
	}
	return plan_cost;
}

static size_t SignatureIntersection(const vector<uint32_t>& lhs,
	const vector<uint32_t>& rhs)
{
	size_t left = 0;
	size_t right = 0;
	size_t common = 0;
	while (left < lhs.size() && right < rhs.size()) {
		if (lhs[left] == rhs[right]) {
			++common;
			++left;
			++right;
		} else if (lhs[left] < rhs[right]) {
			++left;
		} else {
			++right;
		}
	}
	return common;
}

static uint64_t SignatureMinHash(const vector<uint32_t>& signature, uint32_t seed)
{
	uint64_t value = numeric_limits<uint64_t>::max();
	for (uint32_t mid : signature) {
		value = min(value, AdaptiveHash(mid ^ seed));
	}
	return value;
}

// Builds an approximate Jaccard order without an all-pairs comparison. A
// min-hash sort creates local candidate neighborhoods, then a bounded greedy
// pass picks the highest exact-Jaccard successor inside each neighborhood.
static vector<uint32_t> BuildJaccardSourceOrder(
	const vector<AdaptiveColumn>& sample_columns,
	const vector<AdaptiveColumn>& full_columns,
	vector<size_t>* cluster_boundaries)
{
	cluster_boundaries->clear();
	vector<uint32_t> sources;
	for (const auto& column : full_columns) {
		for (uint32_t source : column.sources) {
			sources.push_back(source);
		}
	}
	sort(sources.begin(), sources.end());
	sources.erase(unique(sources.begin(), sources.end()), sources.end());
	if (sources.empty()) {
		return {};
	}

	unordered_map<uint32_t, size_t> source_index;
	source_index.reserve(sources.size() * 2 + 1);
	for (size_t index = 0; index < sources.size(); ++index) {
		source_index.emplace(sources[index], index);
	}
	vector<vector<uint32_t>> signatures(sources.size());
	for (size_t sample_id = 0; sample_id < sample_columns.size(); ++sample_id) {
		for (uint32_t source : sample_columns[sample_id].sources) {
			auto found = source_index.find(source);
			if (found != source_index.end()) {
				signatures[found->second].push_back(static_cast<uint32_t>(sample_id));
			}
		}
	}

	vector<size_t> order(sources.size());
	for (size_t index = 0; index < order.size(); ++index) {
		order[index] = index;
	}
	vector<uint64_t> sketch1(sources.size());
	vector<uint64_t> sketch2(sources.size());
	for (size_t index = 0; index < order.size(); ++index) {
		sketch1[index] = SignatureMinHash(signatures[index], 0x9e3779b9U);
		sketch2[index] = SignatureMinHash(signatures[index], 0x85ebca6bU);
	}
	sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
		const bool lhs_empty = signatures[lhs].empty();
		const bool rhs_empty = signatures[rhs].empty();
		if (lhs_empty != rhs_empty) return !lhs_empty;
		if (sketch1[lhs] != sketch1[rhs]) return sketch1[lhs] < sketch1[rhs];
		if (sketch2[lhs] != sketch2[rhs]) return sketch2[lhs] < sketch2[rhs];
		return sources[lhs] < sources[rhs];
	});

	for (size_t begin = 0; begin < order.size(); begin += kOrderedPlannerJaccardWindow) {
		const size_t end = min(order.size(), begin + kOrderedPlannerJaccardWindow);
		for (size_t current = begin; current + 1 < end; ++current) {
			const vector<uint32_t>& current_signature = signatures[order[current]];
			size_t best = current + 1;
			size_t best_common = 0;
			size_t best_union = 1;
			for (size_t candidate = current + 1; candidate < end; ++candidate) {
				const vector<uint32_t>& candidate_signature = signatures[order[candidate]];
				const size_t common = SignatureIntersection(current_signature, candidate_signature);
				const size_t combined = current_signature.size() + candidate_signature.size() - common;
				if (common * best_union > best_common * combined) {
					best = candidate;
					best_common = common;
					best_union = max(static_cast<size_t>(1), combined);
				}
			}
			swap(order[current + 1], order[best]);
		}
	}

	// A low adjacent Jaccard value is a local cluster boundary. This is not an
	// all-pairs clustering pass: signatures are compared only after the
	// similarity-guided ordering has made likely neighbors adjacent.
	vector<pair<double, size_t>> boundary_scores;
	boundary_scores.reserve(order.size());
	for (size_t position = kAdaptiveLeafWidth;
		position + kAdaptiveLeafWidth <= order.size(); ++position) {
		const vector<uint32_t>& previous = signatures[order[position - 1]];
		const vector<uint32_t>& next = signatures[order[position]];
		if (previous.empty() || next.empty()) {
			continue;
		}
		const size_t common = SignatureIntersection(previous, next);
		const size_t combined = previous.size() + next.size() - common;
		boundary_scores.push_back({static_cast<double>(common) / combined, position});
	}
	sort(boundary_scores.begin(), boundary_scores.end(),
		[](const pair<double, size_t>& lhs, const pair<double, size_t>& rhs) {
			return lhs.first == rhs.first ? lhs.second < rhs.second : lhs.first < rhs.first;
		});
	cluster_boundaries->push_back(0);
	for (const auto& candidate : boundary_scores) {
		if (cluster_boundaries->size() + 1 >= kOrderedPlannerMaxBoundaries) {
			break;
		}
		bool sufficiently_separated = true;
		for (size_t existing : *cluster_boundaries) {
			const size_t distance = candidate.second > existing
				? candidate.second - existing : existing - candidate.second;
			if (distance < kAdaptiveLeafWidth) {
				sufficiently_separated = false;
				break;
			}
		}
		if (sufficiently_separated) {
			cluster_boundaries->push_back(candidate.second);
		}
	}
	cluster_boundaries->push_back(order.size());
	sort(cluster_boundaries->begin(), cluster_boundaries->end());

	vector<uint32_t> result;
	result.reserve(sources.size());
	for (size_t index : order) {
		result.push_back(sources[index]);
	}
	return result;
}

static vector<AdaptiveColumn> RemapColumnsToLogicalOrder(
	const vector<AdaptiveColumn>& columns,
	const unordered_map<uint32_t, uint32_t>& logical_positions)
{
	vector<AdaptiveColumn> remapped;
	remapped.reserve(columns.size());
	for (const auto& column : columns) {
		AdaptiveColumn mapped;
		mapped.mid = column.mid;
		for (uint32_t source : column.sources) {
			auto position = logical_positions.find(source);
			if (position != logical_positions.end()) {
				mapped.sources.add(position->second);
			}
		}
		if (!mapped.sources.isEmpty()) {
			remapped.push_back(std::move(mapped));
		}
	}
	return remapped;
}

static size_t EstimateOrderedIntervalCost(const vector<AdaptiveColumn>& columns,
	const RoaringRelation& right, size_t begin, size_t end)
{
	vector<AdaptiveColumn> interval_columns;
	interval_columns.reserve(columns.size());
	for (const auto& column : columns) {
		AdaptiveColumn local;
		local.mid = column.mid;
		for (uint32_t position : column.sources) {
			if (position >= begin && position < end) {
				local.sources.add(position);
			}
		}
		if (!local.sources.isEmpty()) {
			interval_columns.push_back(std::move(local));
		}
	}
	size_t cost = 0;
	vector<AdaptiveGroup> groups = BuildAdaptiveGroups(interval_columns, right, false, &cost);
	return AdaptiveGroupsCost(groups, cost);
}

static bool BuildOrderedPartitionPlan(const vector<AdaptiveColumn>& sample_columns,
	const vector<AdaptiveColumn>& full_columns, const RoaringRelation& right,
	bool full_dp, OrderedPartitionPlan* plan)
{
	vector<size_t> cluster_boundaries;
	plan->source_order = BuildJaccardSourceOrder(sample_columns, full_columns,
		&cluster_boundaries);
	if (plan->source_order.empty()) {
		return false;
	}
	const size_t source_count = plan->source_order.size();
	if (full_dp && source_count > OrderedFullDpSourceLimit()) {
		return false;
	}
	unordered_map<uint32_t, uint32_t> logical_positions;
	logical_positions.reserve(source_count * 2 + 1);
	for (size_t position = 0; position < source_count; ++position) {
		logical_positions.emplace(plan->source_order[position], static_cast<uint32_t>(position));
	}
	vector<AdaptiveColumn> ordered_sample = RemapColumnsToLogicalOrder(sample_columns, logical_positions);
	if (ordered_sample.empty() || source_count <= kAdaptiveLeafWidth) {
		plan->boundaries = {0, source_count};
		plan->estimated_cost = EstimateOrderedIntervalCost(ordered_sample, right, 0, source_count);
		return true;
	}

	vector<size_t> boundaries;
	if (full_dp) {
		boundaries.reserve(source_count + 1);
		for (size_t boundary = 0; boundary <= source_count; ++boundary) {
			boundaries.push_back(boundary);
		}
	} else {
		boundaries = std::move(cluster_boundaries);
	}

	const size_t unreachable = numeric_limits<size_t>::max();
	vector<size_t> dp(boundaries.size(), unreachable);
	vector<size_t> predecessor(boundaries.size(), 0);
	dp[0] = 0;
	for (size_t end = 1; end < boundaries.size(); ++end) {
		const size_t first = full_dp ? 0 :
			(end > kOrderedPlannerMaxPredecessors
				? end - kOrderedPlannerMaxPredecessors : 0);
		for (size_t begin = first; begin < end; ++begin) {
			if (dp[begin] == unreachable) continue;
			const size_t interval_cost = EstimateOrderedIntervalCost(
				ordered_sample, right, boundaries[begin], boundaries[end]);
			// A partition pays for sampled bitmap intersections and group lookup.
			const size_t partition_penalty = SaturatingMultiply(
				ordered_sample.size(), sizeof(uint32_t) * 2);
			const size_t candidate = SaturatingAdd(dp[begin],
				SaturatingAdd(interval_cost, begin == 0 ? 0 : partition_penalty));
			if (candidate < dp[end]) {
				dp[end] = candidate;
				predecessor[end] = begin;
			}
		}
	}
	if (dp.back() == unreachable) {
		return false;
	}
	plan->boundaries.clear();
	for (size_t position = boundaries.size() - 1;; position = predecessor[position]) {
		plan->boundaries.push_back(boundaries[position]);
		if (position == 0) break;
	}
	reverse(plan->boundaries.begin(), plan->boundaries.end());
	plan->estimated_cost = dp.back();
	return true;
}

struct OuterPatternEntry {
	uint32_t block_id = 0;
	uint64_t hash = 0;
	roaring::Roaring source_mask;
	vector<uint32_t> mids;
	size_t rhs_bytes = 0;
	roaring::Roaring target_union;
};

struct OuterJoinColumn {
	uint32_t mid = 0;
	const roaring::Roaring* sources = nullptr;
	const roaring::Roaring* targets = nullptr;
};

// The outer executor stores its left input inverted by middle vertex. Align
// those columns with right-relation rows through a global sort-merge join.
// Roaring rows themselves are already sorted; only relation key sets are
// sorted here.
static vector<OuterJoinColumn> BuildOuterSortMergeColumns(
	const unordered_map<size_t, roaring::Roaring>& left_columns,
	const RoaringRelation& right)
{
	vector<pair<uint32_t, const roaring::Roaring*>> left_keys;
	vector<pair<uint32_t, const roaring::Roaring*>> right_keys;
	left_keys.reserve(left_columns.size());
	right_keys.reserve(right.size());
	for (const auto& entry : left_columns) {
		if (!entry.second.isEmpty()) {
			left_keys.emplace_back(static_cast<uint32_t>(entry.first), &entry.second);
		}
	}
	for (const auto& entry : right) {
		if (!entry.second.isEmpty()) {
			right_keys.emplace_back(static_cast<uint32_t>(entry.first), &entry.second);
		}
	}
	auto by_mid = [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; };
	sort(left_keys.begin(), left_keys.end(), by_mid);
	sort(right_keys.begin(), right_keys.end(), by_mid);

	vector<OuterJoinColumn> joined;
	joined.reserve(min(left_keys.size(), right_keys.size()));
	size_t left_index = 0;
	size_t right_index = 0;
	while (left_index < left_keys.size() && right_index < right_keys.size()) {
		if (left_keys[left_index].first < right_keys[right_index].first) {
			++left_index;
		} else if (right_keys[right_index].first < left_keys[left_index].first) {
			++right_index;
		} else {
			joined.push_back({left_keys[left_index].first, left_keys[left_index].second,
				right_keys[right_index].second});
			++left_index;
			++right_index;
		}
	}
	return joined;
}

static bool UseOuterCompose()
{
	const char* mode = std::getenv("OUTER_COMPOSE_MODE");
	return mode == nullptr || std::strcmp(mode, "blpc") != 0;
}

static bool UseOuterPatternReuse()
{
	const char* mode = std::getenv("OUTER_PATTERN_REUSE");
	return mode == nullptr || std::strcmp(mode, "0") != 0;
}

static bool UseAdaptiveOuterPartition()
{
	const char* mode = std::getenv("OUTER_ADAPTIVE_PARTITION");
	return mode == nullptr || std::strcmp(mode, "0") != 0;
}

// Ablation only: construct the bounded suffix as the explicit union of every
// exact power instead of the default dual-state recurrence.
static bool UseNaiveBoundedSuffixUnion()
{
	const char* mode = std::getenv("BOUNDED_SUFFIX_MODE");
	return mode != nullptr && std::strcmp(mode, "naive_union") == 0;
}

// Computes A o B as the union of outer products A(:,m) x B(m,:).
// Source masks are grouped per source-id block so mids with the same local
// source set share one target-bitmap union before it is broadcast.
static BlpcComposeResult OuterComposeRoaringFixedDetailed(
	const RoaringRelation& left,
	const RoaringRelation& right)
{
	auto join_begin = std::chrono::steady_clock::now();
	unordered_map<size_t, roaring::Roaring> left_columns;
	left_columns.reserve(left.size() * 2 + 1);
	for (const auto& source_row : left) {
		const uint32_t source = static_cast<uint32_t>(source_row.first);
		for (uint32_t mid : source_row.second) {
			if (right.find(static_cast<size_t>(mid)) != right.end()) {
				left_columns[static_cast<size_t>(mid)].add(source);
			}
		}
	}

	vector<OuterPatternEntry> patterns;
	patterns.reserve(left_columns.size());
	unordered_map<uint32_t, vector<uint32_t>> block_to_pattern_ids;
	BlpcStats stats;

	for (const auto& column : left_columns) {
		const uint32_t mid = static_cast<uint32_t>(column.first);
		auto right_it = right.find(column.first);
		if (right_it == right.end()) {
			continue;
		}
		const size_t rhs_bytes = right_it->second.getSizeInBytes();
		if (!UseOuterPatternReuse()) {
			// Pure outer-product ablation: do not split the source bitmap into
			// blocks and do not extract local patterns. Each mid uses one full
			// inverted source bitmap and is broadcast directly.
			OuterPatternEntry entry;
			entry.block_id = 0;
			entry.source_mask = column.second;
			entry.mids.push_back(mid);
			entry.rhs_bytes = rhs_bytes;
			patterns.push_back(std::move(entry));
			stats.total_local_patterns++;
			continue;
		}
		auto local_masks = SplitRowIntoBlockPatterns(column.second);
		stats.total_local_patterns += local_masks.size();
		for (auto& part : local_masks) {
			const uint32_t block_id = part.first;
			roaring::Roaring& source_mask = part.second;
			const uint64_t hash = HashRoaringBitmap(source_mask);
			auto& candidate_ids = block_to_pattern_ids[block_id];

			uint32_t found_id = std::numeric_limits<uint32_t>::max();
			if (UseOuterPatternReuse()) {
				for (uint32_t pattern_id : candidate_ids) {
					OuterPatternEntry& entry = patterns[pattern_id];
					if (entry.hash == hash && entry.source_mask == source_mask) {
						found_id = pattern_id;
						break;
					}
				}
			}

			if (found_id == std::numeric_limits<uint32_t>::max()) {
				OuterPatternEntry entry;
				entry.block_id = block_id;
				entry.hash = hash;
				entry.source_mask = std::move(source_mask);
				entry.mids.push_back(mid);
				entry.rhs_bytes = rhs_bytes;
				patterns.push_back(std::move(entry));
				candidate_ids.push_back(static_cast<uint32_t>(patterns.size() - 1));
			} else {
				patterns[found_id].mids.push_back(mid);
				patterns[found_id].rhs_bytes += rhs_bytes;
			}
		}
	}

	stats.distinct_local_patterns = patterns.size();
	RoaringRelation out;
	out.reserve(left.size() * 2 + 1);
	for (auto& entry : patterns) {
		const size_t source_count = entry.source_mask.cardinality();
		for (uint32_t mid : entry.mids) {
			auto right_it = right.find(static_cast<size_t>(mid));
			if (right_it != right.end()) {
				stats.actual_union_work += right_it->second.cardinality();
				entry.target_union |= right_it->second;
			}
		}

		if (entry.mids.size() > 1) {
			stats.cacheable_patterns++;
			stats.cache_hits += entry.mids.size() - 1;
			stats.cached_result_bytes += entry.target_union.getSizeInBytes();
		}

		for (uint32_t source : entry.source_mask) {
			out[static_cast<size_t>(source)] |= entry.target_union;
		}
	}

	for (const auto& row : out) {
		if (!row.second.isEmpty()) {
			stats.output_rows++;
			stats.output_pairs += row.second.cardinality();
		}
	}

#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	std::cerr << "[OUTER][join] left_rows=" << left.size()
	          << " right_rows=" << right.size()
	          << " inverted_mids=" << left_columns.size()
	          << " source_block_size=" << GetBlpcBlockSize() << std::endl;
	std::cerr << "[OUTER][patterns] instances=" << stats.total_local_patterns
	          << " distinct_masks=" << stats.distinct_local_patterns
	          << " grouped_masks=" << stats.cacheable_patterns
	          << " reuse=" << (UseOuterPatternReuse() ? "on" : "off")
	          << std::endl;
#endif

	BlpcComposeResult result;
	result.candidate_pairs = ComputeCandidatePairs(left, right);
	result.compose_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - join_begin).count());
	result.stats = stats;
	result.relation = std::move(out);
	return result;
}

static AdaptiveSplit SplitAdaptiveColumns(
	const vector<AdaptiveColumn>& columns, uint64_t midpoint)
{
	AdaptiveSplit split;
	split.left.reserve(columns.size());
	split.right.reserve(columns.size());
	for (const auto& column : columns) {
		const size_t parent_bytes = column.sources.getSizeInBytes();
		const size_t source_count = column.sources.cardinality();
		AdaptiveColumn left_column;
		AdaptiveColumn right_column;
		left_column.mid = column.mid;
		right_column.mid = column.mid;
		for (uint32_t source : column.sources) {
			(source < midpoint ? left_column.sources : right_column.sources).add(source);
		}
		// Reading the parent bitmap, routing uint32_t source ids, and building
		// the two child bitmaps are all paid by an actual source split.
		split.repartition_work = SaturatingAdd(split.repartition_work, parent_bytes);
		split.repartition_work = SaturatingAdd(split.repartition_work,
			SaturatingMultiply(source_count, sizeof(uint32_t)));
		split.repartition_work = SaturatingAdd(split.repartition_work,
			left_column.sources.getSizeInBytes());
		split.repartition_work = SaturatingAdd(split.repartition_work,
			right_column.sources.getSizeInBytes());
		if (!left_column.sources.isEmpty()) {
			split.left.push_back(std::move(left_column));
		}
		if (!right_column.sources.isEmpty()) {
			split.right.push_back(std::move(right_column));
		}
	}
	return split;
}

static AdaptivePlanResult BuildAdaptivePlan(const vector<AdaptiveColumn>& columns,
	const RoaringRelation& right, uint64_t begin, uint64_t end)
{
	size_t keep_cost = 0;
	vector<AdaptiveGroup> groups = BuildAdaptiveGroups(columns, right, false,
		&keep_cost);
	keep_cost = AdaptiveGroupsCost(groups, keep_cost);

	AdaptivePlanResult result;
	result.node = make_unique<AdaptivePlanNode>();
	result.node->begin = begin;
	result.node->end = end;
	if (columns.empty() || end - begin <= kAdaptiveLeafWidth) {
		result.cost = keep_cost;
		return result;
	}

	const uint64_t midpoint = begin + (end - begin) / 2;
	AdaptiveSplit children = SplitAdaptiveColumns(columns, midpoint);
	AdaptivePlanResult left_plan = BuildAdaptivePlan(children.left, right, begin, midpoint);
	AdaptivePlanResult right_plan = BuildAdaptivePlan(children.right, right, midpoint, end);
	const size_t split_cost = SaturatingAdd(
		SaturatingAdd(left_plan.cost, right_plan.cost), children.repartition_work);
	if (keep_cost <= split_cost) {
		result.cost = keep_cost;
		return result;
	}
	result.cost = split_cost;
	result.node->keep_interval = false;
	result.node->left = std::move(left_plan.node);
	result.node->right = std::move(right_plan.node);
	return result;
}

static void ExecuteAdaptiveGroups(const vector<AdaptiveColumn>& columns,
	const RoaringRelation& right, RoaringRelation& out, BlpcStats& stats)
{
	size_t ignored_plan_cost = 0;
	vector<AdaptiveGroup> groups = BuildAdaptiveGroups(columns, right, true,
		&ignored_plan_cost);
	for (auto& group : groups) {
		stats.total_local_patterns += group.mids.size();
		stats.distinct_local_patterns++;
		const size_t source_count = group.sources.cardinality();
		if (group.use_reuse) {
			stats.cacheable_patterns++;
			stats.cache_hits += group.mids.size() - 1;
			for (uint32_t mid : group.mids) {
				auto right_it = right.find(mid);
				if (right_it != right.end()) {
					stats.actual_union_work = SaturatingAdd(stats.actual_union_work,
						right_it->second.cardinality());
				}
			}
			stats.cached_result_bytes = SaturatingAdd(stats.cached_result_bytes,
				group.target_union.getSizeInBytes());
			for (uint32_t source : group.sources) {
				out[static_cast<size_t>(source)] |= group.target_union;
			}
			continue;
		}

		for (uint32_t mid : group.mids) {
			auto right_it = right.find(mid);
			if (right_it == right.end()) {
				continue;
			}
			stats.actual_union_work = SaturatingAdd(stats.actual_union_work,
				SaturatingMultiply(source_count, right_it->second.cardinality()));
			for (uint32_t source : group.sources) {
				out[static_cast<size_t>(source)] |= right_it->second;
			}
		}
	}
}

static void ExecuteAdaptivePlan(const vector<AdaptiveColumn>& columns,
	const RoaringRelation& right, const AdaptivePlanNode& node,
	RoaringRelation& out, BlpcStats& stats)
{
	if (!node.keep_interval) {
		AdaptiveSplit children = SplitAdaptiveColumns(columns,
			node.begin + (node.end - node.begin) / 2);
		ExecuteAdaptivePlan(children.left, right, *node.left, out, stats);
		ExecuteAdaptivePlan(children.right, right, *node.right, out, stats);
		return;
	}
	ExecuteAdaptiveGroups(columns, right, out, stats);
}

static void ExecuteOrderedPartitionPlan(const vector<AdaptiveColumn>& full_columns,
	const RoaringRelation& right, const OrderedPartitionPlan& plan,
	RoaringRelation& out, BlpcStats& stats)
{
	const size_t partition_count = plan.boundaries.size() - 1;
	unordered_map<uint32_t, uint32_t> source_partition;
	source_partition.reserve(plan.source_order.size() * 2 + 1);
	for (size_t partition = 0; partition < partition_count; ++partition) {
		for (size_t position = plan.boundaries[partition];
			position < plan.boundaries[partition + 1]; ++position) {
			source_partition.emplace(plan.source_order[position],
				static_cast<uint32_t>(partition));
		}
	}

	// Route every (source, mid) once. The previous implementation intersected
	// every full column with every partition mask, multiplying the source
	// projection work by the number of DP partitions.
	vector<unordered_map<uint32_t, roaring::Roaring>> routed_columns(partition_count);
	for (const auto& column : full_columns) {
		for (uint32_t source : column.sources) {
			auto partition = source_partition.find(source);
			if (partition != source_partition.end()) {
				routed_columns[partition->second][column.mid].add(source);
			}
		}
	}

	for (auto& partition_map : routed_columns) {
		vector<AdaptiveColumn> partition_columns;
		partition_columns.reserve(partition_map.size());
		for (auto& column : partition_map) {
			if (!column.second.isEmpty()) {
				partition_columns.push_back({column.first, std::move(column.second)});
			}
		}
		ExecuteAdaptiveGroups(partition_columns, right, out, stats);
	}
}

static BlpcComposeResult OuterComposeRoaringAdaptiveDetailed(
	const RoaringRelation& left, const RoaringRelation& right)
{
	auto join_begin = std::chrono::steady_clock::now();
	unordered_map<size_t, roaring::Roaring> left_columns;
	left_columns.reserve(left.size() * 2 + 1);
	uint32_t max_source = 0;
	for (const auto& source_row : left) {
		const uint32_t source = static_cast<uint32_t>(source_row.first);
		max_source = max(max_source, source);
		for (uint32_t mid : source_row.second) {
			left_columns[static_cast<size_t>(mid)].add(source);
		}
	}
	const vector<OuterJoinColumn> joined_columns = BuildOuterSortMergeColumns(left_columns, right);
	vector<AdaptiveColumn> full_columns;
	full_columns.reserve(joined_columns.size());
	for (const OuterJoinColumn& column : joined_columns) {
		full_columns.push_back({column.mid, *column.sources});
	}
	if (full_columns.empty()) {
		BlpcComposeResult empty;
		empty.candidate_pairs = ComputeCandidatePairs(left, right);
		empty.compose_time_ms = static_cast<double>(chrono::duration_cast<chrono::milliseconds>(
			chrono::steady_clock::now() - join_begin).count());
		return empty;
	}

	size_t total_mids = 0;
	size_t sample_left_bytes = 0;
	size_t sample_right_bytes = 0;
	vector<AdaptiveColumn> sample_columns = SelectAdaptivePlannerSample(
		full_columns, right, &total_mids, &sample_left_bytes, &sample_right_bytes);
	std::cout << "[OUTER][smj] inverted_mids=" << left_columns.size()
		<< " matched_mids=" << joined_columns.size()
		<< " right_rows=" << right.size() << std::endl;
	uint64_t root_end = kAdaptiveLeafWidth;
	const uint64_t required_end = static_cast<uint64_t>(max_source) + 1;
	while (root_end < required_end) {
		root_end <<= 1;
	}

	RoaringRelation out;
	out.reserve(left.size() * 2 + 1);
	BlpcStats stats;
	bool use_cluster_binary_plan = false;
	if (UseClusterBinaryAdaptivePlanner()) {
		vector<size_t> ignored_cluster_boundaries;
		vector<uint32_t> source_order = BuildJaccardSourceOrder(sample_columns,
			full_columns, &ignored_cluster_boundaries);
		if (!source_order.empty()) {
			unordered_map<uint32_t, uint32_t> logical_positions;
			logical_positions.reserve(source_order.size() * 2 + 1);
			for (size_t position = 0; position < source_order.size(); ++position) {
				logical_positions.emplace(source_order[position], static_cast<uint32_t>(position));
			}
			vector<AdaptiveColumn> logical_sample = RemapColumnsToLogicalOrder(
				sample_columns, logical_positions);
			vector<AdaptiveColumn> logical_full = RemapColumnsToLogicalOrder(
				full_columns, logical_positions);
			uint64_t logical_end = kAdaptiveLeafWidth;
			while (logical_end < source_order.size()) {
				logical_end <<= 1;
			}
			AdaptivePlanResult plan = BuildAdaptivePlan(logical_sample, right, 0, logical_end);
			RoaringRelation logical_out;
			logical_out.reserve(left.size() * 2 + 1);
			ExecuteAdaptivePlan(logical_full, right, *plan.node, logical_out, stats);
			for (const auto& row : logical_out) {
				if (row.first < source_order.size()) {
					out[static_cast<size_t>(source_order[row.first])] |= row.second;
				}
			}
			function<size_t(const AdaptivePlanNode&)> count_leaves = [&](const AdaptivePlanNode& node) {
				if (node.keep_interval) return static_cast<size_t>(1);
				return count_leaves(*node.left) + count_leaves(*node.right);
			};
			std::cerr << "[OUTER][cluster-binary] total_mids=" << total_mids
				<< " sample_mids=" << sample_columns.size()
				<< " sources=" << source_order.size()
				<< " leaves=" << count_leaves(*plan.node)
				<< " estimated_work=" << plan.cost << std::endl;
			use_cluster_binary_plan = true;
		}
	}
	OrderedPartitionPlan ordered_plan;
	const bool use_ordered_plan = !use_cluster_binary_plan && UseOrderedAdaptivePlanner() &&
		BuildOrderedPartitionPlan(sample_columns, full_columns, right,
			UseFullOrderedAdaptivePlanner(), &ordered_plan);
	if (use_ordered_plan) {
		std::cerr << "[OUTER][ordered-dp] mode="
			<< (UseFullOrderedAdaptivePlanner() ? "full" : "bounded")
			<< " total_mids=" << total_mids
			<< " sample_mids=" << sample_columns.size()
			<< " sources=" << ordered_plan.source_order.size()
			<< " partitions=" << ordered_plan.boundaries.size() - 1
			<< " estimated_work=" << ordered_plan.estimated_cost << std::endl;
		ExecuteOrderedPartitionPlan(full_columns, right, ordered_plan, out, stats);
	} else if (!use_cluster_binary_plan) {
		AdaptivePlanResult plan = BuildAdaptivePlan(sample_columns, right, 0, root_end);
		function<size_t(const AdaptivePlanNode&)> count_leaves = [&](const AdaptivePlanNode& node) {
			if (node.keep_interval) return static_cast<size_t>(1);
			return count_leaves(*node.left) + count_leaves(*node.right);
		};
		std::cerr << "[OUTER][adaptive] total_mids=" << total_mids
			<< " sample_mids=" << sample_columns.size()
			<< " sample_left_bytes=" << sample_left_bytes
			<< " sample_right_bytes=" << sample_right_bytes
			<< " leaf_width=" << kAdaptiveLeafWidth
			<< " leaves=" << count_leaves(*plan.node)
			<< " estimated_work=" << plan.cost
			<< (UseOrderedAdaptivePlanner() ? " ordered_dp_fallback=binary" : "")
			<< std::endl;
		ExecuteAdaptivePlan(full_columns, right, *plan.node, out, stats);
	}
	for (const auto& row : out) {
		if (!row.second.isEmpty()) {
			stats.output_rows++;
			stats.output_pairs += row.second.cardinality();
		}
	}
	BlpcComposeResult result;
	result.candidate_pairs = ComputeCandidatePairs(left, right);
	result.compose_time_ms = static_cast<double>(chrono::duration_cast<chrono::milliseconds>(
		chrono::steady_clock::now() - join_begin).count());
	result.stats = stats;
	result.relation = std::move(out);
	return result;
}

static BlpcComposeResult OuterComposeRoaringDetailed(
	const RoaringRelation& left, const RoaringRelation& right)
{
	if (UseAdaptiveOuterPartition() && UseOuterPatternReuse()) {
		std::cerr << "[OUTER][adaptive-dispatch] enabled" << std::endl;
		return OuterComposeRoaringAdaptiveDetailed(left, right);
	}
	return OuterComposeRoaringFixedDetailed(left, right);
}
#endif

// The bounded-suffix ablation remains available because it changes only the
// bounded closure recurrence, not the relation composition implementation.
static bool UseNaiveBoundedSuffixUnion()
{
	const char* mode = std::getenv("BOUNDED_SUFFIX_MODE");
	return mode != nullptr && std::strcmp(mode, "naive_union") == 0;
}

static BlpcComposeResult ComposeRoaringDetailed(
	const RoaringRelation& left,
	const RoaringRelation& right)
{
	// Keep compose diagnostics and temporary cleanup out of an enclosing
	// incremental-cache scope; planner/merge/statistics retain their own timers.
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	BlpcComposeResult result = UseGcnpCompose() ? GcnpComposeRoaringDetailed(left, right)
		: PlainSequentialComposeRoaringDetailed(left, right);
	RecordCompositionMeasurement(UseGcnpCompose() ? "gcnp_compose" : "plain_compose",
		result.compose_time_ms, result.gcnp_planning_time_ms, result.relation_merge_time_ms,
		result.naive_union_work, result.stats.actual_union_work);
	return result;
}

template <typename CacheValue>
void ReleaseCachedPowerIfDead(int len, int target, std::unordered_map<int, int>& use_counts,
	unordered_map<int, CacheValue>& cache);

template <typename EnsureBodyRowFn>
static RoaringRelation BuildBlpcExactRelation(
	const unordered_set<size_t>& sources,
	int len,
	EnsureBodyRowFn&& ensure_body_row_fn,
	unordered_map<size_t, vector<size_t>>& sparse_rows)
{
	if (len == 0) {
		RoaringRelation identity;
		for (size_t src : sources) {
			identity.emplace(src, roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(src)));
		}
		return identity;
	}

	unordered_set<size_t> all_sources = sources;
	unordered_set<size_t> frontier = sources;
	for (size_t src : frontier) {
		ensure_body_row_fn(src);
	}

	for (int depth = 1; depth < len; ++depth) {
		unordered_set<size_t> next_frontier;
		for (size_t src : frontier) {
			auto it = sparse_rows.find(src);
			if (it == sparse_rows.end()) {
				continue;
			}
			for (size_t dst : it->second) {
				if (all_sources.insert(dst).second) {
					next_frontier.insert(dst);
				}
			}
		}
		if (next_frontier.empty()) {
			break;
		}
		for (size_t src : next_frontier) {
			ensure_body_row_fn(src);
		}
		frontier = std::move(next_frontier);
	}

	RoaringRelation base_relation;
	base_relation.reserve(all_sources.size() * 2 + 1);
	for (size_t src : all_sources) {
		auto it = sparse_rows.find(src);
		if (it != sparse_rows.end() && !it->second.empty()) {
			base_relation.emplace(src, RoaringFromSortedVector(it->second));
		}
	}

	unordered_map<int, RoaringRelation> relation_cache;
	relation_cache.emplace(1, std::move(base_relation));
	size_t cache_peak_entries = relation_cache.size();

#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	std::cerr << "[BLPC][exact] target=" << len
		          << " plan_steps=" << GetBinaryBasisPlan(len).size()
	          << " rows1=" << relation_cache.at(1).size()
	          << " pairs1=" << RoaringRelationPairCount(relation_cache.at(1))
	          << std::endl;
#endif
	auto use_counts = BuildBinaryBasisPlanUseCounts(len);
	for (const auto& step : GetBinaryBasisPlan(len)) {
		if (relation_cache.find(step.value) != relation_cache.end()) {
			continue;
		}
		const int left_len = step.left;
		const int right_len = step.right;
		const RoaringRelation& left_rel = relation_cache.at(left_len);
		const RoaringRelation& right_rel = relation_cache.at(right_len);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
		std::cerr << "[BLPC][step] R" << step.value
		          << " = R" << left_len << " o R" << right_len
		          << " left_rows=" << left_rel.size()
		          << " left_pairs=" << RoaringRelationPairCount(left_rel)
		          << " right_rows=" << right_rel.size()
		          << " right_pairs=" << RoaringRelationPairCount(right_rel)
		          << " mode=blpc"
		          << std::endl;
#endif
		BlpcComposeResult compose_result = ComposeRoaringDetailed(left_rel, right_rel);
		const string left_name = "P" + std::to_string(left_len);
		const string right_name = "P" + std::to_string(right_len);
		const string output_name = "P" + std::to_string(step.value);
		RecordComposeStage(
			left_name + "_o_" + right_name + "_to_" + output_name,
			left_name,
			right_name,
			output_name,
			RoaringRelationPairCount(left_rel),
			RoaringRelationPairCount(right_rel),
			RoaringRelationPairCount(compose_result.relation),
			RoaringRelationNonEmptyRows(left_rel),
			RoaringRelationNonEmptyRows(right_rel),
			RoaringRelationNonEmptyRows(compose_result.relation),
			compose_result.candidate_pairs,
			compose_result.compose_time_ms,
			compose_result.stats.total_local_patterns,
			compose_result.stats.distinct_local_patterns,
			compose_result.naive_union_work,
			compose_result.stats.actual_union_work,
			compose_result.gcnp_planning_time_ms,
			compose_result.relation_merge_time_ms);
		RecordMaterializedRelationStats(
			output_name,
			RoaringRelationPairCount(compose_result.relation),
			RoaringRelationNonEmptyRows(compose_result.relation),
			RoaringRelationMaxRowSize(compose_result.relation));
		relation_cache.emplace(step.value, std::move(compose_result.relation));
		cache_peak_entries = std::max(cache_peak_entries, relation_cache.size());
		ReleaseCachedPowerIfDead(left_len, len, use_counts, relation_cache);
		ReleaseCachedPowerIfDead(right_len, len, use_counts, relation_cache);
	}
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	std::cerr << "[BLPC][dag] peak_entries=" << cache_peak_entries
	          << " final_entries=" << relation_cache.size()
	          << std::endl;
#endif
	return relation_cache.at(len);
}

template <typename CacheValue>
void ReleaseCachedPowerIfDead(int len, int target, std::unordered_map<int, int>& use_counts,
	unordered_map<int, CacheValue>& cache)
{
	auto it = use_counts.find(len);
	if (it == use_counts.end()) {
		return;
	}
	if (--it->second != 0) {
		return;
	}
	use_counts.erase(it);
	if (len != 0 && len != 1 && len != target) {
		cache.erase(len);
	}
}

static void BuildClosurePlanUseCountsDfs(
	int num,
	std::unordered_map<int, int>& use_counts,
	std::unordered_set<int>& expanded)
{
	if (num <= 1 || !expanded.insert(num).second) {
		return;
	}
	const auto split = GetBinaryBasisSplit(num);
	const int left = split.first;
	const int right = split.second;
	if (left == 0 && right == 0) {
		return;
	}
	use_counts[left]++;
	use_counts[right]++;
	BuildClosurePlanUseCountsDfs(left, use_counts, expanded);
	BuildClosurePlanUseCountsDfs(right, use_counts, expanded);
}

static std::unordered_map<int, int> BuildClosurePlanUseCounts(int target)
{
	std::unordered_map<int, int> use_counts;
	std::unordered_set<int> expanded;
	BuildClosurePlanUseCountsDfs(target, use_counts, expanded);
	return use_counts;
}

static const SparseRelation& ExactPowerCachedSparse(
	int num,
	const SparseRelation& base,
	unordered_map<int, SparseRelation>& cache)
{
	auto it = cache.find(num);
	if (it != cache.end()) {
		return it->second;
	}

	SparseRelation value;
	if (num == 0) {
		for (const auto& kv : base) {
			value[kv.first] = {kv.first};
		}
	} else if (num == 1) {
		value = base;
	} else {
		auto use_counts = BuildBinaryBasisPlanUseCounts(num);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
		std::cerr << "[range-single][exact] using binary-basis plan for exact power "
		          << num << " with " << GetBinaryBasisPlan(num).size()
		          << " steps" << std::endl;
#endif
			for (const auto& step : GetBinaryBasisPlan(num)) {
				if (cache.find(step.value) != cache.end()) {
					continue;
				}
				SparseRelation step_value;
				const int left_len = step.left;
				const int right_len = step.right;
				SparseJoinRelation(cache.at(left_len), cache.at(right_len), step_value);
				cache.emplace(step.value, std::move(step_value));
				ReleaseCachedPowerIfDead(left_len, num, use_counts, cache);
				ReleaseCachedPowerIfDead(right_len, num, use_counts, cache);
			}
			return cache.at(num);
	}
	auto inserted = cache.emplace(num, std::move(value));
	return inserted.first->second;
}

static const RoaringRelation& ExactPowerCachedRoaring(
	int num,
	const RoaringRelation& base,
	unordered_map<int, RoaringRelation>& cache)
{
	auto it = cache.find(num);
	if (it != cache.end()) {
		return it->second;
	}

	RoaringRelation value;
	if (num == 0) {
		for (const auto& kv : base) {
			value.emplace(kv.first, roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(kv.first)));
		}
	} else if (num == 1) {
		value = base;
	} else {
		auto use_counts = BuildBinaryBasisPlanUseCounts(num);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
		std::cerr << "[range-single][exact] using binary-basis plan for exact power "
		          << num << " with " << GetBinaryBasisPlan(num).size()
		          << " steps" << std::endl;
#endif
			for (const auto& step : GetBinaryBasisPlan(num)) {
				if (cache.find(step.value) != cache.end()) {
					continue;
				}
				const int left_len = step.left;
				const int right_len = step.right;
				const RoaringRelation& left_relation = cache.at(left_len);
				const RoaringRelation& right_relation = cache.at(right_len);
				BlpcComposeResult composed = ComposeRoaringDetailed(left_relation, right_relation);
				const string left_name = "P" + std::to_string(left_len);
				const string right_name = "P" + std::to_string(right_len);
				const string output_name = "P" + std::to_string(step.value);
				RecordComposeStage(left_name + "_o_" + right_name + "_to_" + output_name,
					left_name, right_name, output_name,
					RoaringRelationPairCount(left_relation), RoaringRelationPairCount(right_relation),
					RoaringRelationPairCount(composed.relation),
					RoaringRelationNonEmptyRows(left_relation), RoaringRelationNonEmptyRows(right_relation),
					RoaringRelationNonEmptyRows(composed.relation), composed.candidate_pairs,
					composed.compose_time_ms, composed.stats.total_local_patterns,
					composed.stats.distinct_local_patterns, composed.naive_union_work,
					composed.stats.actual_union_work,
					composed.gcnp_planning_time_ms, composed.relation_merge_time_ms);
				cache.emplace(step.value, std::move(composed.relation));
				ReleaseCachedPowerIfDead(left_len, num, use_counts, cache);
				ReleaseCachedPowerIfDead(right_len, num, use_counts, cache);
			}
			return cache.at(num);
	}
	auto inserted = cache.emplace(num, std::move(value));
	return inserted.first->second;
}

static const RoaringRelation& BoundedClosureCachedRoaring(
	int num,
	const RoaringRelation& base,
	const unordered_set<size_t>& vertices,
	unordered_map<int, RoaringRelation>& power_cache,
	unordered_map<int, RoaringRelation>& closure_cache,
	std::unordered_map<int, int>& closure_use_counts,
	int closure_target)
{
	auto closure_it = closure_cache.find(num);
	if (closure_it != closure_cache.end()) {
		return closure_it->second;
	}

	if (power_cache.find(0) == power_cache.end()) {
		power_cache.emplace(0, BuildIdentityRoaringRelation(vertices));
	}
	if (power_cache.find(1) == power_cache.end()) {
		power_cache.emplace(1, base);
	}
	if (closure_cache.find(0) == closure_cache.end()) {
		closure_cache.emplace(0, BuildIdentityRoaringRelation(vertices));
	}
	if (closure_cache.find(1) == closure_cache.end()) {
		RoaringRelation s1 = closure_cache.at(0);
		RoaringUnionRelationInPlace(base, s1);
		closure_cache.emplace(1, std::move(s1));
	}
	if (num <= 1) {
		return closure_cache.at(num);
	}

	const auto split = GetBinaryBasisSplit(num);
	const int left = split.first;
	const int right = split.second;
	if (left == 0 && right == 0) {
		return closure_cache.at(num);
	}

	const RoaringRelation& sa = BoundedClosureCachedRoaring(left, base, vertices, power_cache, closure_cache, closure_use_counts, closure_target);
	const RoaringRelation& sb = BoundedClosureCachedRoaring(right, base, vertices, power_cache, closure_cache, closure_use_counts, closure_target);
	// Building the larger power may release intermediate powers from the cache.
	// Build it first, then obtain the left power reference used by this join.
	// Otherwise S12=S4 U (P4 o S8), for example, can retain a dangling P4
	// reference while P8 is being materialized.
	ExactPowerCachedRoaring(right, base, power_cache);
	const RoaringRelation& pa = ExactPowerCachedRoaring(left, base, power_cache);
	const size_t t_candidate_pairs = ComputeCandidatePairs(pa, sb);
	auto t_begin = std::chrono::steady_clock::now();
	BlpcComposeResult t_composed = ComposeRoaringDetailed(pa, sb);
	RoaringRelation pa_sb = std::move(t_composed.relation);
	const double t_comp_time_ms = std::chrono::duration<double, std::milli>(
		std::chrono::steady_clock::now() - t_begin).count();
	RoaringRelation sc = sa;
	auto union_begin = std::chrono::steady_clock::now();
	RoaringUnionRelationInPlace(pa_sb, sc);
	const double union_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - union_begin).count());
	auto inserted = closure_cache.emplace(num, std::move(sc));
	RecordComposeStage(
		"P" + std::to_string(left) + "_o_S" + std::to_string(right) + "_to_T" + std::to_string(num),
		"P" + std::to_string(left),
		"S" + std::to_string(right),
		"T" + std::to_string(num),
		RoaringRelationPairCount(pa),
		RoaringRelationPairCount(sb),
		RoaringRelationPairCount(pa_sb),
		RoaringRelationNonEmptyRows(pa),
		RoaringRelationNonEmptyRows(sb),
		RoaringRelationNonEmptyRows(pa_sb),
		t_candidate_pairs,
		t_comp_time_ms,
		t_composed.stats.total_local_patterns,
		t_composed.stats.distinct_local_patterns,
		t_composed.naive_union_work,
		t_composed.stats.actual_union_work,
		t_composed.gcnp_planning_time_ms,
		t_composed.relation_merge_time_ms);
	RecordMaterializedRelationStats(
		"S" + std::to_string(num),
		RoaringRelationPairCount(inserted.first->second),
		RoaringRelationNonEmptyRows(inserted.first->second),
		RoaringRelationMaxRowSize(inserted.first->second));
	RecordDualStateStep(
		left,
		right,
		num,
		RoaringRelationPairCount(pa),
		power_cache.count(right) ? RoaringRelationPairCount(power_cache.at(right)) : 0,
		power_cache.count(num) ? RoaringRelationPairCount(power_cache.at(num)) : 0,
		RoaringRelationPairCount(sa),
		RoaringRelationPairCount(sb),
		RoaringRelationPairCount(pa_sb),
		RoaringRelationPairCount(inserted.first->second),
		power_cache.count(right) ? ComputeCandidatePairs(pa, power_cache.at(right)) : 0,
		t_candidate_pairs,
		0.0,
		t_comp_time_ms,
		union_time_ms);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	if (left != 0 && left != 1 && left != closure_target) {
		auto it = closure_use_counts.find(left);
		if (it != closure_use_counts.end() && it->second == 1) {
			std::cerr << "[BLPC][dag-release] drop S" << left << std::endl;
		}
	}
	if (right != 0 && right != 1 && right != closure_target) {
		auto it = closure_use_counts.find(right);
		if (it != closure_use_counts.end() && it->second == 1) {
			std::cerr << "[BLPC][dag-release] drop S" << right << std::endl;
		}
	}
#endif
	ReleaseCachedPowerIfDead(left, closure_target, closure_use_counts, closure_cache);
	ReleaseCachedPowerIfDead(right, closure_target, closure_use_counts, closure_cache);
	return inserted.first->second;
}

template <typename EnsureBodyRowFn>
static RoaringRelation BuildNaiveBoundedSuffixRelation(
	const unordered_set<size_t>& sources,
	int max_extra_len,
	EnsureBodyRowFn&& ensure_body_row_fn,
	unordered_map<size_t, vector<size_t>>& sparse_rows)
{
	RoaringRelation suffix = BuildIdentityRoaringRelation(sources);
	cout << "NAIVE_BOUNDED_SUFFIX mode=exact_union"
	     << " max_extra_len=" << max_extra_len
	     << " source_count=" << sources.size() << endl;
	for (int len = 1; len <= max_extra_len; ++len) {
		auto exact_begin = std::chrono::steady_clock::now();
		RoaringRelation exact = BuildBlpcExactRelation(
			sources, len, ensure_body_row_fn, sparse_rows);
		const double exact_time_ms = static_cast<double>(
			std::chrono::duration_cast<std::chrono::milliseconds>(
				std::chrono::steady_clock::now() - exact_begin).count());
		auto union_begin = std::chrono::steady_clock::now();
		RoaringUnionRelationInPlace(exact, suffix);
		const double union_time_ms = static_cast<double>(
			std::chrono::duration_cast<std::chrono::milliseconds>(
				std::chrono::steady_clock::now() - union_begin).count());
		RecordMaterializedRelationStats(
			"E" + std::to_string(len),
			RoaringRelationPairCount(exact),
			RoaringRelationNonEmptyRows(exact),
			RoaringRelationMaxRowSize(exact));
		cout << "NAIVE_BOUNDED_EXACT len=" << len
		     << " pairs=" << RoaringRelationPairCount(exact)
		     << " exact_time_ms=" << exact_time_ms
		     << " union_time_ms=" << union_time_ms << endl;
	}
	return suffix;
}

}

RoaringRelation GcnpComposeRoaringRelation(
	const RoaringRelation& left,
	const RoaringRelation& right)
{
	QueryPhaseTimer overhead(QueryTimingPhase::Other);
	BlpcComposeResult composed = ComposeRoaringDetailed(left, right);
	if (UnionTrace::enabled()) {
		RecordComposeStage(
			(UseGcnpCompose() ? "nested_gcnp_compose" : "nested_plain_compose"),
			"nested_left", "nested_right", "nested_output",
			RoaringRelationPairCount(left), RoaringRelationPairCount(right),
			RoaringRelationPairCount(composed.relation),
			RoaringRelationNonEmptyRows(left), RoaringRelationNonEmptyRows(right),
			RoaringRelationNonEmptyRows(composed.relation), composed.candidate_pairs,
			composed.compose_time_ms, composed.stats.total_local_patterns,
			composed.stats.distinct_local_patterns,
			composed.naive_union_work,
			composed.stats.actual_union_work,
			composed.gcnp_planning_time_ms,
			composed.relation_merge_time_ms);
	}
	return std::move(composed.relation);
}

void Count_state::ensure_nonempty_start_vertices()
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	if (nonempty_start_vertices_ready) {
		return;
	}
	getStartPoints(first_labels, last_labels, nonempty_start_vertices);
	nonempty_start_vertices_ready = true;
}

void Count_state::record_body_stats()
{
	QueryPhaseTimer statistics(QueryTimingPhase::RelationStats);
	size_t pairs = 0, rows = 0, max_row = 0;
	auto base = exact_repetition_bitmap_cache.find(1);
	for (const auto& row : g) {
		if (base != exact_repetition_bitmap_cache.end() && base->second.count(row.first)) continue;
		pairs += row.second.size();
		rows += !row.second.empty();
		max_row = std::max(max_row, row.second.size());
	}
	if (base != exact_repetition_bitmap_cache.end()) {
		for (const auto& row : base->second) {
			const size_t size = row.second.cardinality();
			pairs += size;
			rows += size != 0;
			max_row = std::max(max_row, size);
		}
	}
	RecordBodyRelationStats("RB", pairs, rows, max_row);
}

void Count_state::batch_ensure_body_rows(const unordered_set<size_t>& sources, shared_ptr<NFA_state> start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	ensure_nonempty_start_vertices();
	unordered_set<size_t> missing_valid_sources;
	for (size_t src : sources) {
		if (g.find(src) != g.end()) {
			continue;
		}
		if (nonempty_start_vertices.find(src) != nonempty_start_vertices.end()) {
			missing_valid_sources.insert(src);
		} else {
			auto inserted = g.emplace(src, unordered_set<size_t>{});
			if (body_can_be_empty) {
				inserted.first->second.insert(src);
			}
			g_sparse_rows[src] = SortedUniqueVectorFromSet(inserted.first->second);
		}
	}

	if (!missing_valid_sources.empty()) {
		unordered_set<size_t> out;
		get_basic_re(start_state, g, missing_valid_sources, out);
		for (size_t src : missing_valid_sources) {
			if (body_can_be_empty) {
				g[src].insert(src);
			}
			g_sparse_rows[src] = SortedUniqueVectorFromSet(g[src]);
		}
	}
}

void Count_state::ensure_body_row(size_t vertex, shared_ptr<NFA_state> start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	if (g.find(vertex) != g.end()) {
		if (g_sparse_rows.find(vertex) == g_sparse_rows.end()) {
			g_sparse_rows[vertex] = SortedUniqueVectorFromSet(g[vertex]);
		}
		return;
	}

	ensure_nonempty_start_vertices();
	if (nonempty_start_vertices.find(vertex) == nonempty_start_vertices.end()) {
		auto inserted = g.emplace(vertex, unordered_set<size_t>{});
		if (body_can_be_empty) {
			inserted.first->second.insert(vertex);
		}
		return;
	}

	unordered_set<size_t> out;
	get_basic_re(start_state, g, vertex, out);
	if (body_can_be_empty) {
		g[vertex].insert(vertex);
	}
	g_sparse_rows[vertex] = SortedUniqueVectorFromSet(g[vertex]);
}

const vector<size_t>& Count_state::get_or_build_exact_sparse_row(size_t vertex, int len, shared_ptr<NFA_state> start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	auto& by_len = exact_repetition_sparse_cache[len];
	auto it = by_len.find(vertex);
	if (it != by_len.end()) {
		return it->second;
	}

	unordered_set<size_t> source_set = {vertex};
	unordered_map<size_t, vector<size_t>> out_rows;
	build_exact_sparse_rows_for_sources(source_set, len, start_state, out_rows);
	auto found = out_rows.find(vertex);
	if (found == out_rows.end()) {
		auto inserted = by_len.emplace(vertex, vector<size_t>{});
		return inserted.first->second;
	}
	auto inserted = by_len.emplace(vertex, std::move(found->second));
	return inserted.first->second;
}

void Count_state::build_exact_sparse_rows_for_sources(
	const unordered_set<size_t>& sources,
	int len,
	shared_ptr<NFA_state> start_state,
	unordered_map<size_t, vector<size_t>>& out_rows)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	RoaringRelation bitmaps;
	build_exact_bitmap_rows_for_sources(sources, len, start_state, bitmaps);
	out_rows.clear();
	out_rows.reserve(sources.size());
	auto& cache_rows = exact_repetition_sparse_cache[len];
	QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
	for (size_t src : sources) {
		auto cached = cache_rows.find(src);
		if (cached == cache_rows.end()) {
			vector<size_t> values;
			auto row = bitmaps.find(src);
			if (row != bitmaps.end()) {
				values.reserve(row->second.cardinality());
				for (uint32_t dst : row->second) values.push_back(dst);
			}
			cached = cache_rows.emplace(src, std::move(values)).first;
		}
		out_rows.emplace(src, cached->second);
	}
}

static bool TryBuildSingleStepBodyRows(
	const shared_ptr<NFA_state>& start_state,
	const vector<size_t>& sources, bool nullable,
	RoaringRelation& rows)
{
	if (!start_state || Graph::max_vertex_id > numeric_limits<uint32_t>::max()) return false;
	vector<shared_ptr<NFA_state>> transitions;
	start_state->next_states(transitions);
	vector<string> labels;
	for (const auto& transition : transitions) {
		if (transition->type == Type::MatchState) continue;
		if (transition->type != Type::Basic) return false;
		vector<shared_ptr<NFA_state>> suffix;
		transition->next_states(suffix);
		if (suffix.empty()) return false;
		for (const auto& next : suffix) {
			if (next->type != Type::MatchState) return false;
		}
		labels.push_back(transition->label);
	}
	if (labels.empty()) return false;
	sort(labels.begin(), labels.end());
	labels.erase(unique(labels.begin(), labels.end()), labels.end());
	{
	AlgorithmPhaseTimer traversal(AlgorithmPhase::AT);
	QueryPhaseTimer basic_nfa(QueryTimingPhase::BasicNfa);
	rows.reserve(sources.size());
	// The epsilon-expanded NFA accepts exactly one labeled edge here. Emit
	// bitmap body rows directly instead of three DFS maps and two conversions.
	for (size_t source : sources) {
		roaring::Roaring endpoints;
		if (nullable) endpoints.add(static_cast<uint32_t>(source));
		auto vertex = Graph::adjacency_map.find(source);
		if (vertex != Graph::adjacency_map.end()) {
			for (const string& label : labels) {
				auto neighbors = vertex->second.find(label);
				if (neighbors == vertex->second.end()) continue;
				for (size_t target : neighbors->second) endpoints.add(static_cast<uint32_t>(target));
			}
		}
		rows.emplace(source, std::move(endpoints));
	}
	}
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	std::cout << "[EXACT_BODY] evaluator=single_step_bitmap sources=" << sources.size()
		<< " labels=" << labels.size() << " nullable=" << nullable << std::endl;
	return true;
}

void Count_state::build_exact_bitmap_rows_for_sources(
	const unordered_set<size_t>& sources,
	int len,
	shared_ptr<NFA_state> start_state,
	RoaringRelation& out_rows)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	out_rows.clear();
	if (sources.empty()) {
		return;
	}
	out_rows.reserve(sources.size());

	if (len == 0) {
		auto& cache_rows = exact_repetition_bitmap_cache[len];
		for (size_t src : sources) {
			auto it = cache_rows.find(src);
			if (it == cache_rows.end()) {
				roaring::Roaring identity;
				identity.add(static_cast<uint32_t>(src));
				it = cache_rows.emplace(src, std::move(identity)).first;
			}
			out_rows[src] = it->second;
		}
		return;
	}

	if (len == 1) {
		auto& cache_rows = exact_repetition_bitmap_cache[len];
		vector<size_t> missing_sources;
		missing_sources.reserve(sources.size());
		for (size_t src : sources) {
			auto it = cache_rows.find(src);
			if (it == cache_rows.end()) {
				missing_sources.push_back(src);
			} else {
				if (!it->second.isEmpty()) out_rows.emplace(src, it->second);
			}
		}
		RoaringRelation body_rows;
		if (TryBuildSingleStepBodyRows(start_state, missing_sources, body_can_be_empty, body_rows)) {
			cache_rows.reserve(cache_rows.size() + missing_sources.size());
			for (auto row = body_rows.begin(); row != body_rows.end();) {
				cache_rows.emplace(row->first, row->second);
				if (row->second.isEmpty()) row = body_rows.erase(row);
				else ++row;
			}
			MoveDisjointRelationRows(body_rows, out_rows);
			return;
		}
		for (size_t src : missing_sources) {
			ensure_body_row(src, start_state);
			auto row_it = g_sparse_rows.find(src);
			if (row_it != g_sparse_rows.end()) {
				cache_rows[src] = RoaringFromSortedVector(row_it->second);
			} else {
				cache_rows[src] = {};
			}
			if (!cache_rows[src].isEmpty()) out_rows.emplace(src, cache_rows[src]);
		}
		return;
	}

	vector<size_t> missing_sources;
	missing_sources.reserve(sources.size());
	auto& cache_rows = exact_repetition_bitmap_cache[len];
	for (size_t src : sources) {
		if (cache_rows.find(src) == cache_rows.end()) {
			missing_sources.push_back(src);
		} else {
			if (!cache_rows[src].isEmpty()) out_rows.emplace(src, cache_rows[src]);
		}
	}

	if (!missing_sources.empty()) {
		// Exact counting uses a binary-power chain. The largest power strictly
		// below len is the already-built right operand; the smaller remainder
		// stays on the left. Thus R5=R1 o R4, R6=R2 o R4, R13=R5 o R8, and
		// R33=R1 o R32. A power of two naturally becomes a balanced square.
		const auto split = GetBinaryBasisSplit(len);
		const int left_len = split.first;
		const int right_len = split.second;
		if (left_len <= 0 || right_len <= 0) {
			for (size_t src : missing_sources) {
				cache_rows[src] = {};
			}
		} else {
			unordered_set<size_t> partial_missing;
			const unordered_set<size_t>* missing_set = &sources;
			if (missing_sources.size() != sources.size()) {
				partial_missing.insert(missing_sources.begin(), missing_sources.end());
				missing_set = &partial_missing;
			}

			// Build the binary basis before combining the set bits of len. For
			// example, R7 first establishes R1, R2, and R4; only then does it
			// form R3=R1 o R2 and R7=R3 o R4. The row cache makes the later
			// right-operand lookup a replay for sources already covered here.
			RoaringRelation binary_power_rows;
			build_exact_bitmap_rows_for_sources(
				*missing_set, right_len, start_state, binary_power_rows);

			RoaringRelation left_rows;
			if (left_len == right_len) left_rows = std::move(binary_power_rows);
			else {
				binary_power_rows.clear();
				build_exact_bitmap_rows_for_sources(*missing_set, left_len, start_state, left_rows);
			}

			OperationStopwatch mids_timer;
			unordered_set<size_t> mids = CollectDistinctRelationMids(left_rows);
			const double mids_time_ms = mids_timer.elapsed_ms();
			if (mids.empty()) {
				for (size_t src : missing_sources) {
					cache_rows[src] = {};
				}
				return;
			}
			RoaringRelation right_rows;
			build_exact_bitmap_rows_for_sources(mids, right_len, start_state, right_rows);
			{
			QueryPhaseTimer logging(QueryTimingPhase::Other);
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			std::cout << "[EXACT][binary-chain] R" << len
			          << " = R" << left_len << " o R" << right_len
			          << " requested_sources=" << missing_sources.size()
			          << " left_rows=" << left_rows.size()
			          << " right_sources=" << mids.size()
			          << " cached_rows=" << (sources.size() - missing_sources.size())
			          << std::endl;
			}

			const RoaringRelation& left_relation = left_rows;
			const RoaringRelation& right_relation = right_rows;
			BlpcComposeResult compose_result = ComposeRoaringDetailed(left_relation, right_relation);
			const auto left_stats = MeasureRoaringRelation(left_relation);
			const auto right_stats = MeasureRoaringRelation(right_relation);
			const auto output_stats = MeasureRoaringRelation(compose_result.relation);
			const string left_name = "P" + std::to_string(left_len);
			const string right_name = "P" + std::to_string(right_len);
			const string output_name = "P" + std::to_string(len);
			RecordComposeStage(
				left_name + "_o_" + right_name + "_to_" + output_name,
				left_name,
				right_name,
				output_name,
				left_stats.pairs,
				right_stats.pairs,
				output_stats.pairs,
				left_stats.rows,
				right_stats.rows,
				output_stats.rows,
				compose_result.candidate_pairs,
				compose_result.compose_time_ms,
				compose_result.stats.total_local_patterns,
				compose_result.stats.distinct_local_patterns,
				compose_result.naive_union_work,
				compose_result.stats.actual_union_work,
				compose_result.gcnp_planning_time_ms,
				compose_result.relation_merge_time_ms);
			RecordMaterializedRelationStats(
				output_name,
				output_stats.pairs,
				output_stats.rows,
				output_stats.max_row);
			QueryPhaseTimer cache_store(QueryTimingPhase::IncrementalCache);
			OperationStopwatch cache_timer;
			cache_rows.reserve(cache_rows.size() + missing_sources.size());
			for (size_t src : missing_sources) {
				auto row = compose_result.relation.find(src);
				SharedBitmapRow value;
				if (row != compose_result.relation.end()) value = row->second;
				cache_rows.emplace(src, std::move(value));
			}
			// Most first requests have no cached-hit rows and can hand off the
			// entire packed table. Mixed hits retain the same disjoint rows.
			MoveDisjointRelationRows(compose_result.relation, out_rows);
			const double cache_time_ms = cache_timer.elapsed_ms();
			{
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			std::cout << "[EXACT_CACHE] power=" << len
				<< " sources=" << missing_sources.size()
				<< " output_rows=" << out_rows.size()
				<< " mids_scan_ms=" << mids_time_ms
				<< " cache_handoff_ms=" << cache_time_ms
				<< " output_handoff=packed_move_or_append" << std::endl;
			}
		}
	}
}

const unordered_set<size_t>& Count_state::get_or_build_exact_row(size_t vertex, int len, shared_ptr<NFA_state> start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	auto& by_len = exact_repetition_row_cache[len];
	auto cache_it = by_len.find(vertex);
	if (cache_it != by_len.end()) {
		return cache_it->second;
	}
	const auto& sparse_row = get_or_build_exact_sparse_row(vertex, len, start_state);
	auto& dense_cache = by_len[vertex];
	{
		QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
		dense_cache.insert(sparse_row.begin(), sparse_row.end());
	}
	return dense_cache;
}

const unordered_set<size_t>& Count_state::get_or_build_lazy_range_row(size_t vertex,shared_ptr<NFA_state> start_state)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	auto it = range_repetition_map.find(vertex);
	if (it != range_repetition_map.end()) {
		return it->second;
	}

	auto inserted = range_repetition_map.emplace(vertex, unordered_set<size_t>{});
	auto& result = inserted.first->second;
	if (min == max) {
		const auto& sparse_row = get_or_build_exact_sparse_row(vertex, min, start_state);
		QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
		result.insert(sparse_row.begin(), sparse_row.end());
	}
	return result;
}

void Count_state::build_open_range_rows_for_sources(
	const unordered_set<size_t>& sources,
	int len,
	shared_ptr<NFA_state> start_state,
	unordered_map<size_t, unordered_set<size_t>>& out_rows,
	RoaringRelation* bitmap_out)
{
	// Construct first so the final checkpoint includes local-container release.
	OpenRangeExecutionProfile profile;
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	out_rows.clear();
	auto prefix_begin = std::chrono::steady_clock::now();
	if (len <= 0 || sources.empty()) {
		return;
	}

	RoaringRelation prefix_relation;
	build_exact_bitmap_rows_for_sources(sources, len, start_state, prefix_relation);
	const double prefix_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - prefix_begin).count());
	profile.checkpoint("prefix");
	unordered_set<size_t> mids = CollectDistinctRelationMids(prefix_relation);

	if (mids.empty()) {
		record_body_stats();
		return;
	}
	profile.checkpoint("prefix_mids");

	auto closure_begin = std::chrono::steady_clock::now();
	unordered_set<size_t> closure_vertices = mids;
	unordered_set<size_t> frontier = mids;
	batch_ensure_body_rows(frontier, start_state);
	while (!frontier.empty()) {
		unordered_set<size_t> next_frontier;
		for (size_t src : frontier) {
			auto it = g_sparse_rows.find(src);
			if (it == g_sparse_rows.end()) {
				continue;
			}
			for (size_t dst : it->second) {
				if (closure_vertices.insert(dst).second) {
					next_frontier.insert(dst);
				}
			}
		}
		if (next_frontier.empty()) {
			break;
		}
		batch_ensure_body_rows(next_frontier, start_state);
		frontier = std::move(next_frontier);
	}
	profile.checkpoint("closure_expansion");

	unordered_map<size_t, unordered_set<size_t>> star_graph;
	for (size_t src : closure_vertices) {
		auto& row = star_graph[src];
		auto it = g_sparse_rows.find(src);
		if (it != g_sparse_rows.end()) {
			row.insert(it->second.begin(), it->second.end());
		}
	}
	profile.checkpoint("star_graph");

	unordered_map<size_t, unordered_set<size_t>> star_rows;
	unordered_set<size_t> star_out;
	size_t summary_closure_pairs = 0;
	size_t summary_final_pairs = 0;
	size_t summary_candidate_pairs = 0;
	double summary_final_compose_time_ms = 0.0;
#if RANGE_SINGLE_USE_UNBOUNDED_REWRITE_SCC
	TarjanSCC tarjan(star_graph);
	profile.checkpoint("tarjan");
	unordered_map<size_t, unordered_set<size_t>> scc_adjacency_map;
	create_scc_graph(tarjan, star_graph, scc_adjacency_map);
	const size_t scc_count = tarjan.sccs.size();
	const auto topo_order = topo_sort_comp_dag(scc_adjacency_map, scc_count);
	profile.checkpoint("scc_dag");

#if RANGE_SINGLE_USE_ROARING_OPEN_RANGE
	vector<roaring::Roaring> sources_by_scc(scc_count);
	vector<roaring::Roaring> component_vertices_roaring(scc_count);
	for (size_t comp_id = 0; comp_id < scc_count; ++comp_id) {
		for (size_t v : tarjan.sccs[comp_id]) {
			component_vertices_roaring[comp_id].add(static_cast<uint32_t>(v));
		}
	}
	profile.checkpoint("component_bitmaps");

	size_t prefix_pairs = 0;
	size_t seeded_pairs = 0;
	for (const auto& kv : prefix_relation) {
		unordered_set<size_t> row_sccs;
		prefix_pairs += kv.second.cardinality();
		for (uint32_t mid : kv.second) {
			auto comp_it = tarjan.scc_ids.find(mid);
			if (comp_it != tarjan.scc_ids.end()) {
				row_sccs.insert(comp_it->second);
			}
		}
		for (auto comp_id : row_sccs) {
			const uint64_t before = sources_by_scc[comp_id].cardinality();
			sources_by_scc[comp_id].add(static_cast<uint32_t>(kv.first));
			seeded_pairs += static_cast<size_t>(sources_by_scc[comp_id].cardinality() - before);
		}
	}
	profile.checkpoint("seed_sources");

	for (auto comp_id : topo_order) {
		auto succ_it = scc_adjacency_map.find(comp_id);
		if (succ_it == scc_adjacency_map.end() || sources_by_scc[comp_id].isEmpty()) {
			continue;
		}
		for (auto succ : succ_it->second) {
			sources_by_scc[succ] |= sources_by_scc[comp_id];
		}
	}
	profile.checkpoint("propagate_sources");

	OpenRangeRowBuilder final_builder(prefix_relation);
	// Materialize destinations in ID order rather than Tarjan discovery order.
	// Singleton components then append instead of repeatedly shifting arrays.
	vector<pair<uint32_t, size_t>> output_components;
	output_components.reserve(scc_count);
	for (size_t comp_id = 0; comp_id < scc_count; ++comp_id) {
		if (!sources_by_scc[comp_id].isEmpty()) {
			output_components.emplace_back(component_vertices_roaring[comp_id].minimum(), comp_id);
		}
	}
	sort(output_components.begin(), output_components.end());
	size_t final_pairs = 0;
	size_t propagated_pairs = 0;
	size_t active_sccs = 0;
	for (const auto& component : output_components) {
		const size_t comp_id = component.second;
		const auto& srcs = sources_by_scc[comp_id];
		if (srcs.isEmpty()) {
			continue;
		}
		active_sccs++;
		propagated_pairs += srcs.cardinality();
		const auto& component_vertices = tarjan.sccs[comp_id];
		final_pairs += static_cast<size_t>(srcs.cardinality()) * component_vertices.size();
		star_out.insert(component_vertices.begin(), component_vertices.end());
		if (component_vertices.size() == 1) {
			const auto vertex = static_cast<uint32_t>(component_vertices.front());
			final_builder.add_singleton(srcs, vertex);
		} else {
			final_builder.add_component(srcs, component_vertices_roaring[comp_id]);
		}
	}
	RoaringRelation final_roaring = final_builder.finish();
	profile.checkpoint("final_bitmap_rows");

	{
	QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
	if (!bitmap_out) {
		for (auto& kv : final_roaring) {
			auto& row = out_rows[kv.first];
			for (uint32_t dst : kv.second) {
				row.insert(static_cast<size_t>(dst));
			}
		}
	}
	}
	summary_final_pairs = final_pairs;
	summary_candidate_pairs = 0;
	RecordMaterializedRelationStats(
		"FINAL",
		final_pairs,
		RoaringRelationNonEmptyRows(final_roaring),
		RoaringRelationMaxRowSize(final_roaring));
	if (bitmap_out) bitmap_out->swap(final_roaring);
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	std::cerr << "[range-single][rewrite-scc-roaring] B{" << len << ",inf}"
	          << " => B^" << len << " o B*" << std::endl;
	std::cerr << "[range-single][rewrite-scc-roaring] prefix_exact_pairs=" << prefix_pairs
	          << " mids=" << mids.size()
	          << " seeded_source_scc_pairs=" << seeded_pairs
	          << " propagated_source_scc_pairs=" << propagated_pairs
	          << " active_sccs=" << active_sccs
	          << " scc_count=" << scc_count
	          << std::endl;
	std::cerr << "[range-single][rewrite-scc-roaring] final_pairs=" << final_pairs
	          << " closure_vertices=" << closure_vertices.size()
	          << " star_graph_rows=" << star_graph.size()
	          << " final_rows=" << out_rows.size()
	          << std::endl;
	std::cerr << "[BLPC][dag] unbounded_prefix_rows=" << prefix_relation.size()
	          << " unbounded_prefix_pairs=" << prefix_pairs
	          << " scc_count=" << scc_count
	          << std::endl;
	}
#endif
#else
	vector<unordered_set<size_t>> sources_by_scc(scc_count);
	size_t prefix_pairs = 0;
	size_t seeded_pairs = 0;
	for (const auto& kv : prefix_relation) {
		unordered_set<size_t> row_sccs;
		prefix_pairs += kv.second.cardinality();
		for (uint32_t mid : kv.second) {
			auto comp_it = tarjan.scc_ids.find(mid);
			if (comp_it == tarjan.scc_ids.end()) {
				continue;
			}
			row_sccs.insert(comp_it->second);
		}
		for (auto comp_id : row_sccs) {
			const size_t before = sources_by_scc[comp_id].size();
			sources_by_scc[comp_id].insert(kv.first);
			seeded_pairs += sources_by_scc[comp_id].size() - before;
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
	size_t propagated_pairs = 0;
	size_t active_sccs = 0;
	for (size_t comp_id = 0; comp_id < scc_count; ++comp_id) {
		const auto& srcs = sources_by_scc[comp_id];
		if (srcs.empty()) {
			continue;
		}
		active_sccs++;
		propagated_pairs += srcs.size();
		const auto& component_vertices = tarjan.sccs[comp_id];
		final_pairs += srcs.size() * component_vertices.size();
		star_out.insert(component_vertices.begin(), component_vertices.end());
		for (auto src : srcs) {
			auto& row = out_rows[src];
			row.insert(component_vertices.begin(), component_vertices.end());
		}
	}
	summary_final_pairs = final_pairs;
	summary_candidate_pairs = 0;
	RecordMaterializedRelationStats(
		"FINAL",
		final_pairs,
		DenseRelationNonEmptyRows(out_rows),
		DenseRelationMaxRowSize(out_rows));
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	std::cerr << "[range-single][rewrite-scc] B{" << len << ",inf}"
	          << " => B^" << len << " o B*" << std::endl;
	std::cerr << "[range-single][rewrite-scc] prefix_exact_pairs=" << prefix_pairs
	          << " mids=" << mids.size()
	          << " seeded_source_scc_pairs=" << seeded_pairs
	          << " propagated_source_scc_pairs=" << propagated_pairs
	          << " active_sccs=" << active_sccs
	          << " scc_count=" << scc_count
	          << std::endl;
	std::cerr << "[range-single][rewrite-scc] final_pairs=" << final_pairs
	          << " closure_vertices=" << closure_vertices.size()
	          << " star_graph_rows=" << star_graph.size()
	          << " final_rows=" << out_rows.size()
	          << std::endl;
	std::cerr << "[BLPC][dag] unbounded_prefix_rows=" << prefix_relation.size()
	          << " unbounded_prefix_pairs=" << prefix_pairs
	          << " scc_count=" << scc_count
	          << std::endl;
#endif
#endif
#else
	get_star_re(star_graph, star_rows, mids, star_out);
	RoaringRelation star_roaring;
	for (const auto& kv : star_rows) {
		if (!kv.second.empty()) {
			star_roaring.emplace(kv.first, RoaringFromSortedVector(SortedUniqueVectorFromSet(kv.second)));
		}
	}
	BlpcComposeResult final_compose = ComposeRoaringDetailed(prefix_relation, star_roaring);
	RoaringRelation final_relation = std::move(final_compose.relation);
	summary_closure_pairs = RoaringRelationPairCount(star_roaring);
	summary_final_pairs = RoaringRelationPairCount(final_relation);
	summary_candidate_pairs = final_compose.candidate_pairs;
	summary_final_compose_time_ms = final_compose.compose_time_ms;
	RecordComposeStage(
		"P" + std::to_string(len) + "_o_RBstar_to_FINAL",
		"P" + std::to_string(len),
		"RB_star",
		"FINAL",
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationPairCount(star_roaring),
		RoaringRelationPairCount(final_relation),
		RoaringRelationNonEmptyRows(prefix_relation),
		RoaringRelationNonEmptyRows(star_roaring),
		RoaringRelationNonEmptyRows(final_relation),
		final_compose.candidate_pairs,
		final_compose.compose_time_ms,
		final_compose.stats.total_local_patterns,
		final_compose.stats.distinct_local_patterns,
		final_compose.naive_union_work,
		final_compose.stats.actual_union_work,
		final_compose.gcnp_planning_time_ms,
		final_compose.relation_merge_time_ms);
	RecordMaterializedRelationStats("RB_star",
		RoaringRelationPairCount(star_roaring),
		RoaringRelationNonEmptyRows(star_roaring),
		RoaringRelationMaxRowSize(star_roaring));
	RecordMaterializedRelationStats("FINAL",
		RoaringRelationPairCount(final_relation),
		RoaringRelationNonEmptyRows(final_relation),
		RoaringRelationMaxRowSize(final_relation));
	{
	QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
	for (const auto& kv : final_relation) {
		auto& row = out_rows[kv.first];
		for (uint32_t dst : kv.second) {
			row.insert(static_cast<size_t>(dst));
		}
	}
	}
#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	size_t prefix_pairs = 0;
	size_t star_pairs = 0;
	size_t final_pairs = 0;
	prefix_pairs = RoaringRelationPairCount(prefix_relation);
	for (const auto& kv : star_rows) star_pairs += kv.second.size();
	for (const auto& kv : out_rows) final_pairs += kv.second.size();
	std::cerr << "[range-single][rewrite] B{" << len << ",inf}"
	          << " => B^" << len << " o B*" << std::endl;
	std::cerr << "[range-single][rewrite] prefix_exact_pairs=" << prefix_pairs
	          << " mids=" << mids.size()
	          << " star_pairs=" << star_pairs
	          << " final_pairs=" << final_pairs
	          << std::endl;
	std::cerr << "[range-single][rewrite] star_graph_rows=" << star_graph.size()
	          << " final_rows=" << out_rows.size()
	          << std::endl;
	std::cerr << "[BLPC][dag] unbounded_prefix_rows=" << prefix_relation.size()
	          << " unbounded_prefix_pairs=" << prefix_pairs
	          << std::endl;
#endif
#endif
	record_body_stats();
	RecordMaterializedRelationStats(
		"P" + std::to_string(len),
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationNonEmptyRows(prefix_relation),
		RoaringRelationMaxRowSize(prefix_relation));
	const double closure_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - closure_begin).count()) -
		summary_final_compose_time_ms;
	RecordUnboundedSummary(
		len,
		DenseRelationPairCount(g),
		RoaringRelationPairCount(prefix_relation),
		summary_closure_pairs,
		summary_final_pairs,
		prefix_time_ms,
		closure_time_ms < 0.0 ? 0.0 : closure_time_ms,
		summary_final_compose_time_ms,
		summary_candidate_pairs);
	profile.checkpoint("statistics_and_output");
	}

void Count_state::build_bounded_suffix_bitmap_rows_for_sources(
	const unordered_set<size_t>& sources,
	int len,
	shared_ptr<NFA_state> start_state,
	RoaringRelation& out_rows)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	QueryPhaseTimer measurement(QueryTimingPhase::IncrementalCache);
	out_rows.clear();
	if (sources.empty() || len < 0) {
		return;
	}

	const char* delta_mode = std::getenv("DUAL_STATE_DELTA_MODE");
	const bool mirror_delta = delta_mode == nullptr ||
		std::strcmp(delta_mode, "required_power") == 0;
	const char* trace = std::getenv("DUAL_STATE_DELTA_TRACE");
	const bool trace_delta = trace && std::strcmp(trace, "1") == 0;
	struct DeltaSchedule {
		size_t depth = 0;
		unordered_set<int> required_powers;
	};
	static thread_local DeltaSchedule delta_schedule;
	struct ScheduleScope {
		DeltaSchedule* schedule;
		explicit ScheduleScope(DeltaSchedule* value) : schedule(value) {
			if (schedule) ++schedule->depth;
		}
		~ScheduleScope() {
			if (schedule && --schedule->depth == 0) schedule->required_powers.clear();
		}
	} schedule_scope(mirror_delta ? &delta_schedule : nullptr);
	if (mirror_delta && delta_schedule.depth == 1) {
		// Only reuse exact powers already required by the mirror recurrence.
		// S_len alone must not force construction of P_len.
		unordered_set<int> visited_suffixes;
		std::function<void(int)> require_power = [&](int power) {
			if (!delta_schedule.required_powers.insert(power).second || power <= 1) return;
			const auto parts = GetBinaryBasisSplit(power);
			require_power(parts.first);
			require_power(parts.second);
		};
		std::function<void(int)> visit_suffix = [&](int length) {
			if (length <= 1 || !visited_suffixes.insert(length).second) return;
			const auto parts = GetBinaryBasisSplit(length);
			require_power(parts.second);
			visit_suffix(parts.first);
			visit_suffix(parts.second);
		};
		visit_suffix(len);
	}

	auto& cache_rows = bounded_suffix_bitmap_cache[len];
	unordered_set<size_t> missing_sources;
	for (size_t source : sources) {
		auto cached = cache_rows.find(source);
		if (cached == cache_rows.end()) {
			missing_sources.insert(source);
		} else if (!cached->second.isEmpty()) {
			out_rows.emplace(source, cached->second);
		}
	}
	if (missing_sources.empty()) {
		return;
	}

	if (len == 0) {
		for (size_t source : missing_sources) {
			SharedBitmapRow identity(roaring::Roaring::bitmapOf(1, static_cast<uint32_t>(source)));
			cache_rows.emplace(source, identity);
			out_rows.emplace(source, std::move(identity));
		}
		return;
	}

	if (len == 1) {
		RoaringRelation exact_rows;
		build_exact_bitmap_rows_for_sources(
			missing_sources, 1, start_state, exact_rows);
		for (size_t source : missing_sources) {
			SharedBitmapRow values;
			auto exact = exact_rows.find(source);
			if (exact != exact_rows.end()) values = exact->second;
			values.add(static_cast<uint32_t>(source));
			cache_rows[source] = values;
			out_rows[source] = std::move(values);
		}
		return;
	}

	const auto split = GetBinaryBasisSplit(len);
	const int left_len = split.first;
	const int right_len = split.second;
	if (left_len <= 0 || right_len <= 0) {
		for (size_t source : missing_sources) {
			cache_rows[source] = {};
		}
		return;
	}

	if (mirror_delta) {
		// S includes I: S_(a+b) = S_b U S_a P_b. Both suffix requests
		// keep the original sources; only exact powers expand into mid rows.
		RoaringRelation suffix, left_suffix;
		build_bounded_suffix_bitmap_rows_for_sources(
			missing_sources, right_len, start_state, suffix);
		build_bounded_suffix_bitmap_rows_for_sources(
			missing_sources, left_len, start_state, left_suffix);
		const bool reuse_power = delta_schedule.required_powers.count(len) != 0;
		RoaringRelation exact, left_operand;
		if (reuse_power) {
			build_exact_bitmap_rows_for_sources(missing_sources, len, start_state, exact);
			unordered_set<size_t> uncovered;
			const auto& cached_left = exact_repetition_bitmap_cache[left_len];
			for (size_t source : missing_sources) {
				if (cached_left.find(source) == cached_left.end()) uncovered.insert(source);
			}
			if (!uncovered.empty()) {
				RoaringRelation extra_rows;
				build_exact_bitmap_rows_for_sources(uncovered, left_len, start_state, extra_rows);
			}
			const auto& left_power = exact_repetition_bitmap_cache[left_len];
			for (const auto& [source, row] : left_suffix) {
				const auto power = left_power.find(source);
				if (power == left_power.end() || power->second.isEmpty()) {
					left_operand.emplace(source, row);
				} else {
					auto difference = row - power->second;
					if (!difference.isEmpty()) left_operand.emplace(source, std::move(difference));
				}
			}
			RoaringUnionRelationInPlace(exact, suffix);
		} else {
			left_operand = std::move(left_suffix);
		}
		const auto right_sources = CollectDistinctRelationMids(left_operand);
		RoaringRelation right_power;
		build_exact_bitmap_rows_for_sources(right_sources, right_len, start_state, right_power);
		BlpcComposeResult shifted;
		if (!left_operand.empty() && !right_power.empty()) {
			shifted = ComposeRoaringDetailed(left_operand, right_power);
		}
		RoaringUnionRelationInPlace(shifted.relation, suffix);
		const string left_name = (reuse_power ? "D" : "S") + std::to_string(left_len);
		const string right_name = "P" + std::to_string(right_len);
		const auto left_stats = MeasureRoaringRelation(left_operand);
		const auto right_stats = MeasureRoaringRelation(right_power);
		const auto output_stats = MeasureRoaringRelation(shifted.relation);
		RecordComposeStage(
			"MIRROR_" + left_name + "_o_" + right_name + "_to_T" + std::to_string(len),
			left_name, right_name, "T" + std::to_string(len),
			left_stats.pairs, right_stats.pairs, output_stats.pairs,
			left_stats.rows, right_stats.rows, output_stats.rows,
			shifted.candidate_pairs, shifted.compose_time_ms,
			shifted.stats.total_local_patterns, shifted.stats.distinct_local_patterns,
			shifted.naive_union_work, shifted.stats.actual_union_work,
			shifted.gcnp_planning_time_ms, shifted.relation_merge_time_ms);
		const auto suffix_stats = MeasureRoaringRelation(suffix);
		for (size_t source : missing_sources) {
			SharedBitmapRow values;
			auto row = suffix.find(source);
			if (row != suffix.end()) values = row->second;
			cache_rows[source] = values;
			if (!values.isEmpty()) out_rows.emplace(source, std::move(values));
		}
		RecordMaterializedRelationStats("S" + std::to_string(len),
			suffix_stats.pairs, suffix_stats.rows, suffix_stats.max_row);
		if (trace_delta) {
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			std::cout << "[DualStateMirror] length=" << len
				<< " requested_sources=" << missing_sources.size()
				<< " required_power=" << reuse_power
				<< " left_pairs=" << left_stats.pairs
				<< " right_sources=" << right_sources.size() << std::endl;
		}
		return;
	}

	// Single-node execution is deliberately sequential: finish S_a and P_a,
	// derive the complete demanded RHS source set, then build only S_b(mids).
	// This preserves one complete GCNP input per compose without introducing
	// distributed-style temporal batches on a machine that cannot overlap them.
	RoaringRelation suffix;
	build_bounded_suffix_bitmap_rows_for_sources(
		missing_sources, left_len, start_state, suffix);

	RoaringRelation left_power;
	build_exact_bitmap_rows_for_sources(
		missing_sources, left_len, start_state, left_power);

	unordered_set<size_t> right_sources;
	for (const auto& [_, mids] : left_power) {
		right_sources.insert(mids.begin(), mids.end());
	}
	RoaringRelation right_suffix;
	build_bounded_suffix_bitmap_rows_for_sources(
		right_sources, right_len, start_state, right_suffix);
	BlpcComposeResult shifted = ComposeRoaringDetailed(left_power, right_suffix);
	RoaringUnionRelationInPlace(shifted.relation, suffix);

	RecordComposeStage(
		"P" + std::to_string(left_len) + "_o_S" +
			std::to_string(right_len) + "_to_T" + std::to_string(len),
		"P" + std::to_string(left_len),
		"S" + std::to_string(right_len),
		"T" + std::to_string(len),
		RoaringRelationPairCount(left_power),
		RoaringRelationPairCount(right_suffix),
		RoaringRelationPairCount(shifted.relation),
		RoaringRelationNonEmptyRows(left_power),
		RoaringRelationNonEmptyRows(right_suffix),
		RoaringRelationNonEmptyRows(shifted.relation),
		shifted.candidate_pairs,
		shifted.compose_time_ms,
		shifted.stats.total_local_patterns,
		shifted.stats.distinct_local_patterns,
		shifted.naive_union_work,
		shifted.stats.actual_union_work,
		shifted.gcnp_planning_time_ms,
		shifted.relation_merge_time_ms);

	{
		QueryPhaseTimer cache_store(QueryTimingPhase::IncrementalCache);
		for (size_t source : missing_sources) {
			SharedBitmapRow values;
			auto row = suffix.find(source);
			if (row != suffix.end()) values = row->second;
			cache_rows[source] = values;
			if (!values.isEmpty()) out_rows.emplace(source, std::move(values));
		}
	}
	RecordMaterializedRelationStats(
		"S" + std::to_string(len),
		RoaringRelationPairCount(suffix),
		RoaringRelationNonEmptyRows(suffix),
		RoaringRelationMaxRowSize(suffix));
}

void Count_state::build_bounded_range_rows_for_sources(
	const unordered_set<size_t>& sources,
	int min_len,
	int max_len,
	shared_ptr<NFA_state> start_state,
	unordered_map<size_t, unordered_set<size_t>>& out_rows,
	RoaringRelation* bitmap_out)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	out_rows.clear();
	auto prefix_begin = std::chrono::steady_clock::now();
	if (sources.empty() || min_len < 0 || max_len < min_len) {
		return;
	}

	RoaringRelation prefix_relation;
	build_exact_bitmap_rows_for_sources(sources, min_len, start_state, prefix_relation);
	const double prefix_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - prefix_begin).count());
	unordered_set<size_t> mids = CollectDistinctRelationMids(prefix_relation);

	if (mids.empty()) {
		record_body_stats();
		return;
	}

	const int d = max_len - min_len;
	if (d == 0) {
		if (!bitmap_out) {
			for (const auto& kv : prefix_relation) {
				auto& row = out_rows[kv.first];
				for (uint32_t dst : kv.second) {
					row.insert(static_cast<size_t>(dst));
				}
			}
		}
		record_body_stats();
		RecordMaterializedRelationStats(
			"P" + std::to_string(min_len),
			RoaringRelationPairCount(prefix_relation),
			RoaringRelationNonEmptyRows(prefix_relation),
			RoaringRelationMaxRowSize(prefix_relation));
		if (bitmap_out) bitmap_out->swap(prefix_relation);
		return;
	}

	auto closure_begin = std::chrono::steady_clock::now();
	RoaringRelation bounded;
	if (UseNaiveBoundedSuffixUnion()) {
		// Naive bounded ablation: materialize every exact length separately,
		// then form S_d = I U B1 U ... U Bd. This deliberately does not use
		// the dual-state recurrence S_a U (P_a o S_b).
		bounded = BuildIdentityRoaringRelation(mids);
		{
		AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
		cout << "NAIVE_BOUNDED_SUFFIX mode=exact_union"
		     << " max_extra_len=" << d
		     << " source_count=" << mids.size() << endl;
		}
		for (int exact_len = 1; exact_len <= d; ++exact_len) {
			auto exact_begin = std::chrono::steady_clock::now();
			RoaringRelation exact;
			build_exact_bitmap_rows_for_sources(mids, exact_len, start_state, exact);
			const double exact_time_ms = static_cast<double>(
				std::chrono::duration_cast<std::chrono::milliseconds>(
					std::chrono::steady_clock::now() - exact_begin).count());

			auto union_begin = std::chrono::steady_clock::now();
			RoaringUnionRelationInPlace(exact, bounded);
			const double union_time_ms = static_cast<double>(
				std::chrono::duration_cast<std::chrono::milliseconds>(
					std::chrono::steady_clock::now() - union_begin).count());
			RecordMaterializedRelationStats(
				"E" + std::to_string(exact_len),
				RoaringRelationPairCount(exact),
				RoaringRelationNonEmptyRows(exact),
				RoaringRelationMaxRowSize(exact));
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			cout << "NAIVE_BOUNDED_EXACT len=" << exact_len
			     << " pairs=" << RoaringRelationPairCount(exact)
			     << " exact_time_ms=" << exact_time_ms
			     << " union_time_ms=" << union_time_ms << endl;
		}
	} else {
		build_bounded_suffix_bitmap_rows_for_sources(mids, d, start_state, bounded);
	}
	BlpcComposeResult final_compose = ComposeRoaringDetailed(prefix_relation, bounded);
	RoaringRelation final_relation = std::move(final_compose.relation);
	RecordComposeStage(
		"P" + std::to_string(min_len) + "_o_S" + std::to_string(d) + "_to_FINAL",
		"P" + std::to_string(min_len),
		"S" + std::to_string(d),
		"FINAL",
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationPairCount(bounded),
		RoaringRelationPairCount(final_relation),
		RoaringRelationNonEmptyRows(prefix_relation),
		RoaringRelationNonEmptyRows(bounded),
		RoaringRelationNonEmptyRows(final_relation),
		final_compose.candidate_pairs,
		final_compose.compose_time_ms,
		final_compose.stats.total_local_patterns,
		final_compose.stats.distinct_local_patterns,
		final_compose.naive_union_work,
		final_compose.stats.actual_union_work,
		final_compose.gcnp_planning_time_ms,
		final_compose.relation_merge_time_ms);
	RecordMaterializedRelationStats(
		"P" + std::to_string(min_len),
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationNonEmptyRows(prefix_relation),
		RoaringRelationMaxRowSize(prefix_relation));
	RecordMaterializedRelationStats(
		"S" + std::to_string(d),
		RoaringRelationPairCount(bounded),
		RoaringRelationNonEmptyRows(bounded),
		RoaringRelationMaxRowSize(bounded));
	RecordMaterializedRelationStats(
		"FINAL",
		RoaringRelationPairCount(final_relation),
		RoaringRelationNonEmptyRows(final_relation),
		RoaringRelationMaxRowSize(final_relation));
	{
	QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
	if (!bitmap_out) {
	for (const auto& kv : final_relation) {
		auto& row = out_rows[kv.first];
		for (uint32_t dst : kv.second) {
			row.insert(static_cast<size_t>(dst));
		}
	}
	}
	}

#if RANGE_SINGLE_DEBUG_OPEN_RANGE
	{
	AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
	std::cerr << "[range-single][bounded] B{" << min_len << "," << max_len << "}"
	          << " => B^" << min_len << " o S_" << d << std::endl;
	std::cerr << "[range-single][bounded] prefix_exact_pairs=" << RoaringRelationPairCount(prefix_relation)
	          << " mids=" << mids.size()
	          << " bounded_pairs=" << RoaringRelationPairCount(bounded)
	          << " final_pairs=" << RoaringRelationPairCount(final_relation)
	          << " prefix_rows=" << prefix_relation.size()
	          << " bounded_rows=" << bounded.size()
	          << " final_rows=" << final_relation.size()
	          << std::endl;
		std::cerr << "[BLPC][demand] bounded_cached_power_levels="
		          << exact_repetition_bitmap_cache.size()
		          << " bounded_cached_suffix_levels=" << bounded_suffix_bitmap_cache.size()
	          << " prefix_rows=" << prefix_relation.size()
	          << " prefix_pairs=" << RoaringRelationPairCount(prefix_relation)
	          << std::endl;
	}
#endif
	record_body_stats();
	const double closure_total_time_ms = static_cast<double>(
		std::chrono::duration_cast<std::chrono::milliseconds>(
			std::chrono::steady_clock::now() - closure_begin).count());
	RecordBoundedSummary(
		min_len,
		max_len,
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationPairCount(bounded),
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationPairCount(bounded),
		RoaringRelationPairCount(prefix_relation),
		RoaringRelationPairCount(bounded),
		prefix_time_ms,
		closure_total_time_ms - final_compose.compose_time_ms < 0.0 ? 0.0 :
			closure_total_time_ms - final_compose.compose_time_ms,
		final_compose.compose_time_ms,
		RoaringRelationPairCount(final_relation));
	if (bitmap_out) bitmap_out->swap(final_relation);
}

void Count_state::build_exact_rows_for_sources(
	const unordered_set<size_t>& sources,
	int len,
	shared_ptr<NFA_state> start_state,
	unordered_map<size_t, unordered_set<size_t>>& out_rows,
	RoaringRelation* bitmap_out)
{
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	out_rows.clear();
	if (sources.empty()) {
		return;
	}
	RoaringRelation bitmap_rows;
	build_exact_bitmap_rows_for_sources(sources, len, start_state, bitmap_rows);
	if (bitmap_out) {
		bitmap_out->swap(bitmap_rows);
		return;
	}
	QueryPhaseTimer materialize(QueryTimingPhase::ResultMaterialization);
	for (size_t src : sources) {
		auto& dense_row = out_rows[src];
		dense_row.clear();
		auto it = bitmap_rows.find(src);
		if (it != bitmap_rows.end()) {
			dense_row.insert(it->second.begin(), it->second.end());
		}
	}
}

void Count_state::init_map(int min,int max,shared_ptr<NFA_state> start_state,unordered_set<string> &firstSet,unordered_set<string> &lastSet){
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	auto t0 = std::chrono::steady_clock::now();
	//unordered_map<size_t, unordered_set<size_t>> g;
	unordered_set<size_t> start_vertexes;
	unordered_set<size_t> nonempty_start_vertexes;
	unordered_set<size_t> out;
	auto t1 = std::chrono::steady_clock::now();
	getStartPoints(firstSet, lastSet, nonempty_start_vertexes);
	if (body_can_be_empty) {
		for (const auto& kv : Graph::adjacency_map) {
			start_vertexes.insert(kv.first);
		}
	} else {
		start_vertexes = nonempty_start_vertexes;
	}
	auto t2 = std::chrono::steady_clock::now();
	//cout<<"debug:init_map "<<start_vertexes.size()<<" "<<firstSet.size()<<" "<<lastSet.size()<<endl;
	get_basic_re(start_state, g, nonempty_start_vertexes, out);
	if (body_can_be_empty) {
		for (const auto& kv : Graph::adjacency_map) {
			g[kv.first].insert(kv.first);
		}
	}
	auto t3 = std::chrono::steady_clock::now();
	
	// // 调试：打印 g 的信息
	// int non_empty = 0;
	// std::vector<size_t> non_empty_keys;
	// for (const auto& [k, v] : g) {
	// 	if (!v.empty()) {
	// 		non_empty++;
	// 		if (non_empty_keys.size() < 20) {
	// 			non_empty_keys.push_back(k);
	// 		}
	// 	}
	// }
	// cout << "DEBUG: g.size=" << g.size() << ", non_empty=" << non_empty << endl;
	// cout << "DEBUG: First 20 non-empty keys: ";
	// for (auto k : non_empty_keys) {
	// 	cout << k << " ";
	// }
	cout << endl;
	
	out.clear();
	// for(auto kv:g){
	// 	cout<<"debug init_map gkey:"<<kv.first<<endl;
	// 	for (auto v:kv.second)
	// 		cout<<"\t value:"<<v<<endl;
	// }
	get_range_re(min, max, g, range_repetition_map, start_vertexes, out, internal_start);
	auto t4 = std::chrono::steady_clock::now();
	
	// 调试：打印 range_repetition_map 的信息
	cout << "DEBUG: range_repetition_map.size=" << range_repetition_map.size() << endl;
	const auto init_total_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t0).count();
	const auto init_gsp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	const auto init_basic_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t3 - t2).count();
	const auto init_range_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t4 - t3).count();
	extern QueryTimingStats g_query_timing_stats;
	g_query_timing_stats.count_init_map_ms += init_total_ms;
	g_query_timing_stats.count_init_map_calls++;
	g_query_timing_stats.count_init_map_get_start_points_ms += init_gsp_ms;
	g_query_timing_stats.count_init_map_basic_re_ms += init_basic_ms;
	g_query_timing_stats.count_init_map_range_re_ms += init_range_ms;
	cout << "[Timing] Count_state::init_map:"
		<< " total_ms=" << init_total_ms
		<< " getStartPoints_ms=" << init_gsp_ms
		<< " get_basic_re_ms=" << init_basic_ms
		<< " get_range_re_ms=" << init_range_ms
		<< " start_vertices=" << start_vertexes.size()
		<< " g_size=" << g.size()
		<< " range_map_size=" << range_repetition_map.size()
		<< endl;
	
	// for(auto kv:range_repetition_map){
	// 	cout<<"debug init_map key:"<<kv.first<<endl;
	// 	for (auto v:kv.second)
	// 		cout<<"\t value:"<<v<<endl;
	// }
}

void Count_state::range_dfs(size_t vertex,shared_ptr<NFA_state> start_state,unordered_set<size_t> &result){
	AlgorithmPhaseTimer phase(AlgorithmPhase::MP);
	auto t0 = std::chrono::steady_clock::now();
	shared_ptr<unordered_set<size_t>> start_vertexes_ptr=make_shared<unordered_set<size_t>>();
	start_vertexes_ptr->insert(vertex);
	shared_ptr<unordered_set<size_t>> out_ptr=make_shared<unordered_set<size_t>>();
	for(int i=0;i<=max;i++){
		if (i>=min){
			result.insert(start_vertexes_ptr->begin(), start_vertexes_ptr->end());
		}
		if(i==max){
			break;
		}
		for(auto v:*start_vertexes_ptr){
			if (g.find(v)==g.end()){
				get_basic_re(start_state, g, v, *out_ptr);
			}
			out_ptr->insert(g[v].begin(),g[v].end());
		}
		swap(start_vertexes_ptr,out_ptr);
		out_ptr->clear();
	}
	auto t1 = std::chrono::steady_clock::now();
	extern QueryTimingStats g_query_timing_stats;
	g_query_timing_stats.count_range_dfs_ms += std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
	g_query_timing_stats.count_range_dfs_calls++;
}

// void Count_state::next_states(string label, int op, vector<pair<shared_ptr<NFA_state>, int>>& res, set<int>& counter)
// {
// 	if (op == Operation::Shift)
// 	{
// 		out1->next_states(label, Operation::Shift, res, counter);
// 		if (check(counter)) {
// 			out2->next_states(label, Operation::Check, res,counter);
// 		}
// 	}
// 	else
// 	{
// 		out1->next_states(label, Operation::SetPos0, res,counter);
// 		if (this->min == 0)
// 			out2->next_states(label, Operation::CheckZeroCount, res,counter);
// 	}
// 	return;
// }

//void NFA_state::setReferstate(int i)
//{
//	shared_ptr<NFA_state>out2 = this->out2;
//	if (i <= 2 && i >= 0)
//		group_state = i;
//	if (this->c == Type::Blank)
//		out1->setGroupstate(i);
//	else if (this->c == Type::Split)
//	{
//		out1->setGroupstate(i);
//		out2->setGroupstate(i);
//	}
//}

//ScaleState::ScaleState(bitset<256> s) :NFA_state(Type::Scale), scale(s)
//{
//}

//string ScaleState::toString()
//{
//	return string();
//}

NfaList::NfaList(vector<shared_ptr<NFA_state> >states) :ss(states)
{
	sort(ss.begin(), ss.end(), stateptrCmp());
}

DState::DState(shared_ptr<NfaList> list) :nfalist(list)
{
}
Frag::Frag(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state> > o) :start(s), out1(o)
{
}
Frag::Frag(shared_ptr<NFA_state> s) :start(s)
{
}
Frag::Frag(shared_ptr<NFA_state> s, vector<shared_ptr<NFA_state> > o1, vector<shared_ptr<NFA_state> >o2) :start(s), out1(o1), out2(o2)
{
}
string Frag::toString()
{
	string s = "[Frag start:]" + start->toString() + "[Frag out:";
	for (auto& w : out1)
		s = s + w->toString();
	return s;
}

bool NfalistCmp::operator()(const shared_ptr<NfaList>& n1, const shared_ptr<NfaList>& n2)const
{
	return *n1 < *n2;
}

size_t Hasher::operator()(const shared_ptr<NFA_state>& rhs)const
{
	return rhs->id;
}

size_t Hasher::operator()(const shared_ptr<NfaList>& rhs)const
{
	int ans = 0;
	for (int i = 0; i < rhs->ss.size(); i++)
		ans += hash<shared_ptr<NFA_state> >()(rhs->ss[i]);
	return ans;
}

bool NfaList::operator<(const NfaList& o) const
{
	if (ss.size() != o.ss.size())
		return ss.size() < o.ss.size();
	else
	{
		for (int i = 0; i < ss.size(); i++)
		{
			if ((ss[i]) < (o.ss[i]))
				return true;
			if ((o.ss[i]) < (ss[i]))
				return false;
		}
		return false;
	}
}
