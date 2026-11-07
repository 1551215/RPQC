
#include "NFA_state.h"
#include "AdditionChainTable.h"
#include "ProductionFA.h"
#include "Type.h"
#include "graph.h"
#include "Traversal.h"
#include "RelationOperationMetrics.h"
#include <memory>
#include<regex>
#include<fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/stat.h>
#ifdef __linux__
#include <sys/resource.h>
#include <sys/wait.h>
#include <signal.h>
#include <unistd.h>
#endif
#include <chrono>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <tuple>
#include <vector>
#include <algorithm>
#include <cstdint>
#include <limits>
using namespace std;
extern int mode;

struct MemorySnapshot {
	long long vm_rss_kb = -1;
	long long vm_hwm_kb = -1;
	long long ru_maxrss_kb = -1;
};

static string Trim(const string& s) {
	size_t begin = s.find_first_not_of(" \t\r\n");
	if (begin == string::npos) return "";
	size_t end = s.find_last_not_of(" \t\r\n");
	return s.substr(begin, end - begin + 1);
}

#ifdef __linux__
static long long ReadProcStatusKb(const string& key) {
	ifstream in("/proc/self/status");
	string line;
	while (getline(in, line)) {
		if (line.rfind(key, 0) == 0) {
			istringstream iss(line.substr(key.size()));
			long long kb = -1;
			iss >> kb;
			return kb;
		}
	}
	return -1;
}
#endif

static MemorySnapshot CaptureMemorySnapshot() {
	MemorySnapshot snap;
#ifdef __linux__
	snap.vm_rss_kb = ReadProcStatusKb("VmRSS:");
	snap.vm_hwm_kb = ReadProcStatusKb("VmHWM:");
	struct rusage usage {};
	if (getrusage(RUSAGE_SELF, &usage) == 0) {
		snap.ru_maxrss_kb = static_cast<long long>(usage.ru_maxrss);
	}
#endif
	return snap;
}

static double KbToMb(long long kb) {
	return (kb < 0) ? -1.0 : static_cast<double>(kb) / 1024.0;
}

static long long ReadPositiveEnvLong(const char* name) {
	const char* value = getenv(name);
	if (value == nullptr || *value == '\0') {
		return 0;
	}
	char* end = nullptr;
	const long long parsed = strtoll(value, &end, 10);
	return (end != value && parsed > 0) ? parsed : 0;
}

// Optional correctness artifact: sorted (source, destination) uint32 pairs,
// little endian, eight bytes per pair. Kept outside the timed query path.
static void WritePairDump(const unordered_map<size_t, unordered_set<size_t>>& pairs,
                          uint64_t expected_count) {
    const char* enabled = getenv("RDI_EXPORT_PAIRS");
    if (enabled == nullptr || strcmp(enabled, "1") != 0) return;
    static_assert(sizeof(uint32_t) == 4, "uint32_t required");
    const uint16_t endian_test = 1;
    if (*reinterpret_cast<const unsigned char*>(&endian_test) != 1)
        throw runtime_error("pair dump requires little-endian host");
    vector<size_t> sources;
    sources.reserve(pairs.size());
    for (const auto& row : pairs) sources.push_back(row.first);
    sort(sources.begin(), sources.end());
    FILE* file = fopen("pairs.u32le.tmp", "wb");
    if (!file) throw runtime_error("cannot open pairs.u32le.tmp");
    setvbuf(file, nullptr, _IOFBF, 1 << 20);
    uint64_t written = 0;
    try {
        for (size_t source : sources) {
            if (source > numeric_limits<uint32_t>::max()) throw runtime_error("source ID exceeds uint32");
            vector<size_t> targets(pairs.at(source).begin(), pairs.at(source).end());
            sort(targets.begin(), targets.end());
            for (size_t target : targets) {
                if (target > numeric_limits<uint32_t>::max()) throw runtime_error("target ID exceeds uint32");
                uint32_t entry[2] = {static_cast<uint32_t>(source), static_cast<uint32_t>(target)};
                if (fwrite(entry, sizeof(entry), 1, file) != 1) throw runtime_error("pair dump write failed");
                ++written;
            }
        }
        if (fclose(file) != 0) throw runtime_error("pair dump close failed");
        file = nullptr;
        if (written != expected_count) throw runtime_error("pair dump count mismatch");
        filesystem::rename("pairs.u32le.tmp", "pairs.u32le");
        ofstream meta("pairs.count");
        meta << written << '\n';
        if (!meta) throw runtime_error("pair dump count write failed");
    } catch (...) {
        if (file) fclose(file);
        remove("pairs.u32le.tmp");
        throw;
    }
}

struct QueryVariant {
	string name;
	string bounded_suffix_mode;
	string hybrid_eval_mode;
};

static vector<QueryVariant> QueryVariantsFor(
	int query_index, bool gcnp_enabled)
{
	const string default_suffix = "dual_state";
	const string default_hybrid = "macro";
	vector<QueryVariant> variants;
	// Batch experiments can request exactly the configuration supplied through
	// the environment, bypassing the historical query-index ablation matrix.
	const char* variant_policy = getenv("QUERY_VARIANT_POLICY");
	if (variant_policy != nullptr && std::strcmp(variant_policy, "forced") == 0) {
		const char* suffix_mode = getenv("BOUNDED_SUFFIX_MODE");
		const char* hybrid_mode = getenv("BLPC_HYBRID_EVAL_MODE");
		const char* variant_name = getenv("RUN_VARIANT_NAME");
		variants.push_back({
			(variant_name != nullptr && *variant_name != '\0') ? variant_name : "forced",
			(suffix_mode != nullptr && *suffix_mode != '\0') ? suffix_mode : default_suffix,
			(hybrid_mode != nullptr && *hybrid_mode != '\0') ? hybrid_mode : default_hybrid
		});
		return variants;
	}
	if (!gcnp_enabled) {
		variants.push_back({"dual_state_macro", default_suffix, default_hybrid});
		return variants;
	}
	if (query_index >= 13 && query_index <= 14) {
		variants.push_back({"dual_state_macro", default_suffix, default_hybrid});
		variants.push_back({"naive_bounded_macro", "naive_union", default_hybrid});
		return variants;
	}
	if (query_index >= 17 && query_index <= 21) {
		variants.push_back({"dual_state_macro", default_suffix, default_hybrid});
		variants.push_back({"dual_state_materialize", default_suffix, "materialize"});
		return variants;
	}
	variants.push_back({"dual_state_macro", default_suffix, default_hybrid});
	return variants;
}

#ifdef __linux__
static long long ReadProcessRssKb(pid_t pid) {
	ifstream in("/proc/" + to_string(pid) + "/status");
	string line;
	while (getline(in, line)) {
		if (line.rfind("VmRSS:", 0) == 0) {
			istringstream iss(line.substr(string("VmRSS:").size()));
			long long kb = -1;
			iss >> kb;
			return kb;
		}
	}
	return -1;
}

static long long ReadCgroupNonReclaimableBytes() {
	ifstream in("/sys/fs/cgroup/memory.stat");
	string key;
	long long value = 0;
	long long total = 0;
	while (in >> key >> value) {
		if (key == "anon" || key == "shmem" || key == "kernel_stack" ||
			key == "pagetables" || key == "sock" || key == "slab_unreclaimable") {
			total += value;
		}
	}
	return in.eof() ? total : -1;
}
#endif

static void PrintMemorySnapshot(const string& label, const MemorySnapshot& snap) {
	cout << "[Memory] " << label;
	if (snap.vm_rss_kb >= 0) {
		cout << " VmRSS=" << KbToMb(snap.vm_rss_kb) << " MB";
	}
	if (snap.vm_hwm_kb >= 0) {
		cout << " VmHWM=" << KbToMb(snap.vm_hwm_kb) << " MB";
	}
	if (snap.ru_maxrss_kb >= 0) {
		cout << " ru_maxrss=" << KbToMb(snap.ru_maxrss_kb) << " MB";
	}
	cout << endl;
}

static string DetectDataset(const string& vertex_file, const string& edge_file) {
	string combined = vertex_file + " " + edge_file;
	if (combined.find("dbpedia") != string::npos) return "dbpedia";
	if (combined.find("yago") != string::npos) return "yago";
	if (combined.find("lubm") != string::npos) return "lubm";
	if (combined.find("sf1") != string::npos) return "sf1";
	if (combined.find("sf3") != string::npos) return "sf3";
	return "unknown";
}

static string DetectQueryClass(const string& expr) {
	size_t l = expr.find('{');
	size_t r = expr.find('}', l == string::npos ? 0 : l + 1);
	if (l == string::npos || r == string::npos || r <= l + 1) {
		return "unknown";
	}
	string inner = expr.substr(l + 1, r - l - 1);
	if (inner.find(',') == string::npos) {
		return "exact";
	}
	size_t comma = inner.find(',');
	string rhs = Trim(inner.substr(comma + 1));
	if (rhs.empty()) {
		return "lower_bounded_unbounded";
	}
	return "bounded_range";
}

static void WriteResultsHeader(ofstream& out_stream, const string& vertex_file,
	const string& edge_file)
{
	out_stream << "# Expression Execution Results" << endl;
	out_stream << "# Format: Expression : Time(ms)" << endl;
	out_stream << "# Mode: " << mode << endl;
	out_stream << "# Graph: " << vertex_file << " + " << edge_file << endl;
	out_stream << "# Memory columns: rss_before_mb rss_after_mb hwm_after_mb maxrss_after_mb" << endl;
	out_stream << "#" << endl;
}

static void WriteQueryStatus(const string& query_dir, const string& query_id,
	const string& expression, const string& status, const string& detail = "")
{
	std::error_code error;
	filesystem::create_directories(query_dir, error);
	if (error) {
		cerr << "Unable to create status directory: " << query_dir << ": " << error.message() << endl;
		return;
	}
	ofstream status_stream(query_dir + "/status.txt");
	if (!status_stream.is_open()) {
		return;
	}
	status_stream << "query_id=" << query_id << "\n";
	status_stream << "expression=" << expression << "\n";
	status_stream << "status=" << status << "\n";
	if (!detail.empty()) {
		status_stream << detail << "\n";
	}
}

#ifdef __linux__
static bool PrepareQueryOutputDirectory(const string& query_dir,
	ofstream& out_stream, const string& vertex_file, const string& edge_file)
{
	std::error_code error;
	filesystem::create_directories(query_dir, error);
	if (error || chdir(query_dir.c_str()) != 0) {
		return false;
	}

	cout.flush();
	cerr.flush();
	if (freopen("run.log", "w", stdout) == nullptr ||
		freopen("run.log", "a", stderr) == nullptr) {
		return false;
	}
	out_stream.close();
	out_stream.open("results.txt");
	if (!out_stream.is_open()) {
		return false;
	}
	WriteResultsHeader(out_stream, vertex_file, edge_file);
	out_stream.flush();
	return true;
}
#endif


int main(int argc,char* argv[])
{
	if (argc < 3) {
		cerr << "Usage: " << argv[0] << " <vertex_file> <edge_file> [expression_file]" << endl;
		return 1;
	}

	string vertex_file=argv[1];//顶点文件
	string edge_file=argv[2];//边文件
	PrintMemorySnapshot("before graph load", CaptureMemorySnapshot());
	Graph::loadGraph(vertex_file,edge_file);
	PrintMemorySnapshot("after graph load", CaptureMemorySnapshot());
	cout << "Using in-code binary-basis relation plans" << endl;
	unordered_map<size_t, unordered_set<size_t>> result;
	mode=1;
	
	// 从文件读取表达式
	string exp_file = (argc > 3) ? argv[3] : "./RPQ_data/sythetic_M2.txt";
	ifstream exp_stream(exp_file);
	if (!exp_stream.is_open()) {
		cerr << "Error: Cannot open expression file: " << exp_file << endl;
		return 1;
	}
	
	// The parent only records batch-level status. Per-query files are opened by
	// each child in QUERY_OUTPUT_ROOT/qN after fork().
	string output_file = "./batch_summary.txt";
	ofstream out_stream(output_file);
	if (!out_stream.is_open()) {
		cerr << "Error: Cannot create output file: " << output_file << endl;
		return 1;
	}
	
	cout << "Reading expressions from: " << exp_file << endl;
	cout << "Output results to: " << output_file << endl;
	
	out_stream << "# Batch query status" << endl;
	out_stream << "# Graph: " << vertex_file << " + " << edge_file << endl;
	// Child processes inherit this descriptor. Flush before the first fork so the
	// header is never duplicated by a child buffer.
	out_stream.flush();
	
	string exp;
	int exp_count = 0;
	int success_count = 0;
	int error_count = 0;
	int timeout_count = 0;
	int memory_kill_count = 0;
	const string dataset = DetectDataset(vertex_file, edge_file);
	const char* output_root_env = getenv("QUERY_OUTPUT_ROOT");
	const string query_output_root =
		(output_root_env != nullptr && *output_root_env != '\0') ?
		output_root_env : "./query_logs";
	const long long query_timeout_seconds = ReadPositiveEnvLong("QUERY_TIMEOUT_SECONDS");
	const long long query_memory_limit_kb = ReadPositiveEnvLong("QUERY_MEMORY_LIMIT_KB");
	const long long total_memory_limit_kb = ReadPositiveEnvLong("QUERY_TOTAL_MEMORY_LIMIT_KB");
	const long long node_memory_limit_bytes = ReadPositiveEnvLong("QUERY_NODE_MEMORY_LIMIT_BYTES");
	const long long query_start_index = ReadPositiveEnvLong("QUERY_START_INDEX");
	const long long query_end_index = ReadPositiveEnvLong("QUERY_END_INDEX");
	if (query_timeout_seconds > 0) {
		cout << "Per-query timeout: " << query_timeout_seconds << " s" << endl;
	}
	if (query_memory_limit_kb > 0) {
		cout << "Per-query memory limit: " << query_memory_limit_kb << " KB" << endl;
	}
	if (node_memory_limit_bytes > 0) {
		cout << "Node non-reclaimable memory limit: " << node_memory_limit_bytes << " bytes" << endl;
	}
	if (query_start_index > 0 || query_end_index > 0) {
		cout << "Query index range: "
			<< (query_start_index > 0 ? to_string(query_start_index) : "1")
			<< "-"
			<< (query_end_index > 0 ? to_string(query_end_index) : "end")
			<< endl;
	}
	cout << "Per-query output root: " << query_output_root << endl;
	
	while (getline(exp_stream, exp)) {
		// 跳过空行和注释
		exp = Trim(exp);
		if (exp.empty() || exp[0] == '#') {
			continue;
		}
		
		exp_count++;
		if (query_start_index > 0 && exp_count < query_start_index) {
			continue;
		}
		if (query_end_index > 0 && exp_count > query_end_index) {
			break;
		}
		cout << "\n[Expression #" << exp_count << "] " << exp << endl;
		const bool gcnp_enabled = [] {
			const char* value = getenv("GCNP_MODE");
			return value == nullptr || std::strcmp(value, "plain") != 0;
		}();
		const vector<QueryVariant> variants = QueryVariantsFor(exp_count, gcnp_enabled);
		for (const QueryVariant& variant : variants) {
			const string query_id = "q" + to_string(exp_count) + "_" + variant.name;
			const string query_dir = query_output_root + "/q" +
				to_string(exp_count) + "/" + variant.name;
			cout << "[QueryVariant] " << query_id
				<< " bounded_suffix=" << variant.bounded_suffix_mode
				<< " hybrid_eval=" << variant.hybrid_eval_mode << endl;

#ifdef __linux__
		cout.flush();
		cerr.flush();
		out_stream.flush();
		// Bound the two-process address-space sum, including the loaded graph
		// retained by the parent. Reserve 2 GiB for parent growth and helpers.
		long long child_address_limit_kb = 0;
		if (total_memory_limit_kb > 0) {
			const long long parent_address_kb = ReadProcStatusKb("VmSize:");
			child_address_limit_kb = total_memory_limit_kb - parent_address_kb - 2LL * 1024 * 1024;
			if (parent_address_kb <= 0 || child_address_limit_kb <= parent_address_kb) {
				cout << "QUERY_MEMORY_LIMIT query_id=" << query_id << " insufficient_fork_address_budget" << endl;
				out_stream << query_id << " " << exp << " : MEMKILL insufficient_fork_address_budget" << endl;
				WriteQueryStatus(query_dir, query_id, exp, "MEMKILL", "insufficient_fork_address_budget");
				memory_kill_count++;
				continue;
			}
			cout << "[MemoryBudget] parent_address_kb=" << parent_address_kb
				<< " child_address_limit_kb=" << child_address_limit_kb << endl;
		}
		const long long graph_baseline_rss_kb = ReadProcessRssKb(getpid());
		const pid_t child_pid = fork();
		if (child_pid < 0) {
			cerr << "Unable to fork expression #" << exp_count
				<< " variant=" << variant.name << endl;
			out_stream << exp << " : ERROR fork_failed" << endl;
			out_stream.flush();
			error_count++;
			continue;
		}
		if (child_pid > 0) {
			const auto wait_started = chrono::steady_clock::now();
			bool timed_out = false;
			bool memory_killed = false;
			int child_status = 0;
			struct rusage child_usage {};
			long long last_heartbeat_seconds = 0;
			while (true) {
				const pid_t waited = wait4(child_pid, &child_status, WNOHANG, &child_usage);
				if (waited == child_pid) {
					break;
				}
				if (waited < 0) {
					cerr << "waitpid failed for expression #" << exp_count << endl;
					child_status = -1;
					break;
				}
				const auto elapsed_seconds = chrono::duration_cast<chrono::seconds>(
					chrono::steady_clock::now() - wait_started).count();
				if (query_timeout_seconds > 0 && elapsed_seconds >= query_timeout_seconds) {
					timed_out = true;
					kill(child_pid, SIGKILL);
					wait4(child_pid, &child_status, 0, &child_usage);
					break;
				}
				const long long child_rss_kb = ReadProcessRssKb(child_pid);
				const long long parent_rss_kb = ReadProcessRssKb(getpid());
				const long long node_memory_bytes = ReadCgroupNonReclaimableBytes();
				if (elapsed_seconds - last_heartbeat_seconds >= 60) {
					cout << "[Progress] query_id=" << query_id << " elapsed_seconds=" << elapsed_seconds
						<< " parent_rss_kb=" << parent_rss_kb << " child_rss_kb=" << child_rss_kb << endl;
					last_heartbeat_seconds = elapsed_seconds;
				}
				if ((query_memory_limit_kb > 0 && child_rss_kb >= query_memory_limit_kb) ||
					(total_memory_limit_kb > 0 && parent_rss_kb + child_rss_kb >= total_memory_limit_kb - 2LL * 1024 * 1024) ||
					(node_memory_limit_bytes > 0 && node_memory_bytes >= node_memory_limit_bytes)) {
					memory_killed = true;
					kill(child_pid, SIGKILL);
					wait4(child_pid, &child_status, 0, &child_usage);
					break;
				}
				usleep(100000);
			}

			if (timed_out) {
				cout << "QUERY_TIMEOUT query_id=" << query_id
					<< " timeout_seconds=" << query_timeout_seconds << endl;
				out_stream << query_id << " " << exp
					<< " : TIMEOUT timeout_seconds=" << query_timeout_seconds << endl;
				out_stream.flush();
				WriteQueryStatus(query_dir, query_id, exp,
					"TIMEOUT", "timeout_seconds=" + to_string(query_timeout_seconds));
				timeout_count++;
			} else if (memory_killed) {
				cout << "QUERY_MEMORY_LIMIT query_id=" << query_id
					<< " memory_limit_kb=" << query_memory_limit_kb << endl;
				out_stream << query_id << " " << exp
					<< " : MEMKILL memory_limit_kb=" << query_memory_limit_kb << endl;
				out_stream.flush();
				WriteQueryStatus(query_dir, query_id, exp,
					"MEMKILL", "memory_limit_kb=" + to_string(query_memory_limit_kb));
				memory_kill_count++;
			} else if (WIFEXITED(child_status) && WEXITSTATUS(child_status) == 4) {
				WriteQueryStatus(query_dir, query_id, exp, "MEMKILL", "address_budget_allocation_failed");
				out_stream << query_id << " " << exp << " : MEMKILL address_budget_allocation_failed" << endl;
				memory_kill_count++;
			} else if (WIFEXITED(child_status) && WEXITSTATUS(child_status) == 0) {
				WriteQueryStatus(query_dir, query_id, exp, "SUCCESS");
				success_count++;
			} else {
				cout << "QUERY_ERROR query_id=" << query_id
					<< " child_status=" << child_status << endl;
				WriteQueryStatus(query_dir, query_id, exp,
					"ERROR", "child_status=" + to_string(child_status));
				error_count++;
			}
			// Kernel high-water RSS is available even when the child was killed.
			ofstream peak_stream(query_dir + "/status.txt", std::ios::app);
			peak_stream << "peak_rss_kb=" << max(graph_baseline_rss_kb, static_cast<long long>(child_usage.ru_maxrss)) << "\n"
				<< "graph_baseline_rss_kb=" << graph_baseline_rss_kb << "\n"
				<< "peak_memory_source=wait4_child_ru_maxrss\n";
			continue;
		}
		if (child_address_limit_kb > 0) {
			struct rlimit address_limit {};
			if (getrlimit(RLIMIT_AS, &address_limit) != 0) _exit(3);
			const rlim_t requested = static_cast<rlim_t>(child_address_limit_kb) * 1024;
			address_limit.rlim_cur = min(address_limit.rlim_cur, requested);
			address_limit.rlim_max = min(address_limit.rlim_max, requested);
			if (setrlimit(RLIMIT_AS, &address_limit) != 0) _exit(3);
		}
		setenv("BOUNDED_SUFFIX_MODE", variant.bounded_suffix_mode.c_str(), 1);
		setenv("BLPC_HYBRID_EVAL_MODE", variant.hybrid_eval_mode.c_str(), 1);
		if (!PrepareQueryOutputDirectory(query_dir, out_stream, vertex_file, edge_file)) {
			cerr << "Unable to create query output directory: " << query_dir << endl;
			_exit(2);
		}
#endif
		
		try {
			result.clear();
			ResetQueryTimingStats();
			const string query_class = DetectQueryClass(exp);
			BeginQueryExperiment(dataset, query_id, query_class, exp);
			MemorySnapshot before_expr = CaptureMemorySnapshot();
			PrintMemorySnapshot("before expression " + to_string(exp_count), before_expr);
			auto t0 = chrono::steady_clock::now();
			ResetAlgorithmTiming(t0);
			AlgorithmPhaseTimer diagnostics(AlgorithmPhase::Diagnostics);
			shared_ptr<Production_FA> fa = make_shared<Production_FA>(exp);
			auto t1 = chrono::steady_clock::now();
			cout<<"beigin_init"<<endl;
			fa->init_range_repetitions();
			auto t2 = chrono::steady_clock::now();
			cout << "begin compute" << endl;
			fa->compute_RPQ(result);
			auto t3 = chrono::steady_clock::now();
			MemorySnapshot after_expr = CaptureMemorySnapshot();
			uint64_t resnum = 0;
			{
				QueryPhaseTimer statistics(QueryTimingPhase::RelationStats);
				if (fa->has_bitmap_result) {
					for (const auto& pair : fa->bitmap_result) resnum += pair.second.cardinality();
				} else {
					for (const auto& pair : result) resnum += pair.second.size();
				}
			}
			auto t4 = chrono::steady_clock::now();
			FinishAlgorithmTiming(t4);

			double parse_ms = chrono::duration_cast<chrono::milliseconds>(t1 - t0).count();
			double init_ms = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();
			double compute_ms = chrono::duration_cast<chrono::milliseconds>(t3 - t2).count();
			double count_ms = chrono::duration_cast<chrono::milliseconds>(t4 - t3).count();
			double cost_ms = chrono::duration<double, std::milli>(t4 - t0).count();
			double cost_sec = cost_ms / 1000.0;
			
			cout << "result num:" << resnum
				<< "\ncost time: " << cost_ms << " ms"
				<< "\ncost time: " << cost_sec << " s" << endl;
			cout << "time breakdown (ms):"
				<< " parse=" << parse_ms
				<< " init=" << init_ms
				<< " compute=" << compute_ms
				<< " count=" << count_ms
				<< " total=" << cost_ms << endl;
			PrintQueryTimingStats();
			PrintMemorySnapshot("after expression " + to_string(exp_count), after_expr);
			RecordQueryPeakMemoryMb(KbToMb(after_expr.vm_hwm_kb));
			const auto& qs = GetQueryTimingStats();
			long long count_precompute_basic_ms = qs.count_init_map_basic_re_ms;
			long long count_binary_range_ms = qs.count_init_map_range_re_ms;
			long long count_query_traverse_ms = static_cast<long long>(compute_ms);
			cout << "[Timing] count_overview:"
				<< " precompute_basic_ms=" << count_precompute_basic_ms
				<< " binary_range_ms=" << count_binary_range_ms
				<< " query_traverse_ms=" << count_query_traverse_ms
				<< endl;
			FinalizeQueryExperiment(cost_ms);
			WritePairDump(result, resnum);
			
			// 写入文件：表达式 : 计算时间
			out_stream << exp << " : " << cost_ms << " ms"
				<< " parse_ms=" << parse_ms
				<< " init_ms=" << init_ms
				<< " compute_ms=" << compute_ms
				<< " count_ms=" << count_ms
				<< " rss_before_mb=" << KbToMb(before_expr.vm_rss_kb)
				<< " rss_after_mb=" << KbToMb(after_expr.vm_rss_kb)
				<< " hwm_after_mb=" << KbToMb(after_expr.vm_hwm_kb)
				<< " maxrss_after_mb=" << KbToMb(after_expr.ru_maxrss_kb)
				<< endl;
			out_stream << "# count_overview"
				<< " precompute_basic_ms=" << count_precompute_basic_ms
				<< " binary_range_ms=" << count_binary_range_ms
				<< " query_traverse_ms=" << count_query_traverse_ms
				<< endl;
			out_stream << "# count_path_summary"
				<< " getStartPoints_ms=" << qs.get_start_points_ms
				<< " get_basic_re_batch_ms=" << qs.get_basic_re_batch_ms
				<< " get_basic_re_single_ms=" << qs.get_basic_re_single_ms
				<< " get_range_re_ms=" << qs.get_range_re_ms
				<< " range_join_ms=" << qs.range_join_ms
				<< " join_ms=" << qs.join_ms
				<< " self_join_ms=" << qs.self_join_ms
				<< " get_quora_re_ms=" << qs.get_quora_re_ms
				<< " get_star_re_ms=" << qs.get_star_re_ms
				<< " count_init_map_ms=" << qs.count_init_map_ms
				<< " count_init_map_getStartPoints_ms=" << qs.count_init_map_get_start_points_ms
				<< " count_init_map_basic_re_ms=" << qs.count_init_map_basic_re_ms
				<< " count_init_map_range_re_ms=" << qs.count_init_map_range_re_ms
				<< " count_range_dfs_ms=" << qs.count_range_dfs_ms
				<< " range_complex_hits=" << qs.range_complex_hits
				<< " range_simple_hits=" << qs.range_simple_hits
				<< endl;
			success_count++;
		}
		catch (const std::bad_alloc& e) {
			cerr << "Expression memory allocation failed: " << e.what() << endl;
#ifdef __linux__
			out_stream.flush();
			cerr.flush();
			_exit(4);
#else
			error_count++;
#endif
		}
		catch (const std::exception& e) {
			cerr << "Expression failed: " << e.what() << endl;
			out_stream << exp << " : ERROR " << e.what() << endl;
			error_count++;
		}
		catch (double) {
			cout << exp << " err" << endl;
			out_stream << exp << " : ERROR" << endl;
			error_count++;
		}
#ifdef __linux__
		out_stream.flush();
		cout.flush();
		cerr.flush();
		_exit(error_count == 0 ? 0 : 2);
#endif
		}
	}
	
	exp_stream.close();
	
	// 写入统计信息
	out_stream << "#" << endl;
	out_stream << "# Total: " << exp_count << ", Success: " << success_count
		<< ", Timeout: " << timeout_count
		<< ", MemoryKilled: " << memory_kill_count
		<< ", Error: " << error_count << endl;
	
	out_stream.close();
	
	cout << "\n========================================" << endl;
	cout << "Total expressions processed: " << exp_count << endl;
	cout << "Success: " << success_count
		<< ", Timeout: " << timeout_count
		<< ", MemoryKilled: " << memory_kill_count
		<< ", Error: " << error_count << endl;
	cout << "Results saved to: " << output_file << endl;
	cout << "========================================" << endl;
	// for(auto kv:result){
	// 	cout<<"key:"<<kv.first<<endl;
    //     for(auto v:kv.second){
    //         cout<<"\t value:"<<v<<endl;
    //     }
    //     cout<<endl;
	// }
}
