// Build partition directories from a provided vertex->partition mapping.
// Input formats:
//   vertex_file: one vertex id per line
//   edge_file:   "src label dst" whitespace-separated per line
//   v2p_file:    "vertex pid" whitespace-separated per line
//
// Output:
//   <out_dir>/partition_i/vertices.txt
//   <out_dir>/partition_i/edges.txt
//   <out_dir>/v2p.txt
//   <out_dir>/meta.txt   (strategy=v2p, v2p_file=<out_dir>/v2p.txt)
//
// Usage:
//   build_partitions_from_v2p.exe -v <vertex_file> -e <edge_file> -m <v2p_file> -o <out_dir> -n <P>
#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct V2PMap {
    std::vector<uint64_t> keys;
    std::vector<uint32_t> pids;

    bool Load(const std::string& path, int P) {
        keys.clear();
        pids.clear();
        std::ifstream in(path);
        if (!in.is_open()) return false;
        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            uint64_t v = 0;
            uint32_t pid = 0;
            if (!(iss >> v >> pid)) continue;
            if (P > 0 && pid >= (uint32_t)P) continue;
            keys.push_back(v);
            pids.push_back(pid);
        }
        if (keys.empty()) return false;

        std::vector<size_t> idx(keys.size());
        for (size_t i = 0; i < idx.size(); i++) idx[i] = i;
        std::sort(idx.begin(), idx.end(), [&](size_t a, size_t b) { return keys[a] < keys[b]; });
        std::vector<uint64_t> keys2;
        std::vector<uint32_t> pids2;
        keys2.reserve(keys.size());
        pids2.reserve(pids.size());
        for (size_t i : idx) {
            keys2.push_back(keys[i]);
            pids2.push_back(pids[i]);
        }
        keys.swap(keys2);
        pids.swap(pids2);
        return true;
    }

    bool Lookup(uint64_t v, uint32_t& pid_out) const {
        auto it = std::lower_bound(keys.begin(), keys.end(), v);
        if (it == keys.end() || *it != v) return false;
        size_t pos = static_cast<size_t>(it - keys.begin());
        pid_out = pids[pos];
        return true;
    }
};

static void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0 << " -v <vertex_file> -e <edge_file> -m <v2p_file> -o <out_dir> -n <P>\n";
}

static bool EnsureDir(const fs::path& p) {
    std::error_code ec;
    fs::create_directories(p, ec);
    return !ec;
}

int main(int argc, char** argv) {
    std::string vertex_file, edge_file, v2p_file, out_dir;
    int P = 0;

    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if ((a == "-v" || a == "--vertex-file") && i + 1 < argc) vertex_file = argv[++i];
        else if ((a == "-e" || a == "--edge-file") && i + 1 < argc) edge_file = argv[++i];
        else if ((a == "-m" || a == "--v2p-file") && i + 1 < argc) v2p_file = argv[++i];
        else if ((a == "-o" || a == "--output-dir") && i + 1 < argc) out_dir = argv[++i];
        else if ((a == "-n" || a == "--num-partitions") && i + 1 < argc) P = std::stoi(argv[++i]);
        else if (a == "-h" || a == "--help") {
            PrintUsage(argv[0]);
            return 0;
        }
    }

    if (vertex_file.empty() || edge_file.empty() || v2p_file.empty() || out_dir.empty() || P <= 0) {
        PrintUsage(argv[0]);
        return 1;
    }

    V2PMap map;
    if (!map.Load(v2p_file, P)) {
        std::cerr << "Failed to load v2p file: " << v2p_file << "\n";
        return 1;
    }

    if (!EnsureDir(out_dir)) {
        std::cerr << "Cannot create output dir: " << out_dir << "\n";
        return 1;
    }

    std::vector<std::ofstream> vouts(P), eouts(P);
    for (int pid = 0; pid < P; pid++) {
        fs::path pdir = fs::path(out_dir) / ("partition_" + std::to_string(pid));
        if (!EnsureDir(pdir)) {
            std::cerr << "Cannot create partition dir: " << pdir.string() << "\n";
            return 1;
        }
        vouts[pid].open((pdir / "vertices.txt").string(), std::ios::binary);
        eouts[pid].open((pdir / "edges.txt").string(), std::ios::binary);
        if (!vouts[pid].is_open() || !eouts[pid].is_open()) {
            std::cerr << "Cannot open output files under: " << pdir.string() << "\n";
            return 1;
        }
    }

    // Copy v2p.txt into out_dir (as-is).
    {
        std::ifstream src(v2p_file, std::ios::binary);
        std::ofstream dst((fs::path(out_dir) / "v2p.txt").string(), std::ios::binary);
        dst << src.rdbuf();
    }
    // Write meta.txt.
    {
        std::ofstream meta((fs::path(out_dir) / "meta.txt").string(), std::ios::binary);
        meta << "strategy=v2p\n";
        meta << "v2p_file=" << (fs::path(out_dir) / "v2p.txt").string() << "\n";
    }

    // Write vertices by mapping.
    {
        std::ifstream in(vertex_file);
        if (!in.is_open()) {
            std::cerr << "Cannot open vertex file: " << vertex_file << "\n";
            return 1;
        }
        std::string line;
        uint64_t cnt = 0;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            uint64_t v = 0;
            if (!(iss >> v)) continue;
            uint32_t pid = 0;
            if (!map.Lookup(v, pid)) {
                // Skip unknown vertices to allow partial maps, but warn.
                continue;
            }
            if (pid >= (uint32_t)P) continue;
            vouts[(int)pid] << v << "\n";
            cnt++;
        }
        std::cout << "Wrote vertices: " << cnt << "\n";
    }

    // Write edges to src partition.
    {
        std::ifstream in(edge_file);
        if (!in.is_open()) {
            std::cerr << "Cannot open edge file: " << edge_file << "\n";
            return 1;
        }
        std::string line;
        uint64_t cnt = 0;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            uint64_t src = 0, dst = 0;
            std::string label;
            if (!(iss >> src >> label >> dst)) continue;
            uint32_t pid = 0;
            if (!map.Lookup(src, pid)) continue;
            if (pid >= (uint32_t)P) continue;
            eouts[(int)pid] << src << " " << label << " " << dst << "\n";
            cnt++;
        }
        std::cout << "Wrote edges: " << cnt << "\n";
    }

    std::cout << "Done. Output: " << out_dir << "\n";
    return 0;
}

