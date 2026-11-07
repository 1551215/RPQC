#pragma once

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

struct PartRouter {
    std::string strategy = "hash";
    uint64_t vmin = 0;
    uint64_t vmax = 0;
    uint64_t seed = 0;
    std::string v2p_file;

    std::vector<uint64_t> v2p_keys;
    std::vector<uint32_t> v2p_pids;
    bool v2p_loaded = false;

    bool LoadV2P(const std::string& path, std::string& err) {
        v2p_keys.clear();
        v2p_pids.clear();
        v2p_loaded = false;

        std::ifstream in(path);
        if (!in.is_open()) {
            err = "Cannot open v2p_file: " + path;
            return false;
        }

        std::string line;
        while (std::getline(in, line)) {
            if (line.empty()) continue;
            std::istringstream iss(line);
            uint64_t vertex = 0;
            uint32_t pid = 0;
            if (!(iss >> vertex >> pid)) continue;
            v2p_keys.push_back(vertex);
            v2p_pids.push_back(pid);
        }

        if (v2p_keys.empty()) {
            err = "Empty v2p_file: " + path;
            return false;
        }

        std::vector<size_t> order(v2p_keys.size());
        for (size_t i = 0; i < order.size(); i++) order[i] = i;
        std::sort(order.begin(), order.end(), [&](size_t lhs, size_t rhs) {
            return v2p_keys[lhs] < v2p_keys[rhs];
        });

        std::vector<uint64_t> sorted_keys;
        std::vector<uint32_t> sorted_pids;
        sorted_keys.reserve(v2p_keys.size());
        sorted_pids.reserve(v2p_pids.size());
        for (size_t idx : order) {
            sorted_keys.push_back(v2p_keys[idx]);
            sorted_pids.push_back(v2p_pids[idx]);
        }
        v2p_keys.swap(sorted_keys);
        v2p_pids.swap(sorted_pids);
        v2p_loaded = true;
        return true;
    }

    int PartitionOf(uint64_t vertex, int partitions) const {
        if (partitions <= 1) return 0;

        if (strategy == "v2p") {
            if (!v2p_loaded) return 0;
            auto it = std::lower_bound(v2p_keys.begin(), v2p_keys.end(), vertex);
            if (it == v2p_keys.end() || *it != vertex) return 0;
            size_t pos = static_cast<size_t>(it - v2p_keys.begin());
            uint32_t pid = v2p_pids[pos];
            if (pid >= static_cast<uint32_t>(partitions)) return 0;
            return static_cast<int>(pid);
        }

        if (strategy == "range_id") {
            if (vmax <= vmin) return 0;
            uint64_t span = (vmax - vmin) + 1;
            uint64_t off = (vertex <= vmin) ? 0 : (vertex - vmin);
            uint64_t pid = off * static_cast<uint64_t>(partitions) / span;
            if (pid >= static_cast<uint64_t>(partitions)) pid = static_cast<uint64_t>(partitions) - 1;
            return static_cast<int>(pid);
        }

        if (strategy == "mod") {
            return static_cast<int>(vertex % static_cast<uint64_t>(partitions));
        }

        if (strategy == "hashmix") {
            uint64_t x = vertex ^ seed;
            x += 0x9e3779b97f4a7c15ULL;
            x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
            x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
            x ^= (x >> 31);
            return static_cast<int>(x % static_cast<uint64_t>(partitions));
        }

        return static_cast<int>(std::hash<uint64_t>()(vertex) % static_cast<uint64_t>(partitions));
    }
};

inline bool ParsePartMeta(const std::string& meta, PartRouter& router) {
    if (meta.empty()) return false;
    std::istringstream iss(meta);
    std::string line;
    while (std::getline(iss, line)) {
        if (line.empty()) continue;
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        std::string key = line.substr(0, eq);
        std::string value = line.substr(eq + 1);
        if (key == "strategy") router.strategy = value;
        else if (key == "vmin") router.vmin = std::stoull(value);
        else if (key == "vmax") router.vmax = std::stoull(value);
        else if (key == "seed") router.seed = std::stoull(value);
        else if (key == "v2p_file") router.v2p_file = value;
    }
    return true;
}

inline std::string ReadAllText(const std::string& path) {
    std::ifstream in(path);
    if (!in.is_open()) return "";
    std::ostringstream oss;
    oss << in.rdbuf();
    return oss.str();
}

inline std::string PartitionRootFromPartitionDir(const std::string& partition_dir) {
    std::filesystem::path path(partition_dir);
    return path.parent_path().string();
}

inline bool TryLoadRouterFromRoot(const std::string& partition_root, PartRouter& router, std::string& err) {
    if (partition_root.empty()) return false;

    std::string meta = ReadAllText((std::filesystem::path(partition_root) / "meta.txt").string());
    if (meta.empty()) {
        std::filesystem::path v2p_path = std::filesystem::path(partition_root) / "v2p.txt";
        if (std::filesystem::exists(v2p_path)) {
            meta = "strategy=v2p\n";
            meta += "v2p_file=" + v2p_path.string() + "\n";
        } else {
            return false;
        }
    }

    PartRouter loaded;
    ParsePartMeta(meta, loaded);
    if (loaded.strategy == "v2p") {
        if (loaded.v2p_file.empty()) {
            err = "meta.txt: strategy=v2p requires v2p_file=...";
            return false;
        }
        if (!loaded.LoadV2P(loaded.v2p_file, err)) {
            return false;
        }
    }

    router = std::move(loaded);
    return true;
}

