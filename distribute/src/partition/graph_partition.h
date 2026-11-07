#pragma once
#include <cstddef>
#include <string>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <iostream>
#include <functional>

namespace rpq {

/**
 * Worker 图分区
 * 存储分配给单个 Worker 的图数据
 */
class GraphPartition {
public:
    int partition_id;
    int total_partitions;
    
    // 本地顶点集合
    std::unordered_set<size_t> local_vertices;
    
    // 邻接表: vertex -> label -> [target_vertices]
    std::unordered_map<size_t, std::unordered_map<std::string, std::vector<size_t>>> adjacency_map;
    
    // 标签索引: label -> [vertices with this outgoing label]
    std::unordered_map<std::string, std::unordered_set<size_t>> start_points;
    
    // 标签索引: label -> [vertices with this incoming label]
    std::unordered_map<std::string, std::unordered_set<size_t>> end_points;
    
    GraphPartition(int partition_id, int total_partitions)
        : partition_id(partition_id), total_partitions(total_partitions) {}
    
    /**
     * 判断顶点是否在本分区
     */
    bool IsLocalVertex(size_t v) const {
        // Authoritative membership is based on loaded vertices.txt.
        // This allows experimenting with different partition strategies without hardcoding hash(v)%P.
        return local_vertices.count(v) > 0;
    }

    /**
     * 获取顶点所属的分区 ID
     * NOTE: Kept for legacy callers; strategy-aware routing is implemented in executables.
     */
    static int GetPartitionID(size_t v, int total_partitions) {
        return std::hash<size_t>()(v) % total_partitions;
    }
    
    /**
     * 加载顶点
     */
    void LoadVertex(size_t vertex) {
        local_vertices.insert(vertex);
        adjacency_map[vertex] = std::unordered_map<std::string, std::vector<size_t>>();
    }
    
    /**
     * 加载边
     */
    void LoadEdge(size_t src, size_t dst, const std::string& label) {
        if (adjacency_map[src].find(label) == adjacency_map[src].end()) {
            adjacency_map[src][label] = std::vector<size_t>();
        }
        adjacency_map[src][label].push_back(dst);
        
        // 更新标签索引
        start_points[label].insert(src);
        end_points[label].insert(dst);
    }
    
    /**
     * 从分区目录加载数据
     */
    bool LoadFromDirectory(const std::string& partition_dir) {
        std::string vertex_file = partition_dir + "/vertices.txt";
        std::string edge_file = partition_dir + "/edges.txt";
        
        // 加载顶点
        std::ifstream vf(vertex_file);
        if (!vf.is_open()) {
            std::cerr << "Cannot open vertex file: " << vertex_file << std::endl;
            return false;
        }
        
        std::string line;
        while (std::getline(vf, line)) {
            line.erase(0, line.find_first_not_of(" \t\r\n"));
            line.erase(line.find_last_not_of(" \t\r\n") + 1);
            if (!line.empty()) {
                LoadVertex(std::stoull(line));
            }
        }
        vf.close();
        
        // 加载边
        std::ifstream ef(edge_file);
        if (!ef.is_open()) {
            std::cerr << "Cannot open edge file: " << edge_file << std::endl;
            return false;
        }
        
        while (std::getline(ef, line)) {
            line.erase(0, line.find_first_not_of(" \t\r\n"));
            line.erase(line.find_last_not_of(" \t\r\n") + 1);
            if (!line.empty()) {
                std::stringstream ss(line);
                std::string src_str, label, dst_str;
                ss >> src_str >> label >> dst_str;
                LoadEdge(std::stoull(src_str), std::stoull(dst_str), label);
            }
        }
        ef.close();
        
        std::cout << "Partition " << partition_id << " loaded: " 
                  << local_vertices.size() << " vertices, "
                  << CountEdges() << " edges" << std::endl;
        
        return true;
    }
    
    /**
     * 统计边数
     */
    size_t CountEdges() const {
        size_t count = 0;
        for (const auto& [v, labels] : adjacency_map) {
            for (const auto& [label, targets] : labels) {
                count += targets.size();
            }
        }
        return count;
    }
    
    /**
     * 获取本地起始顶点（根据 firstSet）
     * 只返回真正属于本分区的起始顶点
     */
    std::unordered_set<size_t> get_start_point(
        const std::unordered_set<std::string>& first_set
    ) const {
        std::unordered_set<size_t> result;
        for (const auto& label : first_set) {
            auto it = start_points.find(label);
            if (it != start_points.end()) {
                for (auto v : it->second) {
                    // 双重检查：顶点必须在 start_points 中，且必须属于本分区
                    if (IsLocalVertex(v) && local_vertices.count(v)) {
                        result.insert(v);
                    }
                }
            }
        }
        return result;
    }
};

} // namespace rpq
