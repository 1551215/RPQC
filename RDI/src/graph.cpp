#pragma once
#include"graph.h"
#include <fstream>
#include<sstream>
#include<iostream>
//#include <boost/graph/topological_sort.hpp>
using namespace std;
void Graph::loadVertex(size_t vertex){
    adjacency_map[vertex]=unordered_map<string,vector<size_t>>();
    if (vertex > max_vertex_id) {
        max_vertex_id = vertex;
    }
}

void Graph::loadEdge(size_t src, size_t dst, string pred)
{
    if (src > max_vertex_id) {
        max_vertex_id = src;
    }
    if (dst > max_vertex_id) {
        max_vertex_id = dst;
    }
    if (adjacency_map[src].find(pred) == adjacency_map[src].end())
    {
        adjacency_map[src][pred]=vector<size_t>();
    }
    adjacency_map[src][pred].push_back(dst);
    if (startPoints.find(pred) == startPoints.end())
    {
        startPoints[pred] = {};
    }
    if (endPoints.find(pred) == endPoints.end())
    {
        endPoints[pred] = {};
    }
    startPoints[pred].insert(src);
    endPoints[pred].insert(dst);
}

// void Graph::loadGraph(string sub,string pred,string obj) {
//     static unordered_map<std::string, size_t> vertices;
//     // 用来存储顶点的映射（顶点名称到顶点描述符的映射）
//         // 检查并添加顶点
//         size_t v1, v2;
//         if (vertices.find(sub) == vertices.end()) {
//             v1 = add_vertex(VP{0,sub}, G);
//             vertices[sub] = v1;
//         }
//         else {
//             v1 = vertices[sub];
//         }

//         if (vertices.find(obj) == vertices.end()) {
//             v2 = add_vertex(VP{ 0,sub},G);
//             vertices[obj] = v2;
//         }
//         else {
//             v2 = vertices[obj];
//         }
//         // 添加有向边
//         boost::add_edge(v1, v2, pred, G);
        
//         if (startPoints.find(pred) == startPoints.end())
//         {
//             startPoints[pred] = {};
//         }
//         if (endPoints.find(pred) == endPoints.end())
//         {
//             endPoints[pred] = {};
//         }
//         startPoints[pred].insert(v1);
//         endPoints[pred].insert(v2);

//     // 输出图信息
 
// }

// void Graph::loadGraph(string filename){
//     std::ifstream file(filename); 
//     std::string line;
//     string sub,obj,pred;
//     int i = 0;
//     cout << "load graph start" << endl;
//     if (file.is_open()) {
//         while (getline(file, line)) {
//             i++;
//             if (i % 10000==0) {
//                 cout << i << endl;
//             }
//             std::stringstream ss(line);
//             ss>>sub>>pred>>obj;
//             loadGraph(sub,pred,obj);
//         }
//         file.close();
//     } else {
//         //std::cout << "cannot open file" << std::endl;
//     }
 
//     std::cout << "Graph has " << num_vertices(G) << " vertices and " << num_edges(G) << " edges." << std::endl;
// }

void Graph::loadGraph(string vertex_filename,string edge_filename){
    std::ifstream vertex_file(vertex_filename);
    std::ifstream edge_file(edge_filename);
    if (!vertex_file.is_open()) {
        cerr << "Error: Cannot open vertex file: " << vertex_filename << endl;
        return;
    }
    if (!edge_file.is_open()) {
        cerr << "Error: Cannot open edge file: " << edge_filename << endl;
        return;
    }

    string edge;
    string vertex;
    int line_num = 0;
    while (getline(vertex_file, vertex)) {
        line_num++;
        // 去除首尾空白字符（包括\r\n）
        vertex.erase(0, vertex.find_first_not_of(" \t\r\n"));
        vertex.erase(vertex.find_last_not_of(" \t\r\n") + 1);
        if (!vertex.empty()) {
            try {
                loadVertex(stoi(vertex));
            } catch (const std::exception& e) {
                cerr << "Error parsing vertex file at line " << line_num << ": " << vertex << endl;
                cerr << "Exception: " << e.what() << endl;
                throw;
            }
        }
    }

    line_num = 0;
    while (getline(edge_file, edge)) {
        line_num++;
        // 去除首尾空白字符（包括\r\n）
        edge.erase(0, edge.find_first_not_of(" \t\r\n"));
        edge.erase(edge.find_last_not_of(" \t\r\n") + 1);
        if (!edge.empty()) {
            std::stringstream ss(edge);
            string src,dst,pred;
            ss>>src>>pred>>dst;
            // 去除src和dst末尾可能存在的\r字符
            if (!src.empty()) src.erase(src.find_last_not_of(" \t\r\n") + 1);
            if (!dst.empty()) dst.erase(dst.find_last_not_of(" \t\r\n") + 1);
            try {
                loadEdge(stoi(src),stoi(dst),pred);
            } catch (const std::exception& e) {
                cerr << "Error parsing edge file at line " << line_num << ": " << edge << endl;
                cerr << "src=[" << src << "] dst=[" << dst << "] pred=[" << pred << "]" << endl;
                cerr << "Exception: " << e.what() << endl;
                throw;
            }
        }
    }
    vertex_file.close();
    edge_file.close();
    cout<<"load graph succeed"<<endl;

}

// void Graph::topoSort()
// {
//     boost::topological_sort(G, std::front_inserter(topo_order));
// }

// RDFGraphType Graph::map2graph(unordered_map<size_t, unordered_set<size_t>>& map)
// {
//     RDFGraphType g;
//     for (const auto& pair : map) {
//         size_t u = boost::add_vertex(g); // 添加顶点
//         for (auto v : pair.second) {
//             boost::add_edge(u, v, g); // 添加边
//         }
//     }
//     return g;
// }
