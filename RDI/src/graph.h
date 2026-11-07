#pragma once
#include <stack>
#include <string>
#include <unordered_set>
#include<unordered_map>
#include<vector>
// #include <boost/graph/adjacency_list.hpp>
// #include <boost/graph/graphviz.hpp>
// #include <boost/graph/properties.hpp>
// #include <boost/property_map/property_map.hpp>
// #include <boost/graph/named_function_params.hpp>
using namespace std;
//Vertex property
typedef struct{
    int id;
    string label;
}VP;

//Edge property
struct EP {
    std::string label;
    int op;
};
// typedef boost::adjacency_list<boost::setS, boost::vecS,
//     boost::bidirectionalS,
//     VP,
//     boost::property<boost::edge_weight_t, string>
// > RDFGraphType;
class Graph {
public:
    //inline static RDFGraphType  G;
    //inline static unordered_set<size_t> vertices;
    inline static unordered_map<size_t, unordered_map<string,vector<size_t>>> adjacency_map;
    inline static std::vector<int> topo_order;
    inline static unordered_map<string, unordered_set<size_t> > startPoints;
    inline static unordered_map<string, unordered_set<size_t> > endPoints;
    inline static size_t max_vertex_id = 0;
    //void parseRDF(string filename, vector<std::tuple<std::string, std::string, std::string>>& triples);
    //static void loadGraph(string sub, string pre, string obj);
    //static void loadGraph(string filename);
    static void loadGraph(string vertex_filename,string edge_filename);
    static void loadVertex(size_t vertex);
    static void loadEdge(size_t src, size_t dst,string pred);
    //static void topoSort();
    //static void scc(RDFGraphType& g);
    //static RDFGraphType map2graph(unordered_map<size_t, unordered_set<size_t>>& map);
};

class TarjanSCC {
public:
    std::unordered_map<size_t, std::unordered_set<size_t>> adjacency_map;
    unordered_map<size_t, size_t> ids;
    unordered_map<size_t, size_t> low;
    unordered_map<size_t, bool> on_stack;
    std::stack<size_t> st;
    std::vector<std::vector<size_t>> sccs;
    unordered_map<size_t, size_t> scc_ids;
    size_t id;
    size_t scc_id;
    void dfs(size_t u) {
        ids[u] = id;
        low[u] = id;
        id++;
        st.push(u);
        on_stack[u] = true;
        for (size_t v : adjacency_map[u]) {
            if (ids.find(v) == ids.end()) {
                dfs(v);
                low[u] = std::min(low[u], low[v]);
            }
            else if (on_stack[v]) {
                low[u] = std::min(low[u], ids[v]);
            }
        }
        if (ids[u] == low[u]) {
            std::vector<size_t> scc;
            size_t v;
            do {
                v = st.top();
                st.pop();
                on_stack[v] = false;
                scc.push_back(v);
                scc_ids[v] = scc_id;
            } while (u != v);
            sccs.push_back(scc);
            scc_id++;
        }
    }
    TarjanSCC(const std::unordered_map<size_t, std::unordered_set<size_t>>& adjacency_map) : adjacency_map(adjacency_map) {
        id = 0;
        scc_id = 0;
        for (const auto& entry : adjacency_map) {
            size_t u = entry.first;
            if (ids.find(u) == ids.end()) {
                dfs(u);
            }
        }
    }
    //std::vector<std::vector<size_t>> getSCCs() {
    //    return sccs;
    //}
};
