all : merge_edges node_degree prune_graph

merge_edges : merge_edges.cpp
	g++ -O3 -std=c++0x -o merge_edges merge_edges.cpp -lboost_iostreams -lz -static

node_degree : node_degree.cpp
	g++ -O3 -std=c++0x -o node_degree node_degree.cpp -lboost_iostreams -lz -static

prune_graph : prune_graph.cpp
	g++ -O3 -std=c++0x -o prune_graph prune_graph.cpp -lboost_iostreams -lz -static

