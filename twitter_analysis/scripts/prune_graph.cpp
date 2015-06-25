#include <map>
#include <set>
#include <tuple>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include<boost/iostreams/filter/gzip.hpp>
#include<boost/iostreams/filtering_streambuf.hpp>
#include<boost/iostreams/filtering_stream.hpp>

//
// Prune the graph by edge weight
//
// Format of edge is "src dest attr1 attr2 attr3"
// 

// Written by BC, 3/31/2015

using namespace std;
using namespace boost::iostreams;

typedef map<int,int> keymap;

int main (int argc, char *argv[])
{
   if (argc < 5) {
      printf("Usage: %s <node file> <edge file> <node_degree_file> <output node file> <output edge file> <weight threshold>\n", argv[0]);
		printf("Input is gzip compressed as is output.\n");
		printf("Keep node if sum of in and out weights is greater than a threshold.\n");
      exit(0);
   }

	int weight_threshold = atoi(argv[6]);

	// Read in node degree file
	printf("Reading in node degree file: ");
	string line;
	ifstream infile_node_degree_raw(argv[3], ios_base::in | ios_base::binary);
	filtering_istream infile_node_degree;
	infile_node_degree.push(gzip_decompressor());
	infile_node_degree.push(infile_node_degree_raw);
	int inst_num, node_num, num_read, weight_in, weight_out, total_weight;
	set<int> keep_nodes;
	inst_num = 0;
	while (getline(infile_node_degree, line)) {
      num_read = sscanf(line.c_str(), "%d %d %d", &node_num, &weight_in, &weight_out);
		if (num_read!=3) {
			printf("Corrupted node degree file?: %s\n", line.c_str());
			exit(1);
		}
		total_weight = weight_in + weight_out;
		if (total_weight > weight_threshold) {
			keep_nodes.insert(node_num);
		}
	   if ((inst_num%1000000)==0) {
			printf("%dM ", inst_num/1000000);
			fflush(stdout);
		}
		inst_num++;
	}
	infile_node_degree_raw.close();
	printf("\n");

	// Prune nodes
	printf("Pruning nodes: ");
	ifstream infile_nodes_raw(argv[1], ios_base::in | ios_base::binary);
	filtering_istream infile_nodes;
	infile_nodes.push(gzip_decompressor());
	infile_nodes.push(infile_nodes_raw);
	ofstream outfile_nodes_raw(argv[4], ios_base::out | ios_base::binary);
	filtering_ostream outfile_nodes;
	outfile_nodes.push(gzip_compressor());
	outfile_nodes.push(outfile_nodes_raw);
	inst_num = 0;
	while (getline(infile_nodes, line)) {
      num_read = sscanf(line.c_str(), "%d ", &node_num);
		if (keep_nodes.count(node_num))
			outfile_nodes << line << endl;
	   if ((inst_num%1000000)==0) {
			printf("%dM ", inst_num/1000000);
			fflush(stdout);
		}
		inst_num++;
	}
	outfile_nodes.flush();
	outfile_nodes.reset();
	outfile_nodes_raw.close();
	infile_nodes_raw.close();
	printf("\nKept nodes : %d / %d\n\n", (int) keep_nodes.size(), inst_num);

	// Open input and output edge files
	printf("Pruning edges: ");
	ifstream infile_edges_raw(argv[2], ios_base::in | ios_base::binary);
	filtering_istream infile_edges;
	infile_edges.push(gzip_decompressor());
	infile_edges.push(infile_edges_raw);
	ofstream outfile_edges_raw(argv[5], ios_base::out | ios_base::binary);
	filtering_ostream outfile_edges;
	outfile_edges.push(gzip_compressor());
	outfile_edges.push(outfile_edges_raw);
	int edge_no = 0;
	int kept_edges = 0;
	int idx1, idx2, val[3];

	// Read in and prune edges according to threshold
	while (getline(infile_edges, line)) {
		num_read = sscanf(line.c_str(), "%d %d %d %d %d", &idx1, &idx2, &val[0], &val[1], &val[2]);
	 	if (num_read==-1) { // blank line
	 		continue;
	 	}
      if (num_read!=3 && num_read!=5) {
			printf("Corrupted edge file?: %s\n", line.c_str());
	 		exit(1);
      }
		if (keep_nodes.count(idx1) && keep_nodes.count(idx2)) {
			outfile_edges << line << endl;
			kept_edges += 1;
		}
      if ((edge_no%1000000)==0) {
	 		printf("%dM ", edge_no/1000000);
	 		fflush(stdout);
      }
		edge_no++;
	}
	infile_edges_raw.close();
	outfile_edges.flush();
	outfile_edges.reset();
	printf("Kept edges: %d / %d\n", kept_edges, edge_no);
	printf("\n");

	return 0;

}
