#include <map>
#include <tuple>
#include <string>
#include <fstream>
#include <iostream>
#include <sstream>
#include<boost/iostreams/filter/gzip.hpp>
#include<boost/iostreams/filtering_streambuf.hpp>
#include<boost/iostreams/filtering_stream.hpp>

//
// Node degree in a file using C++ Map
//
// Format of edge is "src dest attr1 attr2 attr3"
// 
// Specialized to integer attributes.  Expected number of attributes is 1 or 3.
//

// Written by BC, 3/30/2015

using namespace std;
using namespace boost::iostreams;

typedef map<int,int> keymap;

int main (int argc, char *argv[])
{
   if (argc < 3) {
      printf("Usage: %s <edge file> <degree file>\n", argv[0]);
		printf("Input is gzip compressed as is output.\n");
      exit(0);
   }

	ifstream infile_raw(argv[1], ios_base::in | ios_base::binary);
	filtering_istream in;
	in.push(gzip_decompressor());
	in.push(infile_raw);

	string line;
   int num_read;   
	int idx1, idx2;
   int edge_no = 0;
	keymap node_out_degree, node_in_degree;
	int val[3], sum;

	while (getline(in, line)) {
      num_read = sscanf(line.c_str(), "%d %d %d %d %d", &idx1, &idx2, &val[0], &val[1], &val[2]);
		if (num_read==-1) { // blank line
			continue;
		}
      if (num_read!=3 && num_read!=5) {
			printf("Problem ?: %s\n", line.c_str());
			exit(1);
      }
		if (node_out_degree.count(idx1)==0)
			node_out_degree[idx1] = 0;
		if (node_out_degree.count(idx2)==0)
			node_out_degree[idx2] = 0;
		if (node_in_degree.count(idx1)==0)
			node_in_degree[idx1] = 0;
		if (node_in_degree.count(idx2)==0)
			node_in_degree[idx2] = 0;
		if (num_read==3)
			sum = val[0];
		else
			sum = val[0]+val[1]+val[2];
		node_out_degree[idx1] += sum;
		node_in_degree[idx2] += sum;
      edge_no++;
      if ((edge_no%1000000)==0) {
			printf("%dM ", edge_no/1000000);
			fflush(stdout);
      }
	}
	infile_raw.close();
	printf("\n\n");

	ofstream outfile_raw(argv[2], ios_base::out | ios_base::binary);
	filtering_ostream out;
	out.push(gzip_compressor());
	out.push(outfile_raw);
	keymap::iterator it_in = node_in_degree.begin();
	keymap::iterator it_out = node_out_degree.begin();
	edge_no = 0;
	for (;(it_in!=node_in_degree.end()) && (it_out!=node_out_degree.end()); it_in++, it_out++) {
		if (edge_no < it_in->first) {
			while (edge_no < it_in->first) {
				out << edge_no << " 0 0" << endl;
				edge_no++;
			}
		} else if (edge_no > it_in->first) {
			printf("node_degree: this shouldn't happen!, %d %d\n", it_in->first, it_out->first);
			exit(1);
		}
		if (it_in->first != it_out->first) {
			printf("node_degree: this shouldn't happen!, %d %d\n", it_in->first, it_out->first);
			exit(1);
		}
		out << it_in->first << " " << it_in->second << " " << it_out->second << endl;
      edge_no++;
      if ((edge_no%1000000)==0) {
			printf("%dM ", edge_no/1000000);
			fflush(stdout);
      }
	}
	out.flush();  // flush doesn't work for gzip compressor filter, need to reset 
	out.reset();  // may be a bug in my version of boost, 1.48
	outfile_raw.close();

	return 0;

}
