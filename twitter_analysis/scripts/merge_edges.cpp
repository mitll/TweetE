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
// Merge edges in a file using C++ Map
//
// Format of edge is "src dest attr1 attr2 attr3"
// 
// Specialized to integer attributes.  Expected number of attributes is 1 or 3.
//

// Written by BC, 3/27/2015

using namespace std;
using namespace boost::iostreams;

class attributes {
   int val[3];
public:
	attributes() {
		val[0] = 0;
		val[1] = 0;
		val[2] = 0;
	}

   attributes(int in_val[]) {
      val[0] = in_val[0];
      val[1] = in_val[1];
      val[2] = in_val[2];
   }

	attributes (const attributes &x) {
		int i;
		for (i=0; i<3; i++)
			val[i] = x.val[i];
	}

	void increment (int in_val[]) {
		int i;
		for (i=0; i<3; i++) {
			if (in_val[i]==-1)
				break;
			val[i] += in_val[i];
		}
	}

	string to_string (){
		stringstream ss;
		int i;
		for (i=0; i<3; i++) {
			if (val[i]==-1)
				break;
			ss << val[i] << " ";
		}
		return ss.str();
	}

	attributes& operator= (const attributes &src) {
		val[0] = src.val[0];
		val[1] = src.val[1];
		val[2] = src.val[2];
		return *this;
	}
};

typedef map<tuple<int,int>,attributes> keymap;

int main (int argc, char *argv[])
{
   if (argc < 3) {
      printf("Usage: %s <src file> <dest file>\n", argv[0]);
      exit(0);
   }

	ifstream infile_raw(argv[1], ios_base::in | ios_base::binary);
	filtering_istream in;
	in.push(gzip_decompressor());
	in.push(infile_raw);

	string line;
   int num_read;   int idx1, idx2, val1, val2, val3;
   int edge_no = 0;

	keymap edge_map;
	int val[3];

	while (getline(in, line)) {
		val[0] = val[1] = val[2] = -1;
      num_read = sscanf(line.c_str(), "%d %d %d %d %d", &idx1, &idx2, &val[0], &val[1], &val[2]);
		tuple<int,int> tp(idx1,idx2);
		if (num_read==-1) { // blank line
			continue;
		}
      if (num_read!=3 && num_read!=5) {
			printf("Problem ?: %s\n", line.c_str());
			exit(1);
      }
		if (edge_map.count(tp)==0) {
			edge_map[tp] = attributes(val);
		} else {
			edge_map[tp].increment(val);
		}

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
	keymap::iterator it;
	edge_no = 0;
	for (it=edge_map.begin(); it!=edge_map.end(); it++) {
		out << get<0>(it->first) << " " << get<1>(it->first) << " " << it->second.to_string() << endl;
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
