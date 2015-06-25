#!/bin/sh
#
# Create graphs from Twitter data.  run_ingest.sh must be run first.
#

# BC, 6/25/2015

min_weights="1 5 10 50 100 500 5000" # Prune nodes with total weight (sum of in and out weights) less than or equal to this value
listfn=lists/list_graph.txt
queue=single-thread
do_cleanup=1
TMP=tmp

# Stage 1 -- serialized tweets to graphs
run_p1=1
num_jobs_p1=100
flag_p1=

# Stage 2 -- merge graphs to intermediate size -- Python
run_p2=1
num_jobs_p2=10
flag_p2=

# Stage 3 -- merge graphs using C++ -- handles big graphs
run_p3=1
flag_p3=

# Stage 4 -- merge edges
run_p4=1

#
# Main code
#
if [ ! -d lists ] ; then
	 mkdir lists
fi
find twitter/serialized/ -type f -name "*.pckl" | sort > $listfn
if [ ! -d graph ] ; then
	 mkdir graph
fi

if [ ! -f graph/twitter_all.nodes.txt.gz ] ; then
	 echo Building graph ...
    
	 if [ ! -d graph/ ] ; then
	     mkdir graph
	 fi

	 if [ $run_p1 = 1 ] ; then
	     # Run this on llgrid
	     echo Running Tweet to graph ...
	     \rm -f $TMP/list_*.txt.gpckl
	     cmd=scripts/tweet_to_graph.py
	     scripts/run_map.py --queue $queue --cmd $cmd --list $listfn --num_jobs $num_jobs_p1 $flag_p1
	 fi

	 if [ $run_p2 = 1 ] ; then
	     echo Running graph combiner, stage 1 ...
	     find $TMP/ -name "list_*.nodes.txt.gz" -print > $TMP/g1.txt.nodes
	     sed 's/nodes/edges/' $TMP/g1.txt.nodes > $TMP/g1.txt.edges
	     paste -d" " $TMP/g1.txt.nodes $TMP/g1.txt.edges > $TMP/g1.txt
	     scripts/run_map.py --queue $queue --cmd scripts/merge_nodes_and_edges.py --list $TMP/g1.txt \
				--args "--outfile_nodes ${TMP}/g1_{0}.nodes.txt.gz --outfile_edges ${TMP}/g1_{0}.edges.txt.gz" --num_jobs $num_jobs_p2 $flag_p2
		  if [ $do_cleanup = 1 ] ; then
				\rm -f $TMP/g1.txt*
  				\rm -f $TMP/list_*.txt.gpckl
  				\rm -f $TMP/list_*.*.txt.gz
		  fi
	 fi

	 if [ $run_p3 = 1 ] ; then
	     echo Running graph combiner, stage 2 ...
	     find $TMP/ -name "g1_*.nodes.txt.gz" -print > $TMP/g2.txt.nodes
		  sed 's/nodes/edges/' $TMP/g2.txt.nodes > $TMP/g2.txt.edges
		  paste -d" " $TMP/g2.txt.nodes $TMP/g2.txt.edges > $TMP/g2.txt
	     scripts/run_map.py --queue $queue --cmd scripts/merge_nodes_and_edges.py --list $TMP/g2.txt \
				--args "--outfile_nodes $TMP/twitter_all.nodes.txt.gz --outfile_edges $TMP/twitter_all.edges.txt.gz.merge --skip_edge_merge" --num_jobs 1 $flag_p3
		  if [ $do_cleanup = 1 ] ; then
				\rm -f $TMP/g2.txt*
				\rm -f $TMP/g1_*.*.txt.gz
		  fi
	 fi
	
	 if [ $run_p4 = 1 ] ; then
		  if [ -f $TMP/twitter_all.edges.txt.gz ] ; then
				echo "$TMP/twitter_all.edges.txt.gz already exists, delete and re-run to regenerate ..."
				exit 1
		  fi
		  scripts/merge_edges $TMP/twitter_all.edges.txt.gz.merge $TMP/twitter_all.edges.txt.gz 
		  cp $TMP/twitter_all.edges.txt.gz graph/
		  cp $TMP/twitter_all.nodes.txt.gz graph/
		  if [ $do_cleanup = 1 ] ; then
				\rm -f $TMP/twitter_all.*
		  fi
	 fi
fi

if [ ! -f graph/node_degree.txt.gz ] ; then
	 echo Finding node degree info ..
	 scripts/node_degree graph/twitter_all.edges.txt.gz graph/node_degree.txt.gz
fi

for min_weight in $min_weights ; do
    if [ ! -f graph/twitter_prune_w${min_weight}.nodes.txt.gz ] ; then
		  echo Pruning graph ...
        scripts/prune_graph graph/twitter_all.nodes.txt.gz graph/twitter_all.edges.txt.gz graph/node_degree.txt.gz \
                            graph/twitter_prune_w${min_weight}.nodes.txt.gz graph/twitter_prune_w${min_weight}.edges.txt.gz ${min_weight}
    fi
done

