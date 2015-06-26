#!/bin/sh
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

#
# Ingest a file of tweets in tsv.gz form and convert to a structured Python serialized dictionary
#

# BC, 6/25/2015

cmd=scripts/run_ingest.py
listfn=lists/list_splits.txt
list_ser_fn=lists/list_splits_serialized.txt
queue=single-thread  # option

lang="--lang en"  # target language for tweet, comment out to get all tweets
# geo_bb="--geo_bounding_box=40.4774,-74.2589,40.9176,-73.7004"  # uses (lat-SW,long-SW,lat-NE,long-NE), comment out to get all tweets

# Create list of Tweet .tsv.gz files
if [ ! -d lists ] ; then
	 mkdir lists
fi
find twitter/user_tweets/ -type f > $listfn

# 
# Ingest into serialied files
#
if [ ! -d twitter/serialized ] ; then
	 mkdir -p twitter/serialized
fi
if [ ! -d tmp ] ; then
	 mkdir tmp
fi
echo Running cmd : ${cmd}
scripts/run_map.py --queue $queue --cmd $cmd --list $listfn --num_jobs 200 --args "$lang $geo_bb"

# 
# List of serialized tweets
#
find twitter/serialized/ -type f > $list_ser_fn
