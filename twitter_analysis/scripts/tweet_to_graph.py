#!/usr/bin/env python
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

#
# Tweet to content + communication graph
#

# Original version, WMC: 7/2013

import argparse
import codecs
import copy
import glob
import gzip
import os
import tweet_tools as tt 
import cPickle as pickle
import networkx as nx
import sys

parser = argparse.ArgumentParser(description="Create a graph from Twitter serialized files")
parser.add_argument("--list", type=str, required=True)
parser.add_argument('--recount-retweets',dest='recount_retweets',action='store_true')
parser.add_argument('--dont-recount-retweets',dest='recount_retweets',action='store_false')
parser.set_defaults(recount_retweets=True)
args = parser.parse_args()
listfn = args.list
recount_retweets = args.recount_retweets

if (recount_retweets):
    print "Recounting retweets"
else:
    print "Not counting retweets"

tmpdir = 'tmp/'
debug = 0  # set to 1 for more info and a smaller processing set

outfile_pckl = os.path.join(tmpdir, os.path.basename(listfn) + ".gpckl")
if (os.path.exists(outfile_pckl)):
    print 'Graph {} already exists, exiting ...'.format(outfile_pckl)
    exit(0)

listfile = open(listfn, 'r')
G = nx.DiGraph()
for fn in listfile:

    fn = fn.rstrip()

    print "Loading file: {}".format(fn)
    sys.stdout.flush()

    # Load in tweets
    fn = fn.rstrip()
    xact = tt.load_tweets(fn)

    # Add to graph
    for ky in xact.keys():
        val = xact[ky]
        
        # user node
        user = '@' + val['userid'].lower()
        if (not G.has_node(user)):
            G.add_node(user)
            G.node[user]['type'] = 'user'

        # Mention list
        if (val.has_key('mentions')):
            mention_list = copy.deepcopy(val['mentions'])
        else:
            mention_list = []

        # Retweets
        if (val.has_key('retweet') and val.has_key('mentions')): 
            user1 = user
            if (not G.has_node(user1)):
                G.add_node(user1)
                G.node[user1]['type'] = 'user'
            for index in val['retweet']:
                found = False
                for j in xrange(0, len(mention_list)):
                    if (mention_list[j][0]==(index+4)):
                        found = True
                        break
                if (found):
                    user2 = '@' + mention_list[j][1].lower()
                    if (not G.has_node(user2)):
                        G.add_node(user2)
                        G.node[user2]['type'] = 'user'
                    if (not G.has_edge(user1, user2)):
                        G.add_edge(user1, user2)
                        G[user1][user2]['count_comm'] = 0
                        G[user1][user2]['count_coc'] = 0
                        G[user1][user2]['count_rt'] = 0
                    G[user1][user2]['count_rt'] += 1
                    del(mention_list[j]) # remove from mention list
                    user1 = user2
                else:  
                    # no destination for retweet
                    # sometimes things that look like retweets aren't
                    # e.g. "CART @ the following"
                    break
                if (not recount_retweets):
                    if (found and len(mention_list)>j):
                        del(mention_list[j:])
                    break

        # person -> person(s)
        if (val.has_key('user_msg') and (len(mention_list) > 0)):
            next_expected_mention = 1  # offset by 1 
            while (mention_list[0][0]==next_expected_mention):
                # assume first mention is recipient
                user2 = '@' + mention_list[0][1].lower()
                if (user==user2): # skip self-loops
                    next_expected_mention += len(mention_list[0][1])+2
                    mention_list = mention_list[1:] 
                    if (len(mention_list)==0):
                        break
                    else:
                        continue
                if (not G.has_node(user2)):
                    G.add_node(user2)
                    G.node[user2]['type'] = 'user'
                if (not G.has_edge(user, user2)):
                    G.add_edge(user, user2)
                    G[user][user2]['count_comm'] = 0
                    G[user][user2]['count_coc'] = 0
                    G[user][user2]['count_rt'] = 0
                G[user][user2]['count_comm'] += 1
                next_expected_mention += len(mention_list[0][1])+2
                mention_list = mention_list[1:] 
                if (len(mention_list)==0):
                    break

        # Remaining mentions, co-occurrence
        if (len(mention_list) > 0):
            for mpair in mention_list:
                mention = '@' + mpair[1].lower()
                if (not G.has_node(mention)):
                    G.add_node(mention)
                    G.node[mention]['type'] = 'user'

            # Mention co-occurrence
            for mp1 in mention_list:
                for mp2 in mention_list:
                    mp1_mention = '@' + mp1[1].lower()
                    mp2_mention = '@' + mp2[1].lower()
                    if (mp1_mention==mp2_mention):  # skip self-loops
                        continue
                    if (not G.has_edge(mp1_mention, mp2_mention)):
                        G.add_edge(mp1_mention, mp2_mention)
                        G[mp1_mention][mp2_mention]['count_coc'] = 0
                        G[mp1_mention][mp2_mention]['count_comm'] = 0
                        G[mp1_mention][mp2_mention]['count_rt'] = 0
                    if (not G.has_edge(mp2_mention, mp1_mention)):
                        G.add_edge(mp2_mention, mp1_mention)
                        G[mp2_mention][mp1_mention]['count_coc'] = 0
                        G[mp2_mention][mp1_mention]['count_comm'] = 0
                        G[mp2_mention][mp1_mention]['count_rt'] = 0
                    G[mp1_mention][mp2_mention]['count_coc'] += 1
                    G[mp2_mention][mp1_mention]['count_coc'] += 1

        # Hashtag co-occurence
        if (val.has_key('hashtags') and (len(val['hashtags'])>1)):
            ht_list = val['hashtags']
            for ht in ht_list:
                ht_val = u"#" + ht[1].lower()
                if (not G.has_node(ht_val)):
                    G.add_node(ht_val)
                    G.node[ht_val]['type'] = 'ht'
            for ht1 in ht_list:
                for ht2 in ht_list:
                    ht1_val = u"#" + ht1[1].lower()
                    ht2_val = u"#" + ht2[1].lower()
                    if (ht1_val==ht2_val):
                        continue
                    if (not G.has_edge(ht1_val, ht2_val)):
                        G.add_edge(ht1_val, ht2_val)
                        G[ht1_val][ht2_val]['count_coc'] = 0
                    if (not G.has_edge(ht2_val, ht1_val)):
                        G.add_edge(ht2_val, ht1_val)
                        G[ht2_val][ht1_val]['count_coc'] = 0
                    G[ht1_val][ht2_val]['count_coc'] += 1
                    G[ht2_val][ht1_val]['count_coc'] += 1

        # User -> Hashtag
        if (val.has_key('hashtags')):
            ht_list = copy.deepcopy(val['hashtags'])
            # Main tweeter connects to all hashtags
            for ht in ht_list:
                ht_val = u"#" + ht[1].lower()
                if (not G.has_node(ht_val)):
                    G.add_node(ht_val)
                    G.node[ht_val]['type'] = 'ht'
                if (not G.has_edge(user, ht_val)):
                    G.add_edge(user, ht_val)
                    G[user][ht_val]['count_ht'] = 0
                G[user][ht_val]['count_ht'] += 1
            if (recount_retweets and val.has_key('retweet') and val.has_key('mentions')):
                for index in val['retweet']:
                    found = False
                    mention_list = val['mentions']
                    for j in xrange(0, len(mention_list)):
                        if (mention_list[j][0]==(index+4)):
                            found = True
                            break
                    if (found):
                        user1 = '@' + mention_list[j][1].lower()
                        for ht_pr in ht_list:
                            if (ht_pr[0] > index):
                                # this user tweeted or retweeted this hashtag
                                ht_val = u"#" + ht_pr[1].lower()
                                if (not G.has_node(user1)):
                                    G.add_node(user1)
                                    G.node[user1]['type'] = 'user'
                                if (not G.has_node(ht_val)):
                                    G.add_node(ht_val)
                                    G.node[ht_val]['type'] = 'ht'
                                if (not G.has_edge(user1, ht_val)):
                                    G.add_edge(user1, ht_val)
                                    G[user1][ht_val]['count_ht'] = 0
                                G[user1][ht_val]['count_ht'] += 1
    if (debug > 0):
        break
listfile.close()

# Save to outfile
print "Saving graph to serialized outfile: {}".format(outfile_pckl)
nx.write_gpickle(G, outfile_pckl)
print "Done"

# Save node list
outfile_nodes = os.path.join(tmpdir, os.path.basename(listfn) + ".nodes.txt.gz")
zf_raw = gzip.open(outfile_nodes, 'w')
zf = codecs.getwriter('utf-8')(zf_raw)
node_id = 0
for n in G.nodes_iter():
    G.node[n]['id'] = node_id
    zf.write(u'{} {}\n'.format(node_id, n))
    node_id += 1
zf.close()

# Save edge list
outfile_edges = os.path.join(tmpdir, os.path.basename(listfn) + ".edges.txt.gz")
zf_raw = gzip.open(outfile_edges, 'w')
zf = codecs.getwriter('utf-8')(zf_raw)
for (n1,n2) in G.edges_iter():
    n1_id = G.node[n1]['id']
    n2_id = G.node[n2]['id']
    type1 = G.node[n1]['type']
    type2 = G.node[n2]['type']
    if (type1=='user' and type2=='user'):
        zf.write(u'{} {} {} {} {}\n'.format(n1_id, n2_id, G[n1][n2]['count_coc'], G[n1][n2]['count_comm'], G[n1][n2]['count_rt']))
    elif (type1=='user' and type2=='ht'):
        zf.write(u'{} {} {}\n'.format(n1_id, n2_id, G[n1][n2]['count_ht']))
    elif (type1=='ht' and type2=='ht'):
        zf.write(u'{} {} {}\n'.format(n1_id, n2_id, G[n1][n2]['count_coc']))
    else:
        raise Exception('tweet_to_graph: unexpected edge type combination')
zf.close()

