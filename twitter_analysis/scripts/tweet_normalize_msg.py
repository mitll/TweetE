#!/usr/bin/env python
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

# 
# Normalize the text content of a Tweet
# 

# BC, 3/30/13

from optparse import OptionParser
import pickle
import re
import tweet_tools as tt
from get_counts import create_utf8_rewrite_hash
from get_counts import normalize
from get_counts import split

def normalize_msgs (transactions, debug):
    h = create_utf8_rewrite_hash()
    for key, value in transactions.items():
        msg = value['msg']
        msgs = split(msg)
        if (debug > 0):
            print u"msg: {}".format(msg)
            # print u"msgs: {}".format(msgs)
        msgs_norm = []
        for sent in msgs:
            msg_norm = normalize(sent, h)
            if (msg_norm == ''):
                continue
            msgs_norm.append(msg_norm)
        value['msg_norm'] = u' '.join(msgs_norm)
        if (debug > 0):
            print u"normalized msg: {}".format(value['msg_norm'])
            print

# Main driver: command line interface
if __name__ == '__main__':

    # Parse input command line options
    parser = OptionParser()
    parser.add_option("--input_file", help="input pickled file of tweets", metavar="FILE")
    parser.add_option("--output_file", help="output pickled file of tweets", metavar="FILE")
    parser.add_option("--verbose", help="verbosity > 0 -> debug mode", metavar="FILE", default=0)
    (Options, args) = parser.parse_args()
    input_file = Options.input_file
    output_file = Options.output_file
    debug = int(Options.verbose)
    if (input_file==None or output_file==None):
        print "Need to specify input and output files -- run with --help for syntax"
        exit(1)

    print 'Reading in file: {}'.format(input_file)
    transactions = tt.load_tweets(input_file)
    print 'Done'

    normalize_msgs(transactions, debug)

    outfile = open(output_file, 'w')
    pickle.dump(transactions, outfile)
    outfile.close()

