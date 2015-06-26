#!/usr/bin/env python
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

# Performs language id on the tweet using the Lui method
# Implemented because it is very quick
# 

# BC, 3/29/13

from optparse import OptionParser
import cPickle as pickle
import re
import tweet_tools as tt
import langid

def add_lid (transactions, debug):
    known_langs = set(['en','es','pt'])
    for key, value in transactions.items():
        msg = value['msg_norm']
        if (debug > 0):
            print u"msg: {}".format(msg)
        if (len(msg.split()) < 5):
            lang = '--'
        else:
            lang_list = langid.rank(msg)
            for lang_pr in lang_list:
                if (lang_pr[0] in known_langs):
                    break
            lang = lang_pr[0]
        value['lid_lui'] = lang
        if (debug > 0):
            print "predicted language lui: {}".format(lang)
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
    transactions = tt.load_tweets(infile)
    print 'Done'

    add_lid(transactions, debug)

    outfile = open(output_file, 'w')
    pickle.dump(transactions, outfile)
    outfile.close()

