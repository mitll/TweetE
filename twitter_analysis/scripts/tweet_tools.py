#!/usr/bin/env python

#
# Various tweet tools
#

# BC, 3/26/13

import cPickle as pickle
import os
# import pickle
import re
import sys

def remove_punctuation(instr):
    out = re.sub(u'[-,?!,":;.()', u"", instr)
    return out

def punctuation_to_space (instr):
    out = re.sub(u'[-,?!,":;.()]', " ", instr)
    return out

def load_tweets (input_file):
    transactions = {}
    try:
        infile = open(input_file, 'r')
        transactions = pickle.load(infile)
        infile.close()
    except IOError as e:
        print "I/O Error -- {} : {}".format(e.errno, e.strerror)
        transactions = {}
    except:
        print "Unexpected error: {}".format(sys.exc_info()[0])
        transactions = {}
    return transactions

def save_tweets (transactions, output_file):
    outfile = open(output_file, 'w')
    pickle.dump(transactions, outfile)
    outfile.close()
