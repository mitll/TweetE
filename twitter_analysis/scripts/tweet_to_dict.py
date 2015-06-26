#!/usr/bin/env python
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

#
# Convert from raw tab delimited to a hash
#

# BC: initial version, 3/26/13; BC: updates for gzip & language filtering, 12/10/14

import argparse
import gzip
import os
import cPickle as pickle
import codecs
import tweet_tools as tt

def in_bounding_box (geo, bounding_box):
    in_box = False
    if (geo[0]>=bounding_box[0] and geo[0]<=bounding_box[2] and geo[1]>=bounding_box[1] and geo[1]<=bounding_box[3]):
        in_box = True
    return in_box

def get_fields (ln):
    output = {}
    f = ln.split('\t')
    num_fields = len(f)

    if (num_fields < 6):
        # No tweet, not interesting
        output = None 
        return output
    output['id'] = f[0]
    output['date'] = f[1]
    output['userid'] = f[4]
    output['msg'] = f[5]

    # Additional non-standard fields
    output['geo'] = tuple([float(x) for x in f[3].strip('()').split(',')])
    output['lid_gnip'] = f[2]

    return output

# Main driver: command line interface
if __name__ == '__main__':

    # Parse input command line options
    parser = argparse.ArgumentParser(description="Initial conversion of TSV file to pickled dictionary file.")
    parser.add_argument("--input_file", help="input file of tweets", type=str, required=True)
    parser.add_argument("--output_file", help="output pickled file",  type=str, required=True)
    parser.add_argument("--lang", help="ISO 639-1 code for target language",  type=str, required=False)
    parser.add_argument("--verbose", help="verbosity > 0 -> debug mode", type=int, default=0)
    parser.add_argument("--bounding_box", help="southwest_lat,southwest_long,northeast_lat,northeast_long no spaces", type=str, required=False)

    args = parser.parse_args()
    input_file = args.input_file
    output_file = args.output_file
    tgt_lang = args.lang
    debug = args.verbose
    bounding_box = None
    if (args.bounding_box):
        bounding_box = tuple([float(x) for x in args.bounding_box.split(",")])

    print 'Reading in file: {}'.format(input_file)
    transactions = {}
    if (input_file.split('.')[-1]=='gz'):
        infile_raw = gzip.open(input_file, 'r')
    else:
        infile_raw = open(input_file, 'r')
    rdr = codecs.getreader('utf-8')
    infile = rdr(infile_raw)
    count = 0
    success = 0
    for ln in infile:
        if ((count % 100000)==0):
            print "\ton line: {}".format(count)
        count += 1
        ln = ln.rstrip()
        xact = get_fields(ln)
        if (xact == None):
            continue
        if ('lid_gnip' in xact) and (tgt_lang!=None) and (xact['lid_gnip']==tgt_lang):
            if (bounding_box!=None) and ('geo' in xact) and (in_bounding_box(xact['geo'], bounding_box)):
                success += 1
                transactions[xact['id']] = xact
            elif (bounding_box is None):
                success += 1
                transactions[xact['id']] = xact
        elif (tgt_lang==None):
            if (bounding_box) and ('geo' in xact) and (in_bounding_box(xact['geo'], bounding_box)):
                success += 1
                transactions[xact['id']] = xact
            elif (bounding_box is None):
                success += 1
                transactions[xact['id']] = xact
        if (debug>0 and success==100):
            break
    infile.close()

    tt.save_tweets(transactions, output_file)

    print "Percentage of tweets kept: {} %".format(100.0*((0.0 + success)/count))
