#!/usr/bin/env python

#
# Ingest tweets from raw format and serialize
# Add meta data
#

# BC 3/27/13

import argparse
import glob
import os
import tweet_tools as tt 
import tweet_simple_metadata as tsm
import tweet_lid as tlid
import tweet_normalize_msg as tnm

parser = argparse.ArgumentParser(description="Run splits of ")
parser.add_argument("--list", type=str, required=True)
parser.add_argument("--lang", type=str, required=False)
parser.add_argument("--geo_bounding_box", type=str, required=False)
args = parser.parse_args()
print 'args is : {}'.format(args)
listfn = args.list

destdir = 'twitter/serialized'
tgt_lang = args.lang
bounding_box = args.geo_bounding_box
tmpdir = 'tmp/'
debug = 0  # set to 1 for more info and a smaller processing set

additional_args = ""
if (tgt_lang!=None):
    additional_args += "--lang {} ".format(tgt_lang)
if (bounding_box!=None):
    additional_args += "--bounding_box={}".format(bounding_box)

if (not os.path.exists(destdir)):
    os.makedirs(destdir)

listfile = open(listfn, 'r')
for fn in listfile:

    # Run initial raw to hash format -- output is serialized to pickle
    # So the output is pickled tweets (!)
    fn = fn.rstrip()
    out_fn = 'tw_' + os.path.basename(os.path.dirname(fn)) + '_' + os.path.basename(fn).split('.')[0] + '.pckl'
    outfile = os.path.join(destdir, out_fn)
    print 'outfile name is : {}'.format(outfile)

    if (os.path.exists(outfile)):
        print "Outfile already exists, skipping ..., delete to regenerate: {}".format(outfile)
        continue
    tmpfile = os.path.join(tmpdir, out_fn)
    cmd = "scripts/tweet_to_dict.py --in {} --out {} --verbose {} {}".format(fn, tmpfile, debug, additional_args)
    print "Running command: {}".format(cmd)
    os.system(cmd)

    # Now add simple metadata -- at tags, hashtags, links
    print "Adding simple metadata ..."
    transactions = tt.load_tweets(tmpfile)
    os.unlink(tmpfile)
    tsm.add_simple_metadata(transactions, debug)

    # Normalize
    print "Performing normalization ..."
    tnm.normalize_msgs(transactions, debug)

    # Language id on text
    # print "Performing language recognition ..."
    # tlid.add_lid(transactions, debug)

    # Save it
    print "Saving to serialized file ... ",
    print "outfile: {}".format(outfile)
    tt.save_tweets(transactions, outfile)
    print "Done"

    if (debug > 0):
        break

listfile.close()
