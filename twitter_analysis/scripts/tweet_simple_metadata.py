#!/usr/bin/env python

# Grabs a lot of the simple meta-data out of the tweet and puts them in a hash:
# @tags -> at_tags
# #tags -> hash_tags
# http:// -> http_links

# BC, 3/26/13

from optparse import OptionParser
import cPickle as pickle
import re
import tweet_tools as tt

def add_simple_metadata (transactions, debug):
    for key, value in transactions.items():
        # msg = u' ' + value['msg'] + u' '
        # msg_space = u' ' + tt.punctuation_to_space(value['msg']) + u' '
        # msg_space = tt.punctuation_to_space(value['msg'])
        msg = value['msg']
        if (debug > 0):
            print u"msg: {}".format(msg)
            # print u"msg_space: {}".format(msg_space)

        # http links
        m = re.finditer('(http:\/\/\S+)', msg)
        mval = [(k.start(), k.group()) for k in m]
        if (len(mval) > 0):
            value['http_links'] = mval
            if (debug > 0):
                print u"http links: {}".format(value['http_links'])

        # hashtags
        # m = re.finditer(u'(\#\S+)', msg_space)
        m = re.finditer(u'(\#[a-zA-Z0-9_]+)', msg)
        mval = [(k.start()+1, k.group()[1:]) for k in m] # Don't save the # with every hashtag
        if (len(mval) > 0):
            value['hashtags'] = mval
            if (debug > 0):
                print u"hashtags: {}".format(value['hashtags'])
        
        # find at-mentions
        # m = re.finditer('(\@\S+)', msg_space)
        m = re.finditer('(\@[a-zA-Z0-9_]+)', msg)
        mval = [(k.start()+1, k.group()[1:]) for k in m] # Don't save the @ with every at-mention
        if (len(mval) > 0):
            value['mentions'] = mval
            if (debug > 0):
                print u"mentions: {}".format(value['mentions'])

        # Retweet locations
        m = [loc.start() for loc in re.finditer(u"RT @", msg)]
        if (len(m) > 0):
            value['retweet'] = m
            if (debug > 0):
                print u"Retweets found: {} {}".format(len(m), m)

        # User message
        m = re.search(u"^\s?@", msg)
        if (m != None):
            value['user_msg'] = True
            if (debug > 0):
                print u"User-to-user message"

        if (debug > 0):
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

    add_simple_metadata(transactions, debug)

    outfile = open(output_file, 'w')
    pickle.dump(transactions, outfile)
    outfile.close()

