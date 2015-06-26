#!/usr/bin/env python
#
# Copyright (c) 2015
# Massachusetts Institute of Technology
#
# All Rights Reserved
#

# 
# Display the pickled tweet file
# 

# BC, 3/27/13

from optparse import OptionParser
import tweet_tools as tt
import sys
import codecs

# Main driver: command line interface
if __name__ == '__main__':

    # Parse input command line options
    parser = OptionParser()
    parser.add_option("--input_file", help="input pickled file of tweets", metavar="FILE")
    parser.add_option("--output_file", help="optional text output file", metavar="FILE")
    parser.add_option("--key", help="key of the form key=value, e.g. lid_lui=fr", metavar="FILE")
    (Options, args) = parser.parse_args()
    input_file = Options.input_file
    output_file = Options.output_file
    key_search = Options.key

    if (input_file==None):
        print "Need to specify input file -- run with --help for syntax"
        exit(1)

    if (key_search!=None):
        (ky1, ky2) = key_search.split("=")

    print 'Reading in file: {}'.format(input_file)
    transactions = tt.load_tweets(input_file)
    print 'Done'

    if (output_file==None):
        outfile = codecs.getwriter('utf-8')(sys.stdout)
    else:
        outfile = codecs.open(output_file, 'w', encoding='utf-8')

    for key in transactions.keys():
        value = transactions[key]
        if (key_search!=None):
            if (value.has_key(ky1)):
                if (value[ky1] != ky2):
                    continue
            else:
                continue

        for ky in sorted(value.keys()):
            if (type(value[ky])==list):
                outfile.write(u"{} [".format(ky))
                for val in value[ky]:
                   if (type(val)==tuple):
                       outfile.write(u"({},{}) ".format(val[0],val[1]))
                   else:
                       outfile.write(u"{} ".format(val))
                outfile.write(u"\b]\n")
            else:
                outfile.write(u"{} {}\n".format(ky, value[ky]))
        outfile.write(u"\n")

    if (outfile!=None):
        outfile.close()
