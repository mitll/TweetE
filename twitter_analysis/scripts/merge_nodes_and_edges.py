#!/usr/bin/env python

#
# Merge nodes from multiple files
#

import argparse
import codecs
import gzip
import os
import shutil
import sys

# Original version, WMC: 3/24/2015

def read_nodes(fn):
    zf_raw = gzip.open(fn,'r')
    zf = codecs.getreader('utf-8')(zf_raw)
    nodes = {}
    max_id = -1
    for ln in zf:
        f = ln.rstrip().split()
        id = int(f[0])
        nodes[f[1]] = id
        if (id > max_id):
            max_id = id
    zf.close()
    return (nodes, max_id)

def merge_nodes(fn, nodes, id_start):
    zf_raw = gzip.open(fn, 'r')
    zf = codecs.getreader('utf-8')(zf_raw)
    node_map = {}
    for ln in zf:
        f = ln.rstrip().split()
        idx = int(f[0])
        ky = f[1]
        if (ky in nodes):
            node_map[idx] = nodes[ky]
        else:
            node_map[idx] = id_start
            nodes[ky] = id_start
            id_start += 1
    zf.close()

    return (nodes, node_map, id_start)

def map_edges(fn, output_file, node_map):
    zf_raw = gzip.open(fn, 'r')
    zf = codecs.getreader('utf-8')(zf_raw)
    for ln in zf:
        f = ln.rstrip().split()
        f[0] = int(f[0])
        f[1] = int(f[1])
        f[0] = str(node_map[f[0]])
        f[1] = str(node_map[f[1]])
        output_file.write('{}\n'.format(' '.join(f)))

def merge_edges(fn_edges_tmp, fn_edges):
    # Read in edges and merge
    edges = {}
    zf_raw = gzip.open(fn_edges_tmp, 'r')
    zf = codecs.getreader('utf-8')(zf_raw)
    for ln in zf:
        f = ln.rstrip().split()
        r = [int(x) for x in f]
        ky = (r[0],r[1])
        rest = r[2:]
        if (ky not in edges):
            edges[ky] = rest
        else:
            if len(edges[ky])!=len(rest):
                raise Exception('merge_nodes_and_edges.py: attributes on edge are different dimension')
            for j in xrange(0,len(rest)):
                edges[ky][j] += rest[j]
    zf.close()

    # Write out new edges
    zf_raw = gzip.open(fn_edges, 'w')
    zf = codecs.getwriter('utf-8')(zf_raw)
    for (ky, val) in edges.iteritems():
        zf.write('{} {} {}\n'.format(ky[0], ky[1], ' '.join([str(x) for x in val])))
    zf.close()

# Main driver: command line interface
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Combine multiple graph in a directory.")
    parser.add_argument("--list", type=str, required=True)
    parser.add_argument("--outfile_nodes", type=str, required=True)
    parser.add_argument("--outfile_edges", type=str, required=True)
    parser.add_argument("--skip_edge_merge", action='store_true', default=False)

    args = parser.parse_args()
    listfn = args.list
    skip_edge_merge = args.skip_edge_merge

    output_nodes_fn = args.outfile_nodes
    output_edges_fn = args.outfile_edges
    if (skip_edge_merge):
        output_edges_fn_tmp = output_edges_fn
    else:
        output_edges_fn_tmp = output_edges_fn + '.tmp'
    if os.path.exists(output_nodes_fn) or os.path.exists(output_edges_fn):
        print 'Output files already exist: {} {}'.format(output_nodes_fn, output_edges_fn)
        exit(0)

    # First pass: read in nodes and merge
    # Relabel edges, but don't combine duplicates yet
    listfile = open(listfn, 'r')
    first = True
    output_edges_tmp = gzip.open(output_edges_fn_tmp, 'w')
    for ln in listfile:
        fn = ln.rstrip().split()
        if (first):
            # Load in first set of nodes
            print "Reading in first set of nodes ..."
            (nodes, max_id) = read_nodes(fn[0])
            node_id = max_id + 1
            print "Done"
            sys.stdout.flush()
            print "Reading in first set of edges ..."
            f = gzip.open(fn[1], 'r')
            for ln in f:
                output_edges_tmp.write(ln)
            print "Done"
            sys.stdout.flush()
            first = False
        else:
            # Loop over graphs and combine
            print "Merging: {}".format(fn)
            node_id_start = node_id
            (nodes, node_map, node_id) = merge_nodes(fn[0], nodes, node_id)
            print 'Added {} new nodes'.format(node_id-node_id_start)
            map_edges(fn[1], output_edges_tmp, node_map)
            sys.stdout.flush()

    # Write out nodes and wrap up temporary edges
    output_edges_tmp.close()
    print "Writing out nodes ..."
    sys.stdout.flush()
    zf_raw = gzip.open(output_nodes_fn, 'w')
    zf = codecs.getwriter('utf-8')(zf_raw)
    for (ky, node_id) in nodes.iteritems():
        zf.write(u"{} {}\n".format(node_id, ky))
    zf.close()
    del nodes
    print "Done!"

    # Second pass:
    # Find duplicate edges and combine
    if not skip_edge_merge:
        print "Combining duplicate edges ..."
        sys.stdout.flush()
        merge_edges(output_edges_fn_tmp, output_edges_fn)
        print "Done!"
        # Clean up
        os.unlink(output_edges_fn_tmp)
    else:
        print 'Skipped edge merge step.  Duplicates may exist in the edge file!'


