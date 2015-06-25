#!/usr/bin/env python

# Run a command per listfile using parallelization.  
# Essentially a 'map' (without the reduce).
#
# BC, 3/30/13

import glob
import os
import re
import argparse

def run_single_thread (jobfn):
    job_file = open(jobfn, 'r')
    job_success = True
    for ln in job_file:
        ln = ln.rstrip()
        if (ln=="EOJ"):
            job_success = True
            continue
        if (not job_success):
            continue
        try:
            status = os.system("{}".format(ln))
            if (status!=0):
                job_success = False
        except Exception as e:
            job_success = False
    job_file.close()

# Main driver: command line interface
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Run a command across splits of a list file")
    parser.add_argument("--list", type=str, required=True)
    parser.add_argument("--cmd", type=str, required=True)
    parser.add_argument("--args", type=str, default="")
    parser.add_argument("--num_jobs", type=int, default=100)
    parser.add_argument("--mem_req", type=str, required=False)
    parser.add_argument("--queue", type=str, required=False, default='normal')
    parser.add_argument("--slots_req", type=str, required=False)
    parser.add_argument("--debug", action='store_true', default=False)

    args = parser.parse_args()
    debug = args.debug
    listfn = args.list
    cmd = args.cmd
    cmd_args = args.args
    num_jobs =args.num_jobs
    mem_req = args.mem_req
    slots_req = args.slots_req
    queue_name = args.queue

    jobfn='tmp/cmds.lst'
    max_num_jobs_on_queue=100  # how many jobs on queue max

    # Number of lines in file
    listfile = open(listfn, 'r')
    filelist = []
    num_lines = 0
    for ln in listfile:
        ln = ln.rstrip()
        filelist.append(ln)
        num_lines += 1
    listfile.close()
    if (num_lines < num_jobs):
        num_jobs = num_lines

    # Split into multiple files
    for file in glob.glob('tmp/list_*.txt'):
        os.unlink(file)
    print "number of lines: {}".format(num_lines)
    print "number of jobs: {}".format(num_jobs)
    num_per_job = num_lines/num_jobs
    num_rem = num_lines % num_jobs
    print "number per job: {}".format(num_per_job)
    i = 0
    file_num = 0
    while True:
        outfile = open('tmp/list_{}.txt'.format(file_num), 'w')
        for j in xrange(0,num_per_job):
            if (i>=num_lines):
                break
            outfile.write('{}\n'.format(filelist[i]))
            i += 1
        if (i>=num_lines):
            break
        if (num_rem > 0):
            outfile.write('{}\n'.format(filelist[i]))
            num_rem -= 1
            i += 1
        outfile.close()
        file_num += 1
        if (file_num>=num_jobs):
            break

    # Write out job file
    outfile = open(jobfn, 'w')
    for i in xrange(0,num_jobs):
        argstr = cmd_args.format(i)
        outfile.write('{} --list tmp/list_{}.txt {}\n'.format(cmd, i, argstr))
        outfile.write('EOJ\n')
    outfile.close()
    if (debug):
        exit(1)

    # Now run jobs
    if (queue_name == 'single-thread'):
        run_single_thread(jobfn)
    elif (queue_name == 'multi-thread'):
        run_single_thread(jobfn)  # TODO
    elif (re.search("^spark-", queue_name)):
        print 'Running on spark ...'

    # Clean up
    for file in glob.glob('tmp/list_*.txt'):
        os.unlink(file)
    os.unlink('tmp/cmds.lst')
