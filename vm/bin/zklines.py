#!/usr/bin/env python

__author__ = "Gurminder Sunner"

import sys
import os
import argparse

script_dir = os.path.dirname( os.path.realpath(__file__) )
sys.path.append( script_dir )
import zkcmd

def process_line(zk_client, line):
    parts =line.split(None,2)
    cmd=parts[0]
    cmd_args=parts[1:]
    zkcmd.doCmd(zk_client, cmd, cmd_args)

def process_file(zk_client, f):
    for line_raw in f:
        line = line_raw.strip() # remove whiespace and nl
        if len(line) > 0:
            process_line(zk_client, line)

def getOpts():
    parser = argparse.ArgumentParser(description='Some Description')
    parser.add_argument('--zk-hosts', help="the zookeeper hosts", required=True)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = vars(parser.parse_args())
    return opts

def main():
    opts = getOpts()
    #print opts
    zk_client = zkcmd.getKazooClient(opts['zk_hosts'])
    zk_client.start()
    filenames = opts['args']
    if len(filenames) > 0:
        for filename in filenames:
            f = open(filename)
            process_file(zk_client, f)
            f.close()
    else:
        process_file(zk_client, sys.stdin)
    zk_client.stop()

if __name__ == "__main__":
    main()

