#!/usr/bin/env python

__author__ = "Gurminder Sunner"

import pprint
import sys
import argparse
from kazoo.client import KazooClient

def doCmdUsingHosts(zk_hosts, cmd, cmd_args):
    zk_client = getKazooClient(zk_hosts)
    zk_client.start()
    doCmd(zk_client, cmd, cmd_args)
    zk_client.stop()

def doCmd(zk_client, cmd, cmd_args):
    if cmd == 'set':
        thePath = cmd_args[0]
        theValue = cmd_args[1]
        retVal = None
        if zk_client.exists(thePath):
            retVal = zk_client.set(thePath,theValue)
        else:
            retVal = zk_client.create(thePath,theValue,makepath=True)
        print "[{cmd}][{thePath}][{theValue}]".format(cmd=cmd,thePath=thePath,theValue=theValue)
    elif cmd == 'get':
        thePath = cmd_args[0]
        retVal = None
        theValue = None
        if zk_client.exists(thePath):
            retVal = zk_client.get(thePath)
            theValue = retVal[0]
        print "[{cmd}][{thePath}][{theValue}]".format(cmd=cmd,thePath=thePath,theValue=theValue)

def getOpts():
    parser = argparse.ArgumentParser(description='Some Description')
    parser.add_argument('--zk-hosts', help="the zookeeper hosts", required=True)
    parser.add_argument('--cmd', help="the cmd to use", required=True)
    parser.add_argument('--cmd-args', help="the cmd args to use", nargs='+')
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = vars(parser.parse_args())
    return opts

def getKazooClient(zk_hosts):
    zk_client = KazooClient(hosts=zk_hosts)
    return zk_client

def main():
    opts = getOpts()
    #print opts
    ##doCmd(opts['zk_hosts'], opts['cmd'], opts['cmd_args'])
    doCmdUsingHosts(opts['zk_hosts'], opts['cmd'], opts['cmd_args'])

if __name__ == "__main__":
    main()



