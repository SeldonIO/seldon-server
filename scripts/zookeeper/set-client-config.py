#!/usr/bin/env python
import sys, getopt, argparse
from kazoo.client import KazooClient
import json

def loadZookeeperOptions(opts,zk):
    node = "/all_clients/"+opts['client']+"/offline/semvec"
    if zk.exists(node):
        data, stat = zk.get(node)
        jStr = data.decode("utf-8")
        print "Found zookeeper configuration:",jStr
        j = json.loads(jStr)
        for key in j:
            opts[key] = j[key]

def activateModel(args,folder,zk):
    node = "/all_clients/"+args.client+"/svtext"
    print "Activating model in zookeper at node ",node," with data ",folder
    if zk.exists(node):
        zk.set(node,folder)
    else:
        zk.create(node,folder,makepath=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='set-client-config')
    parser.add_argument('-z', '--zookeeper', help='zookeeper hosts', required=True)
    parser.add_argument('--clientVariable', help='client variable name', default="$CLIENT")

    args = parser.parse_args()
    opts = vars(args)

    zk = KazooClient(hosts=args.zookeeper)
    zk.start()

    for line in sys.stdin:
        line = line.rstrip()
        parts = line.split()
        if len(parts) == 3 and not line.startswith("#"):
            clients = parts[0].split(',')
            node = parts[1]
            value = parts[2]
            print "--------------------------"
            print parts[0],node,"->",value
            for client in clients:
                nodeClient = node.replace(args.clientVariable,client)
                valueClient = value.replace(args.clientVariable,client)
                print "----"
                print nodeClient
                print valueClient
                if zk.exists(nodeClient):
                    zk.set(nodeClient,valueClient)
                else:
                    zk.create(nodeClient,valueClient,makepath=True)
                    
    zk.stop()


