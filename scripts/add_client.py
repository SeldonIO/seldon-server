#!/usr/bin/env python
import argparse, os, json
import seldon_utils as seldon
from kazoo.client import KazooClient
dir = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser(prog="add_client",
	description="Adds a new client to the Seldon Server")
parser.add_argument("client",nargs=1,help="The client to add")
parser.add_argument("--json",help="extra JSON configuration for the client")
parser.add_argument("--props",help="Relative path to the file with the props", default='../server_config.json')
parser.add_argument("--db",help="The name of the DB to use (from the config file), default is to use the first one mentioned.")
parser.add_argument("--zookeeper",help="Location of zookeeper (hosts)", default="localhost")

args = parser.parse_args()
filename = os.path.join(dir, args.props)
zk = KazooClient(hosts=args.zookeeper)
zk.start()
if not os.path.exists(filename):
    print "Properties file doesn't exist at", filename, ", please create it before running this script"
    exit(1)
with open(filename) as data_file:
    data = json.load(data_file)
seldon.clientSetup(zk,[{"name":args.client[0],"db":args.db}],data["db"],"/all_clients")

print "Finished successfully"