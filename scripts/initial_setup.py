#!/usr/bin/env python
import argparse, traceback
import os
from kazoo.client import KazooClient
import json, random
import seldon_utils as seldon

def lowercase_clients(data):
    clients = data['clients']
    for client in clients:
        client['name'] = client['name'].lower()

requiredSections = ["db","memcached"]
propertyToZkNode = dict()
propertyToZkNode["db"] = "/config/dbcp"
propertyToZkNode["memcached"] = "/config/memcached_servers"
propertyToZkNode["statsd"] = "/config/statsd"
propertyToZkNode["clients"] = "/all_clients"

dir = os.path.dirname(os.path.abspath(__file__))
parser = argparse.ArgumentParser(prog="PROG",description="Sets up the Seldon Server")
parser.add_argument("--props",help="Relative path to the file with the props", default='../server_config.json')
parser.add_argument("--zookeeper",help="Location of zookeeper (hosts)", default="localhost")
parser.add_argument("--js-consumer-key",help="Client js consumer key else a random one will be used", default=None)
parser.add_argument("--all-consumer-key",help="Client all consumer key else a random one will be used", default=None)
parser.add_argument("--all-consumer-secret",help="Client all consumer secret else a random one will be used", default=None)
args = parser.parse_args()

consumer_details={}
consumer_details['js_consumer_key']     = args.js_consumer_key      if args.js_consumer_key     != None else None
consumer_details['all_consumer_key']    = args.all_consumer_key     if args.all_consumer_key    != None else None
consumer_details['all_consumer_secret'] = args.all_consumer_secret  if args.all_consumer_secret != None else None

filename = os.path.join(dir, args.props)
if not os.path.exists(filename):
    print "Properties file doesn't exist at", filename, ", please create it before running this script"
    exit(1)
with open(filename) as data_file:
    data = json.load(data_file)
    lowercase_clients(data)

for section in requiredSections:
    if not section in data:
        print "Must have section",section,"in config JSON"
        exit(1)

zk = KazooClient(hosts=args.zookeeper)
zk.start()

try:
	seldon.dbSetup(zk,data["db"],propertyToZkNode["db"])
	seldon.memcachedSetup(zk,data["memcached"],propertyToZkNode["memcached"])
	seldon.clientSetup(zk,data["clients"],data["db"],propertyToZkNode["clients"], consumer_details)
	print "Finished succesfully"
except KeyError as e:
	print "Property missing from config json:",e
	traceback.print_exc()

