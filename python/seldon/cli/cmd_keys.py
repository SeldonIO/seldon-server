import pprint
import argparse
import os
import sys
import re
import json
import MySQLdb

import db_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli keys', description='Seldon CLI')
    parser.add_argument('--action', help="the action to use", required=False, choices=['list'])
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('--scope', help="the key scope", required=False, choices=['js','all'])
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def json_to_dict(json_data):
    return json.loads(json_data)

KEYS_SQL_CLIENT = "select short_name,consumer_key,consumer_secret,scope from api.consumer where short_name=%(client_name)s"
KEYS_SQL_CLIENT_SCOPE = "select short_name,consumer_key,consumer_secret,scope from api.consumer where short_name=%(client_name)s and scope=%(scope)s"
KEYS_SQL_SCOPE = "select short_name,consumer_key,consumer_secret,scope from api.consumer where scope=%(scope)s"
KEYS_SQL = "select short_name,consumer_key,consumer_secret,scope from api.consumer"


def action_list(command_data,opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + "/config/dbcp/_data_"
    f = open(data_fpath)
    jsonStr = f.read()
    data = json_to_dict(jsonStr)
    f.close()

    db_info = None
    for db_info in data['dbs']:
        dbSettings = {}
        dbSettings["host"]=re.search('://(.*?):(.*?),',db_info["jdbc"]).groups()[0]
        dbSettings["user"]=db_info["user"]
        dbSettings["password"]=db_info["password"]
        dbSettings["name"] = db_info["name"]
        res = db_utils.get_keys(dbSettings,opts.client_name,opts.scope)
    print json.dumps(res)


def cmd_keys(gopts,command_data, command_args):
    actions = {
        "default" : action_list,
        "list" : action_list
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        actions["default"](command_data, opts)
    else:
        if actions.has_key(action):
            actions[action](command_data, opts)
        else:
            print "Invalid action[{}]".format(action)
