import pprint
import argparse
import os
import sys
import json
import errno

import zk_utils

gdata = {
    'data_path': "/config/memcached/_data_",
    'node_path': "/config/memcached",
    'default_data': {
        'numClients': 1,
        'servers': "localhost:11211"
    },
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

def json_to_dict(json_data):
    return json.loads(json_data)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli memcached', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False, choices=['setup', 'commit'])
    parser.add_argument('--numClients', help="number of clients", required=False)
    parser.add_argument('--servers', help="the server list", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def action_setup(command_data, opts):
    print "Setting up memcached"
    zkroot = command_data["zkdetails"]["zkroot"]

    data_fpath = zkroot + gdata['data_path']
    zk_client=command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    has_any_value_changed = False

    if opts.numClients != None:
        data["numClients"] = opts.numClients
        has_any_value_changed = True

    if opts.servers != None:
        data["servers"] = opts.servers
        has_any_value_changed = True

    if has_any_value_changed:
        write_data_to_file(data_fpath, data)

def action_commit(command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + gdata['data_path']
    if not os.path.isfile(data_fpath):
        write_data_to_file(data_fpath, gdata["default_data"])
    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client=command_data["zkdetails"]["zk_client"]
    node_path=gdata["node_path"]
    zk_utils.node_set(zk_client, node_path, data_json)

def ensure_local_data_file_exists(zk_client, zkroot):
    data_fpath = zkroot + gdata['data_path']
    if not os.path.isfile(data_fpath):
        node_path=gdata["node_path"]
        node_value = zk_utils.node_get(zk_client, node_path)
        data = json_to_dict(node_value) if node_value != None else gdata["default_data"]
        write_data_to_file(data_fpath, data)

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def action_default(command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()
    print "memcached:"
    print "    numClients: {numClients}".format(numClients=data["numClients"])
    print "    servers: {servers}".format(servers=data["servers"])

def cmd_memcached(gopts,command_data, command_args):
    actions = {
        "default" : action_default,
        "setup" : action_setup,
        "commit" : action_commit,
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

