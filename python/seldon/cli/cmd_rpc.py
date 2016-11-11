import pprint
import argparse
import os
import json
import sys
import errno
import copy
import shutil
from subprocess import call

import zk_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}


def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli rpc', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False, choices=['show','set', 'remove'])
    parser.add_argument('--client-name', help="the name of the client", required=True)
    parser.add_argument('--proto', help="the proto buffer file with request and optional reply class", required=False)
    parser.add_argument('--request-class', help="the request class", required=False)
    parser.add_argument('--reply-class', help="the reply class", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def remove_path(path):
    shutil.rmtree(path)

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def json_to_dict(json_data):
    return json.loads(json_data)

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())


def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False


def show_rpc(data):
    json = dict_to_json(data, True)
    print json

def action_show(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the rpc settings for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/rpc/_data_"
    if os.path.isfile(data_fpath):
        f = open(data_fpath)
        json = f.read()
        f.close()
        data = json_to_dict(json)
        show_rpc(data)
    else:
        print "Unable to show rpc definition for client[{client_name}]".format(**locals())


def create_jar(grpc_home,proto_file,jar_file):
    cmd = grpc_home+"/create-proto-jar.sh "+proto_file+" "+jar_file
    print "Creating jar file from proto file. This may take some time..."
    sys.stdout.flush()
    return call(cmd, shell=True)

def action_set(command_data, opts):
    if "grpc_util_home" in command_data["conf_data"]:
        grpc_home = command_data["conf_data"]["grpc_util_home"]
    else:
        print "Need the location for grpc_util_home to create proto jar via maven"
        sys.exit(1)

    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the rpc settings for"
        sys.exit(1)

    proto_file = opts.proto
    if proto_file == None:
        print "Need --proto argument"
        sys.exit(1)

    request_class = opts.request_class
    if request_class == None:
        print "Need request class to setup!"
        sys.exit(1)

    jar_file = "/seldon-data/rpc/jar/"+client_name+".jar"
    if not create_jar(grpc_home,opts.proto,jar_file) == 0:
        print "Failed to create jar file from proto"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]

    if opts.reply_class == None:
        data = {"jarFilename":jar_file,"requestClassName":opts.request_class}
    else:
        data = {"jarFilename":jar_file,"requestClassName":opts.request_class,"replyClassName":opts.reply_class}

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/rpc/_data_"
    node_path = gdata["all_clients_node_path"] + "/" + client_name + "/rpc"

    write_data_to_file(data_fpath, data)

    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client=command_data["zkdetails"]["zk_client"]
    zk_utils.node_set(zk_client, node_path, data_json)



def action_remove(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the rpc settings for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/rpc"
    node_path = gdata["all_clients_node_path"] + "/" + client_name + "/rpc"

    zk_utils.node_delete(zk_client,node_path)
    remove_path(data_fpath)
    

def cmd_rpc(gopts,command_data, command_args):
    actions = {
        "default" : action_show,
        "show" : action_show,
        "set" : action_set,
        "remove" : action_remove
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

