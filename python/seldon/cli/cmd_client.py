import pprint
import argparse
import os
import json

import zk_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli client', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def get_data_fpath(zkroot, client):
    return zkroot + gdata["all_clients_node_path"] + "/" + client + "/_data_"

def json_to_dict(json_data):
    return json.loads(json_data)

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True) if isinstance(data,dict) else str(data)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def action_list(command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]

    all_clients_fpath = zkroot + gdata["all_clients_node_path"]
    if not os.path.isdir(all_clients_fpath):
        # the dir for all_clients doesnt exist
        if zk_client.exists(gdata["all_clients_node_path"]):
            client_nodes = zk_client.get_children(gdata["all_clients_node_path"])
            def write_node_value_to_file(node_path):
                node_value = zk_utils.node_get(zk_client, node_path)
                node_value = node_value.strip()
                if zk_utils.is_json_data(node_value):
                    data = json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
                else:
                    data = str(node_value)
                data_fpath = zkroot + node_path + "/_data_"
                write_data_to_file(data_fpath, data)
            for client_node in client_nodes:
                node_path=gdata["all_clients_node_path"]+"/"+client_node
                write_node_value_to_file(node_path)
                client_child_nodes = zk_client.get_children(gdata["all_clients_node_path"]+"/"+client_node)
                for client_child_node in client_child_nodes:
                    node_path=gdata["all_clients_node_path"]+"/"+client_node+"/"+client_child_node
                    write_node_value_to_file(node_path)

    if not os.path.isdir(all_clients_fpath):
        print "No clients found!"
    else:
        for client in os.listdir(zkroot + gdata["all_clients_node_path"]):
            data_fpath = get_data_fpath(zkroot, client)
            f = open(data_fpath)
            json = f.read()
            data = json_to_dict(json)
            f.close()
            print "client[{client}]:".format(**locals())
            DB_JNDI_NAME = data["DB_JNDI_NAME"] if isinstance(data, dict) and data.has_key("DB_JNDI_NAME") else ""
            print "    DB_JNDI_NAME: "+DB_JNDI_NAME

def cmd_client(command_data, command_args):
    actions = {
        "default" : action_list,
        "list" : action_list,
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

