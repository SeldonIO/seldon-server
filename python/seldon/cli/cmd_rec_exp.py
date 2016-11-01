import pprint
import argparse
import sys
import os

import zk_utils
import seldon_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli rec_alg', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=True, choices=['show','configure'])
    parser.add_argument('--client-name', help="the name of the client", required=True)

    parser.add_argument('--cache-enabled', help="enable cache or not", required=False, choices=['true','false'])
    parser.add_argument('--default-locale', help="set the deafult locale to use eg 'us-en'", required=False)
    parser.add_argument('--explanations-enabled', help="enable the explanaions or not", required=False, choices=['true','false'])

    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def write_data_to_file(data_fpath, data):
    json = seldon_utils.dict_to_json(data, True) if isinstance(data,dict) else str(data)
    seldon_utils.mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def write_node_value_to_file(zk_client, zkroot, node_path):
    node_value = zk_utils.node_get(zk_client, node_path)
    node_value = node_value.strip()
    if zk_utils.is_json_data(node_value):
        data = seldon_utils.json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
    else:
        data = str(node_value)
    data_fpath = zkroot + node_path + "/_data_"
    write_data_to_file(data_fpath, data)

def ensure_client_has_recommendation_explanation(zkroot, zk_client, client_name):
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/recommendation_explanation/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)
    if not os.path.isfile(data_fpath):
        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/recommendation_explanation"
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_rec_exp = '{"cache_enabled":true,"default_locale":"us-en","explanations_enabled":false}'
            data = seldon_utils.json_to_dict(default_rec_exp)
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def action_show(command_data, opts):
    client_name = opts.client_name
    if (client_name == None) or (len(client_name) == 0):
        print "Need client name to comfigure explanations for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/recommendation_explanation/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)
    if not os.path.isfile(data_fpath):
        print "Explanations not configured for this client"
        sys.exit(1)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/recommendation_explanation/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)

    print "Explanations configuration, for client: "+client_name
    data_keys = data.keys()
    data_keys.sort()
    for data_key in data_keys:
        data_value = data[data_key]
        data_value = "true" if data_value == True else data_value
        data_value = "false" if data_value == False else data_value
        line = "    {data_key}: {data_value}".format(**locals())
        print line

def action_configure(command_data, opts):
    client_name = opts.client_name
    if (client_name == None) or (len(client_name) == 0):
        print "Need client name to comfigure explanations for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_recommendation_explanation(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/recommendation_explanation/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)

    if opts.cache_enabled != None:
        data["cache_enabled"] = True if opts.cache_enabled == "true" else False
    if opts.default_locale!= None:
        data["default_locale"] = opts.default_locale
    if opts.explanations_enabled != None:
        data["explanations_enabled"] = True if opts.explanations_enabled == "true" else False

    write_data_to_file(data_fpath, data)
    node_path = gdata["all_clients_node_path"]+"/"+client_name+"/recommendation_explanation"
    zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))
    action_show(command_data, opts)

def cmd_rec_exp(gopts,command_data, command_args):
    actions = {
        "show" : action_show,
        "configure" : action_configure,
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        print "Need action"
        sys.exit(1)
    else:
        if actions.has_key(action):
            actions[action](command_data, opts)
        else:
            print "Invalid action[{}]".format(action)
