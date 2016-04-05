import pprint
import argparse
import sys
import os
import json

import zk_utils
import seldon_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli memcached', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False)
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('--model-name', help="the name of the client", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def write_data_to_file(data_fpath, data):
    json = seldon_utils.dict_to_json(data, True) if isinstance(data,dict) else str(data)
    seldon_utils.mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def action_add(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to add model for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    model_name = opts.model_name
    if model_name == None:
        print "Need model name to use"
        sys.exit(1)

    default_models = command_data["conf_data"]["default_models"]
    if model_name not in default_models.keys():
        print "Invalid model name: {model_name}".format(**locals())
        sys.exit(1)

    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline/{model_name}/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)

    zk_client = command_data["zkdetails"]["zk_client"]
    if not os.path.isfile(data_fpath):
        node_path = "{all_clients_node_path}/{client_name}/offline/{model_name}".format(all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_model_data = default_models[model_name]["config"]
            if default_model_data.has_key("inputPath"):
                default_model_data["inputPath"]=command_data["conf_data"]["seldon_models"]
            if default_model_data.has_key("outputPath"):
                default_model_data["outputPath"]=command_data["conf_data"]["seldon_models"]
            data = default_model_data
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))
    else:
        print "Model [{model_name}] already added".format(**locals())

def action_list(command_data, opts):
    default_models = command_data["conf_data"]["default_models"]
    models = default_models.keys()
    print "models:"
    for idx,model in enumerate(models):
        print "    {model}".format(**locals())

def cmd_model(command_data, command_args):
    actions = {
        "default" : action_list,
        "list" : action_list,
        "add" : action_add,
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

