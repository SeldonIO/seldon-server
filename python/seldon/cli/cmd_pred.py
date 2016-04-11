import pprint
import argparse
import sys
import os
import json
import errno

import zk_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

CONFIG_MICROSERVICE_URL="io.seldon.algorithm.external.url"
CONFIG_MICROSERVICE_NAME="io.seldon.algorithm.external.name"
EXTERNAL_PREDICTOR="externalPredictionServer"

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli predict_alg', description='Seldon CLI')
    parser.add_argument('--action', help="the action to use", required=True, choices=['list','show','add','delete','commit'])
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('--predictor-name', help="the name of predictor", required=False)
    parser.add_argument('--config', help="algorithm specific config in the form x=y", required=False, action='append')
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

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

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True) if isinstance(data,dict) else str(data)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def write_node_value_to_file(zk_client, zkroot, node_path):
    node_value = zk_utils.node_get(zk_client, node_path)
    node_value = node_value.strip()
    if zk_utils.is_json_data(node_value):
        data = json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
    else:
        data = str(node_value)
    data_fpath = zkroot + node_path + "/_data_"
    write_data_to_file(data_fpath, data)


def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def show_algs(data):
    algorithms = data["algorithms"]
    print "algorithms:"
    for alg in algorithms:
        print "    {alg_name}".format(alg_name=alg["name"])
        for config_item in alg["config"]:
            print "        {n}={v}".format(n=config_item["name"],v=config_item["value"])


def ensure_client_has_algs(zkroot, zk_client, client_name):
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/predict_algs/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)
    if not os.path.isfile(data_fpath):
        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/predict_algs"
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_alg_json = '{"algorithms":[]}'
            data = json_to_dict(default_alg_json)
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, dict_to_json(data))

def action_show(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the algs for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/predict_algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)
    show_algs(data)

def has_config(opts,name):
    if not opts.config is None:
        for nv in opts.config:
            if nv.split('=')[0] == name:
                return True
    return False


def action_add(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to add algs for"
        sys.exit(1)

    predictor_name = opts.predictor_name
    if predictor_name == None:
        print "Need predictor name"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    default_algorithms = command_data["conf_data"]["default_predictors"]
    predictors = default_algorithms.keys()

    if predictor_name not in predictors:
        print "Invalid predictor[{predictor_name}]".format(**locals())
        sys.exit(1)


    if predictor_name == EXTERNAL_PREDICTOR:
        if not (has_config(opts,CONFIG_MICROSERVICE_URL) and has_config(opts,CONFIG_MICROSERVICE_NAME)):
            print "You must supply "+CONFIG_MICROSERVICE_URL+" and "+CONFIG_MICROSERVICE_NAME+" for "+EXTERNAL_PREDICTOR
            sys.exit(1)
        
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/predict_algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    algorithms = data["algorithms"]
    predictor_data = {
            'name': predictor_name,
            'config': default_algorithms[predictor_name]["config"]
    }

    if not opts.config is None:
        for nv in opts.config:
            (name,value) = nv.split('=')
            predictor_data['config'].append({"name":name,"value":value})

    algorithms.append(predictor_data)
    write_data_to_file(data_fpath, data)
    print "Added [{predictor_name}]".format(**locals())
    show_algs(data)

def action_delete(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to add algs for"
        sys.exit(1)

    predictor_name = opts.predictor_name
    if predictor_name == None:
        print "Need predictor name"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/predict_algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    default_predictors = command_data["conf_data"]["default_predictors"]
    predictors = default_predictors.keys()

    if predictor_name not in predictors:
        print "Invalid predictor[{predictor_name}]".format(**locals())
        sys.exit(1)

    algorithms = data["algorithms"]

    length_before_removal = len(algorithms)
    def predictor_filter(item):
        if item["name"] == predictor_name:
            return False
        else:
            return True
    filtered_algorithms = filter(predictor_filter, algorithms)
    length_after_removal = len(filtered_algorithms)
    data["algorithms"] = filtered_algorithms
    if length_after_removal < length_before_removal:
        write_data_to_file(data_fpath, data)
        print "Removed [{predictor_name}]".format(**locals())

def action_list(command_data, opts):
    print "Default predictors:"
    default_algorithms = command_data["conf_data"]["default_predictors"]
    for predictor in default_algorithms:
        print "    {predictor}".format(**locals())

def action_commit(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to add algs for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/predict_algs/_data_"
    if not os.path.isfile(data_fpath):
        "Data to commit not found!!"
    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client = command_data["zkdetails"]["zk_client"]
    node_path = gdata["all_clients_node_path"] + "/" + client_name + "/predict_algs"
    zk_utils.node_set(zk_client, node_path, data_json)

def cmd_pred(gopts,command_data, command_args):
    actions = {
        "list" : action_list,
        "show" : action_show,
        "add" : action_add,
        "delete" : action_delete,
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
