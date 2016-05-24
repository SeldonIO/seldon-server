import pprint
import argparse
import sys
import os
import json
import errno

import zk_utils

import cmd_pred

gdata = {
    'all_clients_node_path': "/all_clients",
}

CONFIG_MICROSERVICE_URL="io.seldon.algorithm.external.url"
CONFIG_MICROSERVICE_NAME="io.seldon.algorithm.external.name"
EXTERNAL_RECOMMENDER="externalItemRecommendationAlgorithm"

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli rec_alg', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=True, choices=['list','add','delete','show','commit','create'])
    parser.add_argument('--alg-type', help="type of algorithm", required=False, choices=['recommendation','prediction'], default='recommendation')
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('--recommender-name', help="the name of recommender", required=False)
    parser.add_argument('--config', help="algorithm specific config in the form x=y", required=False, action='append')
    parser.add_argument('-f','--json-file', help="the json file to use for creating algs or '-' for stdin", required=False)
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


def ensure_client_has_algs(zk_client):


        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/algs"
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_alg_json = '{"algorithms":[{"config":[],"filters":[],"includers":[],"name":"recentItemsRecommender"}],"combiner":"firstSuccessfulCombiner"}'
            data = json_to_dict(default_alg_json)
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, dict_to_json(data))



def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True) if isinstance(data,dict) else str(data)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def get_data_from_file(data_fpath):
    if os.path.isfile(data_fpath):
        f = open(data_fpath)
        data = f.read()
        f.close()
        return data
    else:
        return ""



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



def add_model_activate(zkroot,client_name,activate_path):
    node_fpath = zkroot + activate_path + "/_data_"
    data = get_data_from_file(node_fpath).rstrip()
    if len(data) > 0:
        clients = data.split(',')
        for client in clients:
            if client == client_name:
                return
    else:
        clients = []
    clients.append(client_name)
    data = ",".join(clients)
    write_data_to_file(node_fpath,data)


def remove_model_activate(zkroot,client_name,activate_path):
    node_fpath = zkroot + activate_path + "/_data_"
    data = get_data_from_file(node_fpath).rstrip()
    print "data is ",data
    if len(data)>0:
        clients = data.split(',')
        for client in clients:
            print "looking at ",client
            if client == client_name:
                clients.remove(client)
                data = ",".join(clients)
                write_data_to_file(node_fpath,data)

def show_algs(data):
    json = dict_to_json(data, True)
    print json

def ensure_client_has_algs(zkroot, zk_client, client_name):
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/algs/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)
    if not os.path.isfile(data_fpath):
        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/algs"
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_alg_json = '{"algorithms":[{"config":[],"filters":[],"includers":[],"name":"recentItemsRecommender"}],"combiner":"firstSuccessfulCombiner"}'
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

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/alg_rectags/_data_"
    if os.path.isfile(data_fpath):
        f = open(data_fpath)
        json = f.read()
        f.close()
        data = json_to_dict(json)
        show_algs(data)
    else:
        print "Unable to show recommenders definition for client[{client_name}]".format(**locals())

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

    recommender_name = opts.recommender_name
    if recommender_name == None:
        print "Need recommender name"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    default_algorithms = command_data["conf_data"]["default_algorithms"]
    recommenders = default_algorithms.keys()

    if recommender_name not in recommenders:
        print "Invalid recommender[{recommender_name}]".format(**locals())
        sys.exit(1)


    if recommender_name == EXTERNAL_RECOMMENDER:
        if not (has_config(opts,CONFIG_MICROSERVICE_URL) and has_config(opts,CONFIG_MICROSERVICE_NAME)):
            print "You must supply "+CONFIG_MICROSERVICE_URL+" and "+CONFIG_MICROSERVICE_NAME+" for "+EXTERNAL_RECOMMENDER
            sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    algorithms = data["algorithms"]
    includers = default_algorithms[recommender_name]["includers"] if default_algorithms[recommender_name].has_key("includers") else []
    recommender_data = {
            'filters':[],
            'includers': includers,
            'name': recommender_name,
            'config': default_algorithms[recommender_name]["config"]
    }


    if not opts.config is None:
        for nv in opts.config:
            (name,value) = nv.split('=')
            recommender_data['config'].append({"name":name,"value":value})

    algorithms.append(recommender_data)
    write_data_to_file(data_fpath, data)

    if default_algorithms[recommender_name].has_key("zk_activate_node"):
        add_model_activate(zkroot,client_name,default_algorithms[recommender_name]["zk_activate_node"])

    print "Added [{recommender_name}]".format(**locals())
    show_algs(data)

def action_delete(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to add algs for"
        sys.exit(1)

    recommender_name = opts.recommender_name
    if recommender_name == None:
        print "Need recommender name"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    default_algorithms = command_data["conf_data"]["default_algorithms"]
    recommenders = default_algorithms.keys()

    if recommender_name not in recommenders:
        print "Invalid recommender[{recommender_name}]".format(**locals())
        sys.exit(1)

    algorithms = data["algorithms"]

    length_before_removal = len(algorithms)
    def recommender_filter(item):
        if item["name"] == recommender_name:
            return False
        else:
            return True
    filtered_algorithms = filter(recommender_filter, algorithms)
    length_after_removal = len(filtered_algorithms)
    data["algorithms"] = filtered_algorithms
    if length_after_removal < length_before_removal:
        write_data_to_file(data_fpath, data)

        if default_algorithms[recommender_name].has_key("zk_activate_node"):
            remove_model_activate(zkroot,client_name,default_algorithms[recommender_name]["zk_activate_node"])

        print "Removed [{recommender_name}]".format(**locals())

def action_list(command_data, opts):
    print "Default recommenders:"
    default_algorithms = command_data["conf_data"]["default_algorithms"]
    for recommender in default_algorithms:
        print "    {recommender}".format(**locals())

def action_commit(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to commit data for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/alg_rectags/_data_"
    if os.path.isfile(data_fpath):
        f = open(data_fpath)
        data_json = f.read()
        f.close()

        zk_client = command_data["zkdetails"]["zk_client"]
        node_path = gdata["all_clients_node_path"] + "/" + client_name + "/alg_rectags"
        zk_utils.node_set(zk_client, node_path, data_json)

        # activate any required models
        data = json_to_dict(data_json)
        recommender_set = set()
        if data["defaultStrategy"].has_key("algorithms"):
            for alg in data["defaultStrategy"]["algorithms"]:
                recommender_set.add(alg["name"])
        elif data["defaultStrategy"].has_key("variations"):
            for variation in data["defaultStrategy"]["variations"]:
                for alg in variation["config"]["algorithms"]:
                    recommender_set.add(alg["name"])
        default_algorithms = command_data["conf_data"]["default_algorithms"]
        for recommender_name in recommender_set:
            if default_algorithms[recommender_name].has_key("zk_activate_node"):
                print "activate",recommender_name
                node_path = default_algorithms[recommender_name]["zk_activate_node"]
                node_fpath = zkroot + node_path + "/_data_"
                data_models = get_data_from_file(node_fpath)
                zk_utils.node_set(zk_client, node_path, data_models)

        return

    #TODO remove the following once only using alg_rectags
    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    if not os.path.isfile(data_fpath):
        print "Data to commit not found!!"
        sys.exit(1)
    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client = command_data["zkdetails"]["zk_client"]
    node_path = gdata["all_clients_node_path"] + "/" + client_name + "/algs"
    zk_utils.node_set(zk_client, node_path, data_json)

    # activate any required models

    default_algorithms = command_data["conf_data"]["default_algorithms"]
    data = json_to_dict(data_json)
    algorithms = data["algorithms"]
    print "algorithms:"
    for alg in algorithms:
        alg_name=alg["name"]
        if default_algorithms[alg_name].has_key("zk_activate_node"):
            node_path = default_algorithms[alg_name]["zk_activate_node"]
            node_fpath = zkroot + node_path + "/_data_"
            data_models = get_data_from_file(node_fpath)
            zk_utils.node_set(zk_client, node_path, data_models)

def action_create(command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]

    #check_valid_client_name
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to create algs for"
        sys.exit(1)
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    #check_valid_json_file
    json_file_contents = ""
    json_file = opts.json_file
    if json_file == None:
        print "Need json-file to use for creating algs"
        sys.exit(1)
    if json_file == "-":
        json_file_contents = sys.stdin.read()
    else:
        if not os.path.isfile(json_file):
            print "Unable find file[{json_file}]".format(**locals())
            sys.exit(1)
        f = open(json_file)
        json_file_contents = f.read()
        f.close()

    # ensure valid data
    data = json_to_dict(json_file_contents)

    #do the model activate
    recommender_set = set()
    if data["defaultStrategy"].has_key("algorithms"):
        for alg in data["defaultStrategy"]["algorithms"]:
            recommender_set.add(alg["name"])
    elif data["defaultStrategy"].has_key("variations"):
        for variation in data["defaultStrategy"]["variations"]:
            for alg in variation["config"]["algorithms"]:
                recommender_set.add(alg["name"])
    default_algorithms = command_data["conf_data"]["default_algorithms"]
    for recommender_name in recommender_set:
        if default_algorithms[recommender_name].has_key("zk_activate_node"):
            add_model_activate(zkroot,client_name,default_algorithms[recommender_name]["zk_activate_node"])

    #save to zkoot
    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/alg_rectags/_data_"
    write_data_to_file(data_fpath, data)

def cmd_alg(gopts,command_data, command_args):
    actions = {
        "list" : action_list,
        "show" : action_show,
        "add" : action_add,
        "delete" : action_delete,
        "commit" : action_commit,
        "create" : action_create,
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        print "Running default list action"
        actions["default"](command_data, opts)
    else:
        if actions.has_key(action):
            actions[action](command_data, opts)
        else:
            print "Invalid action[{}]".format(action)

