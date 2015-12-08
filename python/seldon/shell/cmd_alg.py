import os
import pprint
import json
import errno

import zk_utils

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

gdata = {
    'all_clients_node_path': "/all_clients",
    'help_cmd_strs_list' : [
        ("alg", "shows the Default recommenders"),
        ("alg show <clientName>", "show algs for client"),
        ("alg add <clientName>", "add algs for client"),
        ("alg delete <clientName>", "pick alg to delete for client"),
        ("alg promote <clientName>", "pick alg to promote for client"),
        ("alg commit <clientName>", "commit chnanges")
    ],
}

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

def show_algs(data):
    combiner = data["combiner"]
    algorithms = data["algorithms"]
    print "algorithms:"
    for alg in algorithms:
        print "    {alg_name}".format(alg_name=alg["name"])
        for config_item in alg["config"]:
            print "        {n}={v}".format(n=config_item["name"],v=config_item["value"])
    print "combiner: "+combiner

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def subcmd_show(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to show the algs for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)
    show_algs(data)

def subcmd_add(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to add algs for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    default_algorithms = command_data["default_algorithms"]
    recommenders = default_algorithms.keys()
    for idx,recommender in enumerate(recommenders):
        print "    [{idx}] {recommender}".format(**locals())
    q="Choose recommender: "
    recommender_idx = None
    try:
        recommender_idx = int(raw_input(q))
    except ValueError:
        pass

    recommender_name = recommenders[recommender_idx] if recommender_idx != None and recommender_idx>=0 and recommender_idx<len(recommenders) else None

    if recommender_name == None:
        print "Invaild recommender"
        return

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
    algorithms.append(recommender_data)
    write_data_to_file(data_fpath, data)
    print "Added [{recommender_name}]".format(**locals())
    show_algs(data)

def subcmd_delete(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to delete algs for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    algorithms = data["algorithms"]
    print "algorithms:"
    for idx, alg in enumerate(algorithms):
        print "    [{idx}] {alg_name}".format(idx=idx, alg_name=alg["name"])

    q="Choose recommender to delete: "
    recommender_idx = None
    try:
        recommender_idx = int(raw_input(q))
    except ValueError:
        pass

    if recommender_idx == None or (not (recommender_idx>=0 and recommender_idx<len(algorithms))):
        print "Invalid choice!"
        return

    recommender_name=algorithms[recommender_idx]["name"]
    del(algorithms[recommender_idx])
    write_data_to_file(data_fpath, data)
    print "Removed [{recommender_name}]".format(**locals())

def subcmd_promote(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to promote algs for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_algs(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = json_to_dict(json)

    algorithms = data["algorithms"]
    print "algorithms:"
    for idx, alg in enumerate(algorithms):
        print "    [{idx}] {alg_name}".format(idx=idx, alg_name=alg["name"])

    q="Choose recommender to promote: "
    recommender_idx = None
    try:
        recommender_idx = int(raw_input(q))
    except ValueError:
        pass

    if recommender_idx == None or (not (recommender_idx>=1 and recommender_idx<len(algorithms))):
        print "Invalid choice!"
        return

    recommender_name=algorithms[recommender_idx]["name"]
    algorithms[recommender_idx], algorithms[recommender_idx-1]  = algorithms[recommender_idx-1], algorithms[recommender_idx]
    write_data_to_file(data_fpath, data)
    print "Promoted [{recommender_name}]".format(**locals())

def subcmd_commit(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to delete algs for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/algs/_data_"
    if not os.path.isfile(data_fpath):
        "Data to commit not found!!"
    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client = command_data["zkdetails"]["zk_client"]
    node_path = gdata["all_clients_node_path"] + "/" + client_name + "/algs"
    zk_utils.node_set(zk_client, node_path, data_json)

def subcmd_default(command_data):
    print "Default recommenders:"
    default_algorithms = command_data["default_algorithms"]
    for recommender in default_algorithms:
        print "    {recommender}".format(**locals())

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_alg(args, command_data):
    if args == "":
        subcmd_default(command_data)
    else:
        subcmd = args.split()[0]
        subcmd_args = args.split()[1:]
        command_data["subcmd"] = subcmd
        command_data["subcmd_args"] = subcmd_args
        if subcmds.has_key(subcmd):
            subcmds[subcmd](command_data)
        else:
            print "unkown subcmd[%s]" % subcmd

subcmds = {
    "help" : subcmd_help,
    "show" : subcmd_show,
    "add" : subcmd_add,
    "delete" : subcmd_delete,
    "promote" : subcmd_promote,
    "commit" : subcmd_commit,
}

