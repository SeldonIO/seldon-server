import os
import json
import pprint
import errno
import re

import zk_utils
import seldon_utils

gdata = {
    'all_clients_node_path': "/all_clients",
    'help_cmd_strs_list' : [
        ("client", "shows list of clients already setup"),
        ("client setup <clientName> <dbName>", "to create a new client/TODO deal with existing")
    ],
}

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

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def subcmd_list(command_data):
    zk_client = command_data["zkdetails"]["zk_client"]
    client_nodes = zk_client.get_children('/all_clients')
    for client_node in client_nodes:
        print client_node

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True) if isinstance(data,dict) else str(data)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def get_data_fpath(zkroot, client):
    return zkroot + gdata["all_clients_node_path"] + "/" + client + "/_data_"

def subcmd_default(command_data):
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
            print client
            data_fpath = get_data_fpath(zkroot, client)
            f = open(data_fpath)
            json = f.read()
            print json
            data = json_to_dict(json)
            f.close()
            print "client[{client}]:".format(**locals())
            DB_JNDI_NAME = data["DB_JNDI_NAME"] if isinstance(data, dict) and data.has_key("DB_JNDI_NAME") else ""
            print "    DB_JNDI_NAME: "+DB_JNDI_NAME

def add_client(zk_client, zkroot, client_name, db_name, consumer_details=None):
    data_fpath = zkroot + "/config/dbcp/_data_"
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    db_info = None
    for db_info_entry in data['dbs']:
        if db_info_entry['name'] == db_name:
            db_info = db_info_entry
            break

    if db_info == None:
        print "Invalid db name[{db_name}]".format(**locals())
        return

    dbSettings = {}
    dbSettings["host"]=re.search('://(.*?):(.*?),',db_info["jdbc"]).groups()[0]
    dbSettings["user"]=db_info["user"]
    dbSettings["password"]=db_info["password"]
    seldon_utils.addApiDb(db_name, dbSettings)
    seldon_utils.addClientDb(client_name, dbSettings, consumer_details=None)
    # write to local file
    data_fpath = get_data_fpath(zkroot, client_name)
    data = {'DB_JNDI_NAME': db_name}
    write_data_to_file(data_fpath, data)
    # write to zookeeper
    node_path=gdata["all_clients_node_path"]+"/"+client_name
    data_json = dict_to_json(data)
    zk_utils.node_set(zk_client, node_path, data_json)

def subcmd_setup(command_data):
    client_name_to_setup = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    db_name_to_use = command_data["subcmd_args"][1] if len(command_data["subcmd_args"])>1 else None
    if client_name_to_setup == None:
        print "Need client name to setup"
        return

    # check if this client exists
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    data_fpath = get_data_fpath(zkroot, client_name_to_setup)
    if not os.path.isfile(data_fpath):
        print "Trying to create the client"
        if db_name_to_use == None:
            print "Need a db name"
            return
        add_client(zk_client, zkroot, client_name_to_setup, db_name_to_use)

def subcmd_alg(command_data):
    client_name_to_setup = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name_to_setup == None:
        print "Need client name to setup"
        return
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name_to_setup}/algs/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name_to_setup=client_name_to_setup)
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    combiner = data["combiner"]
    algorithms = data["algorithms"]
    print "combiner: "+combiner
    print "algorithms:"
    for alg in algorithms:
        print "    {alg_name}".format(alg_name=alg["name"])

def subcmd_algsetup(command_data):
    pp(command_data)

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_client(args, command_data):
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
    "list" : subcmd_list,
    "setup" : subcmd_setup,
    "alg" : subcmd_alg,
    "algsetup" : subcmd_algsetup,
}

