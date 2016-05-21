import pprint
import argparse
import os
import json
import sys
import re

import zk_utils
import seldon_utils
import spark_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli client', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False, choices=['list','setup','processactions','processevents'])
    parser.add_argument('--db-name', help="the name of the db", required=False)
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('--input-date-string', help="The date to process in YYYYMMDD format", required=False)
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

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def add_client(gopts,command_data,zk_client, zkroot, client_name, db_name, consumer_details=None):
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
        sys.exit(1)

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


def add_client_dashboard(gopts,command_data,client_name):
    if "grafana_endpoint" in command_data["conf_data"]:
        grafana = command_data["conf_data"]["grafana_endpoint"]
        if not (grafana is None or grafana == ""):
            seldon_utils.add_grafana_dashboard(grafana,client_name,gopts.quiet)
    
def action_list(gopts,command_data, opts):
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

def action_setup(gopts,command_data, opts):
    db_name_to_use = opts.db_name
    client_name_to_setup = opts.client_name
    if db_name_to_use == None:
        print "Need db name to use"
        sys.exit(1)
    if client_name_to_setup == None:
        print "Need client name to setup"
        sys.exit(1)

    # check if this client exists
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    data_fpath = get_data_fpath(zkroot, client_name_to_setup)
    if not os.path.isfile(data_fpath):
        print "Trying to create the client"
        add_client(gopts,command_data,zk_client, zkroot, client_name_to_setup, db_name_to_use)
        add_client_dashboard(gopts,command_data,client_name_to_setup)
    else:
        add_client_dashboard(gopts,command_data,client_name_to_setup)
        print "Client already exists!"

def action_processactions(gopts,command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    def get_valid_client():
        if not is_existing_client(zkroot, client_name):
            print "Invalid client[{client_name}]".format(**locals())
            sys.exit(1)
        return client_name

    def get_valid_input_date_string():
        input_date_string = opts.input_date_string
        if input_date_string == None:
            print "Need input date string!"
            sys.exit(1)
        return input_date_string

    client_name = opts.client_name
    if client_name != None:
        client_name = get_valid_client()

    job_info = command_data["conf_data"]["processactions"]["job_info"]
    if client_name != None:
        job_info["cmd_args"].append("--single-client")
        job_info["cmd_args"].append("%CLIENT_NAME%")

    input_date_string = get_valid_input_date_string()
    replacements = [
        ("%INPUT_DATE_STRING%", input_date_string),
    ]

    def appy_replacements(item):
        for rpair in replacements:
            if rpair[1] != None:
                item = item.replace(rpair[0],rpair[1])
        return item


    cmd_args = job_info["cmd_args"]
    job_info["cmd_args"] = map(appy_replacements, cmd_args)

    spark_utils.run_spark_job(command_data, job_info, client_name)

def action_processevents(gopts,command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    def get_valid_client():
        if not is_existing_client(zkroot, client_name):
            print "Invalid client[{client_name}]".format(**locals())
            sys.exit(1)
        return client_name

    def get_valid_input_date_string():
        input_date_string = opts.input_date_string
        if input_date_string == None:
            print "Need input date string!"
            sys.exit(1)
        return input_date_string

    client_name = opts.client_name
    if client_name != None:
        client_name = get_valid_client()

    job_info = command_data["conf_data"]["processevents"]["job_info"]
    if client_name != None:
        job_info["cmd_args"].append("--single-client")
        job_info["cmd_args"].append("%CLIENT_NAME%")

    input_date_string = get_valid_input_date_string()
    replacements = [
        ("%INPUT_DATE_STRING%", input_date_string),
    ]

    def appy_replacements(item):
        for rpair in replacements:
            if rpair[1] != None:
                item = item.replace(rpair[0],rpair[1])
        return item


    cmd_args = job_info["cmd_args"]
    job_info["cmd_args"] = map(appy_replacements, cmd_args)

    spark_utils.run_spark_job(command_data, job_info, client_name)

def cmd_client(gopts,command_data, command_args):
    actions = {
        "default" : action_list,
        "list" : action_list,
        "setup" : action_setup,
        "processactions" : action_processactions,
        "processevents" : action_processevents,
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        actions["default"](gopts,command_data, opts)
    else:
        if actions.has_key(action):
            actions[action](gopts,command_data, opts)
        else:
            print "Invalid action[{}]".format(action)

