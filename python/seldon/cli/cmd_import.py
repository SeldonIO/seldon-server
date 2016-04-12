import pprint
import argparse
import os
import sys
import re

import seldon_utils
import import_items_utils
import import_users_utils
import import_actions_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli import', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=True, choices=['items','users','actions'])
    parser.add_argument('--client-name', help="the name of the client", required=True)
    parser.add_argument('--file-path', help="path to the data file", required=True)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def get_db_settings(zkroot, client_name):
    def get_db_jndi_name():
        data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/_data_"
        f = open(data_fpath)
        json = f.read()
        data = seldon_utils.json_to_dict(json)
        f.close()
        DB_JNDI_NAME = data["DB_JNDI_NAME"] if isinstance(data, dict) and data.has_key("DB_JNDI_NAME") else ""
        return DB_JNDI_NAME

    def get_db_info(db_name):
        data_fpath = zkroot + "/config/dbcp/_data_"
        f = open(data_fpath)
        json = f.read()
        data = seldon_utils.json_to_dict(json)
        f.close()

        db_info = None
        for db_info_entry in data['dbs']:
            if db_info_entry['name'] == db_name:
                db_info = db_info_entry
                break
        return db_info

    db_name = get_db_jndi_name()
    db_info = get_db_info(db_name)

    if db_info == None:
        print "Invalid db name[{db_name}]".format(**locals())
        return None

    dbSettings = {}
    dbSettings["host"]=re.search('://(.*?):(.*?),',db_info["jdbc"]).groups()[0]
    dbSettings["user"]=db_info["user"]
    dbSettings["password"]=db_info["password"]
    return dbSettings

def action_items(command_data, opts):
    client_name = opts.client_name
    data_file_fpath = opts.file_path

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        sys.exit(1)

    db_settings = get_db_settings(zkroot, client_name)

    import_items_utils.import_items(client_name, db_settings, data_file_fpath)

def action_users(command_data, opts):
    client_name = opts.client_name
    data_file_fpath = opts.file_path

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        sys.exit(1)

    db_settings = get_db_settings(zkroot, client_name)

    import_users_utils.import_users(client_name, db_settings, data_file_fpath)

def action_actions(command_data, opts):
    client_name = opts.client_name
    data_file_fpath = opts.file_path

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        sys.exit(1)

    db_settings = get_db_settings(zkroot, client_name)

    out_file_dir = command_data["conf_data"]["seldon_models"] + "/" + client_name + "/actions/1"
    out_file_fpath = out_file_dir + "/actions.json"

    seldon_utils.mkdir_p(out_file_dir)

    import_actions_utils.import_actions(client_name, db_settings, data_file_fpath, out_file_fpath)

def cmd_import(gopts,command_data, command_args):
    actions = {
        "items" : action_items,
        "users" : action_users,
        "actions" : action_actions,
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
