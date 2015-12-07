import os
import pprint
import seldon_utils
import re

import import_items_utils
import import_users_utils
import import_actions_utils

gdata = {
    'all_clients_node_path': "/all_clients",
    'help_cmd_strs_list' : [
        ("import", "todo"),
        ("import items <clientName> </path/to/items.csv>", "import items for client"),
        ("import users <clientName> </path/to/users.csv>", "import users for client"),
        ("import actions <clientName> </path/to/actions.csv>", "import actions for client")
    ],
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

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

def subcmd_items(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to add model for"
        return

    data_file_fpath = command_data["subcmd_args"][1] if len(command_data["subcmd_args"])>1 else None
    if data_file_fpath == None:
        print "Need data file path to import"
        return

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        return

    db_settings = get_db_settings(zkroot, client_name)

    import_items_utils.import_items(client_name, db_settings, data_file_fpath)

def subcmd_users(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to add model for"
        return

    data_file_fpath = command_data["subcmd_args"][1] if len(command_data["subcmd_args"])>1 else None
    if data_file_fpath == None:
        print "Need data file path to import"
        return

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        return

    db_settings = get_db_settings(zkroot, client_name)

    import_users_utils.import_users(client_name, db_settings, data_file_fpath)

def subcmd_actions(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to add model for"
        return

    data_file_fpath = command_data["subcmd_args"][1] if len(command_data["subcmd_args"])>1 else None
    if data_file_fpath == None:
        print "Need data file path to import"
        return

    zkroot = command_data["conf_data"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    if not os.path.isfile(data_file_fpath):
        print "Invalid file[{data_file_fpath}]".format(**locals())
        return

    db_settings = get_db_settings(zkroot, client_name)

    out_file_dir = command_data["conf_data"]["seldon_models"] + "/" + client_name + "/actions/1"
    out_file_fpath = out_file_dir + "/actions.json"

    seldon_utils.mkdir_p(out_file_dir)

    import_actions_utils.import_actions(client_name, db_settings, data_file_fpath, out_file_fpath)

def subcmd_default(command_data):
    print "todo default!"

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]+10
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_import(args, command_data):
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
    "items" : subcmd_items,
    "users" : subcmd_users,
    "actions" : subcmd_actions,
}

