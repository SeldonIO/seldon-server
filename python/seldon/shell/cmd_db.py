import pprint
import cmdutils
import json
import re
import os
import errno
import copy

import zk_utils

gdata = {
    'data_path': "/config/dbcp/_data_",
    'node_path': "/config/dbcp",
    'default_data': {
        'dbs': [
            {
                'jdbc': "jdbc:mysql:replication://127.0.0.1:3306,127.0.0.1:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true",
                'password': "mypass",
                "name": "ClientDB",
                'user': "root"
            }
         ]
    },
    'help_cmd_strs_list' : [
        ("db", "show the currently setup databases"),
        ("db setup <SomeDb>", "setup the db given"),
        ("db commit", "commit changes in file to zookeeper")
    ],
}

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

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def ensure_local_data_file_exists(zk_client, zkroot):
    data_fpath = zkroot + gdata['data_path']
    if not os.path.isfile(data_fpath):
        node_path=gdata["node_path"]
        node_value = zk_utils.node_get(zk_client, node_path)
        data = json_to_dict(node_value) if node_value != None else gdata["default_data"]
        write_data_to_file(data_fpath, data)

def subcmd_setup(command_data):
    def get_input_var(var_name, var_current_value):
        var_current_value_to_display = (var_current_value[:45] + '...') if len(var_current_value) > 45 else var_current_value
        q="{var_name}[{var_current_value}]:".format(var_name=var_name,var_current_value=var_current_value_to_display)
        var_val = raw_input(q)
        var_val = var_val.strip()
        is_changed = len(var_val) > 0
        var_val = var_val if is_changed else var_current_value
        return var_val,is_changed

    db_to_setup = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if db_to_setup == None:
        print "Need db name to setup"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)
    print "Setting up Databases"

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    has_any_value_changed = False

    db_entries_found = [db_entry for db_entry in data["dbs"] if db_entry["name"]==db_to_setup]
    db_entry = db_entries_found[0] if len(db_entries_found)>0 else None

    if db_entry == None:
        db_entry = copy.deepcopy(gdata["default_data"]["dbs"][0])
        db_entry["name"] = db_to_setup
        data["dbs"].append(db_entry)
        has_any_value_changed = True

    name,is_changed = get_input_var("name",db_entry["name"]); has_any_value_changed |= is_changed
    jdbc,is_changed = get_input_var("jdbc",db_entry["jdbc"]); has_any_value_changed |= is_changed
    user,is_changed = get_input_var("user",db_entry["user"]); has_any_value_changed |= is_changed
    password,is_changed = get_input_var("password",db_entry["password"]); has_any_value_changed |= is_changed

    db_entry["name"] = name
    db_entry["jdbc"] = jdbc
    db_entry["user"] = user
    db_entry["password"] = password

    if has_any_value_changed:
        write_data_to_file(data_fpath, data)

def subcmd_delete(command_data):
    db_to_delete = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if db_to_delete == None:
        print "Need db name to delete"
        return

    zkroot = command_data["zkdetails"]["zkroot"]

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    index_found_to_delete = [idx for idx,db_entry in enumerate(data["dbs"]) if db_entry["name"]==db_to_delete]
    if len(index_found_to_delete) >0:
        idx_to_delete = index_found_to_delete[0]
        del data["dbs"][idx_to_delete]
        print "Removed the entry for {db_to_delete}".format(**locals())
        write_data_to_file(data_fpath, data)
    else:
        print "No entry for {db_to_delete} found!".format(**locals())

def subcmd_list(command_data):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    db_list = [str(db['name']) for db in data['dbs']]
    list_str = ", ".join(db_list)
    print list_str

def subcmd_default(command_data):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()
    dbs = data['dbs']
    for db in dbs:
        name = db["name"]
        jdbc = db["jdbc"]
        user = db["user"]
        password = db["password"]
        print "db[{name}]:".format(**locals())
        print "    jdbc: {jdbc}".format(**locals())
        print "    user: {user}".format(**locals())
        print "    password: {password}".format(**locals())

def subcmd_commit(command_data):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    data_fpath = zkroot + gdata['data_path']
    ensure_local_data_file_exists(zk_client, zkroot)

    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client=command_data["zkdetails"]["zk_client"]
    node_path=gdata["node_path"]
    zk_utils.node_set(zk_client, node_path, data_json)

def cmd_db(args, command_data):
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

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

subcmds = {
    "help" : subcmd_help,
    "setup" : subcmd_setup,
    "list" : subcmd_list,
    "commit" : subcmd_commit,
    "delete" : subcmd_delete
}

