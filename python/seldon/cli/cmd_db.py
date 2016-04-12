import pprint
import argparse
import os
import json
import sys
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
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli db', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False, choices=['show','list','setup','commit'])
    parser.add_argument('--db-name', help="the name of the db", required=False)
    parser.add_argument('--db-user', help="the user", required=False)
    parser.add_argument('--db-password', help="the password for the user", required=False)
    parser.add_argument('--db-jdbc', help="the jdbc string", required=False)
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

def json_to_dict(json_data):
    return json.loads(json_data)

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

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

def action_list(command_data, opts):
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

def action_setup(command_data, opts):
    db_to_setup = opts.db_name
    if db_to_setup == None:
        print "Need db-name to setup!"
        sys.exit(1)

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

    if (opts.db_user != None) and (db_entry["user"] != opts.db_user):
        db_entry["user"] = opts.db_user
        has_any_value_changed = True

    if (opts.db_password != None) and (db_entry["password"] != opts.db_password):
        db_entry["password"] = opts.db_password
        has_any_value_changed = True

    if (opts.db_jdbc != None) and (db_entry["jdbc"] != opts.db_jdbc):
        db_entry["jdbc"] = opts.db_jdbc
        has_any_value_changed = True

    if has_any_value_changed:
        write_data_to_file(data_fpath, data)

def action_show(command_data, opts):
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

def action_commit(command_data, opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + gdata['data_path']
    if not os.path.isfile(data_fpath):
        write_data_to_file(data_fpath, gdata["default_data"])
    f = open(data_fpath)
    data_json = f.read()
    f.close()

    zk_client=command_data["zkdetails"]["zk_client"]
    node_path=gdata["node_path"]
    zk_utils.node_set(zk_client, node_path, data_json)

def cmd_db(gopts,command_data, command_args):
    actions = {
        "default" : action_show,
        "show" : action_show,
        "list" : action_list,
        "setup" : action_setup,
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

