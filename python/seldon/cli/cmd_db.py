import pprint
import argparse
import os
import json

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
    parser = argparse.ArgumentParser(prog='seldon-cli memcached', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def json_to_dict(json_data):
    return json.loads(json_data)

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

def cmd_db(command_data, command_args):
    actions = {
        "default" : action_show,
        "show" : action_show,
        "list" : action_list,
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

