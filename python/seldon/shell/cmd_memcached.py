import pprint
import cmdutils
import json
import re
import os
import errno
import zk_utils

gdata = {
    'data_path': "/config/memcached/_data_",
    'node_path': "/config/memcached",
    'default_data': {
        'numClients': 1,
        'servers': "localhost:11211"
    },
    'help_cmd_strs_list' : [
        ("memcached", "shows the current setup"),
        ("memcached setup", "change the current setup - write chnages to file"),
        ("memcached commit", "commit changes in file to zookeeper")
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

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def subcmd_commit(command_data):
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

def subcmd_setup(command_data):
    zkroot = command_data["zkdetails"]["zkroot"]
    def get_input_var(var_name, var_current_value):
        q="{var_name}[{var_current_value}]:".format(var_name=var_name,var_current_value=var_current_value)
        var_val = raw_input(q)
        var_val = var_val.strip()
        is_changed = len(var_val) > 0
        var_val = var_val if is_changed else var_current_value
        return var_val,is_changed

    print "Setting up memcached"
    data_fpath = zkroot + gdata['data_path']
    zk_client=command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()

    has_any_value_changed = False

    numClients,is_changed = get_input_var("numClients",data["numClients"])
    numClients = int(numClients)
    has_any_value_changed = has_any_value_changed | is_changed

    servers,is_changed = get_input_var("servers",data["servers"])
    has_any_value_changed = has_any_value_changed | is_changed

    data = gdata["default_data"]
    data["numClients"] = numClients
    data["servers"] = servers
    if has_any_value_changed:
        write_data_to_file(data_fpath, data)

def ensure_local_data_file_exists(zk_client, zkroot):
    data_fpath = zkroot + gdata['data_path']
    if not os.path.isfile(data_fpath):
        node_path=gdata["node_path"]
        node_value = zk_utils.node_get(zk_client, node_path)
        data = json_to_dict(node_value) if node_value != None else gdata["default_data"]
        write_data_to_file(data_fpath, data)

def subcmd_default(command_data):
    zkroot = command_data["zkdetails"]["zkroot"]
    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_local_data_file_exists(zk_client, zkroot)

    data_fpath = zkroot + gdata['data_path']
    f = open(data_fpath)
    json = f.read()
    data = json_to_dict(json)
    f.close()
    print "memcached:"
    print "    numClients: {numClients}".format(numClients=data["numClients"])
    print "    servers: {servers}".format(servers=data["servers"])

def subcmd_test(command_data):
    print "-- Testing --"
    zk_client=command_data["zkdetails"]["zk_client"]
    node_path=gdata["node_path"]+'x'
    node_value = zk_utils.node_get(zk_client, node_path)
    pp(node_value)

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_memcached(subcmd, command_data):
    if subcmd == "":
        subcmd_default(command_data)
    else:
        if subcmds.has_key(subcmd):
            subcmds[subcmd](command_data)
        else:
            print "unkown subcmd[%s]" % subcmd

subcmds = {
    "help" : subcmd_help,
    "setup" : subcmd_setup,
    "commit" : subcmd_commit,
    "test" : subcmd_test
}

