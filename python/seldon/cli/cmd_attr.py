import pprint
import argparse
import sys
import os

import seldon_utils
import zk_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli client', description='Seldon Cli')
    parser.add_argument('--action', help="the action to use", required=False)
    parser.add_argument('--client-name', help="the name of the client", required=False)
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def is_existing_client(zkroot, client_name):
    client_names = os.listdir(zkroot + gdata["all_clients_node_path"])
    if client_name in client_names:
        return True
    else:
        return False

def ensure_client_has_attr(zkroot, zk_client, client_name):
    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/attr/_data_".format(
            zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)
    if not os.path.isfile(data_fpath):
        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/attr"
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_attr_json = '{"types":[{"type_attrs":[{"name":"title","value_type":"string"}],"type_id":1,"type_name":"defaulttype"}]}'
            data = seldon_utils.json_to_dict(default_attr_json)
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))

def write_data_to_file(data_fpath, data):
    json = seldon_utils.dict_to_json(data, True) if isinstance(data,dict) else str(data)
    seldon_utils.mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def show_attr(data):
    attr_types = data["types"]
    print "types:"
    for attr_type in attr_types:
        attr_type_name = attr_type["type_name"]
        attr_type_id = attr_type["type_id"]
        attr_type_attrs = attr_type["type_attrs"]
        print "   [{attr_type_name}]".format(**locals())
        print "       type_id: {attr_type_id}".format(**locals())
        print "       type_attrs:"
        for attr_type_attr in attr_type_attrs:
            attrib_name = attr_type_attr["name"]
            attrib_value = attr_type_attr["value_type"]
            attrib_value_str = "enum["+",".join(attrib_value)+"]" if isinstance(attrib_value,list) else attrib_value
            print "           {attrib_name}: {attrib_value_str}".format(**locals())

def write_node_value_to_file(zk_client, zkroot, node_path):
    node_value = zk_utils.node_get(zk_client, node_path)
    node_value = node_value.strip()
    if zk_utils.is_json_data(node_value):
        data = seldon_utils.json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
    else:
        data = str(node_value)
    data_fpath = zkroot + node_path + "/_data_"
    write_data_to_file(data_fpath, data)

def action_show(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the attr for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_attr(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/attr/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)
    show_attr(data)

def action_edit(command_data, opts):
    client_name = opts.client_name
    if client_name == None:
        print "Need client name to show the attr for"
        sys.exit(1)

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        sys.exit(1)

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_attr(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/attr/_data_"
    #do the edit
    from subprocess import call
    editor=seldon_utils.get_editor()
    call([editor, data_fpath])

    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)

    if data is None:
        print "Invalid attr json!"
    else:
        write_data_to_file(data_fpath, data)
        node_path = gdata["all_clients_node_path"]+"/"+client_name+"/attr"
        zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))
        show_attr(data)

def cmd_attr(command_data, command_args):
    actions = {
        "default" : action_show,
        "show" : action_show,
        "edit" : action_edit,
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

