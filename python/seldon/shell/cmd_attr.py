import os
import json
import pprint
import errno
import re

import zk_utils
import seldon_utils
import attr_schema_utils

gdata = {
    'all_clients_node_path': "/all_clients",
    'help_cmd_strs_list' : [
        ("attr", "todo"),
        ("attr edit <clientName>", "edit the attributes for client"),
        ("attr show <clientName>", "show the attributes for client"),
        ("attr apply <clientName>", "todo")
    ],
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def write_data_to_file(data_fpath, data):
    json = seldon_utils.dict_to_json(data, True) if isinstance(data,dict) else str(data)
    seldon_utils.mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def write_node_value_to_file(zk_client, zkroot, node_path):
    node_value = zk_utils.node_get(zk_client, node_path)
    node_value = node_value.strip()
    if zk_utils.is_json_data(node_value):
        data = seldon_utils.json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
    else:
        data = str(node_value)
    data_fpath = zkroot + node_path + "/_data_"
    write_data_to_file(data_fpath, data)

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

def subcmd_show(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to show the attr for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_attr(zkroot, zk_client, client_name)

    data_fpath = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/attr/_data_"
    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)
    show_attr(data)

def subcmd_edit(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to edit the attr for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

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

def subcmd_apply(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to apply the attr for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    zk_client = command_data["zkdetails"]["zk_client"]
    ensure_client_has_attr(zkroot, zk_client, client_name)

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

    def get_db_settings():
        dbSettings = {}
        dbSettings["host"]=re.search('://(.*?):(.*?),',db_info["jdbc"]).groups()[0]
        dbSettings["user"]=db_info["user"]
        dbSettings["password"]=db_info["password"]
        return dbSettings

    db_name = get_db_jndi_name()
    db_info = get_db_info(db_name)

    if db_info == None:
        print "Invalid db name[{db_name}]".format(**locals())
        return

    dbSettings = get_db_settings()

    scheme_file_path = zkroot + gdata["all_clients_node_path"] + "/" + client_name + "/attr/_data_"
    clean = True
    attr_schema_utils.create_schema(client_name, dbSettings, scheme_file_path, clean)
    clean = False
    attr_schema_utils.create_schema(client_name, dbSettings, scheme_file_path, clean)

def subcmd_default(command_data):
    print "todo default!"

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_attr(args, command_data):
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
    "edit" : subcmd_edit,
    "apply" : subcmd_apply,
}

