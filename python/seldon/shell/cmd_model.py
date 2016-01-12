import os
import pprint
from subprocess import call

import zk_utils
import seldon_utils

gdata = {
    'all_clients_node_path': "/all_clients",
    'help_cmd_strs_list' : [
        ("model add <clientName>", "add a model for client"),
        ("model edit <clientName>", "edit a model for client"),
        ("model show <clientName>", "show the added models for client"),
        ("model train <clientName>", "choose model and run offline training")
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

def subcmd_add(command_data):
    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if client_name == None:
        print "Need client name to add model for"
        return

    zkroot = command_data["zkdetails"]["zkroot"]
    if not is_existing_client(zkroot, client_name):
        print "Invalid client[{client_name}]".format(**locals())
        return

    default_models = command_data["default_models"]
    models = default_models.keys()
    for idx,model in enumerate(models):
        print "    [{idx}] {model}".format(**locals())
    q="Choose model: "
    model_idx = None
    try:
        model_idx = int(raw_input(q))
    except ValueError:
        pass

    model_name = models[model_idx] if model_idx != None and model_idx>=0 and model_idx<len(models) else None

    if model_name == None:
        print "Invaild model"
        return

    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline/{model_name}/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)

    zk_client = command_data["zkdetails"]["zk_client"]
    if not os.path.isfile(data_fpath):
        node_path = "{all_clients_node_path}/{client_name}/offline/{model_name}".format(all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)
        if zk_client.exists(node_path):
            write_node_value_to_file(zk_client, zkroot, node_path)
        else:
            default_model_data = default_models[model_name]["config"]
            if default_model_data.has_key("inputPath"):
                default_model_data["inputPath"]=command_data["seldon_models"]
            if default_model_data.has_key("outputPath"):
                default_model_data["outputPath"]=command_data["seldon_models"]
            data = default_model_data
            write_data_to_file(data_fpath, data)
            zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))
    else:
        print "Model [{model_name}] already added".format(**locals())

def subcmd_default(command_data):
    pass

def subcmd_edit(command_data):
    def have_client_name():
        if client_name == None:
            print "Need client name to edit model for"
            return False
        else:
            return True

    def have_valid_client_name():
        if not is_existing_client(zkroot, client_name):
            print "Invalid client[{client_name}]".format(**locals())
            return False
        else:
            return True

    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if not have_client_name(): return
    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]
    if not have_valid_client_name(): return

    models_for_client_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)

    models = os.listdir(models_for_client_fpath)

    for idx,model in enumerate(models):
        print "    [{idx}] {model}".format(**locals())
    q="Choose model: "
    model_idx = None
    try:
        model_idx = int(raw_input(q))
    except ValueError:
        pass

    model_name = models[model_idx] if model_idx != None and model_idx>=0 and model_idx<len(models) else None

    if model_name == None:
        print "Invaild model"
        return

    data_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline/{model_name}/_data_".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)

    #do the edit
    editor=seldon_utils.get_editor()
    call([editor, data_fpath])

    f = open(data_fpath)
    json = f.read()
    f.close()
    data = seldon_utils.json_to_dict(json)

    if data is None:
        print "Invalid model json!"
    else:
        write_data_to_file(data_fpath, data)
        node_path = "{all_clients_node_path}/{client_name}/offline/{model_name}".format(all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name,model_name=model_name)
        pp(node_path)
        zk_utils.node_set(zk_client, node_path, seldon_utils.dict_to_json(data))

def subcmd_show(command_data):
    def have_client_name():
        if client_name == None:
            print "Need client name to edit model for"
            return False
        else:
            return True

    def have_valid_client_name():
        if not is_existing_client(zkroot, client_name):
            print "Invalid client[{client_name}]".format(**locals())
            return False
        else:
            return True

    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if not have_client_name(): return
    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]
    if not have_valid_client_name(): return

    models_for_client_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)

    models = os.listdir(models_for_client_fpath)

    for idx,model in enumerate(models):
        print "    {model}".format(**locals())

def run_spark_job(command_data, job_info, client_name):
    conf_data = command_data["conf_data"]
    spark_home = conf_data["spark_home"]
    seldon_spark_home = conf_data["seldon_spark_home"]
    seldon_version = conf_data["seldon_version"]
    zk_hosts = conf_data["zk_hosts"]

    cmd = job_info["cmd"].replace("%SPARK_HOME%", spark_home)

    cmd_args = job_info["cmd_args"]

    replacements = [
        ("%CLIENT_NAME%", client_name),
        ("%SPARK_HOME%", spark_home),
        ("%SELDON_SPARK_HOME%", seldon_spark_home),
        ("%SELDON_VERSION%", seldon_version),
        ("%ZK_HOSTS%", zk_hosts),
    ]

    def appy_replacements(item):
        for rpair in replacements:
            item = item.replace(rpair[0],rpair[1])
        return item

    cmd_args = map(appy_replacements, cmd_args)

    call([cmd]+cmd_args)

def subcmd_train(command_data):
    def have_client_name():
        if client_name == None:
            print "Need client name to edit model for"
            return False
        else:
            return True

    def have_valid_client_name():
        if not is_existing_client(zkroot, client_name):
            print "Invalid client[{client_name}]".format(**locals())
            return False
        else:
            return True

    client_name = command_data["subcmd_args"][0] if len(command_data["subcmd_args"])>0 else None
    if not have_client_name(): return
    zk_client = command_data["zkdetails"]["zk_client"]
    zkroot = command_data["zkdetails"]["zkroot"]
    if not have_valid_client_name(): return

    models_for_client_fpath = "{zkroot}{all_clients_node_path}/{client_name}/offline".format(zkroot=zkroot,all_clients_node_path=gdata["all_clients_node_path"],client_name=client_name)

    models = os.listdir(models_for_client_fpath)

    for idx,model in enumerate(models):
        print "    [{idx}] {model}".format(**locals())
    q="Choose model: "
    model_idx = None
    try:
        model_idx = int(raw_input(q))
    except ValueError:
        pass

    model_name = models[model_idx] if model_idx != None and model_idx>=0 and model_idx<len(models) else None

    if model_name == None:
        print "Invaild model"
        return

    default_models = command_data["default_models"]
    model_training = default_models[model_name]["training"]
    job_type = model_training["job_type"]
    job_info = model_training["job_info"]

    job_handlers = {
            'spark' : run_spark_job
    }

    if job_handlers.has_key(job_type):
        job_handlers[job_type](command_data, job_info, client_name)
    else:
        print "No handler found for job_type[{job_type}]".format(**locals())

def subcmd_help(command_data):
    lmargin_size=command_data["help_formatting"]["lmargin_size"]
    cmd_size=command_data["help_formatting"]["cmd_size"]
    lmargin_pad=" "
    for help_strs in gdata["help_cmd_strs_list"]:
        cmd_str = help_strs[0]
        cmd_help = help_strs[1]
        print "{lmargin_pad:<{lmargin_size}}{cmd_str:<{cmd_size}} - {cmd_help}".format(**locals())

def cmd_model(args, command_data):
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
    "add" : subcmd_add,
    "edit" : subcmd_edit,
    "train" : subcmd_train,
    "show" : subcmd_show,
}

