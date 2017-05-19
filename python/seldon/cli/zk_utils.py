import json
from os import walk
import os 
import sys
import errno

def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc: # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else: raise


def is_json_data(data):
    if (data != None) and (len(data)>0):
        return data[0] == '{' or data[0] == '['
    else:
        return False

def push_all_nodes(zk_client,zkroot):
    for (dirpath, dirnames, filenames) in walk(zkroot):
        for filename in filenames:
            file_path = dirpath + "/" + filename
            f = open(file_path)
            data = f.read()
            f.close()
            node_path = file_path.replace(zkroot,"").replace("/_data_","")
            node_set(zk_client,node_path,data)

def get_all_nodes_list(zk_client, start_node, all_nodes_list):
    #print "processing: {}".format(start_node)
    try:
        children = zk_client.get_children(start_node)
        for child in children:
            child = str(child)
            node_path = start_node+"/"+child if start_node != '/' else "/"+child
            all_nodes_list.add(node_path)
            get_all_nodes_list(zk_client, node_path, all_nodes_list)
    except kazoo.exceptions.NoNodeError:
        pass

def write_data_to_file(data_fpath, data):
    json = dict_to_json(data, True) if isinstance(data,dict) else str(data)
    mkdir_p(os.path.dirname(data_fpath))
    f = open(data_fpath,'w')
    f.write(json)
    f.write('\n')
    f.close()
    print "Writing data to file[{data_fpath}]".format(**locals())

def dict_to_json(d, expand=False):
    return json.dumps(d, sort_keys=True, indent=4, separators=(',', ': ')) if expand else json.dumps(d, sort_keys=True, separators=(',',':'))

def json_to_dict(json_data):
    return json.loads(json_data)

def pull_all_nodes(zk_client,zkroot):
    all_nodes_list = set()
    nodes = ["/config","/all_clients"]
    for node in nodes:
        start_node = node
        get_all_nodes_list(zk_client, start_node, all_nodes_list)
    all_nodes_list = list(all_nodes_list)
    for node_path in all_nodes_list:
        if node_path == "/config/topics" or node_path == "/config/clients" or node_path == "/config/changes" or node_path == "/config/users":
            print "Ignoring kafka data node ",node_path
        else:
            print "trying to sync ",node_path
            node_value = node_get(zk_client,node_path)
            if not node_value is None:
                node_value = node_value.strip()
                if is_json_data(node_value):
                    data = json_to_dict(node_value) if node_value != None and len(node_value)>0 else ""
                else:
                    data = str(node_value)
                data_fpath = zkroot + node_path + "/_data_"
                write_data_to_file(data_fpath, data)

def json_compress(json_data):
    d = json.loads(json_data)
    return json.dumps(d, sort_keys=True, separators=(',',':'))

def node_set(zk_client, node_path, node_value):
    if is_json_data(node_value):
        node_value = json_compress(node_value)
    node_value = node_value.strip() if node_value != None else node_value

    if zk_client.exists(node_path):
        retVal = zk_client.set(node_path,node_value)
    else:
        retVal = zk_client.create(node_path,node_value,makepath=True)
    print "updated zk node[{node_path}]".format(node_path=node_path)

def node_get(zk_client, node_path):
    theValue = None
    if zk_client.exists(node_path):
        theValue = zk_client.get(node_path)
        theValue = theValue[0]
    return theValue.strip() if theValue != None else theValue

def node_delete(zk_client, node_path):
    if zk_client.exists(node_path):
        retVal = zk_client.delete(node_path)
        print "deleted zk node[{node_path}]".format(node_path=node_path)
