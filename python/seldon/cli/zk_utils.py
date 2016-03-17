import json

def is_json_data(data):
    if (data != None) and (len(data)>0):
        return data[0] == '{' or data[0] == '['
    else:
        return False

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

