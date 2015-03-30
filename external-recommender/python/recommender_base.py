from flask import Flask, jsonify
from flask import request
import pprint
import sys
import memcache
import json

app = Flask(__name__)
_recommender_config = None

def extract_input():
    user_id = long(request.args.get('user_id'))
    item_id = long(request.args.get('item_id'))
    client = request.args.get('client')
    limit = int(request.args.get('limit'))
    exclusion_items = request.args.get('exclusion_items')
    exclusion_items_list = map(lambda x: long(x), exclusion_items.split(","))
    recent_interactions = request.args.get('recent_interactions')
    recent_interactions_list = map(lambda x: long(x), recent_interactions.split(","))
    data_key = request.args.get('data_key')
    input = {
        "user_id" : user_id,
        "item_id" : item_id,
        "client" : client,
        "limit" : limit,
        "exclusion_items_list" : exclusion_items_list,
        "recent_interactions_list": recent_interactions_list,
        "data_key": data_key
    }
    return input

def get_data_set(raw_data):
    return set(json.loads(raw_data))

def get_memcache_client(memcache_config):
    host = memcache_config['host']
    port = memcache_config['port']
    conn_str="%s:%s" % (host,port)
    client = memcache.Client([ conn_str ])
    return client

def memcache_get(memcache_config, key):
    key=str(key)
    client = get_memcache_client(memcache_config)
    return client.get(key)

def format_recs(recs):
    formatted_recs_list=[]
    for i in recs.keys():
        formatted_recs_list.append({
            "item": i,
            "score": recs[i]
        })
    return { "recommended": formatted_recs_list }


@app.route('/recommend', methods=['GET'])
def recommend():
    if _recommender_config == None:
        return '[]'
    alg = _recommender_config['alg']
    if alg == None:
        return '[]'

    input = extract_input()
    pprint.pprint(input)

    memcache_config = _recommender_config['memcache']
    raw_data = memcache_get(memcache_config, input['data_key'])
    raw_data = raw_data if raw_data != None else '[]'
    data_set = get_data_set(raw_data)
    data_set = data_set - set(input['exclusion_items_list'])

    recs = alg(
            input['user_id'],
            input['item_id'],
            input['client'],
            input['recent_interactions_list'],
            data_set,
            input['limit']
            )

    f=format_recs(recs)
    json = jsonify(f)
    return json

def run(recommender_config):
    global _recommender_config
    _recommender_config = recommender_config
    app.debug = True
    app.run()

