from flask import Flask, jsonify
from flask import request
import pprint
import sys
import pylibmc
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
    data_keys_list = map(lambda x: long(x), request.args.get('data_key').split(","))
    input = {
        "user_id" : user_id,
        "item_id" : item_id,
        "client" : client,
        "limit" : limit,
        "exclusion_items_list" : exclusion_items_list,
        "recent_interactions_list": recent_interactions_list,
        "data_keys_list": data_keys_list
    }
    return input

def get_data_set(raw_data):
    return set(json.loads(raw_data))

def memcache_get(key):
    key=str(key)
    value=None
    mc_pool = _recommender_config['mc_pool']
    with mc_pool.reserve(block=True) as mc:
        value = mc.get(key)
    return value

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
    data_set = set()
    for data_key in input['data_keys_list']:
        raw_data = memcache_get(data_key)
        raw_data = raw_data if raw_data != None else '[]'
        data_set |= get_data_set(raw_data)
    
    data_set -= set(input['exclusion_items_list'])

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
    mc_servers = _recommender_config['memcache']['servers']
    mc_pool_size = _recommender_config['memcache']['pool_size']
    mc = pylibmc.Client(mc_servers)
    mc_pool = pylibmc.ClientPool(mc, mc_pool_size)
    _recommender_config['mc_pool'] = mc_pool
    app.debug = True
    app.run()

