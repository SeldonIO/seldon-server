import importlib
from flask import Flask, jsonify
from flask import request
app = Flask(__name__)
import json
import pprint
import pylibmc
import hashlib

app.config.from_object('recommender_config')
_recs_mod = importlib.import_module(app.config['RECOMMENDER_ALG'])
_mc_pool = None

def extract_input():
    user_id = long(request.args.get('user_id'))
    client = request.args.get('client')
    limit = int(request.args.get('limit'))
    exclusion_items = request.args.get('exclusion_items')
    if len(exclusion_items) > 0:
        exclusion_items_list = map(lambda x: long(x), exclusion_items.split(","))
    else:
        exclusion_items_list = []
    recent_interactions = request.args.get('recent_interactions')
    if len(recent_interactions) > 0:
        recent_interactions_list = map(lambda x: long(x), recent_interactions.split(","))
    else:
        recent_interactions_list = []
    data_keys_list = map(lambda x: str(x), request.args.get('data_key').split(","))
    input = {
        "user_id" : user_id,
        "client" : client,
        "limit" : limit,
        "exclusion_items_list" : exclusion_items_list,
        "recent_interactions_list": recent_interactions_list,
        "data_keys_list": data_keys_list
    }
    return input

def format_recs(recs):
    formatted_recs_list=[]
    for i in recs.keys():
        formatted_recs_list.append({
            "item": i,
            "score": recs[i]
        })
    return { "recommended": formatted_recs_list }

def get_data_set(raw_data):
    return set(json.loads(raw_data))

def memcache_set(key,value,time):
    key=str(key)
    key = hashlib.md5(key).hexdigest()
    with _mc_pool.reserve(block=True) as mc:
        mc.set(key,value,time)

def memcache_get(key):
    key=str(key)
    key = hashlib.md5(key).hexdigest()
    value=None
    with _mc_pool.reserve(block=True) as mc:
        value = mc.get(key)
    return value

@app.route('/recommend', methods=['GET'])
def recommend():
    input = extract_input()
    pprint.pprint(input)

    data_set = set()
    for data_key in input['data_keys_list']:
        raw_data = memcache_get(data_key)
        print raw_data
        raw_data = raw_data if raw_data != None else '[]'
        data_set |= get_data_set(raw_data)

    data_set -= set(input['exclusion_items_list'])

    recs = _recs_mod.get_recommendations(
            input['user_id'],
            input['client'],
            input['recent_interactions_list'],
            data_set,
            input['limit']
            )

    f=format_recs(recs)
    json = jsonify(f)
    return json

def mc_init():
    global _mc_pool
    mc_servers = app.config['MEMCACHE']['servers']
    mc_pool_size = app.config['MEMCACHE']['pool_size']
    mc = pylibmc.Client(mc_servers)
    _mc_pool = pylibmc.ClientPool(mc, mc_pool_size)

mc_init()
_recs_mod.init(_mc_pool,app.config)
app.debug = True

if __name__ == "__main__":
    app.run()

