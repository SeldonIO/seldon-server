from flask import Blueprint, render_template, jsonify, current_app
from flask import request
import json

recommend_blueprint = Blueprint('recommend', __name__)

def extract_input():
    user_id = long(request.args.get('user_id'))
    client = request.args.get('client')
    limit = int(request.args.get('limit'))
    exclusion_items = request.args.get('exclusion_items')
    if not exclusion_items is None and len(exclusion_items) > 0:
        exclusion_items_list = map(lambda x: long(x), exclusion_items.split(","))
    else:
        exclusion_items_list = []
    recent_interactions = request.args.get('recent_interactions')
    if not recent_interactions is None and len(recent_interactions) > 0:
        recent_interactions_list = map(lambda x: long(x), recent_interactions.split(","))
    else:
        recent_interactions_list = []
    data_keys = request.args.get('data_key')
    if not data_keys is None:
        data_keys_list = map(lambda x: str(x), data_keys.split(","))
    else:
        data_keys_list = []
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
    for (item,score) in recs:
        formatted_recs_list.append({
            "item": item,
            "score": score
        })
    return { "recommended": formatted_recs_list }

def get_data_set(raw_data):
    return set(json.loads(raw_data))

def memcache_get(key):
    key=str(key)
    value=None
    _mc_pool = current_app.config.get("seldon_memcache")
    if not _mc_pool is None:
        with _mc_pool.reserve(block=True) as mc:
            value = mc.get(key)
        return value
    else:
        return None


@recommend_blueprint.route('/recommend',methods=['GET','POST'])
def do_recommend():
    """
    recommendation endpoint

    - extract parameters from call
    - extract items to score from any data keys provided
    - get recommender from Flask app config
    - call recommender
    - construct JSON response
    """
    input = extract_input()

    data_set = set()
    for data_key in input['data_keys_list']:
        raw_data = memcache_get(data_key)
        raw_data = raw_data if raw_data != None else '[]'
        data_set |= get_data_set(raw_data)

    data_set -= set(input['exclusion_items_list'])

    recommender = current_app.config["seldon_recommender"]
    recs = recommender.recommend(input['user_id'],ids=data_set,recent_interactions=input['recent_interactions_list'],limit=input['limit'],client=input['client'])

    f=format_recs(recs)
    json = jsonify(f)
    return json


