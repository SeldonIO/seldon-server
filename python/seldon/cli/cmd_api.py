import pprint
import argparse
import os
import sys
import re
import json
import MySQLdb
import requests

import db_utils

gdata = {
    'all_clients_node_path': "/all_clients",
}

def pp(o):
    p = pprint.PrettyPrinter(indent=4)
    p.pprint(o)

def getOpts(args):
    parser = argparse.ArgumentParser(prog='seldon-cli api', description='Seldon CLI')
    parser.add_argument('--action', help="the action to use", required=False, choices=['call'])
    parser.add_argument('--client-name', help="the name of the client", required=True)
    parser.add_argument('--endpoint', help="api to use", required=True, choices=['/js/action/new','/js/recommendations','/js/predict','/js/event/new','/actions','/users/recommendations','/predict','/events',"/items"])
    parser.add_argument('--method', help="http method", required=False, default='GET', choices=['GET','POST'])
    parser.add_argument('--user', help="user", required=False, default="1")
    parser.add_argument('--item', help="item", required=False)
    parser.add_argument('--type', help="type", required=False, default="1")
    parser.add_argument('--limit', help="limit", required=False, default="5")
    parser.add_argument('--json', help="json", required=False, default="{}")
    parser.add_argument('--dimensions', help="dimensions", required=False, action='append')
    parser.add_argument('--full', help="whether to return full attributes true|false", required=False)
    parser.add_argument('--attributes', help="attributes", required=False, action='append')
    parser.add_argument('args', nargs=argparse.REMAINDER) # catch rest (non-options) as args
    opts = parser.parse_args(args)
    return opts

def json_to_dict(json_data):
    return json.loads(json_data)


def get_auth(command_data,opts):
    zkroot = command_data["zkdetails"]["zkroot"]
    data_fpath = zkroot + "/config/dbcp/_data_"
    f = open(data_fpath)
    jsonStr = f.read()
    data = json_to_dict(jsonStr)
    f.close()

    if opts.endpoint.startswith("/js/"):
        scope = "js"
    else:
        scope = "all"

    db_info = None
    for db_info in data['dbs']:
        dbSettings = {}
        dbSettings["host"]=re.search('://(.*?):(.*?),',db_info["jdbc"]).groups()[0]
        dbSettings["user"]=db_info["user"]
        dbSettings["password"]=db_info["password"]
        dbSettings["name"] = db_info["name"]
        res = db_utils.get_keys(dbSettings,opts.client_name,scope)
        if len(res)>0:
            break

    if len(res) == 1:
        return res[0]
    else:
        print "Failed to find single auth key"
        print json.dumps(res)
        sys.exit(1)

def get_action_params(opts,params):
    params["user"] = opts.user
    if opts.item is None:
        params["item"] = 1
    else:
        params["item"] = opts.item
    params["type"] = opts.type
    return params

def get_js_recommend_params(opts,params):
    params["user"] = opts.user
    if not opts.item is None:
        params["item"] = opts.item
    if not opts.dimensions is None:
        params["dimensions"] = opts.dimensions
    params["type"] = opts.type
    params["limit"] = opts.limit
    if not opts.attributes is None:
        params["attributes"] = opts.attributes
    return params

def get_oauth_recommend_params(opts,params):
    if not opts.item is None:
        params["item"] = opts.item
    if not opts.dimensions is None:
        params["dimensions"] = opts.dimensions
    params["type"] = opts.type
    params["limit"] = opts.limit
    return params

def get_js_predict_params(opts,params):
    params["json"] = opts.json
    return params

def get_oauth_items_params(opts,params):
    if not opts.full is None:
        params["full"] = opts.full
    return params

def call_js(gopts,command_data,opts,auth):
    params = {}
    params["consumer_key"] = auth['key']
    params["jsonpCallback"]= "j"
    if opts.endpoint == "/js/action/new":
        params = get_action_params(opts,params)
    elif opts.endpoint == "/js/recommendations":
        params = get_js_recommend_params(opts,params)
    elif opts.endpoint == "/js/predict":
        params = get_js_predict_params(opts,params)
    elif opts.endpoint == '/js/event/new':
        params = get_js_predict_params(opts,params)
    url = command_data['conf_data']["server_endpoint"] + opts.endpoint
    r = requests.get(url,params=params)
    if not gopts.quiet:
        print "response code",r.status_code
    if r.status_code == requests.codes.ok:
        res = re.sub(r'^j\(',"",r.text)
        res = re.sub(r'\)$',"",res)
        print res

def call_oauth(gopts,command_data,opts,token):
    data = {}
    params = {}
    params['oauth_token'] = token
    headers = {}
    headers["content-type"] = "application/json"
    if opts.endpoint == "/actions":
        data = get_action_params(opts,data)
        url = command_data['conf_data']["server_endpoint"] + opts.endpoint
        r = requests.post(url,data=json.dumps(data),params=params,headers=headers)
    elif opts.endpoint == "/users/recommendations":
        params = get_oauth_recommend_params(opts,params)
        url = command_data['conf_data']["server_endpoint"] + "/users/"+opts.user+"/recommendations"
        r = requests.get(url,params=params)
    elif opts.endpoint == "/predict":
        url = command_data['conf_data']["server_endpoint"] + opts.endpoint
        r = requests.post(url,data=opts.json,params=params,headers=headers)
    elif opts.endpoint == "/events":
        url = command_data['conf_data']["server_endpoint"] + opts.endpoint
        r = requests.post(url,data=opts.json,params=params,headers=headers)
    elif opts.endpoint == "/items" and opts.method == 'GET':
        params = get_oauth_items_params(opts,params)
        url = command_data['conf_data']["server_endpoint"] + opts.endpoint
        r = requests.get(url,params=params)
    elif opts.endpoint == "/items" and opts.method == 'POST':
        url = command_data['conf_data']["server_endpoint"] + opts.endpoint
        r = requests.post(url,data=opts.json,params=params,headers=headers)
    else:
        print "unknown endpoint and method"
        sys.exit(1)

    if not gopts.quiet:
        print "response code",r.status_code
    if r.status_code == requests.codes.ok:
        res = re.sub(r'^j\(',"",r.text)
        res = re.sub(r'\)$',"",res)
        print res
    

def get_token(gopts,command_data,opts,auth):
    params = {}
    params["consumer_key"] = auth["key"]
    params["consumer_secret"] = auth["secret"]
    url = command_data['conf_data']["server_endpoint"] + "/token"
    r = requests.get(url,params=params)
    if not gopts.quiet:
        print "response code",r.status_code
    if r.status_code == requests.codes.ok:
        res = re.sub(r'^j\(',"",r.text)
        res = re.sub(r'\)$',"",res)
        return json_to_dict(res)['access_token']
        

def action_call(gopts,command_data,opts):
    auth = get_auth(command_data,opts)
    if opts.endpoint.startswith("/js"):
        call_js(gopts,command_data,opts,auth)
    else:
        token = get_token(gopts,command_data,opts,auth)
        call_oauth(gopts,command_data,opts,token)

def cmd_api(gopts,command_data, command_args):
    actions = {
        "default" : action_call,
        "call" : action_call
    }

    opts = getOpts(command_args)

    action = opts.action
    if action == None:
        actions["default"](gopts,command_data, opts)
    else:
        if actions.has_key(action):
            actions[action](gopts,command_data, opts)
        else:
            print "Invalid action[{}]".format(action)
