import random, math
import operator
from collections import defaultdict
from seldon.xgb import *
import seldon.pipeline.pipelines as pl
import json


def init(config):
    print "initialised"
    global tailq
    xgboost_config = config['XGBOOST']
    global pipeline
    pipeline = pl.Pipeline(local_models_folder="models",models_folder=xgboost_config['featuresPath'],aws_key=xgboost_config.get("awsKey",None),aws_secret=xgboost_config.get("awsSecret",None))
    pipeline.transform_init()
    global modelLocation
    modelLocation = xgboost_config["modelPath"]
    global xgSeldon 
    xgSeldon = XGBoostSeldon(**xgboost_config)
    xgSeldon.load_models()

def score(json):
    print json
    jsonTransformed = pipeline.transform_json(json)
    print jsonTransformed
    return xgSeldon.predict_json(jsonTransformed)

def get_predictions(client,json):
    return (score(json),modelLocation)



