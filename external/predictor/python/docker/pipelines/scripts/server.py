import sys, getopt, argparse
import importlib
from flask import Flask, jsonify
from flask import request
app = Flask(__name__)
import json
import pprint
from sklearn.pipeline import Pipeline
import seldon.pipeline.util as sutl
import pandas as pd

def extract_input():
    client = request.args.get('client')
    j = json.loads(request.args.get('json'))
    input = {
        "client" : client,
        "json" : j
    }
    return input


@app.route('/predict', methods=['GET'])
def predict():
    print "predict called"
    input = extract_input()
    print input,args.pipeline
    df = pw.create_dataframe(input["json"])
    print df
    preds = pipeline.predict_proba(df)
    print preds
    idMap = pipeline._final_estimator.get_class_id_map()
    formatted_recs_list=[]
    for index, proba in enumerate(preds[0]):
        print index,proba
        if index in idMap:
            indexName = idMap[index]
        else:
            indexName = str(index)
        formatted_recs_list.append({
            "prediction": str(proba),
            "predictedClass": indexName,
            "confidence" : str(proba)
        })
    ret = { "predictions": formatted_recs_list , "model" : args.model_name }
    json = jsonify(ret)
    return json


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog='microservice')
    parser.add_argument('--model_name', help='name of model', required=True)
    parser.add_argument('--pipeline', help='location of prediction pipeline', required=True)
    parser.add_argument('--aws_key', help='aws key', required=False)
    parser.add_argument('--aws_secret', help='aws secret', required=False)

    args = parser.parse_args()
    opts = vars(args)

    pw = sutl.Pipeline_wrapper(aws_key=args.aws_key,aws_secret=args.aws_secret)
    pipeline = pw.load_pipeline(args.pipeline)

    app.run(host="0.0.0.0", debug=True)
