import sys
import importlib
from flask import Flask, jsonify
from flask import request
app = Flask(__name__)
import json
import pprint
from sklearn.pipeline import Pipeline
import seldon.pipeline.util as sutl
import random


app.config.from_object('server_config')
rint = random.randint(1,999999)
if 'AWS_KEY' in app.config:
    pw = sutl.Pipeline_wrapper(work_folder='/tmp/pl_'+str(rint),aws_key=app.config['AWS_KEY'],aws_secret=app.config['AWS_SECRET'])
else:
    pw = sutl.Pipeline_wrapper(work_folder='/tmp/pl_'+str(rint))
pipeline = pw.load_pipeline(app.config['PIPELINE'])

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
    input = extract_input()
    print input
    df = pw.create_dataframe(input["json"])
    preds = pipeline.predict_proba(df)
    idMap = pipeline._final_estimator.get_class_id_map()
    formatted_recs_list=[]
    for index, proba in enumerate(preds[0]):
        if index in idMap:
            indexName = idMap[index]
        else:
            indexName = str(index)
        formatted_recs_list.append({
            "prediction": str(proba),
            "predictedClass": indexName,
            "confidence" : str(proba)
        })
    ret = { "predictions": formatted_recs_list , "model" : app.config['MODEL'] }
    json = jsonify(ret)
    return json


if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)
