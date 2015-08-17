import importlib
from flask import Flask, jsonify
from flask import request
app = Flask(__name__)
import json
import pprint

app.config.from_object('server_config')
_recs_mod = importlib.import_module(app.config['PREDICTION_ALG'])

def extract_input():
    client = request.args.get('client')
    j = json.loads(request.args.get('json'))
    input = {
        "client" : client,
        "json" : j
    }
    return input

def format_predictions(predictions):
    formatted_recs_list=[]
    for (score,classId,confidence) in predictions:
        formatted_recs_list.append({
            "prediction": score,
            "predictedClass": str(classId),
            "confidence" : confidence
        })
    return { "predictions": formatted_recs_list }

@app.route('/predict', methods=['GET'])
def predict():
    print "predict called"
    input = extract_input()
    print input
    recs = _recs_mod.get_predictions(
            input['client'],
            input['json'])
    print "recs returned ",recs
    f=format_predictions(recs)
    json = jsonify(f)
    return json

_recs_mod.init(app.config)
app.debug = True

if __name__ == "__main__":
    app.run(host="0.0.0.0", debug=True)

