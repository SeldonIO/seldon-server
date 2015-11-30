from flask import Blueprint, render_template, jsonify, current_app
from flask import request
import json

predict_blueprint = Blueprint('predict', __name__)

def extract_input():
    client = request.args.get('client')
    if not request.args.get('json') is None:
        jStr = request.args.get('json')
    else:
        jStr = request.form.get('json')
    j = json.loads(jStr)
    input = {
        "client" : client,
        "json" : j
    }
    return input

@predict_blueprint.route('/predict',methods=['GET','POST'])
def do_predict():
    """
    prediction endpoint

    - get recommeder from Flask app config
    - create dataframe from JSON in call
    - call prediction pipeline 
    - get class id mapping
    - construct result
    """
    input = extract_input()
    print input
    pw = current_app.config["seldon_pipeline_wrapper"]
    pipeline = current_app.config["seldon_pipeline"]
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
    ret = { "predictions": formatted_recs_list , "model" : current_app.config['seldon_model_name'] }
    json = jsonify(ret)
    return json
