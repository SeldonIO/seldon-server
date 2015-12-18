from flask import Blueprint, render_template, jsonify, current_app
from flask import request
import json


extension_blueprint = Blueprint('extension', __name__)

def extract_input():
    if not request.args.get('json') is None:
        jStr = request.args.get('json')
    else:
        jStr = request.form.get('json')
    j = json.loads(jStr)
    return j

@extension_blueprint.route('/extension',methods=['GET','POST'])
def do_extension():
    """
    extension endpoint

    """
    input = extract_input()
    print "input is ",input
    extension = current_app.config["seldon_extension"]
    preds = extension.predict(input=input)
    print "returned preds",preds
    json = jsonify(preds)
    return json
