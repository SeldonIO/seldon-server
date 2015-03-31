import importlib
from flask import Flask, jsonify
app = Flask(__name__)
import json

app.config.from_object('recommender_config')
_recs_mod = importlib.import_module(app.config['RECOMMENDER_ALG'])

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

@app.route('/recommend', methods=['GET'])
def recommend():
    recs = _recs_mod.get_recommendations()
    f=format_recs(recs)
    json = jsonify(f)
    return json

if __name__ == "__main__":
    app.run(debug=True)

