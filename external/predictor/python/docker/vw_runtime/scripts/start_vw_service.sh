#!/bin/bash

set -o nounset
set -o errexit

MODELS_FOLDER=$1

python setup.py --model_path ${MODELS_FOLDER}
vw -i model  -t --daemon --quiet --port 26542 -r raw_predictions.txt
python server.py
#gunicorn -w 8 -b 127.0.0.1:5000 server:app

