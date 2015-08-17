#!/bin/bash

set -o nounset
set -o errexit

vw -i model  -t --daemon --quiet --port 26542 -r raw_predictions.txt
python server.py
#gunicorn -w 8 -b 127.0.0.1:5000 server:app

