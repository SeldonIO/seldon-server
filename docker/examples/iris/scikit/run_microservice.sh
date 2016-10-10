#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/scripts/start_prediction_microservice.py --pipeline /data/iris/scikit_models/1 --model_name model_scikit
