#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/scripts/start_prediction_microservice.py --pipeline /data/iris/keras_models/1 --model_name model_keras
