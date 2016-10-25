#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/python/iris_rpc_microservice.py --pipeline /data/iris/xgb_models/1 --model-name model_xgb
