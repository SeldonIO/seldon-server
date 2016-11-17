#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/rpc_microservice.py --pipeline /seldon-data/seldon-models/tensorflow_deep_mnist/1 --model_name tensorflow_deep_mnist
