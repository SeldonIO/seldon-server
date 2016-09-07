#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/scripts/start_prediction_microservice.py --pipeline /seldon-data/seldon-models/tensorflow_deep_mnist/1 --model_name tensorflow_deep_mnist
