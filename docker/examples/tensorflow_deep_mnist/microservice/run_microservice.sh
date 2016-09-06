#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/scripts/start_prediction_microservice.py --pipeline /home/seldon/deep_mnist_pipeline --model_name tensorflow_deep_mnist
