#!/bin/bash

set -o nounset
set -o errexit

python /home/seldon/scripts/start_prediction_microservice.py --pipeline /seldon-data/seldon-models/US_stocks/1 --model_name US_stocks
