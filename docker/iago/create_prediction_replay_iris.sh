#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 4 ]; then
    echo "need <client name> <file> <num_api_calls> <endpoint>"
    exit -1
fi

CLIENT=$1
FILE=$2
NUM_API=$3
ENDPOINT=$4

seldon-cli -q keys --client-name ${CLIENT} --scope js > key.json
python create_prediction_replay.py --key key.json --replay ${FILE}  --feature '{"name":"f1","type":"numeric","min":0,"max":5}' --feature '{"name":"f2","type":"numeric","min":0,"max":5}' --feature '{"name":"f3","type":"numeric","min":0,"max":5}' --feature '{"name":"f4","type":"numeric","min":0,"max":5}' --num ${NUM_API} --endpoint ${ENDPOINT}
