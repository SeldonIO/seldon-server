#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 5 ]; then
    echo "need <client name> <file> <num_items> <num_api_calls> <endpoint>"
    exit -1
fi

CLIENT=$1
FILE=$2
NUM_ITEMS=$3
NUM_ACTIONS=$4
ENDPOINT=$5

echo "get js keys"
seldon-cli -q keys --client-name ${CLIENT} --scope js > key.json
echo "get items"
seldon-cli -q api --client-name ${CLIENT} --endpoint /items --limit ${NUM_ITEMS} > items.json
echo "create replay"
python create_recommendation_replay.py --key key.json --items items.json --replay ${FILE} --num-actions ${NUM_ACTIONS} --endpoint ${ENDPOINT}
