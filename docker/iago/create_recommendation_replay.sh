#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <client name> <file>"
    exit -1
fi

CLIENT=$1
FILE=$2

seldon-cli -q keys --client-name ${CLIENT} --scope js > key.json
seldon-cli -q api --client-name ${CLIENT} --endpoint /items --limit 10 --full true > items.json
python create_recommendation_replay.py --key key.json --items items.json --replay ${FILE}
