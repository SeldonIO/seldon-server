#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <model_url> <model_folder>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
MODEL_URL=$1
MODEL_FOLDER=$2

curl -o "saved_model.ckpt" ${MODEL_URL}
python ${STARTUP_DIR}/create_pipeline.py --model ${MODEL_FOLDER} --load "saved_model.ckpt"