#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 1 ]; then
    echo "need <model_folder>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
MODEL_FOLDER=$1

python ${STARTUP_DIR}/create_pipeline.py --model ${MODEL_FOLDER}