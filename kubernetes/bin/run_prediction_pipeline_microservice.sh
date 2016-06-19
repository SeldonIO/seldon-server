#!/bin/bash

set -o nounset
set -o errexit
set -v

if [ "$#" -ne 4 ]; then
    echo "need <microservice_name> <pipeline_model_folder> <client> <num_replicas>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
NAME=$1
PIPELINE_FOLDER=$2
CLIENT=$3
NUM_REPLICAS=$4

function create_microservice_conf {
    
    mkdir -p ${STARTUP_DIR}/../conf/microservices
    if test -f "${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json"; 
    then 
	echo "The microservice already exists. Will make a backup to .prev";
	cp ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json.prev
    fi
    cat ${STARTUP_DIR}/../conf/microservice_pipeline.template | sed -e "s|%NAME%|${NAME}|" | sed -e "s|%PIPELINE_FOLDER%|${PIPELINE_FOLDER}|" | sed -e "s|%REPLICAS%|${NUM_REPLICAS}|"  > ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}


function run_microservice {

    kubectl apply -f ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}

function configure_seldon {

    ${STARTUP_DIR}/seldon-cli predict_alg --action delete --client-name ${CLIENT} --predictor-name externalPredictionServer
    ${STARTUP_DIR}/seldon-cli predict_alg  --action add --client-name ${CLIENT} --predictor-name externalPredictionServer --config io.seldon.algorithm.external.url=http://${NAME}:5000/predict --config io.seldon.algorithm.external.name=${NAME}
    ${STARTUP_DIR}/seldon-cli predict_alg --action commit --client-name ${CLIENT}

}


function start_microservice {

    create_microservice_conf

    run_microservice
    
    configure_seldon

}


start_microservice "$@"




