#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 5 ]; then
    echo "need <microservice_name_1> <microservice_image_1> <microservice_name_2> <microservice_image_2> <client>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
NAME1=$1
IMAGE1=$2
NAME2=$3
IMAGE2=$4
CLIENT=$5

function create_microservice_conf () {
    NAME=$1
    IMAGE=$2
    mkdir -p ${STARTUP_DIR}/../conf/microservices
    if test -f "${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json"; 
    then 
	echo "The microservice ${NAME} already exists. Will make a backup to .prev";
	cp ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json.prev
    fi
    cat ${STARTUP_DIR}/../conf/microservice.json.in | sed -e "s|%NAME%|${NAME}|" | sed -e "s|%IMAGE%|${IMAGE}|" > ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}


function run_microservice () {
    NAME=$1
    kubectl apply -f ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}

function configure_seldon {

    cat <<EOF | ${STARTUP_DIR}/seldon-cli predict_alg --action create --client-name ${CLIENT} -f -
{
    "variations": [
        {
            "config": {
                "algorithms": [
                    {
                        "config": [
                            {
                                "name": "io.seldon.algorithm.external.url",
                                "value": "http://${NAME1}:5000/predict"
                            },
                            {
                                "name": "io.seldon.algorithm.external.name",
                                "value": "${NAME1}"
                            }
                        ],
                        "name": "externalPredictionServer"
                    }
                ]
            },
            "label": "${NAME1}",
            "ratio": 0.5
        },
        {
            "config": {
                "algorithms": [
                    {
                        "config": [
                            {
                                "name": "io.seldon.algorithm.external.url",
                                "value": "http://${NAME2}:5000/predict"
                            },
                            {
                                "name": "io.seldon.algorithm.external.name",
                                "value": "${NAME2}"
                            }
                        ],
                        "name": "externalPredictionServer"
                    }
                ]
            },
            "label": "${NAME2}",
            "ratio": 0.5
        }
    ]
}
EOF
    ${STARTUP_DIR}/seldon-cli predict_alg --action commit --client-name ${CLIENT}

}


function start_microservice {

    create_microservice_conf $NAME1 $IMAGE1
    create_microservice_conf $NAME2 $IMAGE2

    run_microservice $NAME1
    run_microservice $NAME2
    
    configure_seldon

}


start_microservice "$@"




