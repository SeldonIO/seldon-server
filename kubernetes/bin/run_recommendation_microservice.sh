#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 3 ]; then
    echo "need <microservice_name> <microservice_image> <client>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
NAME=$1
IMAGE=$2
CLIENT=$3

function create_microservice_conf {

    mkdir -p ${STARTUP_DIR}/../conf/microservices
    if test -f "${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json";
    then
	echo "The microservice already exists. Will make a backup to .prev";
	cp ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json.prev
    fi
    cat ${STARTUP_DIR}/../conf/microservice.json.in | sed -e "s|%NAME%|${NAME}|" | sed -e "s|%IMAGE%|${IMAGE}|" > ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}


function run_microservice {

    kubectl apply -f ${STARTUP_DIR}/../conf/microservices/microservice-${NAME}.json

}

function configure_seldon {

    cat <<EOF | ${STARTUP_DIR}/seldon-cli rec_alg --action create --client-name ${CLIENT} -f -
{
    "defaultStrategy": {
        "algorithms": [
            {
                "config": [
                    {
                        "name": "io.seldon.algorithm.inclusion.itemsperincluder",
                        "value": 1000
                    },
                    {
                        "name": "io.seldon.algorithm.external.url",
                        "value": "http://${NAME}:5000/recommend"
                    },
                    {
                        "name": "io.seldon.algorithm.external.name",
                        "value": "${NAME}"
                    }
                ],
                "filters": [],
                "includers": [
                    "recentItemsIncluder"
                ],
                "name": "externalItemRecommendationAlgorithm"
            }
        ],
        "combiner": "firstSuccessfulCombiner"
    },
    "recTagToStrategy": {}
}
EOF
    ${STARTUP_DIR}/seldon-cli rec_alg --action commit --client-name ${CLIENT}

}


function start_microservice {

    create_microservice_conf

    run_microservice

    configure_seldon

}


start_microservice "$@"

