#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <input proto file> <output jar file>"
    exit -1
fi

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
PROTO_FILE=$1
JAR_FILE=$2

function create_job_conf {
    
    mkdir -p ${STARTUP_DIR}/../conf/jobs
    cat ${STARTUP_DIR}/../conf/rpc/create-proto-jar-job.template.json | sed -e "s|%PROTO_FILE%|${PROTO_FILE}|" | sed -e "s|%JAR_FILE%|${JAR_FILE}|" > ${STARTUP_DIR}/../conf/jobs/grpc-util.json

}


function run_job {

    kubectl apply -f ${STARTUP_DIR}/../conf/jobs/grpc-util.json

}

function create_jar {

    create_job_conf

    run_job
    
}


create_jar "$@"




