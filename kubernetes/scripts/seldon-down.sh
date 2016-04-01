#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"
SELDON_WITH_SPARK=${SELDON_WITH_SPARK:-false}

function stop_core_services {
    
    echo "Stopping core services"
    kubectl delete -f ${STARTUP_DIR}/../conf/mysql.yml
    kubectl delete -f ${STARTUP_DIR}/../conf/memcache.yml
    kubectl delete -f ${STARTUP_DIR}/../conf/zookeeper.yml
    kubectl delete -f ${STARTUP_DIR}/../conf/control.yml
    kubectl delete -f ${STARTUP_DIR}/../conf/td-agent-server.yml
    kubectl delete -f ${STARTUP_DIR}/../conf/server.yml

}

function stop_spark {
    spark_running=$(kubectl get pod --selector=component=spark-master --no-headers=true | wc -l)
    if [ "$spark_running" -ne "0" ]; then
	echo 'Stopping Spark Cluster'
	kubectl delete -f ${STARTUP_DIR}/../conf/spark-master.yml
	kubectl delete -f ${STARTUP_DIR}/../conf/spark-workers.yml
    fi
}


function seldon_down {

    stop_core_services

    stop_spark

}


seldon_down "$@"



