#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

SELDON_WITH_SPARK=${SELDON_WITH_SPARK:-false}
SELDON_WITH_GLUSTERFS=${SELDON_WITH_GLUSTERFS:-false}
KCMD="kubectl exec seldon-control -i bash"

function start_core_services {
    echo "Starting core servces"
    kubectl create -f ${STARTUP_DIR}/../conf/mysql.json
    kubectl create -f ${STARTUP_DIR}/../conf/memcache.json
    kubectl create -f ${STARTUP_DIR}/../conf/zookeeper.json
    kubectl create -f ${STARTUP_DIR}/../conf/control.json
    kubectl create -f ${STARTUP_DIR}/../conf/td-agent-server.json

    while true; do
	non_running_states=$(kubectl get -o json pods  | jq -r '.items[].status.phase' | grep -v Running | wc -l)
	if [[ "$non_running_states" == "0" ]]; then
	    break
	else
	    echo "Waiting for pods to be running as found $non_running_states in non-running state"
	    echo "Sleeping for 3 seconds..."
	    sleep 3
	fi
    done
}

function start_api_server {
    echo "Starting Seldon API server"
    kubectl create -f ${STARTUP_DIR}/../conf/server.json    
}

function setup_basic_conf {

    # Setup memcached
    echo "seldon-cli memcached --action setup --servers memcached1:11211,memcached2:11211" | ${KCMD}
    echo "seldon-cli memcached --action commit" | ${KCMD}
    # Setup DB
    echo "seldon-cli db --action setup --db-name ClientDB --db-jdbc 'jdbc:mysql:replication://mysql:3306,mysql:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true'" | ${KCMD}
    echo "seldon-cli db --action commit" | ${KCMD}
    # Setup Test client
    echo "seldon-cli client --action setup --db-name ClientDB --client-name test" | ${KCMD}

}

function start_spark {
    if $SELDON_WITH_SPARK ; then
	echo 'Creating Spark Cluster'
	kubectl create -f ${STARTUP_DIR}/../conf/spark-master.json
	while true; do
	    non_running_states=$(kubectl get -o json pods  | jq -r '.items[].status.phase' | grep -v Running | wc -l)
	    if [[ "$non_running_states" == "0" ]]; then
		break
	    else
		echo "Waiting for pods to be running as found $non_running_states in non-running state"
		echo "Sleeping for 3 seconds..."
		sleep 3
	    fi
	done
	kubectl create -f ${STARTUP_DIR}/../conf/spark-workers.json
    fi
}

function start_glusterfs_service {
    if $SELDON_WITH_GLUSTERFS ; then
	echo 'Creating Glusterfs service'
	kubectl create -f ${STARTUP_DIR}/../conf/glusterfs.json
    fi
}


function seldon_up {

    start_glusterfs_service

    start_core_services

    start_spark

    setup_basic_conf

    start_api_server
}


seldon_up "$@"

