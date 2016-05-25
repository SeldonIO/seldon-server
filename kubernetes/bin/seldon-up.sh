#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

SELDON_WITH_SPARK=${SELDON_WITH_SPARK:-true}
SELDON_WITH_GLUSTERFS=${SELDON_WITH_GLUSTERFS:-false}
KCMD="kubectl exec seldon-control -i bash"

function start_core_services {
    echo "Starting core servces"
    kubectl create -f ${STARTUP_DIR}/../conf/mysql.json
    kubectl create -f ${STARTUP_DIR}/../conf/memcache.json
    kubectl create -f ${STARTUP_DIR}/../conf/zookeeper.json
    kubectl create -f ${STARTUP_DIR}/../conf/control.json
    kubectl create -f ${STARTUP_DIR}/../conf/influxdb-grafana.json
    kubectl create -f ${STARTUP_DIR}/../conf/kafka.json
    kubectl create -f ${STARTUP_DIR}/../conf/td-agent-server.json

    while true; do
        non_running_states=$(get_non_running_states)
        if [[ "$non_running_states" == "0" ]]; then
            break
        else
	    kubectl get pods
            echo "Waiting for pods to be running as found $non_running_states in non-running state"
            echo "Sleeping for 5 seconds..."
            sleep 5
        fi
    done
}

function start_api_server {
    echo "Starting Seldon API server"
    kubectl create -f ${STARTUP_DIR}/../conf/server.json
}

function setup_basic_conf {

    #push any existing conf to zookeeper
    ${STARTUP_DIR}/seldon-cli client --action zk_push
    # Setup memcached
    ${STARTUP_DIR}/seldon-cli memcached --action setup --servers memcached1:11211,memcached2:11211
    ${STARTUP_DIR}/seldon-cli memcached --action commit
    # Setup DB
    ${STARTUP_DIR}/seldon-cli db --action setup --db-name ClientDB --db-jdbc 'jdbc:mysql:replication://mysql:3306,mysql:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true'
    echo "seldon-cli db --action commit" | ${KCMD}
    # Setup Test client
    ${STARTUP_DIR}/seldon-cli client --action setup --db-name ClientDB --client-name test

}

function start_spark {
    if $SELDON_WITH_SPARK ; then
        echo 'Creating Spark Cluster'
        kubectl create -f ${STARTUP_DIR}/../conf/spark-master.json
        while true; do
            non_running_states=$(get_non_running_states)
            if [[ "$non_running_states" == "0" ]]; then
                break
            else
		kubectl get pods
                echo "Waiting for pods to be running as found $non_running_states in non-running state"
                echo "Sleeping for 3 seconds..."
                sleep 3
            fi
        done
        kubectl create -f ${STARTUP_DIR}/../conf/spark-workers.json
	echo "Allowing spark workers to start..."
	sleep 5
	#kubectl logs spark-master-controller-mqu8j | grep Registering work
	kubectl create -f ${STARTUP_DIR}/../conf/analytics/impressions-spark-streaming.json
	kubectl create -f ${STARTUP_DIR}/../conf/analytics/requests-spark-streaming.json
    fi
}

function start_glusterfs_service {
    if $SELDON_WITH_GLUSTERFS ; then
	echo 'Creating Glusterfs service'
	kubectl create -f ${STARTUP_DIR}/../conf/glusterfs.json
    fi
}


function setup_influxdb {

    INFLUXDB_POD=`kubectl get pods -l name=influxGrafana | sed 1d | cut -d' ' -f1 |sed -e 's/^[ \t]*//'`
    kubectl exec ${INFLUXDB_POD} -- influx --execute 'create database IF NOT EXISTS seldon'

}


function seldon_up {

    start_glusterfs_service

    start_core_services

    start_spark

    setup_basic_conf

    start_api_server

    setup_influxdb
}

function get_non_running_states {
    kubectl get pods | sed 1d | grep -v Running | wc -l | sed -e 's/^[ \t]*//'
}

seldon_up "$@"


