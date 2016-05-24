#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

function seldon_down {

    kubectl delete pods,services,deployments,replicationcontrollers -l service=seldon
}


seldon_down "$@"



