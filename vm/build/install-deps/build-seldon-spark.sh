#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

setup_jar_file() {
    if [[ ! -d "${SELDON_SERVER_HOME}/offline-jobs/spark/target" ]]; then
        (cd ${SELDON_SERVER_HOME}/offline-jobs/spark && mvn package)
    fi

    JARFILE_PATH=$(ls ${SELDON_SERVER_HOME}/offline-jobs/spark/target/seldon-spark-*-with-dependencies.jar)
    JARFILE=$(basename ${JARFILE_PATH})
    JARFILE_VERSION=$(echo ${JARFILE}|sed -e 's/seldon-spark-//' -e 's/-jar-with-dependencies.jar//')

    echo "JARFILE_PATH[${JARFILE_PATH}]"
    echo "JARFILE[${JARFILE}]"
    echo "JARFILE_VERSION[${JARFILE_VERSION}]"
}

echo "-- building seldon spark --"

SELDON_SERVER_HOME=${INSTALL_DEPS_SELDON_SERVER_HOME}

setup_jar_file

