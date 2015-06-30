#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

setup_war_file() {
    if [[ ! -d "${SELDON_SERVER_HOME}/server/target" ]]; then
        (cd ${SELDON_SERVER_HOME}/server && mvn package -DskipTests -q)
    fi

    WARFILE_PATH=$(ls ${SELDON_SERVER_HOME}/server/target/seldon-server-*.war)
    WARFILE=$(basename ${WARFILE_PATH})
    WARFILE_VERSION=$(echo ${WARFILE}|sed -e 's/seldon-server-//' -e 's/.war$//')

    echo "WARFILE_PATH[${WARFILE_PATH}]"
    echo "WARFILE[${WARFILE}]"
    echo "WARFILE_VERSION[${WARFILE_VERSION}]"
}

echo "-- building seldon api --"

SELDON_SERVER_HOME=${INSTALL_DEPS_SELDON_SERVER_HOME}

setup_war_file

