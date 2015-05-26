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

extract_war_file() {
    if [[ ! -d "${WEBAPPS_DIR}.orig" ]]; then
        cp -R ${WEBAPPS_DIR} ${WEBAPPS_DIR}.orig
    fi
    rm -rf ${WEBAPPS_DIR}
    mkdir -p ${WEBAPPS_DIR}/ROOT
    unzip -qx -d ${WEBAPPS_DIR}/ROOT/ ${WARFILE_PATH} &> /dev/null
}

create_setenv() {
    local DATA=$(sed -e '0,/^__DATA__$/d' -e "s|%SELDON_ZKSERVERS%|${SELDON_ZKSERVERS}|g"  "$0")
    printf '%s\n' "$DATA" > ${INSTALL_DEPS_TOMCAT_HOME}/bin/setenv.sh
}

echo "-- setting up webapps for seldon api --"

SELDON_SERVER_HOME=${INSTALL_DEPS_SELDON_SERVER_HOME}
WEBAPPS_DIR=${INSTALL_DEPS_TOMCAT_HOME}/webapps
SELDON_ZKSERVERS=localhost

setup_war_file
extract_war_file
create_setenv

exit

__DATA__
# setenv.sh

export SELDON_ZKSERVERS=%SELDON_ZKSERVERS%

