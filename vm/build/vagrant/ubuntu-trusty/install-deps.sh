#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export INSTALL_DEPS_DIR=/vagrant/install-deps
export INSTALL_DEPS_DOCKER_USER=vagrant
export INSTALL_DEPS_TOMCAT_HOME=/home/vagrant/apps/tomcat
export INSTALL_DEPS_SPARK_HOME=/home/vagrant/apps/spark
export INSTALL_DEPS_SELDON_SERVER_HOME=/home/vagrant/seldon-server

##${INSTALL_DEPS_DIR}/install-locale.sh
##${INSTALL_DEPS_DIR}/install-linux-image-extra.sh
#${INSTALL_DEPS_DIR}/install-docker.sh
#${INSTALL_DEPS_DIR}/install-vim.sh
#${INSTALL_DEPS_DIR}/install-jdk7.sh
#${INSTALL_DEPS_DIR}/install-tomcat.sh
#${INSTALL_DEPS_DIR}/install-spark.sh
#${INSTALL_DEPS_DIR}/install-bash-profile.sh
#${INSTALL_DEPS_DIR}/install-python-deps.sh
#${INSTALL_DEPS_DIR}/install-misc.sh

