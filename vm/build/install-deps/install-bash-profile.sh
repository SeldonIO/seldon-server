#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

data=$(sed -e '0,/^__DATA__$/d' -e "s|%INSTALL_DEPS_TOMCAT_HOME%|${INSTALL_DEPS_TOMCAT_HOME}|g"  -e "s|%INSTALL_DEPS_SPARK_HOME%|${INSTALL_DEPS_SPARK_HOME}|g" -e "s|%INSTALL_DEPS_ANACONDA_HOME%|${INSTALL_DEPS_ANACONDA_HOME}|g" "$0")
printf '%s\n' "$data" > ~/.bash_profile

exit

__DATA__
# .bash_profile
# Get the aliases and functions
if [ -f ~/.bashrc ]; then
         . ~/.bashrc
fi

# User specific environment and startup programs

export TOMCAT_HOME=%INSTALL_DEPS_TOMCAT_HOME%
export SPARK_HOME=%INSTALL_DEPS_SPARK_HOME%
export ANACONDA_HOME=%INSTALL_DEPS_ANACONDA_HOME%

export PATH=${ANACONDA_HOME}/bin:${PATH}

export LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib

