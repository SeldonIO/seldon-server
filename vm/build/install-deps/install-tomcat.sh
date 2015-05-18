#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

#install tomcat
TOMCAT_VERSION=7.0.61

sudo apt-get install -y wget
rm -rfv /tmp/tomcat-download
mkdir -p /tmp/tomcat-download
wget -O /tmp/tomcat-download/apache-tomcat-${TOMCAT_VERSION}.tar.gz http://archive.apache.org/dist/tomcat/tomcat-7/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz
cd /tmp/tomcat-download
tar xvf apache-tomcat-${TOMCAT_VERSION}.tar.gz
mkdir -p ~/apps
cd ~/apps
rm -rfv apache-tomcat-${TOMCAT_VERSION} tomcat
mv /tmp/tomcat-download/apache-tomcat-${TOMCAT_VERSION} .
ln -sn apache-tomcat-${TOMCAT_VERSION} tomcat

