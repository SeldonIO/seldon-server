#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

#Install spark
SPARK_VERSION=1.3.0
sudo apt-get install -y wget
[[ ! -f /tmp/spark.tgz ]] && wget -O /tmp/spark.tgz http://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-cdh4.tgz
mkdir -p ~/apps
cd ~/apps
rm -rf spark-${SPARK_VERSION}-bin-cdh4 spark
tar xvf /tmp/spark.tgz
ln -sn spark-${SPARK_VERSION}-bin-cdh4 spark
rm -fv /tmp/spark.tgz

