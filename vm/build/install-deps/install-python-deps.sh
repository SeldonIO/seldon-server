#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

ANACONDA_INSTALL_FILE=Anaconda2-2.4.0-Linux-x86_64.sh
wget -O /tmp/${ANACONDA_INSTALL_FILE} http://static.seldon.io/vm-resources/anaconda/${ANACONDA_INSTALL_FILE}

bash /tmp/Anaconda2-2.4.0-Linux-x86_64.sh -b

# use anaconda for rest of python install
export PATH=/home/ubuntu/anaconda2/bin:${PATH}

sudo apt-get update
sudo apt-get install -y make libmysqlclient-dev mysql-client-core-5.6
pip install unicodecsv
pip install MySQL-python
pip install kazoo

sudo apt-get install -y git
sudo apt-get install -y g++
pip install -e git+git://github.com/SeldonIO/wabbit_wappa#egg=wabbit-wappa-3.0.2
pip install seldon

