#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

sudo apt-get install -y python-pip make python-dev libmysqlclient-dev mysql-client-core-5.6
sudo pip install unicodecsv
sudo pip install MySQL-python
sudo pip install kazoo

