#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

# install other packages
sudo apt-get install -y zip
sudo apt-get install -y maven

