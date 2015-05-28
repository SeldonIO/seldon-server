#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

# Ensure docker will use aufs
sudo apt-get install -y -q $LINUX_IMAGE_EXTRA_PACKAGE

