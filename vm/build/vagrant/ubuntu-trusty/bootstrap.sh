#!/usr/bin/env bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

sudo apt-get update
sudo locale-gen en_GB.UTF-8

sudo apt-get install git -y

