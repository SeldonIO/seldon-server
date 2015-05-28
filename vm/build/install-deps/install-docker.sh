#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

# install docker
[ -e /usr/lib/apt/methods/https ] || {
  sudo apt-get update -y
  sudo apt-get install -y apt-transport-https
}
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
sudo sh -c "echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list"
sudo apt-get update
sudo apt-get install -y lxc-docker.1.6.0

echo "Enabling user[${INSTALL_DEPS_DOCKER_USER}] for docker"
sudo usermod -aG docker $INSTALL_DEPS_DOCKER_USER

