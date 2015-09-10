#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive
NODE_VERSION=v0.12.0
NODE_ARCH_FILE=node-${NODE_VERSION}-linux-x64.tar.gz

install_node_related_stuff() {
    ###############################################################################
    # Install node and related stuff
    mkdir -p ~/tmp
    cd ~/tmp
    wget https://nodejs.org/dist/${NODE_VERSION}/${NODE_ARCH_FILE}
    sudo tar xvf ~/tmp/${NODE_ARCH_FILE} -C /opt
    rm -fv ~/tmp/${NODE_ARCH_FILE}
    cd /opt
    sudo ln -sn node-${NODE_VERSION}-linux-x64 node
    sudo ln -sn /opt/node/bin/node /usr/local/bin/node
    sudo ln -sn /opt/node/bin/npm /usr/local/bin/npm
    ###############################################################################
}

install_yo_bower_grunt() {
    ###############################################################################
    # Install yo, bower, grunt with the right modules using the package.json in movie-demo-frontend project
    mkdir -p ~/tmp
    cd ~/tmp
    git clone https://github.com/SeldonIO/movie-demo-frontend
    cd movie-demo-frontend
    echo "Installing yo, bower, grunt-cli"
    sudo npm install -g yo bower grunt-cli
    sudo chown -R ${INSTALL_DEPS_VM_USER}:${INSTALL_DEPS_VM_USER} ~/.npm
    rm -rf ~/tmp/movie-demo-frontend/
    ###############################################################################
}

install_node_related_stuff
install_yo_bower_grunt

