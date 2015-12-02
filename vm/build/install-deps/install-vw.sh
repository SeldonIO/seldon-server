#!/bin/bash

set -o nounset
set -o errexit

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

export DEBIAN_FRONTEND=noninteractive

#Install vw
VW_VERSION=8.1

sudo apt-get install -y build-essential automake autoconf libxmu-dev g++ gcc libpthread-stubs0-dev libtool libboost-program-options-dev libboost-python-dev zlib1g-dev libc6 libgcc1 libstdc++6 libblas-dev liblapack-dev git telnet procps memcached libmemcached-dev

if [ -z "${LD_LIBRARY_PATH+x}" ]; then
    export LD_LIBRARY_PATH=/usr/local/lib
else
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
fi

cd /usr/local/src
sudo git clone https://github.com/JohnLangford/vowpal_wabbit.git
cd vowpal_wabbit
sudo git checkout ${VW_VERSION}
sudo ./autogen.sh
sudo ./configure --with-boost-libdir=/usr/lib/x86_64-linux-gnu
sudo make
sudo make test
sudo make install

sudo rm -rf /usr/local/src/vowpal_wabbit

