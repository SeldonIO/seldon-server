#!/bin/bash

set -o nounset
set -o errexit

kubectl create -f conf/mysql.yml
kubectl create -f conf/memcache.yml
kubectl create -f conf/zookeeper.yml
kubectl create -f conf/control.yml
kubectl create -f conf/td-agent-server.yml
