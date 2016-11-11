#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <proto file> <jar file>"
    exit -1
fi

proto_file=$1
jar_file=$2

STARTUP_DIR="$( cd "$( dirname "$0" )" && pwd )"

DIR=$(dirname "${jar_file}")
cd ${STARTUP_DIR}
mkdir -p ${DIR}
mkdir -p src/main/proto
cp ${proto_file} src/main/proto
mvn -q clean package
cp target/seldon-grpc-util-1.0.0.jar ${jar_file}

