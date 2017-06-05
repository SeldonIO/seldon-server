#!/bin/bash

set -o nounset
set -o errexit

if [ "$#" -ne 2 ]; then
    echo "need <replay script> <requests-per-sec>"
    exit -1
fi

REPLAY=$1
REQ_RATE=$2

cd /iago/tmp
rm -f replay.txt
ln -s ${REPLAY} replay.txt

cat web.ramp-up.scala.in | sed 's/%REQ_RATE%/'${REQ_RATE}'/' > web.ramp-up.scala
java -Xmx3000M -jar iago-0.6.14.jar -f web.ramp-up.scala



