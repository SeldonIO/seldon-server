#!/bin/bash

set -o nounset
set -o errexit

echo "Warning: this assumes your seldon server is running locally on port 30000. If not you will need to change host"

JSON=`echo "seldon-cli --quiet keys --client-name test --scope js" | kubectl exec seldon-control -i  bash`
KEY=`echo $JSON | jq -r '.[0].key'`
curl -s "http://127.0.0.1:30000/js/predict?consumer_key=${KEY}&jsonpCallback=j&f1=1.6&f2=2.7&f3=5.3&f4=1.9" | sed 's/^j(//' | sed 's/)$//' | jq

