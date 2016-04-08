#!/bin/bash

set -o nounset
set -o errexit

echo "Warning: this assumes your seldon server is running locally on port 30000. If not you will need to change host"

JSON=`echo "seldon-cli --quiet keys --client-name reuters --scope js" | kubectl exec seldon-control -i  bash`
KEY=`echo $JSON | jq -r '.[0].key'`
curl -s "http://127.0.0.1:30000/js/recommendations?consumer_key=${KEY}&user=1&type=1&limit=5&jsonpCallback=j" | sed 's/^j(//' | sed 's/)$//' | jq

