#!/bin/bash

set -o nounset
set -o errexit

KCMD="kubectl exec seldon-control -i bash"

# Setup memcached
echo "seldon-cli memcached --action setup --servers memcached1:11211,memcached2:11211" | ${KCMD}
echo "seldon-cli memcached --action commit" | ${KCMD}
# Setup DB
echo "seldon-cli db --action setup --db-name ClientDB --db-jdbc 'jdbc:mysql:replication://mysql:3306,mysql:3306/?characterEncoding=utf8&useServerPrepStmts=true&logger=com.mysql.jdbc.log.StandardLogger&roundRobinLoadBalance=true&transformedBitIsBoolean=true&rewriteBatchedStatements=true'" | ${KCMD}
echo "seldon-cli db --action commit" | ${KCMD}
# Setup Test client
echo "seldon-cli client --action setup --db-name ClientDB --client-name test" | ${KCMD}

