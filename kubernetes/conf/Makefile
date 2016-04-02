SHELL=/bin/bash

#
# Simple config for hostPath setting for demo cluster
# Will change to more complex config if we allow alternate storage volumes for demo
#

.PHONY: microservice conf

DATA_VOLUME="hostPath": {"path": "/seldon-data"}
#DATA_VOLUME="glusterfs": {"endpoints": "glusterfs-cluster","path": "gv0","readOnly": false}

MICROSERVICE_NAME=reuters-example
MICROSERVICE_IMAGE=seldonio/reuters-example
MICROSERVICE_VERSION=1.0


control.json:control.json.in
	cat control.json.in | DATA_VOLUME='${DATA_VOLUME}' envsubst  > control.json

mysql.json:mysql.json.in
	cat mysql.json.in | DATA_VOLUME='${DATA_VOLUME}' envsubst  > mysql.json

td-agent-server.json:td-agent-server.json.in
	cat td-agent-server.json.in | DATA_VOLUME='${DATA_VOLUME}' envsubst  > td-agent-server.json

spark-master.json: spark-master.json.in
	cat spark-master.json.in | DATA_VOLUME='${DATA_VOLUME}' envsubst > spark-master.json

spark-workers.json: spark-workers.json.in
	cat spark-workers.json.in | DATA_VOLUME='${DATA_VOLUME}' envsubst > spark-workers.json

conf:control.json mysql.json td-agent-server.json spark-master.json spark-workers.json

microservice-${MICROSERVICE_NAME}.json:microservice.json.in
	cat microservice.json.in | MICROSERVICE_NAME=${MICROSERVICE_NAME} MICROSERVICE_IMAGE=${MICROSERVICE_IMAGE} MICROSERVICE_VERSION=${MICROSERVICE_VERSION} envsubst > microservice-${MICROSERVICE_NAME}.json

microservice:microservice-${MICROSERVICE_NAME}.json

clean:
	rm -f mysql.json
	rm -f control.json
	rm -f td-agent-server.json
	rm -f spark-master.json
	rm -f spark-workers.json
	rm -f microservice-${MICROSERVICE_NAME}.json
