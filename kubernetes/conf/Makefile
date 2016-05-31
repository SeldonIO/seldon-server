SHELL=/bin/bash

SELDON_HOME := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

BUILD_VERSIONS_GENERATED_FILE=$(SELDON_HOME)/build/build_versions_generated

#
# Simple config for hostPath setting for demo cluster
# Will change to more complex config if we allow alternate storage volumes for demo
#

.PHONY: microservice conf

#
# core conf
#

#
# Example data volume conf for hostPath and glusterfs
#
DATA_VOLUME="hostPath": {"path": "/seldon-data"}
#DATA_VOLUME="glusterfs": {"endpoints": "glusterfs-cluster","path": "gv0","readOnly": false}

#
# Example seldon server service types. Change to the kubernetes type you need.
#
SELDON_SERVICE_TYPE=NodePort
#SELDON_SERVICE_TYPE=LoadBalancer

control.json:control.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat control.json.in | sed \
			-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
			-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
			> control.json

mysql.json:mysql.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat mysql.json.in | sed \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e  "s|%SELDON_MYSQL_IMAGE_VERSION%|$${SELDON_MYSQL_IMAGE_VERSION}|" \
				> mysql.json

zookeeper.json:zookeeper.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat zookeeper.json.in | sed \
				-e  "s|%SELDON_ZOOKEEPER_K8S_IMAGE_VERSION%|$${SELDON_ZOOKEEPER_K8S_IMAGE_VERSION}|" \
				> zookeeper.json

td-agent-server.json:td-agent-server.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat td-agent-server.json.in | sed \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e  "s|%SELDON_TDAGENT_SERVER_IMAGE_VERSION%]|$${SELDON_TDAGENT_SERVER_IMAGE_VERSION}|" \
				 > td-agent-server.json

spark-master.json: spark-master.json.in
	cat spark-master.json.in | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' > spark-master.json

spark-workers.json: spark-workers.json.in
	cat spark-workers.json.in | sed -e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' > spark-workers.json

influxdb-grafana.json: influxdb-grafana.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat influxdb-grafana.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e  "s|%SELDON_INFLUXDB_IMAGE_VERSION%|$${SELDON_INFLUXDB_IMAGE_VERSION}|" \
				-e  "s|%SELDON_GRAFANA_IMAGE_VERSION%|$${SELDON_GRAFANA_IMAGE_VERSION}|" \
				> influxdb-grafana.json

examples/reuters/import-data-job.json: examples/reuters/import-data-job.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/reuters/import-data-job.json.in  | sed \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e "s|%SELDON_EXAMPLES_REUTERS_DATA_IMAGE_VERSION%|$${SELDON_EXAMPLES_REUTERS_DATA_IMAGE_VERSION}|" \
				> examples/reuters/import-data-job.json

examples/ml100k/ml100k-import.json: examples/ml100k/ml100k-import.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/ml100k/ml100k-import.json.in  | sed \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e "s|%SELDON_EXAMPLES_ML100K_IMAGE_VERSION%|$${SELDON_EXAMPLES_ML100K_IMAGE_VERSION}|" \
				> examples/ml100k/ml100k-import.json

server.json :server.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
	source $(SELDON_HOME)/build/build_versions; \
		cat server.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e "s|%SELDON_SERVER_IMAGE_VERSION%|$${SELDON_SERVER_IMAGE_VERSION}|" \
				-e  "s|%SELDON_TDAGENT_NODE_IMAGE_VERSION%|$${SELDON_TDAGENT_NODE_IMAGE_VERSION}|" \
				> server.json

analytics/impressions-spark-streaming.json: analytics/impressions-spark-streaming.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat analytics/impressions-spark-streaming.json.in  | sed \
				-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
				> analytics/impressions-spark-streaming.json

analytics/requests-spark-streaming.json: analytics/requests-spark-streaming.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat analytics/requests-spark-streaming.json.in  | sed \
				-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
				> analytics/requests-spark-streaming.json

analytics/predictions-spark-streaming.json: analytics/predictions-spark-streaming.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat analytics/predictions-spark-streaming.json.in  | sed \
				-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
				> analytics/predictions-spark-streaming.json


#
# Development conf
#

dev/server.json :dev/server.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat dev/server.json.in | sed \
				-e 's|%SELDON_HOME%|$(SELDON_HOME)|' \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e "s|%SELDON_SERVER_IMAGE_VERSION%|$${SELDON_SERVER_IMAGE_VERSION}|" \
				> dev/server.json

dev/control.json :dev/control.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat dev/control.json.in | sed \
				-e 's|%SELDON_HOME%|$(SELDON_HOME)|' \
				-e 's|%DATA_VOLUME%|$(DATA_VOLUME)|' \
				-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
				> dev/control.json

dev/iago.json :dev/iago.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(SELDON_HOME)/build/build_versions; \
		cat dev/iago.json.in | sed \
				-e "s|%SELDON_IAGO_IMAGE_VERSION%|$${SELDON_IAGO_IMAGE_VERSION}|" \
				> dev/iago.json


#
# glusterfs
#

GLUSTERFS_IP1=192.168.0.149
GLUSTERFS_IP2=192.168.0.248

glusterfs.json:glusterfs.json.in
	cat glusterfs.json.in | sed -e 's|%GLUSTERFS_IP1%|${GLUSTERFS_IP1}|'| sed -e 's|%GLUSTERFS_IP2%|${GLUSTERFS_IP2}|' > glusterfs.json

conf:control.json mysql.json td-agent-server.json spark-master.json spark-workers.json examples/reuters/import-data-job.json glusterfs.json server.json examples/ml100k/ml100k-import.json dev/server.json dev/control.json influxdb-grafana.json analytics/predictions-spark-streaming.json analytics/impressions-spark-streaming.json analytics/requests-spark-streaming.json dev/iago.json zookeeper.json


#
# microservice conf
#

MICROSERVICE_NAME=reuters-example
MICROSERVICE_IMAGE=seldonio/reuters-example
MICROSERVICE_VERSION=1.0


microservice-${MICROSERVICE_NAME}.json:microservice.json.in
	cat microservice.json.in | sed -e 's|%NAME%|${MICROSERVICE_NAME}|' | sed -e 's|%IMAGE%|${MICROSERVICE_IMAGE}|' | sed -e 's|%VERSION%|${MICROSERVICE_VERSION}|' > microservice-${MICROSERVICE_NAME}.json

microservice:microservice-${MICROSERVICE_NAME}.json

$(BUILD_VERSIONS_GENERATED_FILE):
	@$(SELDON_HOME)/build/generate-build-versions

clean:
	rm -f mysql.json
	rm -f control.json
	rm -f td-agent-server.json
	rm -f spark-master.json
	rm -f spark-workers.json
	rm -f server.json
	rm -f influxdb-grafana.json
	rm -f examples/reuters/import-data-job.json
	rm -f examples/ml100k/ml100k-import.json
	rm -f microservice-${MICROSERVICE_NAME}.json
	rm -f glusterfs.json
	rm -f zookeeper.json
	rm -f dev/server.json
	rm -f dev/control.json
	rm -f dev/iago.json
	rm -f analytics/*.json
