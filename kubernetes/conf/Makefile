SHELL=/bin/bash

SELDON_HOME := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

BUILD_VERSIONS_GENERATED_FILE=$(SELDON_HOME)/build/build_versions_generated

#
# Simple config for hostPath setting for demo cluster
# Will change to more complex config if we allow alternate storage volumes for demo
#

.PHONY: conf

#
# core conf
#

#
# Example seldon server service types. Change to the kubernetes type you need.
#
SELDON_SERVICE_TYPE=NodePort
#SELDON_SERVICE_TYPE=LoadBalancer

GRAFANA_ADMIN_PASSWORD=admin

GOOGLE_SQL_INSTANCE=hpsc-164217:europe-west1:hpsql1

SELDON_SERVER_NODEPORT=30015
SELDON_SERVER_DEBUG_NODEPORT=30016
SELDON_SERVER_RPC_NODEPORT=30017
LOCUST_NODEPORT=30018
SELDON_SERVER_DEBUG_COMMAND="command":["/home/seldon/start-server-debug.sh"]

# 
# Default Resources (edit for your own needs)
#
SELDON_SERVER_RESOURCES="requests":{ "memory" : "3Gi" }
MYSQL_RESOURCES="requests":{ "memory" : "3Gi" }
MEMCACHE_RESOURCES="requests":{ "memory" : "260Mi" }

control.json:control.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat control.json.in | sed \
			-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
			-e  "s|%GRAFANA_ADMIN_PASSWORD%|${GRAFANA_ADMIN_PASSWORD}|" \
			> control.json && echo "created $@"

mysql.json:mysql.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat mysql.json.in | sed \
				-e  's|%MYSQL_RESOURCES%|$(MYSQL_RESOURCES)|' \
				-e  "s|%SELDON_MYSQL_IMAGE_VERSION%|$${SELDON_MYSQL_IMAGE_VERSION}|" \
				> mysql.json && echo "created $@"

mysql-google-cloudsql.json:mysql-google-cloudsql.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat mysql-google-cloudsql.json.in | sed \
				-e  's|%GOOGLE_SQL_INSTANCE%|${GOOGLE_SQL_INSTANCE}|' \
				> mysql-google-cloudsql.json && echo "created $@"

memcache.json:memcache.json.in
	@cat memcache.json.in | sed \
				-e  's|%MEMCACHE_RESOURCES%|$(MEMCACHE_RESOURCES)|' \
				> memcache.json && echo "created $@"

zookeeper.json:zookeeper.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat zookeeper.json.in | sed \
				-e  "s|%SELDON_ZOOKEEPER_K8S_IMAGE_VERSION%|$${SELDON_ZOOKEEPER_K8S_IMAGE_VERSION}|" \
				> zookeeper.json && echo "created $@"

kafka.json:kafka.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat kafka.json.in | sed \
				-e  "s|%SELDON_KAFKA_K8S_IMAGE_VERSION%|$${SELDON_KAFKA_K8S_IMAGE_VERSION}|" \
				> kafka.json && echo "created $@"

td-agent-server.json:td-agent-server.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat td-agent-server.json.in | sed \
				-e  "s|%SELDON_TDAGENT_SERVER_IMAGE_VERSION%|$${SELDON_TDAGENT_SERVER_IMAGE_VERSION}|" \
				> td-agent-server.json && echo "created $@"

spark-master.json: spark-master.json.in
	@cat spark-master.json.in > spark-master.json && echo "created $@"

spark-workers.json: spark-workers.json.in
	@cat spark-workers.json.in > spark-workers.json && echo "created $@"

influxdb-grafana.json: influxdb-grafana.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat influxdb-grafana.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				-e  "s|%GRAFANA_ADMIN_PASSWORD%|${GRAFANA_ADMIN_PASSWORD}|" \
				-e  "s|%SELDON_INFLUXDB_IMAGE_VERSION%|$${SELDON_INFLUXDB_IMAGE_VERSION}|" \
				-e  "s|%SELDON_GRAFANA_IMAGE_VERSION%|$${SELDON_GRAFANA_IMAGE_VERSION}|" \
				> influxdb-grafana.json && echo "created $@"

spark-ui.json: spark-ui.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat spark-ui.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				> spark-ui.json && echo "created $@"

proxy-server.json: proxy-server.json.in
	@source $(SELDON_HOME)/build/build_versions; \
		cat proxy-server.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				> proxy-server.json && echo "created $@"

examples/reuters/import-data-job.json: examples/reuters/import-data-job.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/reuters/import-data-job.json.in  | sed \
				-e "s|%SELDON_EXAMPLES_REUTERS_DATA_IMAGE_VERSION%|$${SELDON_EXAMPLES_REUTERS_DATA_IMAGE_VERSION}|" \
				> examples/reuters/import-data-job.json && echo "created $@"

examples/finefoods/train-finefoods.json: examples/finefoods/train-finefoods.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/finefoods/train-finefoods.json.in  | sed \
				-e "s|%SELDON_FINEFOODS_XGBOOST_IMAGE_VERSION%|$${SELDON_FINEFOODS_XGBOOST_IMAGE_VERSION}|" \
				> examples/finefoods/train-finefoods.json && echo "created $@"

examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json: examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json.in  | sed \
				-e "s|%SELDON_TENSORFLOW_DEEP_MNIST_IMAGE_VERSION%|$${SELDON_TENSORFLOW_DEEP_MNIST_IMAGE_VERSION}|" \
				> examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json && echo "created $@"

examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json: examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json.in  | sed \
				-e "s|%SELDON_TENSORFLOW_DEEP_MNIST_IMAGE_VERSION%|$${SELDON_TENSORFLOW_DEEP_MNIST_IMAGE_VERSION}|" \
				> examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json && echo "created $@"

examples/ml100k/ml100k-import.json: examples/ml100k/ml100k-import.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/ml100k/ml100k-import.json.in  | sed \
				-e "s|%SELDON_EXAMPLES_ML100K_IMAGE_VERSION%|$${SELDON_EXAMPLES_ML100K_IMAGE_VERSION}|" \
				> examples/ml100k/ml100k-import.json && echo "created $@"

examples/ml10m/ml10m-import-item-similarity.json: examples/ml10m/ml10m-import-item-similarity.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/ml10m/ml10m-import-item-similarity.json.in  | sed \
				-e "s|%SELDON_EXAMPLES_ML10M_IMAGE_VERSION%|$${SELDON_EXAMPLES_ML10M_IMAGE_VERSION}|" \
				> examples/ml10m/ml10m-import-item-similarity.json

examples/ml10m/ml10m-import-matrix-factorization.json: examples/ml10m/ml10m-import-matrix-factorization.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/ml10m/ml10m-import-matrix-factorization.json.in  | sed \
				-e "s|%SELDON_EXAMPLES_ML10M_IMAGE_VERSION%|$${SELDON_EXAMPLES_ML10M_IMAGE_VERSION}|" \
				> examples/ml10m/ml10m-import-matrix-factorization.json

examples/US_stocks/train-US-stocks.json: examples/US_stocks/train-US-stocks.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat examples/US_stocks/train-US-stocks.json.in  | sed \
				-e "s|%SELDON_US_STOCKS_IMAGE_VERSION%|$${SELDON_US_STOCKS_IMAGE_VERSION}|" \
				> examples/US_stocks/train-US-stocks.json && echo "created $@"


server.json :server.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
	source $(SELDON_HOME)/build/build_versions; \
		cat server.json.in | sed \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				-e 's|%SELDON_SERVER_NODEPORT%|$(SELDON_SERVER_NODEPORT)|' \
				-e 's|%SELDON_SERVER_RPC_NODEPORT%|$(SELDON_SERVER_RPC_NODEPORT)|' \
				-e 's|%SELDON_SERVER_RESOURCES%|$(SELDON_SERVER_RESOURCES)|' \
				-e "s|%SELDON_SERVER_IMAGE_VERSION%|$${SELDON_SERVER_IMAGE_VERSION}|" \
				-e  "s|%SELDON_TDAGENT_NODE_IMAGE_VERSION%|$${SELDON_TDAGENT_NODE_IMAGE_VERSION}|" \
				> server.json && echo "created $@"

analytics/impressions-kafka-stream.json: analytics/impressions-kafka-stream.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat analytics/impressions-kafka-stream.json.in  | sed \
				-e "s|%SELDON_STREAM_BUILD_IMAGE_VERSION%|$${SELDON_STREAM_BUILD_IMAGE_VERSION}|" \
				-e "s|%SELDON_STREAM_VERSION%|$${SELDON_STREAM_VERSION}|" \
				> analytics/impressions-kafka-stream.json

analytics/predictions-kafka-stream.json: analytics/predictions-kafka-stream.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat analytics/predictions-kafka-stream.json.in  | sed \
				-e "s|%SELDON_STREAM_BUILD_IMAGE_VERSION%|$${SELDON_STREAM_BUILD_IMAGE_VERSION}|" \
				-e "s|%SELDON_STREAM_VERSION%|$${SELDON_STREAM_VERSION}|" \
				> analytics/predictions-kafka-stream.json

models/stream-itemsim-create.json: models/stream-itemsim-create.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat models/stream-itemsim-create.json.in  | sed \
				-e "s|%SELDON_STREAM_ITEMSIM_IMAGE_VERSION%|$${SELDON_STREAM_ITEMSIM_IMAGE_VERSION}|" \
				-e "s|%SELDON_STREAM_VERSION%|$${SELDON_STREAM_VERSION}|" \
				> models/stream-itemsim-create.json

models/stream-itemsim-dbupload.json: models/stream-itemsim-dbupload.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat models/stream-itemsim-dbupload.json.in  | sed \
				-e "s|%SELDON_STREAM_ITEMSIM_IMAGE_VERSION%|$${SELDON_STREAM_ITEMSIM_IMAGE_VERSION}|" \
				-e "s|%SELDON_STREAM_VERSION%|$${SELDON_STREAM_VERSION}|" \
				> models/stream-itemsim-dbupload.json

#
# Development conf
#

dev/server.json :dev/server.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
	source $(SELDON_HOME)/build/build_versions; \
		cat dev/server.json.in | sed \
				-e 's|%SELDON_HOME%|$(SELDON_HOME)|' \
				-e 's|%SELDON_SERVER_NODEPORT%|$(SELDON_SERVER_NODEPORT)|' \
				-e 's|%SELDON_SERVER_RPC_NODEPORT%|$(SELDON_SERVER_RPC_NODEPORT)|' \
				-e 's|%SELDON_SERVER_DEBUG_NODEPORT%|$(SELDON_SERVER_DEBUG_NODEPORT)|' \
				-e 's|%SELDON_SERVER_RESOURCES%|$(SELDON_SERVER_RESOURCES)|' \
				-e 's|%SELDON_SERVER_DEBUG_COMMAND%|$(SELDON_SERVER_DEBUG_COMMAND)|' \
				-e "s|%SELDON_SERVER_IMAGE_VERSION%|$${SELDON_SERVER_IMAGE_VERSION}|" \
				-e  "s|%SELDON_TDAGENT_NODE_IMAGE_VERSION%|$${SELDON_TDAGENT_NODE_IMAGE_VERSION}|" \
				> dev/server.json && echo "created $@"

dev/control.json :dev/control.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat dev/control.json.in | sed \
				-e 's|%SELDON_HOME%|$(SELDON_HOME)|' \
				-e  "s|%GRAFANA_ADMIN_PASSWORD%|${GRAFANA_ADMIN_PASSWORD}|" \
				-e "s|%SELDON_CONTROL_IMAGE_VERSION%|$${SELDON_CONTROL_IMAGE_VERSION}|" \
				> dev/control.json && echo "created $@"

dev/iago.json :dev/iago.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(SELDON_HOME)/build/build_versions; \
		cat dev/iago.json.in | sed \
				-e "s|%SELDON_IAGO_IMAGE_VERSION%|$${SELDON_IAGO_IMAGE_VERSION}|" \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				> dev/iago.json
dev/locust-master.json.template :dev/locust-master.json.template.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(SELDON_HOME)/build/build_versions_generated; \
		cat dev/locust-master.json.template.in | sed \
				-e "s|%SELDON_LOCUST_IMAGE_VERSION%|$${SELDON_LOCUST_IMAGE_VERSION}|" \
				-e 's|%SERVICE_TYPE%|$(SELDON_SERVICE_TYPE)|' \
				-e 's|%LOCUST_NODEPORT%|$(LOCUST_NODEPORT)|' \
				> dev/locust-master.json.template

dev/locust-slave.json.template :dev/locust-slave.json.template.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(SELDON_HOME)/build/build_versions_generated; \
		cat dev/locust-slave.json.template.in | sed \
				-e "s|%SELDON_LOCUST_IMAGE_VERSION%|$${SELDON_LOCUST_IMAGE_VERSION}|" \
				> dev/locust-slave.json.template

rpc/create-proto-jar-job.template.json :rpc/create-proto-jar-job.json.in $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(SELDON_HOME)/build/build_versions; \
		cat rpc/create-proto-jar-job.json.in | sed \
				-e "s|%SELDON_GRPC_UTIL_IMAGE_VERSION%|$${SELDON_GRPC_UTIL_IMAGE_VERSION}|" \
				> rpc/create-proto-jar-job.template.json


#
# glusterfs
#

GLUSTERFS_IP1=192.168.0.149
GLUSTERFS_IP2=192.168.0.248

glusterfs.json:glusterfs.json.in
	@cat glusterfs.json.in | sed -e 's|%GLUSTERFS_IP1%|${GLUSTERFS_IP1}|'| sed -e 's|%GLUSTERFS_IP2%|${GLUSTERFS_IP2}|' > glusterfs.json && echo "created $@"

conf:control.json mysql.json memcache.json td-agent-server.json spark-master.json spark-workers.json examples/reuters/import-data-job.json glusterfs.json server.json examples/ml100k/ml100k-import.json dev/server.json dev/control.json influxdb-grafana.json dev/iago.json dev/locust-master.json.template dev/locust-slave.json.template zookeeper.json kafka.json examples/ml10m/ml10m-import-item-similarity.json examples/ml10m/ml10m-import-matrix-factorization.json analytics/impressions-kafka-stream.json analytics/predictions-kafka-stream.json examples/finefoods/train-finefoods.json examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json microservice_pipeline.rest.template microservice_pipeline.rpc.template models/stream-itemsim-create.json models/stream-itemsim-dbupload.json rpc/create-proto-jar-job.template.json examples/US_stocks/train-US-stocks.json mysql-google-cloudsql.json spark-ui.json proxy-server.json


#
# microservice conf
#

microservice_pipeline.rest.template: microservice_pipeline.rest.template.in  $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat microservice_pipeline.rest.template.in  | sed \
				-e "s|%SELDON_PYTHON_PACKAGE_VERSION%|$${SELDON_PYTHON_PACKAGE_VERSION}|" \
				> microservice_pipeline.rest.template && echo "created $@"

microservice_pipeline.rpc.template: microservice_pipeline.rpc.template.in  $(BUILD_VERSIONS_GENERATED_FILE)
	@source $(BUILD_VERSIONS_GENERATED_FILE); \
		cat microservice_pipeline.rpc.template.in  | sed \
				-e "s|%SELDON_PYTHON_PACKAGE_VERSION%|$${SELDON_PYTHON_PACKAGE_VERSION}|" \
				> microservice_pipeline.rpc.template && echo "created $@"



$(BUILD_VERSIONS_GENERATED_FILE):
	@$(SELDON_HOME)/build/generate-build-versions

clean:
	rm -f $(BUILD_VERSIONS_GENERATED_FILE)
	rm -f mysql.json
	rm -f memcache.json
	rm -f control.json
	rm -f td-agent-server.json
	rm -f spark-master.json
	rm -f spark-workers.json
	rm -f server.json
	rm -f influxdb-grafana.json
	rm -f examples/reuters/import-data-job.json
	rm -f examples/ml100k/ml100k-import.json
	rm -f examples/ml10m/ml10m-import-item-similarity.json
	rm -f examples/ml10m/ml10m-import-matrix-factorization.json
	rm -f examples/finefoods/train-finefoods.json
	rm -f examples/tensorflow_deep_mnist/train-tensorflow-deep-mnist.json
	rm -f examples/tensorflow_deep_mnist/load-model-tensorflow-deep-mnist.json
	rm -f microservice-${MICROSERVICE_NAME}.json
	rm -f glusterfs.json
	rm -f zookeeper.json
	rm -f kafka.json
	rm -f dev/server.json
	rm -f dev/control.json
	rm -f dev/iago.json
	rm -f dev/locust-slave.json.template
	rm -f dev/locust-master.json.template
	rm -f analytics/*.json
	rm -f microservice_pipeline.rest.template
	rm -f microservice_pipeline.rpc.template
	rm -f models/stream-itemsim-create.json
	rm -f models/stream-itemsim-dbupload.json
	rm -f rpc/create-proto-jar-job.template.json
	rm -f mysql-google-cloudsql.json
	rm -f spark-ui.json
	rm -f proxy-server.json
