SHELL=/bin/bash

#
# Simple config for hostPath setting for demo cluster
# Will change to more complex config if we allow alternate storage volumes for demo
#

.PHONY: microservice

BASE_PATH=/data
MYSQL_DATA_PATH=${BASE_PATH}/mysql
CONF_DATA_PATH=${BASE_PATH}/conf
TDAGENT_DATA_PATH=${BASE_PATH}/seldon-data
SPARK_DATA_PATH=${BASE_PATH}/seldon-models
MICROSERVICE_NAME=reuters-example
MICROSERVICE_IMAGE=seldonio/reuters-example
MICROSERVICE_VERSION=1.0

mysql.yml: mysql.yml.in
	cat mysql.yml.in | sed 's=$$(DATA_PATH)=${MYSQL_DATA_PATH}=g' > mysql.yml

control.yml: control.yml.in
	cat control.yml.in | sed 's=$$(CONF_PATH)=${CONF_DATA_PATH}=g' | sed 's=$$(MODELS_PATH)=${SPARK_DATA_PATH}=g' > control.yml

td-agent-server.yml: td-agent-server.yml.in
	cat td-agent-server.yml.in | sed 's=$$(DATA_PATH)=${TDAGENT_DATA_PATH}=g' > td-agent-server.yml

spark-master.yml: spark-master.yml.in
	cat spark-master.yml.in | sed 's=$$(DATA_PATH)=${SPARK_DATA_PATH}=g' > spark-master.yml

spark-workers.yml: spark-workers.yml.in
	cat spark-workers.yml.in | sed 's=$$(DATA_PATH)=${SPARK_DATA_PATH}=g' > spark-workers.yml

microservice-${MICROSERVICE_NAME}.yml:microservice.yml.in
	cat microservice.yml.in | sed 's=$$(NAME)=${MICROSERVICE_NAME}=g' | sed 's=$$(IMAGE)=${MICROSERVICE_IMAGE}=g' | sed 's=$$(VERSION)=${MICROSERVICE_VERSION}=g' > microservice-${MICROSERVICE_NAME}.yml

microservice:microservice-${MICROSERVICE_NAME}.yml

clean:
	rm -f mysql.yml
	rm -f control.yml
	rm -f td-agent-server.yml
	rm -f spark-master.yml
	rm -f spark-workers.yml
