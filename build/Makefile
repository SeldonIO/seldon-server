SHELL:=/bin/bash

notarget:
	@echo need target

ALL_LIST=\
	seldon_python_package \
	seldonio_pyseldon \
	seldon_spark_build \
	seldonio_seldon_control \
	seldonio_seldon_server \
	seldonio_seldon_stream \
	seldonio_examples_ml100k \
	seldonio_examples_ml10m \
	seldonio_examples_reuters_data \
	seldonio_reuters_example \
	seldonio_iris_keras \
	seldonio_mysql \
	seldonio_grafana \
	seldonio_iago \
	seldonio_influxdb \
	seldonio_zookeeper_k8s \
	seldonio_kafka_k8s \
	seldonio_tdagent_server \
	seldonio_tdagent_node \
	seldonio_iris_vw \
	seldonio_iris_xgboost \
	seldonio_iris_xgboost_rpc \
	seldonio_iris_scikit \
	seldonio_finefoods_xgboost \
	seldonio_stream_itemsim \
	seldonio_examples_tensorflow_deep_mnist


NO_PUBLISH_LIST=\
    seldon_python_package \
    publish_seldon_spark_build

#build
build_seldon_python_package: build_versions_generated
	@./dobuild seldon_python_package
build_seldonio_pyseldon: build_versions_generated
	@./dobuild seldonio_pyseldon
build_seldon_spark_build: build_versions_generated
	@./dobuild seldon_spark_build
build_seldonio_seldon_control: build_versions_generated
	@./dobuild seldonio_seldon_control
build_seldonio_seldon_server: build_versions_generated
	@./dobuild seldonio_seldon_server
build_seldonio_seldon_stream: build_versions_generated
	@./dobuild seldonio_seldon_stream
build_seldonio_examples_ml100k: build_versions_generated
	@./dobuild seldonio_examples_ml100k
build_seldonio_examples_ml10m: build_versions_generated
	@./dobuild seldonio_examples_ml10m
build_seldonio_examples_reuters_data: build_versions_generated
	@./dobuild seldonio_examples_reuters_data
build_seldonio_reuters_example: build_versions_generated
	@./dobuild seldonio_reuters_example
build_seldonio_iris_keras: build_versions_generated
	@./dobuild seldonio_iris_keras
build_seldonio_grafana: build_versions_generated
	@./dobuild seldonio_grafana
build_seldonio_iago: build_versions_generated
	@./dobuild seldonio_iago
build_seldonio_mysql: build_versions_generated
	@./dobuild seldonio_mysql
build_seldonio_influxdb: build_versions_generated
	@./dobuild seldonio_influxdb
build_seldonio_tdagent_node: build_versions_generated
	@./dobuild seldonio_tdagent_node
build_seldonio_tdagent_server: build_versions_generated
	@./dobuild seldonio_tdagent_server
build_seldonio_zookeeper_k8s: build_versions_generated
	@./dobuild seldonio_zookeeper_k8s
build_seldonio_kafka_k8s: build_versions_generated
	@./dobuild seldonio_kafka_k8s
build_seldonio_iris_vw: build_versions_generated
	@./dobuild seldonio_iris_vw
build_seldonio_iris_xgboost: build_versions_generated
	@./dobuild seldonio_iris_xgboost
build_seldonio_iris_xgboost_rpc: build_versions_generated
	@./dobuild seldonio_iris_xgboost_rpc
build_seldonio_iris_scikit: build_versions_generated
	@./dobuild seldonio_iris_scikit
build_seldonio_finefoods_xgboost: build_versions_generated
	@./dobuild seldonio_finefoods_xgboost
build_seldonio_stream_itemsim: build_versions_generated
	@./dobuild seldonio_stream_itemsim
build_seldonio_examples_tensorflow_deep_mnist: build_versions_generated
	@./dobuild seldonio_examples_tensorflow_deep_mnist

#publish
publish_seldonio_pyseldon: build_versions_generated
	@./dopublish seldonio_pyseldon
publish_seldonio_seldon_control: build_versions_generated
	@./dopublish seldonio_seldon_control
publish_seldonio_seldon_server: build_versions_generated
	@./dopublish seldonio_seldon_server
publish_seldonio_seldon_stream: build_versions_generated
	@./dopublish seldonio_seldon_stream
publish_seldonio_examples_ml100k: build_versions_generated
	@./dopublish seldonio_examples_ml100k
publish_seldonio_examples_ml10m: build_versions_generated
	@./dopublish seldonio_examples_ml10m
publish_seldonio_examples_reuters_data: build_versions_generated
	@./dopublish seldonio_examples_reuters_data
publish_seldonio_reuters_example: build_versions_generated
	@./dopublish seldonio_reuters_example
publish_seldonio_iris_keras: build_versions_generated
	@./dopublish seldonio_iris_keras
publish_seldonio_mysql: build_versions_generated
	@./dopublish seldonio_mysql
publish_seldonio_grafana: build_versions_generated
	@./dopublish seldonio_grafana
publish_seldonio_iago: build_versions_generated
	@./dopublish seldonio_iago
publish_seldonio_influxdb: build_versions_generated
	@./dopublish seldonio_influxdb
publish_seldonio_zookeeper_k8s: build_versions_generated
	@./dopublish seldonio_zookeeper_k8s
publish_seldonio_kafka_k8s: build_versions_generated
	@./dopublish seldonio_kafka_k8s
publish_seldonio_tdagent_server: build_versions_generated
	@./dopublish seldonio_tdagent_server
publish_seldonio_tdagent_node: build_versions_generated
	@./dopublish seldonio_tdagent_node
publish_seldonio_iris_vw: build_versions_generated
	@./dopublish seldonio_iris_vw
publish_seldonio_iris_xgboost: build_versions_generated
	@./dopublish seldonio_iris_xgboost
publish_seldonio_iris_xgboost_rpc: build_versions_generated
	@./dopublish seldonio_iris_xgboost_rpc
publish_seldonio_iris_scikit: build_versions_generated
	@./dopublish seldonio_iris_scikit
publish_seldonio_finefoods_xgboost: build_versions_generated
	@./dopublish seldonio_finefoods_xgboost
publish_seldonio_stream_itemsim: build_versions_generated
	@./dopublish seldonio_stream_itemsim
publish_seldonio_examples_tensorflow_deep_mnist: build_versions_generated
	@./dopublish seldonio_examples_tensorflow_deep_mnist

build_all:
	@for item in $(ALL_LIST) ; do \
			TARGET="build_$$item" && make $${TARGET}; \
			if [ ! $$? -eq 0 ]; then \
				break; \
			fi \
		done

publish_all:
	@for item in $(ALL_LIST) ; do \
            if [[ ! "$(NO_PUBLISH_LIST)" =~ "$${item}" ]];then \
                    TARGET="publish_$$item" && make $${TARGET}; \
                    if [ ! $$? -eq 0 ]; then \
                        break; \
                    fi \
            fi \
		done

clean:
	@rm -fv build_versions_generated

build_versions_generated: build_versions
	@./generate-build-versions

