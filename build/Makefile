notarget:
	@echo need target

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

build_seldonio_examples_ml100k: build_versions_generated
	@./dobuild seldonio_examples_ml100k

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

build_seldonio_iris_vw: build_versions_generated
	@./dobuild seldonio_iris_vw

build_seldonio_iris_xgboost: build_versions_generated
	@./dobuild seldonio_iris_xgboost

build_all:
	@make build_seldon_python_package
	@make build_seldonio_pyseldon
	@make build_seldon_spark_build
	@make build_seldonio_seldon_control
	@make build_seldonio_seldon_server
	@make build_seldonio_examples_ml100k
	@make build_seldonio_examples_reuters_data
	@make build_seldonio_reuters_example
	@make build_seldonio_iris_keras
	@make build_seldonio_mysql
	@make build_seldonio_grafana
	@make build_seldonio_iago
	@make build_seldonio_influxdb
	@make build_seldonio_zookeeper_k8s
	@make build_seldonio_tdagent_server
	@make build_seldonio_tdagent_node
	#@make build_seldonio_iris_vw
	@make build_seldonio_iris_xgboost

clean:
	@rm -fv build_versions_generated

build_versions_generated: build_versions
	@./generate-build-versions


#publish
publish_seldonio_pyseldon: build_versions_generated
	@./dopublish seldonio_pyseldon
publish_seldonio_seldon_control: build_versions_generated
	@./dopublish seldonio_seldon_control
publish_seldonio_seldon_server: build_versions_generated
	@./dopublish seldonio_seldon_server
publish_seldonio_examples_ml100k: build_versions_generated
	@./dopublish seldonio_examples_ml100k
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
publish_seldonio_tdagent_server: build_versions_generated
	@./dopublish seldonio_tdagent_server
publish_seldonio_tdagent_node: build_versions_generated
	@./dopublish seldonio_tdagent_node

publish_all:
	@make publish_seldonio_pyseldon
	@make publish_seldonio_seldon_control
	@make publish_seldonio_seldon_server
	@make publish_seldonio_examples_ml100k
	@make publish_seldonio_examples_reuters_data
	@make publish_seldonio_reuters_example
	@make publish_seldonio_iris_keras
	@make publish_seldonio_mysql
	@make publish_seldonio_grafana
	@make publish_seldonio_iago
	@make publish_seldonio_influxdb
	@make publish_seldonio_zookeeper_k8s
	@make publish_seldonio_tdagent_server
	@make publish_seldonio_tdagent_node


