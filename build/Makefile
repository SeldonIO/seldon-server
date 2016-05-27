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

build_seldonio_mysql: build_versions_generated
	@./dobuild seldonio_mysql

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
	#@make build_seldonio_iris_vw
	@make build_seldonio_iris_xgboost

clean:
	@rm -fv build_versions_generated

build_versions_generated: build_versions
	@./generate-build-versions


#publish
publish_seldonio_examples_ml100k: build_versions_generated
	@./dopublish seldonio_examples_ml100k

