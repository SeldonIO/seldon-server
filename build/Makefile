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


#Examples
build_seldonio_iris_keras: build_versions_generated
	@./dobuild seldonio_iris_keras
build_seldonio_examples_ml100k: build_versions_generated
	@./dobuild seldonio_examples_ml100k

build_all:
	@make build_seldon_python_package
	@make build_seldonio_pyseldon
	@make build_seldon_spark_build
	@make build_seldonio_seldon_control

clean:
	@rm -fv build_versions_generated

build_versions_generated: build_versions
	@./generate-build-versions


#publish
publish_seldonio_examples_ml100k: build_versions_generated
	@./dopublish seldonio_examples_ml100k

