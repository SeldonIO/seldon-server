notarget:
	@echo need target

LOG_APPEND=

build_seldon_python_package: build_versions_generated
	@./dobuild seldon_python_package 2>&1 | tee $(LOG_APPEND) build.log

build_seldonio_pyseldon: build_versions_generated
	@./dobuild seldonio_pyseldon 2>&1 | tee $(LOG_APPEND) build.log

build_seldon_spark_build: build_versions_generated
	@./dobuild seldon_spark_build 2>&1 | tee $(LOG_APPEND) build.log

build_seldonio_seldon_control: build_versions_generated
	@./dobuild seldonio_seldon_control 2>&1 | tee $(LOG_APPEND) build.log


build_all:
	@rm -f build.log
	@make LOG_APPEND=-a build_seldon_python_package
	@make LOG_APPEND=-a build_seldonio_pyseldon
	@make LOG_APPEND=-a build_seldon_spark_build
	@make LOG_APPEND=-a build_seldonio_seldon_control

clean:
	@rm -fv build_versions_generated

build_versions_generated: build_versions
	@./generate-build-versions

