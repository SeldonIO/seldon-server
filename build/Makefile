notarget:
	@echo need target

LOG_APPEND=

build_seldon_python_package:
	@./generate-build-versions
	@./dobuild seldon_python_package 2>&1 | tee $(LOG_APPEND) build.log

build_seldonio_pyseldon:
	@./generate-build-versions
	@./dobuild seldonio_pyseldon 2>&1 | tee $(LOG_APPEND) build.log

build_seldon_spark_build:
	@./generate-build-versions
	@./dobuild seldon_spark_build 2>&1 | tee $(LOG_APPEND) build.log

build_all:
	@rm -f build.log
	@make LOG_APPEND=-a build_seldon_python_package
	@make LOG_APPEND=-a build_seldonio_pyseldon
	@make LOG_APPEND=-a build_seldon_spark_build

