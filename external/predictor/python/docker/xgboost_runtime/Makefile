VERSION=1.0
IMAGE=xgboost_runtime

build:
	docker build -t ${IMAGE} .


run_interactive:
	docker run --name="xg_runtime" --rm -it --rm xgboost_runtime bash

run_dev:
	docker run --name="xg_runtime" --rm -it -p 5000:5000 -v /seldon-models:/seldon-models -v ${PWD}/scripts:/xgboost/xgboost_runtime --rm xgboost_runtime bash

