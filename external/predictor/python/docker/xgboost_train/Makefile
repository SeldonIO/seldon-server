VERSION=1.0
IMAGE=xgboost_train

build:
	rm -rf python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules


run_interactive:
	docker run --name="xgboost_train" -it --net=host --rm xgboost_train bash

run_dev:
	docker run --name="xgboost_train" -it -v ${PWD}/python-modules:/python-modules/seldon -v ${PWD}/scripts:/xgboost/xgboost_train -v /seldon-models:/seldon-models  --net=host --rm xgboost_train bash
