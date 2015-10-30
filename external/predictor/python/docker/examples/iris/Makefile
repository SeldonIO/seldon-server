SHELL=/bin/bash
IMAGE=seldonio/iris_pipeline
VERSION=1.1

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	SHUF=shuf
	HOST_IP=localhost
endif
ifeq ($(UNAME_S),Darwin)
	SHUF=gshuf
	BOOT2DOCKER_HOST_IP:=$(shell boot2docker ip 2> /dev/null)
	HOST_IP:=$(BOOT2DOCKER_HOST_IP)
endif

.PHONY:build_docker 

# Download iris data
data/iris.data:
	mkdir -p data
	cd data ; wget --quiet http://archive.ics.uci.edu/ml/machine-learning-databases/iris/iris.data

# create JSON events data
data/iris/events/1/iris.json:data/iris.data
	mkdir -p data/iris/events/1
	cat data/iris.data | python create-json.py | $(SHUF) > data/iris/events/1/iris.json

# build docker iris feature extraction pipleline
build_pipeline:Dockerfile
	docker build -t ${IMAGE} .

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)


#
# XGBoost example
#

xgboost_pipeline:data/iris/events/1/iris.json
	docker run --rm -t -v ${PWD}/data:/data seldonio/iris_pipeline bash -c "python /pipeline/xgb_pipeline.py --events /data/iris/events/1 --models /data/iris/xgb_models/1"

xgboost_runtime:
	docker run --name="xgboost_runtime" -d -p 5001:5000  -v ${PWD}/data:/data seldonio/pipelines bash -c "python /scripts/server.py --pipeline /data/iris/xgb_models/1 --model_name model_xgb"
	touch xgboost_runtime

stop_xgboost_runtime:
	docker rm -f xgboost_runtime
	rm -f xgboost_runtime

test_xgboost_runtime:xgboost_runtime
	curl -G  "http://$(HOST_IP):5001/predict?client=iris" --data-urlencode 'json={"f1": 4.6, "f2": 3.2, "f3": 1.4, "f4": 0.2}'

#
# VW example
#

vw_pipeline:data/iris/events/1/iris.json
	docker run --rm -t -v ${PWD}/data:/data seldonio/iris_pipeline bash -c "python /pipeline/vw_pipeline.py --events /data/iris/events/1 --models /data/iris/vw_models/1"

vw_runtime:
	docker run --name="vw_runtime" -d -p 5000:5000  -v ${PWD}/data:/data seldonio/pipelines bash -c "python /scripts/server.py --pipeline /data/iris/vw_models/1 --model_name model_vw"
	touch vw_runtime


###needs changing

# create vw model
data/iris/vw/1:data/iris/features/1
	docker run --rm -t -v ${PWD}/data:/data seldonio/vw_train:1.1 bash -c "cd /vw/vw_train ; python vw_train.py --client iris --day 1 --inputPath /data/ --outputPath /data/ --vwArgs '--passes 3 --oaa 3' --target nameId --include f1 f2 f3 f4 --train_filename train.vw --target_readable name"


# start vw microservice for iris

# stop vw microservice
stop_vw_runtime:
	docker rm -f vw_runtime
	rm -f vw_runtime

# send test JSON for prediction against vw microservice
test_vw_runtime:vw_runtime
	curl -G  "http://$(HOST_IP):5000/predict?client=iris" --data-urlencode 'json={"f1": 4.6, "f2": 3.2, "f3": 1.4, "f4": 0.2}'



