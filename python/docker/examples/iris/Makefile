SHELL=/bin/bash
IMAGE=seldonio/iris_pipeline
VERSION=1.3

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

data/iris/xgb_models/1:data/iris/events/1/iris.json
	docker run --rm -t -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/pipeline/xgb_pipeline.py --events /data/iris/events/1 --models /data/iris/xgb_models/1"

xgboost_runtime:data/iris/xgb_models/1
	docker run --name="xgboost_runtime" -d -p 5001:5000  -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/scripts/start_prediction_microservice.py --pipeline /data/iris/xgb_models/1 --model_name model_xgb"
	touch xgboost_runtime

test_xgboost_runtime:xgboost_runtime
	curl -G  "http://$(HOST_IP):5001/predict?client=iris" --data-urlencode 'json={"f1": 4.6, "f2": 3.2, "f3": 1.4, "f4": 0.2}'

stop_xgboost_runtime:
	docker rm -f xgboost_runtime
	rm -f xgboost_runtime


#
# VW example
#

data/iris/vw_models/1:data/iris/events/1/iris.json
	docker run --rm -t -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/pipeline/vw_pipeline.py --events /data/iris/events/1 --models /data/iris/vw_models/1"

vw_runtime:data/iris/vw_models/1
	docker run --name="vw_runtime" -d -p 5000:5000  -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/scripts/start_prediction_microservice.py --pipeline /data/iris/vw_models/1 --model_name model_vw"
	touch vw_runtime

# send test JSON for prediction against vw microservice
test_vw_runtime:vw_runtime
	curl -G  "http://$(HOST_IP):5000/predict?client=iris" --data-urlencode 'json={"f1": 4.6, "f2": 3.2, "f3": 1.4, "f4": 0.2}'


# stop vw microservice
stop_vw_runtime:
	docker rm -f vw_runtime
	rm -f vw_runtime


#
# Keras example
#

data/iris/keras_models/1:data/iris/events/1/iris.json
	docker run --rm -t -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/pipeline/keras_pipeline.py --events /data/iris/events/1 --models /data/iris/keras_models/1"


keras_runtime:data/iris/keras_models/1
	docker run --name="keras_runtime" -d -p 5002:5000  -v ${PWD}/data:/data seldonio/iris_pipeline:1.3 bash -c "python /home/seldon/scripts/start_prediction_microservice.py --pipeline /data/iris/keras_models/1 --model_name model_keras"
	touch keras_runtime

test_keras_runtime:keras_runtime
	curl -G  "http://$(HOST_IP):5002/predict?client=iris" --data-urlencode 'json={"f1": 4.6, "f2": 3.2, "f3": 1.4, "f4": 0.2}'



