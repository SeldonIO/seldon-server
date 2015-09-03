VERSION=1.0
IMAGE=seldonio/xgboost_train

build:
	rm -rf python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)

run_interactive:
	docker run --name="xgboost_train" -it --net=host --rm ${IMAGE} bash

