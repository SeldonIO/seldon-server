VERSION=1.1
IMAGE=seldonio/xgboost_train

build:
	rm -rf python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)

run_interactive:
	docker run --name="xgboost_train" -it --net=host --rm ${IMAGE} bash

