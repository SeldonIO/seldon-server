SHELL=/bin/bash
IMAGE=seldonio/iris_xgboost
VERSION=1.0

build:Dockerfile
	docker build -t ${IMAGE} .

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)




