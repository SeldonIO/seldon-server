VERSION=1.0.2
IMAGE=seldonio/xgboost_runtime

build:
	docker build -t ${IMAGE} .


tag:
	docker tag $(IMAGE) $(IMAGE):$(VERSION)


push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)

run_interactive:
	docker run --name="xg_runtime" --rm -it --rm ${IMAGE} bash

