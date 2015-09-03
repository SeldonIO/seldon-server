VERSION=1.0
IMAGE=seldonio/xgboost_runtime

build:
	docker build -t ${IMAGE} .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)

run_interactive:
	docker run --name="xg_runtime" --rm -it --rm ${IMAGE} bash

