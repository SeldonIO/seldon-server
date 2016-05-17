IMAGE_VERSION=3.4.8
IMAGE_NAME=seldonio/zookeeper-k8s

build_image:
	docker build --force-rm=true -t $(IMAGE_NAME):$(IMAGE_VERSION) .

shell:
	docker run -it --rm ${IMAGE_NAME}:${IMAGE_VERSION} bash

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(IMAGE_VERSION)

