VERSION=1.12
IMAGE=seldonio/pyseldon

build:
	docker build -t ${IMAGE} .

shell:
	docker run -it --rm seldonio/pyseldon bash

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)


