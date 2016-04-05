VERSION=1.0
IMAGE=seldonio/ngram

build:
	docker build -t ${IMAGE} .

shell:
	docker run -it --rm ${IMAGE} bash

tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)





