VERSION=1.6
IMAGE=seldonio/pyseldon

build:
	rm -fr python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

shell:
	docker run -it --rm seldonio/pyseldon bash

clean:
	rm -rf ./python-modules
tag:
	docker tag -f $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)


