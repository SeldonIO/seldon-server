VERSION=1.0.1
IMAGE=seldonio/vw_train

build:
	rm -r python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules

tag:
	docker tag $(IMAGE) $(IMAGE):$(VERSION)

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(VERSION)

run_interactive:
	docker run --name="vw_train" -it --net=host --rm ${IMAGE} bash

