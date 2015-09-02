VERSION=1.0
IMAGE=seldonio/pipelines

build:
	rm -fr python-modules
	cp -R ../../seldon python-modules
	docker build -t ${IMAGE} .

clean:
	rm -rf ./python-modules

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)

