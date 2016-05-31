SELDON_TDAGENT_SERVER_IMAGE_VERSION=00
IMAGE_NAME=seldonio/td-agent-server

build_image:
	docker build --force-rm=true -t $(IMAGE_NAME):$(SELDON_TDAGENT_SERVER_IMAGE_VERSION) .

push_to_dockerhub:
	docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(SELDON_TDAGENT_SERVER_IMAGE_VERSION)

clean:
	@rm -fv Dockerfile

