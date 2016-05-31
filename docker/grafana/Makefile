SELDON_GRAFANA_IMAGE_VERSION=00
IMAGE_NAME=seldonio/grafana

build_image:
	docker build --force-rm=true -t $(IMAGE_NAME):$(SELDON_GRAFANA_IMAGE_VERSION) .

push_to_dockerhub:
	docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(SELDON_GRAFANA_IMAGE_VERSION)

clean:
	@rm -fv Dockerfile

