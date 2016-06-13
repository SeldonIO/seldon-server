SELDON_CONTROL_IMAGE_VERSION=00
SELDON_EXAMPLES_ML10M_IMAGE_VERSION=00
IMAGE_NAME=examples-ml10m

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_CONTROL_IMAGE_VERSION%/$(SELDON_CONTROL_IMAGE_VERSION)/' > Dockerfile

build_image: Dockerfile
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_EXAMPLES_ML10M_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(SELDON_EXAMPLES_ML10M_IMAGE_VERSION)

clean:
	@rm -fv Dockerfile

