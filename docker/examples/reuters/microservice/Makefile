SELDON_PYTHON_PACKAGE_VERSION=00
SELDON_REUTERS_EXAMPLE_IMAGE_VERSION=00
IMAGE_NAME=seldonio/reuters-example

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | \
		sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

build_image: Dockerfile
	docker build --force-rm=true -t $(IMAGE_NAME):$(SELDON_REUTERS_EXAMPLE_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(SELDON_REUTERS_EXAMPLE_IMAGE_VERSION)

clean:
	@rm -fv Dockerfile

