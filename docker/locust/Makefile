SHELL:=/bin/bash

SELDON_PYTHON_PACKAGE_VERSION=2.1
SELDON_LOCUST_IMAGE_VERSION=1.0
IMAGE_NAME=locust

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

build_image: Dockerfile
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_LOCUST_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(SELDON_LOCUST_VERSION)

clean:
	@rm -fv Dockerfile

