SHELL:=/bin/bash

SELDON_PYTHON_PACKAGE_VERSION=2.0.7
SELDON_SIMULATION_INNE_IMAGE_VERSION=2.0.7
IMAGE_NAME=simulation_inne

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

build_image: Dockerfile
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_SIMULATION_INNE_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(SELDON_SIMULATION_INNE_IMAGE_VERSION)

clean:
	@rm -fv Dockerfile


