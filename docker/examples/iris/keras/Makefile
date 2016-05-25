SHELL=/bin/bash
IMAGE=seldonio/iris_keras
SELDON_IRIS_KERAS_IMAGE_VERSION=00

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

build_image:Dockerfile
	docker build --force-rm=true -t ${IMAGE}:$(SELDON_IRIS_KERAS_IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(SELDON_IRIS_KERAS_IMAGE_VERSION)



clean:
	@rm -fv Dockerfile

