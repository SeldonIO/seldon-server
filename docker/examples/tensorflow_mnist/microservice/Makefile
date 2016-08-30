SHELL=/bin/bash
IMAGE=seldonio/deep_mnist
SELDON_DEEP_MNIST_IMAGE_VERSION=01
SELDON_PYTHON_PACKAGE_VERSION=2.0.5

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile

build_pipeline:
	python create_pipeline.py

build_image:Dockerfile
	docker build --force-rm=true -t ${IMAGE}:$(SELDON_DEEP_MNIST_IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(SELDON_DEEP_MNIST_IMAGE_VERSION)


clean:
	@rm -fv Dockerfile
