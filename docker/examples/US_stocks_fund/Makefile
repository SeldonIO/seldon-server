SHELL=/bin/bash
IMAGE=seldonio/us_stocks
SELDON_US_STOCKS_IMAGE_VERSION=01
SELDON_PYTHON_PACKAGE_VERSION=2.2.1

Dockerfile: Dockerfile.in
	@cat Dockerfile.in | sed 's/%SELDON_PYTHON_PACKAGE_VERSION%/$(SELDON_PYTHON_PACKAGE_VERSION)/' > Dockerfile


build_image:Dockerfile
	docker build --force-rm=true -t ${IMAGE}:$(SELDON_US_STOCKS_IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE)
	@docker login -u seldonio && \
		docker push $(IMAGE):$(SELDON_US_STOCKS_IMAGE_VERSION)


clean:
	@rm -fv Dockerfile
