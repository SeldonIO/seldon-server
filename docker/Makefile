

build_image:
	cd ${IMAGE_NAME} ; docker build --force-rm=true -t $(IMAGE_NAME):$(IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push $(IMAGE_NAME):$(IMAGE_VERSION)


