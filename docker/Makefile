
IMAGE_VERSION=`grep '\#VERSION' Dockerfile | cut -d= -f2`

build_image:
	cd ${IMAGE_FOLDER} ; docker build --force-rm=true -t seldonio/$(IMAGE_FOLDER):$(IMAGE_VERSION) .

push_to_dockerhub:
	@docker login -u seldonio && \
		docker push seldonio/$(IMAGE_FOLDER):$(IMAGE_VERSION)


