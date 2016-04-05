
IMAGE_VERSION=`grep '\#VERSION' Dockerfile | cut -d= -f2`
IMAGE_NAME=`echo "${IMAGE_FOLDER}" | sed 's|/|-|g'`

build_image:
	cd ${IMAGE_FOLDER} ; docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(IMAGE_VERSION)


