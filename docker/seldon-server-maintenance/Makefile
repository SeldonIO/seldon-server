SELDON_SERVER_MAINTENANCE_IMAGE_VERSION=00
IMAGE_NAME=seldon-server-maintenance


build_image: 
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_SERVER_MAINTENANCE_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(SELDON_SERVER_MAINTENANCE_IMAGE_VERSION)

