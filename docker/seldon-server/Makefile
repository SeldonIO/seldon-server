SELDON_SERVER_IMAGE_VERSION=00
IMAGE_NAME=seldon-server

seldon-server.tar.gz:
	@cd ../../.. && \
		tar -cvf seldon-server.tar seldon-server && \
		gzip -v seldon-server.tar
	@mv -v ../../../seldon-server.tar.gz .

build_image: seldon-server.tar.gz
	docker build --force-rm=true -t seldonio/$(IMAGE_NAME):$(SELDON_SERVER_IMAGE_VERSION) .

push_to_dockerhub:
	cd ${IMAGE_FOLDER} ; docker login -u seldonio && \
		docker push seldonio/$(IMAGE_NAME):$(SELDON_SERVER_IMAGE_VERSION)

clean:
	rm -f seldon-server.tar.gz

